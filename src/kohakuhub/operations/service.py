"""Acceptance and query service for durable operations."""

from __future__ import annotations

import hashlib
import json
import asyncio
import os
import weakref
from contextlib import asynccontextmanager
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4
from urllib.parse import parse_qsl, urlparse

import psycopg

from kohakuhub import lakefs_mutation_gateway as mutation_gateway
from kohakuhub.logger import get_logger

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .store import OperationStore
from .finalizer import finalize_commit_domain
from .types import CommitIntentRecord, OperationRecord


logger = get_logger("OPERATIONS")


class IdempotencyConflict(ValueError):
    """The same idempotency key was used for a different request."""


class OperationNotCancellable(ValueError):
    """The operation crossed a boundary where cancellation is unsafe."""


class CommitInProgress(ValueError):
    """A different unresolved commit owns the repository/ref mutation window."""

    def __init__(self, intent: CommitIntentRecord):
        super().__init__("another commit for this repository/ref is unresolved")
        self.intent = intent


class OperationInProgress(ValueError):
    """A different repository-scoped destructive operation is active."""

    def __init__(self, operation: OperationRecord):
        super().__init__("another repository operation is active")
        self.operation = operation


class QuotaExceeded(ValueError):
    """A commit's reserved net byte delta would exceed a quota bucket."""


def canonical_mutation_ref(ref: str) -> str:
    """Map API and intent ref spellings to one advisory-lock namespace."""

    value = str(ref).strip()
    if value == "__repository__":
        return value
    if value.startswith(("branch:", "tag:", "commit:")):
        return value
    return f"branch:{value}"


_FORBIDDEN_PAYLOAD_KEYS = {
    "authorization",
    "authheader",
    "authorizationheader",
    "proxyauthorization",
    "cookie",
    "setcookie",
    "credential",
    "credentials",
    "xamzcredential",
    "apikey",
    "xapikey",
    "accesskey",
    "accesskeyid",
    "awsaccesskeyid",
    "secretkey",
    "secretaccesskey",
    "awssecretaccesskey",
    "securitytoken",
    "xamzsecuritytoken",
    "sessiontoken",
    "awssessiontoken",
    "signature",
    "sig",
    "awssignature",
    "xamzsignature",
    "bearer",
    "password",
    "presignedurl",
    "secret",
    "token",
}

# A semaphore is bound to the event loop that first blocks on it.  Keep the
# default cache per loop, and let one application explicitly share a limiter
# owner across its service instances (the worker uses this for both lanes).
_FENCE_LIMITERS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _fence_connection_limiter(
    database_url: str,
    configured_limit: int | None = None,
    *,
    limiter_owner: Any | None = None,
) -> asyncio.Semaphore:
    """Bound dedicated advisory-lock connections for one runtime owner."""

    limit = configured_limit
    if limit is None:
        limit = int(os.getenv("KOHAKU_HUB_FENCE_MAX_CONNECTIONS", "4"))
    if limit < 1:
        raise ValueError("KOHAKU_HUB_FENCE_MAX_CONNECTIONS must be positive")
    loop = asyncio.get_running_loop()
    if limiter_owner is None:
        loop_limiters = _FENCE_LIMITERS.setdefault(loop, {})
    else:
        owner_limiters = getattr(limiter_owner, "_khub_fence_limiters", None)
        if owner_limiters is None:
            owner_limiters = weakref.WeakKeyDictionary()
            setattr(limiter_owner, "_khub_fence_limiters", owner_limiters)
        loop_limiters = owner_limiters.setdefault(loop, {})
    current = loop_limiters.get(database_url)
    if current is None or current[0] != limit:
        current = (limit, asyncio.Semaphore(limit))
        loop_limiters[database_url] = current
    return current[1]


def _normalized_payload_key(key: Any) -> str:
    return "".join(
        character for character in str(key).casefold() if character.isalnum()
    )


def _is_sensitive_payload_key(key: Any) -> bool:
    normalized = _normalized_payload_key(key)
    return normalized in _FORBIDDEN_PAYLOAD_KEYS or normalized.endswith(
        ("token", "password", "secret", "credential", "signature", "apikey")
    )


def _canonical_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    value = json.loads(json.dumps(dict(payload), separators=(",", ":"), sort_keys=True))
    _assert_safe_payload(value)
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    return value, hashlib.sha256(encoded).hexdigest()


def _assert_safe_payload(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if _is_sensitive_payload_key(key):
                raise ValueError(f"sensitive field is not allowed in {path}.{key}")
            if (
                _normalized_payload_key(key) in {"header", "headername", "key", "name"}
                and isinstance(child, str)
                and _is_sensitive_payload_key(child)
            ):
                raise ValueError(f"sensitive field is not allowed in {path}.{key}")
            _assert_safe_payload(child, f"{path}.{key}")
    elif isinstance(value, list):
        if (
            len(value) == 2
            and isinstance(value[0], str)
            and _is_sensitive_payload_key(value[0])
        ):
            raise ValueError(f"sensitive field is not allowed in {path}[0]")
        for index, child in enumerate(value):
            _assert_safe_payload(child, f"{path}[{index}]")
    elif isinstance(value, str) and "://" in value:
        parsed = urlparse(value)
        if parsed.scheme.casefold() not in {"http", "https"}:
            return
        query_keys = {
            key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
        }
        if parsed.username is not None or parsed.password is not None or any(
            _is_sensitive_payload_key(key) for key in query_keys
        ):
            raise ValueError(f"credential-bearing URL is not allowed in {path}")


class OperationService:
    def __init__(
        self,
        pool: Any,
        app: Any,
        registry: OperationRegistry = DEFAULT_REGISTRY,
        *,
        database_url: str | None = None,
        fence_connection_limit: int | None = None,
    ):
        self.pool = pool
        self.app = app
        self.registry = registry
        self.store = OperationStore(pool)
        self.database_url = database_url
        self.fence_connection_limit = fence_connection_limit

    def _accept_request(
        self,
        *,
        kind: str,
        resource_key: str,
        payload: Mapping[str, Any],
        requested_by_user_id: int | None,
        idempotency_key: str | None,
        repository_id: int | None,
        trigger: str,
        expected_head: str | None,
    ) -> tuple[Any, dict[str, Any], str, str]:
        spec = self.registry.get(kind)
        canonical_payload, payload_hash = _canonical_payload(payload)
        if not resource_key or len(resource_key) > 512:
            raise ValueError("resource_key must be non-empty and at most 512 characters")
        if idempotency_key is not None and not (1 <= len(idempotency_key) <= 256):
            raise ValueError("idempotency_key must be between 1 and 256 characters")
        if trigger not in {"api", "schedule", "reconcile", "system"}:
            raise ValueError("unsupported operation trigger")

        request_hash = hashlib.sha256(
            json.dumps(
                {
                    "kind": kind,
                    "payload": canonical_payload,
                    "payload_hash": payload_hash,
                    "repository_id": repository_id,
                    "resource_key": resource_key,
                    "expected_head": expected_head,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()
        return spec, canonical_payload, request_hash, payload_hash

    async def accept_in_transaction(
        self,
        connection: Any,
        operation_id: UUID | None = None,
        *,
        kind: str,
        resource_key: str,
        payload: Mapping[str, Any],
        requested_by_user_id: int | None,
        idempotency_key: str | None = None,
        repository_id: int | None = None,
        trigger: str = "api",
        expected_head: str | None = None,
    ) -> OperationRecord:
        spec, canonical_payload, request_hash, _payload_hash = self._accept_request(
            kind=kind,
            resource_key=resource_key,
            payload=payload,
            requested_by_user_id=requested_by_user_id,
            idempotency_key=idempotency_key,
            repository_id=repository_id,
            trigger=trigger,
            expected_head=expected_head,
        )
        operation_id = operation_id or uuid4()
        delivery_key = f"operation:{operation_id}:step:0"
        now = datetime.now(timezone.utc)
        if requested_by_user_id is not None and idempotency_key is not None:
            # A missing row cannot be locked by SELECT ... FOR UPDATE.  The
            # advisory transaction lock serializes the first insert.
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    f"khub-operation-idempotency:{requested_by_user_id}:{idempotency_key}",
                ),
            )
            existing = await self.store.get_operation_by_idempotency(
                connection,
                requested_by_user_id,
                idempotency_key,
                for_update=True,
            )
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "idempotency key is already bound to another request"
                    )
                return existing

        if repository_id is not None and kind.startswith("repository."):
            blocking = await self.store.get_blocking_repository_operation(
                connection, repository_id=repository_id
            )
            if blocking is not None:
                raise OperationInProgress(blocking)

        await self.store.insert_operation(
            connection,
            {
                "id": operation_id,
                "kind": kind,
                "handler_version": spec.version,
                "repository_id": repository_id,
                "resource_key": resource_key,
                "requested_by_user_id": requested_by_user_id,
                "trigger": trigger,
                "idempotency_key": idempotency_key,
                "request_hash": request_hash,
                "state": "accepted",
                "phase": "queued",
                "progress_current": 0,
                "expected_head": expected_head,
                "created_at": now,
                "updated_at": now,
            },
        )
        step_id = await self.store.insert_step(
            connection,
            {
                "operation_id": operation_id,
                "sequence": 0,
                "step_name": spec.step_name,
                "step_version": spec.version,
                "state": "pending",
                "delivery_key": delivery_key,
                "input_json": canonical_payload,
                "checkpoint_json": {},
            },
        )
        task = self.app.tasks[spec.task_name]
        job_id = await task.configure(
            connection=connection,
            queue=spec.queue,
            priority=spec.priority,
            lock=spec.delivery_lock(resource_key),
            queueing_lock=delivery_key,
        ).defer_async(operation_id=str(operation_id), step_id=str(step_id))
        await self.store.set_job_id(connection, step_id, job_id)
        operation = await self.store.get_operation(connection, operation_id)
        assert operation is not None
        return operation

    async def accept(
        self,
        *,
        kind: str,
        resource_key: str,
        payload: Mapping[str, Any],
        requested_by_user_id: int | None,
        idempotency_key: str | None = None,
        repository_id: int | None = None,
        trigger: str = "api",
        expected_head: str | None = None,
    ) -> OperationRecord:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                operation = await self.accept_in_transaction(
                    connection,
                    kind=kind,
                    resource_key=resource_key,
                    payload=payload,
                    requested_by_user_id=requested_by_user_id,
                    idempotency_key=idempotency_key,
                    repository_id=repository_id,
                    trigger=trigger,
                    expected_head=expected_head,
                )
            return operation

    async def get(self, operation_id: UUID) -> OperationRecord | None:
        async with self.pool.connection() as connection:
            return await self.store.get_operation(connection, operation_id)

    async def get_commit_intent(self, intent_id: UUID) -> CommitIntentRecord | None:
        async with self.pool.connection() as connection:
            return await self.store.get_commit_intent(connection, intent_id)

    async def ensure_commit_observation_operation(
        self, intent_id: UUID
    ) -> OperationRecord | None:
        """Create the authenticated status handle for an unresolved commit.

        This operation is only a durable observation handle.  It never owns
        the LakeFS mutator and therefore cannot cause a second commit dispatch.
        """

        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None:
                    return None
                if intent.observation_operation_id is not None:
                    return await self.store.get_operation(
                        connection, intent.observation_operation_id
                    )
                if intent.state in {"abandoned", "finalized"}:
                    return None
                operation = await self.accept_in_transaction(
                    connection,
                    kind="commit.observe.v1",
                    resource_key=f"commit-observe:{intent.id}",
                    payload={
                        "intent_id": str(intent.id),
                        "repository_id": intent.repository_id,
                        "ref": intent.ref,
                        "marker": intent.marker,
                    },
                    requested_by_user_id=intent.requested_by_user_id,
                    idempotency_key=f"commit-observe:{intent.id}",
                    repository_id=intent.repository_id,
                    trigger="reconcile",
                    expected_head=intent.base_head,
                )
                bound = await self.store.bind_observation_operation(
                    connection, intent.id, operation.id
                )
                if bound is None:
                    current = await self.store.get_commit_intent(
                        connection, intent.id
                    )
                    if current is None or current.observation_operation_id is None:
                        raise IdempotencyConflict(
                            "commit observation operation binding was lost"
                        )
                    return await self.store.get_operation(
                        connection, current.observation_operation_id
                    )
                return operation

    async def sync_commit_observation_operation(
        self, intent_id: UUID
    ) -> OperationRecord | None:
        """Reflect intent settlement in its status-only observation operation."""

        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None or intent.observation_operation_id is None:
                    return None
                if intent.state == "finalized":
                    return await self.store.finish_observation_operation(
                        connection,
                        intent.observation_operation_id,
                        state="succeeded",
                        result_json=dict(intent.result_json or {}),
                    )
                if intent.state == "abandoned":
                    return await self.store.finish_observation_operation(
                        connection,
                        intent.observation_operation_id,
                        state="succeeded",
                        result_json={
                            "outcome": "no_effect",
                            "error_code": intent.error_code,
                        },
                    )
                return await self.store.get_operation(
                    connection, intent.observation_operation_id
                )

    @asynccontextmanager
    async def repository_ref_fence(
        self,
        repository_id: int,
        ref: str,
        *,
        exclude_intent_id: UUID | None = None,
        exclude_operation_id: UUID | None = None,
        scope: str = "mutation",
    ):
        """Hold a cross-process LakeFS mutation fence.

        Ordinary mutations hold a shared repository lock and an exclusive
        repository/ref lock.  Repository-wide cutovers hold an exclusive
        repository lock.  The fence connection is deliberately not borrowed
        from the operation pool: a remote LakeFS call must not consume a
        transaction-pool slot for its entire duration.
        """

        if scope not in {"mutation", "cutover"}:
            raise ValueError("unsupported mutation fence scope")
        if not self.database_url:
            raise RuntimeError("a dedicated PostgreSQL fence connection is required")

        lock_ref = canonical_mutation_ref(ref)
        cutover = scope == "cutover" or lock_ref == "__repository__"
        limiter = _fence_connection_limiter(
            self.database_url,
            self.fence_connection_limit,
            limiter_owner=getattr(self.app, "khub_fence_limiter_owner", self.app),
        )
        await limiter.acquire()
        try:
            async with await psycopg.AsyncConnection.connect(
                self.database_url, autocommit=True
            ) as connection:
                acquired: list[tuple[str, bool]] = []
                capability_token = None
                try:
                    repository_key = f"khub-repository:v1:{int(repository_id)}"
                    if cutover:
                        await connection.execute(
                            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                            (repository_key,),
                        )
                        acquired.append((repository_key, False))
                        blocking = await self.store.get_blocking_repository_intent(
                            connection,
                            repository_id=repository_id,
                            exclude_intent_id=exclude_intent_id,
                        )
                    else:
                        await connection.execute(
                            "SELECT pg_advisory_lock_shared(hashtextextended(%s, 0))",
                            (repository_key,),
                        )
                        acquired.append((repository_key, True))
                        ref_key = (
                            f"khub-repository-ref:v1:{int(repository_id)}:{lock_ref}"
                        )
                        await connection.execute(
                            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                            (ref_key,),
                        )
                        acquired.append((ref_key, False))
                        candidates = [ref]
                        if lock_ref not in candidates:
                            candidates.append(lock_ref)
                        if lock_ref.startswith("branch:"):
                            raw_ref = lock_ref.removeprefix("branch:")
                            if raw_ref not in candidates:
                                candidates.append(raw_ref)
                        blocking = None
                        for candidate in candidates:
                            blocking = await self.store.get_blocking_commit_intent(
                                connection,
                                repository_id=repository_id,
                                ref=candidate,
                                exclude_intent_id=exclude_intent_id,
                            )
                            if blocking is not None:
                                break
                    if blocking is None:
                        blocking = await self.store.get_blocking_repository_operation(
                            connection,
                            repository_id=repository_id,
                            exclude_operation_id=exclude_operation_id,
                        )
                    if blocking is not None:
                        raise CommitInProgress(blocking)
                    lakefs_repository = await self.store.get_lakefs_repository(
                        connection, repository_id
                    )
                    capability_token = mutation_gateway.activate_capability(
                        mutation_gateway.MutationCapability(
                            repository_id=int(repository_id),
                            ref="__repository__" if cutover else lock_ref,
                            scope="cutover" if cutover else "mutation",
                            lakefs_repositories=(
                                frozenset({lakefs_repository})
                                if lakefs_repository
                                else frozenset()
                            ),
                        )
                    )
                    # Expose the checked-out connection to callers that need to
                    # perform a short, fenced domain transaction after an
                    # external observation.  The connection remains owned by
                    # this context until the advisory locks are released.
                    yield connection
                finally:
                    if capability_token is not None:
                        mutation_gateway.reset_capability(capability_token)
                    async def unlock() -> None:
                        for key, shared in reversed(acquired):
                            function = (
                                "pg_advisory_unlock_shared"
                                if shared
                                else "pg_advisory_unlock"
                            )
                            await connection.execute(
                                f"SELECT {function}(hashtextextended(%s, 0))",
                                (key,),
                            )

                    # Cancellation must not return a pooled connection while a
                    # session advisory lock is still held. Wait for cleanup even
                    # when the handler/request is being cancelled.
                    cleanup = asyncio.create_task(unlock())
                    cancelled = False
                    while not cleanup.done():
                        try:
                            await asyncio.shield(cleanup)
                        except asyncio.CancelledError:
                            cancelled = True
                    await cleanup
                    if cancelled:
                        raise asyncio.CancelledError
        finally:
            limiter.release()

    @asynccontextmanager
    async def repository_name_fence(self, resource_key: str):
        """Fence allocation of a repository name before a row exists.

        Repository creation has no stable ``Repository.id`` yet.  It therefore
        uses a separate advisory namespace keyed by the normalized public
        repository identity, and the gateway only permits
        ``create_repository`` while this capability is active.
        """

        if not resource_key:
            raise ValueError("repository fence key must be non-empty")
        if not self.database_url:
            raise RuntimeError("a dedicated PostgreSQL fence connection is required")

        limiter = _fence_connection_limiter(
            self.database_url,
            self.fence_connection_limit,
            limiter_owner=getattr(self.app, "khub_fence_limiter_owner", self.app),
        )
        await limiter.acquire()
        try:
            async with await psycopg.AsyncConnection.connect(
                self.database_url, autocommit=True
            ) as connection:
                lock_key = f"khub-repository-name:v1:{resource_key}"
                await connection.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                )
                capability_token = mutation_gateway.activate_capability(
                    mutation_gateway.MutationCapability(
                        repository_id=None,
                        ref="__repository_name__",
                        scope="repository_name",
                        lakefs_repositories=frozenset(),
                    )
                )
                try:
                    yield connection
                finally:
                    mutation_gateway.reset_capability(capability_token)
                    cleanup = asyncio.create_task(
                        connection.execute(
                            "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                            (lock_key,),
                        )
                    )
                    cancelled = False
                    while not cleanup.done():
                        try:
                            await asyncio.shield(cleanup)
                        except asyncio.CancelledError:
                            cancelled = True
                    await cleanup
                    if cancelled:
                        raise asyncio.CancelledError
        finally:
            limiter.release()

    async def prepare_commit_intent(
        self,
        *,
        repository_id: int,
        ref: str,
        base_head: str,
        payload: Mapping[str, Any],
        intent_id: UUID | None = None,
        requested_by_user_id: int | None = None,
        idempotency_key: str | None = None,
        request_hash: str | None = None,
        quota_delta: int = 0,
    ) -> CommitIntentRecord:
        """Persist a commit intent before any LakeFS staging mutation."""

        if not ref or not base_head:
            raise ValueError("commit intent requires ref and base_head")
        canonical_payload, payload_hash = _canonical_payload(payload)
        intent_id = intent_id or uuid4()
        marker = f"khub:v1:{intent_id}"
        prepared_deadline = datetime.now(timezone.utc) + timedelta(
            seconds=float(os.getenv("KOHAKU_HUB_COMMIT_PREPARE_SECONDS", "300"))
        )
        async with self.pool.connection() as connection:
            async with connection.transaction():
                if requested_by_user_id is not None and idempotency_key is not None:
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (
                            "khub-commit-idempotency:v1:"
                            f"{requested_by_user_id}:{repository_id}:{ref}:{idempotency_key}",
                        ),
                    )
                    existing_request = await self.store.get_commit_intent_by_request(
                        connection,
                        requested_by_user_id=requested_by_user_id,
                        repository_id=repository_id,
                        ref=ref,
                        idempotency_key=idempotency_key,
                        for_update=True,
                    )
                    if existing_request is not None:
                        if existing_request.request_hash != request_hash:
                            raise IdempotencyConflict(
                                "commit idempotency key is already bound to another request"
                            )
                        return existing_request
                existing = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if existing is not None:
                    if existing.payload_hash != payload_hash:
                        raise IdempotencyConflict(
                            "commit intent id is already bound to another payload"
                        )
                    return existing
                blocking = await self.store.get_blocking_commit_intent(
                    connection,
                    repository_id=repository_id,
                    ref=ref,
                    exclude_intent_id=intent_id,
                )
                if blocking is not None:
                    raise CommitInProgress(blocking)
                intent = await self.store.insert_commit_intent(
                    connection,
                    {
                        "id": intent_id,
                        "repository_id": repository_id,
                        "requested_by_user_id": requested_by_user_id,
                        "ref": ref,
                        "base_head": base_head,
                        "marker": marker,
                        "idempotency_key": idempotency_key,
                        "request_hash": request_hash,
                        "payload_hash": payload_hash,
                        "payload_json": canonical_payload,
                        "state": "prepared",
                        "prepared_deadline_at": prepared_deadline,
                    },
                )
                await self._reserve_commit_quota(
                    connection,
                    intent_id=intent.id,
                    quota_delta=quota_delta,
                )
                return intent

    async def record_prepared_staging_path(
        self,
        intent_id: UUID,
        *,
        path: str,
        recursive: bool = False,
    ) -> CommitIntentRecord:
        """Record one staging target before mutating the LakeFS branch."""

        return await self.record_prepared_staging_paths(
            intent_id,
            paths=({"path": path, "recursive": recursive},),
        )

    async def record_prepared_staging_paths(
        self,
        intent_id: UUID,
        *,
        paths: Sequence[Mapping[str, Any]],
    ) -> CommitIntentRecord:
        """Record all staging targets before mutating the LakeFS branch.

        This gives stale-preparation recovery enough information to reset the
        unchanged branch safely after an API process dies mid-preparation,
        while keeping one commit from doing a database round-trip per file.
        """

        entries = []
        for target in paths:
            path = str(target.get("path", ""))
            if not path:
                raise ValueError("staging path is required")
            entries.append({"path": path, "recursive": bool(target.get("recursive", False))})
        if not entries:
            raise ValueError("at least one staging path is required")

        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None or intent.state != "prepared":
                    raise OperationNotCancellable(
                        "commit preparation is no longer mutable"
                    )
                payload = dict(intent.payload_json)
                paths = list(payload.get("staging_paths", []))
                for entry in entries:
                    if entry not in paths:
                        paths.append(entry)
                payload["staging_paths"] = paths
                canonical, payload_hash = _canonical_payload(payload)
                updated = await self.store.update_prepared_commit_payload(
                    connection,
                    intent_id,
                    payload=canonical,
                    payload_hash=payload_hash,
                    expected_version=intent.version,
                )
                if updated is None:
                    raise IdempotencyConflict(
                        "commit intent changed while recording staging"
                    )
                deadline = datetime.now(timezone.utc) + timedelta(
                    seconds=float(os.getenv("KOHAKU_HUB_COMMIT_PREPARE_SECONDS", "300"))
                )
                refreshed = await self.store.refresh_prepared_deadline(
                    connection, intent_id, deadline
                )
                if refreshed is None:
                    raise IdempotencyConflict(
                        "commit intent changed while refreshing preparation"
                    )
                return refreshed

    async def _reserve_commit_quota(
        self,
        connection: Any,
        *,
        intent_id: UUID,
        quota_delta: int,
    ) -> None:
        """Reserve a positive commit delta while repository/owner rows are locked."""

        if quota_delta <= 0:
            return
        # The intent row carries the repository id, so load it from the locked
        # intent rather than trusting a second caller-supplied identifier.
        intent_cursor = await connection.execute(
            "SELECT repository_id FROM khub_commit_intents WHERE id = %s FOR UPDATE",
            (intent_id,),
        )
        intent_row = await intent_cursor.fetchone()
        if intent_row is None:
            raise ValueError("commit intent disappeared during quota reservation")
        repository_id = int(intent_row[0])
        repo_cursor = await connection.execute(
            """SELECT private, quota_bytes, used_bytes, owner_id
               FROM repository WHERE id = %s FOR UPDATE""",
            (repository_id,),
        )
        repo = await repo_cursor.fetchone()
        if repo is None:
            raise ValueError("repository does not exist for quota reservation")
        is_private, repo_quota, repo_used, owner_id = repo
        owner_cursor = await connection.execute(
            """SELECT private_quota_bytes, public_quota_bytes,
                      private_used_bytes, public_used_bytes
               FROM "user" WHERE id = %s FOR UPDATE""",
            (owner_id,),
        )
        owner = await owner_cursor.fetchone()
        if owner is None:
            raise ValueError("repository owner does not exist for quota reservation")
        quota = owner[0] if is_private else owner[1]
        used = owner[2] if is_private else owner[3]

        buckets: list[tuple[str, int, int | None, int]] = []
        if repo_quota is not None:
            buckets.append(("repository", int(repository_id), int(repo_quota), int(repo_used)))
        if quota is not None:
            buckets.append(("namespace", int(owner_id), int(quota), int(used)))

        for scope_type, scope_id, limit, used_bytes in buckets:
            active = await connection.execute(
                """SELECT COALESCE(SUM(reserved_bytes), 0)
                   FROM khub_quota_reservations
                   WHERE scope_type = %s AND scope_id = %s
                     AND is_private = %s AND state = 'reserved'""",
                (scope_type, scope_id, bool(is_private)),
            )
            active_bytes = int((await active.fetchone())[0] or 0)
            if used_bytes + active_bytes + quota_delta > limit:
                raise QuotaExceeded(
                    f"{scope_type} storage quota would be exceeded"
                )

        for scope_type, scope_id, _limit, _used_bytes in buckets:
            await self.store.insert_quota_reservation(
                connection,
                {
                    "id": uuid4(),
                    "intent_id": intent_id,
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                    "is_private": bool(is_private),
                    "reserved_bytes": quota_delta,
                    "state": "reserved",
                },
            )

    async def update_prepared_commit_payload(
        self, intent_id: UUID, *, payload: Mapping[str, Any]
    ) -> CommitIntentRecord:
        """Persist the staged, secret-free finalization payload before commit."""

        canonical_payload, payload_hash = _canonical_payload(payload)
        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None:
                    raise ValueError("commit intent does not exist")
                updated = await self.store.update_prepared_commit_payload(
                    connection,
                    intent_id,
                    payload=canonical_payload,
                    payload_hash=payload_hash,
                    expected_version=intent.version,
                )
                if updated is None:
                    raise IdempotencyConflict(
                        "commit intent changed while preparing its finalization payload"
                    )
                return updated

    async def mark_commit_intent_dispatch_started(
        self,
        intent_id: UUID,
        *,
        remote_timeout_seconds: float = 120.0,
        quiet_period_seconds: float = 30.0,
    ) -> CommitIntentRecord:
        now = datetime.now(timezone.utc)
        remote_deadline = now + timedelta(seconds=remote_timeout_seconds)
        observe_not_before = remote_deadline + timedelta(seconds=quiet_period_seconds)
        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None:
                    raise ValueError("commit intent does not exist")
                if intent.state in {"committed", "finalized"}:
                    return intent
                if intent.state not in {"prepared", "dispatch_started", "uncertain"}:
                    raise OperationNotCancellable(
                        "commit intent is no longer before external dispatch"
                    )
                updated = await self.store.update_commit_intent(
                    connection,
                    intent_id,
                    state="dispatch_started",
                    from_states=("prepared", "dispatch_started", "uncertain"),
                    expected_version=intent.version,
                    error_code=None,
                    error_summary=None,
                )
                if updated is None:
                    raise IdempotencyConflict(
                        "commit intent changed before external dispatch"
                    )
                await connection.execute(
                    """UPDATE khub_commit_intents
                       SET dispatch_started_at = COALESCE(dispatch_started_at, %s),
                           remote_deadline_at = COALESCE(remote_deadline_at, %s),
                           observe_not_before = COALESCE(observe_not_before, %s),
                           updated_at = CURRENT_TIMESTAMP,
                           version = version + 1
                       WHERE id = %s""",
                    (now, remote_deadline, observe_not_before, intent_id),
                )
                final = await self.store.get_commit_intent(connection, intent_id)
                assert final is not None
                return final

    async def abandon_commit_intent(
        self,
        intent_id: UUID,
        *,
        error_code: str | None = None,
        confirmed_no_effect: bool = False,
        expected_version: int | None = None,
    ) -> None:
        """Close an intent only when the external operation had no effect.

        ``prepared`` intents have not crossed the LakeFS commit boundary and
        may be abandoned by local cleanup.  Once dispatch started, callers
        must explicitly provide affirmative no-effect evidence; a timeout or
        a negative observation is not sufficient.
        """
        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None or intent.state in {"finalized", "committed"}:
                    return
                if intent.state in {"dispatch_started", "uncertain"} and not confirmed_no_effect:
                    raise OperationNotCancellable(
                        "ambiguous commit requires affirmative no-effect evidence"
                    )
                updated = await self.store.update_commit_intent(
                    connection,
                    intent_id,
                    state="abandoned",
                    from_states=(
                        "prepared",
                        "dispatch_started",
                        "uncertain",
                        "reconciliation_required",
                    ),
                    expected_version=(
                        intent.version
                        if expected_version is None
                        else expected_version
                    ),
                    error_code=error_code,
                    error_summary="external commit was confirmed to have no effect",
                )
                if updated is None:
                    # A stale observer must not release quota or complete the
                    # status handle for a newer intent version.
                    return
                await self.store.transition_quota_reservations(
                    connection,
                    intent_id,
                    from_state="reserved",
                    to_state="released",
                )
                if intent.observation_operation_id is not None:
                    await self.store.finish_observation_operation(
                        connection,
                        intent.observation_operation_id,
                        state="succeeded",
                        result_json={
                            "outcome": "no_effect",
                            "error_code": error_code,
                        },
                    )

    async def mark_commit_intent_uncertain(
        self,
        intent_id: UUID,
        *,
        error_code: str,
        error_summary: str,
        expected_version: int | None = None,
    ) -> CommitIntentRecord | None:
        """Retain a commit barrier when observation finds conflicting evidence."""

        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None:
                    return None
                if intent.state in {"finalized", "abandoned"}:
                    return intent
                return await self.store.update_commit_intent(
                    connection,
                    intent_id,
                    state="uncertain",
                    from_states=(
                        "dispatch_started",
                        "committed",
                        "reconciliation_required",
                        "uncertain",
                    ),
                    expected_version=(
                        intent.version
                        if expected_version is None
                        else expected_version
                    ),
                    error_code=error_code,
                    error_summary=error_summary,
                )

    async def abandon_prepared_commit_intent(
        self,
        intent_id: UUID,
        *,
        expected_version: int,
        error_code: str,
    ) -> bool:
        """Close a stale prepared intent only if it was not changed meanwhile."""

        async with self.pool.connection() as connection:
            async with connection.transaction():
                return await self.abandon_prepared_commit_intent_on_connection(
                    connection,
                    intent_id,
                    expected_version=expected_version,
                    error_code=error_code,
                )

    async def abandon_prepared_commit_intent_on_connection(
        self,
        connection: Any,
        intent_id: UUID,
        *,
        expected_version: int,
        error_code: str,
    ) -> bool:
        """Connection-bound variant used while a repository fence is held."""

        intent = await self.store.get_commit_intent(
            connection, intent_id, for_update=True
        )
        if intent is None or intent.state != "prepared":
            return False
        if intent.version != expected_version:
            return False
        updated = await self.store.update_commit_intent(
            connection,
            intent_id,
            state="abandoned",
            from_states=("prepared",),
            expected_version=expected_version,
            error_code=error_code,
            error_summary="stale preparation was cleaned after the ref was fenced",
        )
        if updated is None:
            return False
        await self.store.transition_quota_reservations(
            connection,
            intent_id,
            from_state="reserved",
            to_state="released",
        )
        if intent.observation_operation_id is not None:
            await self.store.finish_observation_operation(
                connection,
                intent.observation_operation_id,
                state="succeeded",
                result_json={"outcome": "no_effect", "error_code": error_code},
            )
        return True

    async def mark_commit_intent_committed(
        self,
        intent_id: UUID,
        *,
        lakefs_commit_id: str,
        result_json: Mapping[str, Any] | None = None,
    ) -> CommitIntentRecord | None:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                existing = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if existing is None:
                    return None
                if existing.state == "finalized":
                    if existing.lakefs_commit_id != lakefs_commit_id:
                        raise IdempotencyConflict(
                            "finalized commit intent cannot change its LakeFS commit id"
                        )
                    return existing
                if existing.lakefs_commit_id not in (None, lakefs_commit_id):
                    raise IdempotencyConflict(
                        "commit intent is already bound to another LakeFS commit"
                    )
                return await self.store.update_commit_intent(
                    connection,
                    intent_id,
                    state="committed",
                    lakefs_commit_id=lakefs_commit_id,
                    result_json=dict(result_json) if result_json else None,
                    from_states=(
                        "prepared",
                        "dispatch_started",
                        "committed",
                        "reconciliation_required",
                        "uncertain",
                    ),
                    expected_version=existing.version,
                )

    async def finalize_commit_intent(
        self,
        intent_id: UUID,
        *,
        payload: Mapping[str, Any],
        result_json: Mapping[str, Any],
        requested_by_user_id: int | None,
        idempotency_key: str,
    ) -> OperationRecord | None:
        """Finalize KHub commit state and enqueue postprocess atomically.

        The external LakeFS commit has already succeeded.  If this transaction
        fails, the prepared/committed intent remains available to the reaper;
        callers must keep the original commit success response.
        """

        async with self.pool.connection() as connection:
            async with connection.transaction():
                intent = await self.store.get_commit_intent(
                    connection, intent_id, for_update=True
                )
                if intent is None:
                    return None
                if intent.state == "finalized" and intent.operation_id is not None:
                    return await self.store.get_operation(connection, intent.operation_id)
                if intent.state not in {
                    "prepared",
                    "dispatch_started",
                    "committed",
                    "reconciliation_required",
                }:
                    return None
                if not intent.lakefs_commit_id:
                    raise ValueError("commit intent has no confirmed LakeFS commit id")
                payload_value = dict(intent.payload_json)
                supplied_payload = dict(payload)
                supplied_commit_id = supplied_payload.pop("commit_id", None)
                if supplied_commit_id is not None and str(supplied_commit_id) != str(
                    intent.lakefs_commit_id
                ):
                    raise IdempotencyConflict(
                        "finalization payload commit id differs from the intent"
                    )
                _, supplied_hash = _canonical_payload(supplied_payload)
                if supplied_hash != intent.payload_hash:
                    raise IdempotencyConflict(
                        "finalization payload differs from the persisted commit intent"
                    )
                payload_value["commit_id"] = intent.lakefs_commit_id
                await finalize_commit_domain(connection, payload=payload_value)
                operation = await self.accept_in_transaction(
                    connection,
                    operation_id=intent.id,
                    kind="commit.postprocess.v1",
                    resource_key=f"commit-postprocess:{intent.repository_id}:{payload_value['commit_id']}",
                    payload=payload_value,
                    requested_by_user_id=(
                        intent.requested_by_user_id
                        if intent.requested_by_user_id is not None
                        else requested_by_user_id
                    ),
                    idempotency_key=idempotency_key,
                    repository_id=intent.repository_id,
                    expected_head=str(payload_value["commit_id"]),
                )
                finalized = await self.store.update_commit_intent(
                    connection,
                    intent.id,
                    state="finalized",
                    operation_id=operation.id,
                    lakefs_commit_id=str(intent.lakefs_commit_id),
                    result_json=dict(result_json),
                    finalize=True,
                    from_states=("committed", "reconciliation_required"),
                    expected_version=intent.version,
                )
                if finalized is None:
                    raise IdempotencyConflict(
                        "commit intent changed before finalization could be recorded"
                    )
                await self.store.transition_quota_reservations(
                    connection,
                    intent.id,
                    from_state="reserved",
                    to_state="consumed",
                )
                if intent.observation_operation_id is not None:
                    await self.store.finish_observation_operation(
                        connection,
                        intent.observation_operation_id,
                        state="succeeded",
                        result_json=dict(result_json),
                    )
                return operation

    async def cancel(self, operation_id: UUID) -> OperationRecord | None:
        job_id: int | None = None
        async with self.pool.connection() as connection:
            async with connection.transaction():
                existing = await self.store.get_operation(
                    connection, operation_id, for_update=True
                )
                if existing is None:
                    return None
                if existing.state in {"dispatch_started", "uncertain"}:
                    raise OperationNotCancellable(
                        "operation crossed an external side-effect boundary"
                    )
                if existing.state in {
                    "succeeded",
                    "failed",
                    "cancelled",
                    "cleanup_pending",
                }:
                    return existing
                step = await self.store.get_active_step_for_operation(
                    connection, operation_id
                )
                if step is not None and step.state == "running":
                    spec = self.registry.get(existing.kind)
                    if not spec.cancel_while_running:
                        raise OperationNotCancellable(
                            "operation handler cannot be cancelled while running"
                        )
                job_id = step.procrastinate_job_id if step is not None else None
                operation = await self.store.request_cancel(connection, operation_id)

        # Procrastinate owns delivery cancellation. KHub state remains the
        # authority, so a transient failure here is recovered by the
        # reconciler rather than changing the public cancellation result.
        if job_id is not None:
            manager = getattr(self.app, "job_manager", None)
            cancel_job = getattr(manager, "cancel_job_by_id_async", None)
            if cancel_job is not None:
                try:
                    await cancel_job(job_id, abort=True)
                except Exception as exc:
                    # The durable state remains cancel_requested so the
                    # reconciler can retry delivery cancellation.  Do not
                    # hide the dependency failure from operators.
                    logger.warning(
                        "Failed to cancel Procrastinate job "
                        f"{job_id} for operation {operation_id}: {exc}"
                    )
        return operation

    async def mark_dispatch_started(
        self,
        operation_id: UUID,
        step_id: int,
        *,
        expected_source: str,
        expected_target: str,
        remote_deadline: datetime,
        observe_not_before: datetime,
        external_marker: str,
    ) -> None:
        """Persist the no-blind-retry boundary before an external call."""

        async with self.pool.connection() as connection:
            async with connection.transaction():
                operation = await self.store.get_operation(
                    connection, operation_id, for_update=True
                )
                step = await self.store.get_step(connection, step_id, for_update=True)
                if operation is None or step is None or step.operation_id != operation_id:
                    raise ValueError("operation step does not exist")
                if operation.state in {"cancel_requested", "cancelled"}:
                    raise OperationNotCancellable(
                        "operation was cancelled before external dispatch"
                    )
                if operation.state in {"dispatch_started", "uncertain"}:
                    return
                if operation.state not in {"accepted", "running"} or step.state not in {
                    "pending",
                    "running",
                }:
                    raise OperationNotCancellable(
                        "operation is no longer before the external dispatch boundary"
                    )
                await connection.execute(
                    """UPDATE khub_operation_steps
                       SET state = 'dispatch_started', external_marker = %s,
                           expected_source = %s, expected_target = %s,
                           updated_at = CURRENT_TIMESTAMP
                       WHERE id = %s AND operation_id = %s""",
                    (
                        external_marker,
                        expected_source,
                        expected_target,
                        step_id,
                        operation_id,
                    ),
                )
                await connection.execute(
                    """UPDATE khub_repository_operations
                       SET state = 'dispatch_started', dispatch_started_at = CURRENT_TIMESTAMP,
                           remote_deadline_at = %s, observe_not_before = %s,
                           updated_at = CURRENT_TIMESTAMP, version = version + 1
                       WHERE id = %s""",
                    (remote_deadline, observe_not_before, operation_id),
                )

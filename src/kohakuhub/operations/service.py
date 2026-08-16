"""Acceptance and query service for durable operations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .store import OperationStore
from .types import OperationRecord


class IdempotencyConflict(ValueError):
    """The same idempotency key was used for a different request."""


class OperationNotCancellable(ValueError):
    """The operation crossed a boundary where cancellation is unsafe."""


_FORBIDDEN_PAYLOAD_KEYS = {
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "password",
    "presigned_url",
    "secret",
    "token",
}


def _canonical_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    value = json.loads(json.dumps(dict(payload), separators=(",", ":"), sort_keys=True))
    _assert_safe_payload(value)
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    return value, hashlib.sha256(encoded).hexdigest()


def _assert_safe_payload(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _FORBIDDEN_PAYLOAD_KEYS or normalized.endswith("_token"):
                raise ValueError(f"sensitive field is not allowed in {path}.{key}")
            _assert_safe_payload(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_payload(child, f"{path}[{index}]")


class OperationService:
    def __init__(self, pool: Any, app: Any, registry: OperationRegistry = DEFAULT_REGISTRY):
        self.pool = pool
        self.app = app
        self.registry = registry
        self.store = OperationStore(pool)

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
        spec = self.registry.get(kind)
        canonical_payload, payload_hash = _canonical_payload(payload)
        if not resource_key or len(resource_key) > 512:
            raise ValueError("resource_key must be non-empty and at most 512 characters")
        if idempotency_key is not None and not (1 <= len(idempotency_key) <= 256):
            raise ValueError("idempotency_key must be between 1 and 256 characters")

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

        operation_id = uuid4()
        delivery_key = f"operation:{operation_id}:step:0"
        now = datetime.now(timezone.utc)

        async with self.pool.connection() as connection:
            async with connection.transaction():
                if requested_by_user_id is not None and idempotency_key is not None:
                    # A missing row cannot be locked by SELECT ... FOR UPDATE.
                    # Serialize the first insert for this actor/key so
                    # concurrent retries converge on the same operation.
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
                    lock=(
                        f"{spec.lock_prefix}:{resource_key}"
                        if spec.lock_prefix
                        else None
                    ),
                    queueing_lock=delivery_key,
                ).defer_async(operation_id=str(operation_id), step_id=str(step_id))
                await self.store.set_job_id(connection, step_id, job_id)

            operation = await self.store.get_operation(connection, operation_id)
            assert operation is not None
            return operation

    async def get(self, operation_id: UUID) -> OperationRecord | None:
        async with self.pool.connection() as connection:
            return await self.store.get_operation(connection, operation_id)

    async def cancel(self, operation_id: UUID) -> OperationRecord | None:
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
                return await self.store.request_cancel(connection, operation_id)

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

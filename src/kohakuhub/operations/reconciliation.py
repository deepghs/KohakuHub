"""Durable repair scans for missing deliveries and commit finalization."""

from __future__ import annotations

import time
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from kohakuhub.utils.lakefs import get_lakefs_client

from .metrics import (
    OPERATION_BACKLOG,
    OPERATION_JOBS_REPAIRED,
    OPERATION_RETENTION_BACKLOG,
    OPERATION_RETENTION_PRUNED,
    OPERATION_STALLED,
    OPERATION_UNCERTAIN,
    QUEUE_DEPTH,
    QUEUE_OLDEST_AGE_SECONDS,
    RECONCILIATION_BACKLOG,
    RECONCILIATION_DURATION,
    RECONCILIATION_LAG_SECONDS,
    RECONCILIATION_RUNS,
)
from kohakuhub import lakefs_mutation_gateway as mutation_gateway
from .registry import DEFAULT_REGISTRY, OperationRegistry
from .service import OperationService
from .store import OperationStore
from .handlers import finalize_revert_commit
from .types import operation_max_attempts

NO_EFFECT_CURSOR = "__khub_observation_no_effect__"
AMBIGUOUS_MARKER_CURSOR = "__khub_observation_ambiguous_marker__"
OBSERVATION_CURSOR_PREFIX = "__khub_observation_cursor_v1__:"
KNOWN_WORKER_QUEUES = ("control-v1", "sync-v1", "bulk-v1", "cleanup-v1")


async def _recover_stalled_deliveries(
    app: Any, *, seconds_since_heartbeat: float, limit: int
) -> set[int]:
    """Return stalled Procrastinate deliveries to ``todo`` before ledger repair.

    Procrastinate intentionally leaves a job in ``doing`` when its worker
    process disappears.  Its supported stalled-job query also covers the
    post-worker-prune case where ``worker_id`` has become NULL.  Requeue the
    bounded batch first; the ledger pass below then decides whether the step
    may retry or must enter observation.
    """

    manager = getattr(app, "job_manager", None)
    get_stalled = getattr(manager, "get_stalled_jobs", None)
    retry_job = getattr(manager, "retry_job_by_id_async", None)
    if get_stalled is None or retry_job is None:
        return set()

    stalled = await get_stalled(seconds_since_heartbeat=seconds_since_heartbeat)
    recovered: set[int] = set()
    for job in list(stalled)[:limit]:
        job_id = int(job.id)
        try:
            await retry_job(job_id, retry_at=datetime.now(timezone.utc))
        except Exception:
            # Another worker/reconciler may have changed the delivery first.
            # The durable ledger query remains authoritative on the next pass.
            continue
        recovered.add(job_id)
    return recovered


async def _observe_runtime_metrics(
    connection: Any, *, retention_hours: int
) -> None:
    """Publish bounded control-plane gauges from the durable PostgreSQL state."""

    for state in (
        "accepted",
        "running",
        "cancel_requested",
        "dispatch_started",
        "uncertain",
        "cleanup_pending",
    ):
        OPERATION_BACKLOG.labels(state=state).set(0)
    for queue in KNOWN_WORKER_QUEUES:
        for status in ("todo", "doing"):
            QUEUE_DEPTH.labels(queue=queue, status=status).set(0)
        QUEUE_OLDEST_AGE_SECONDS.labels(queue=queue).set(0)

    rows = await connection.execute(
        """SELECT queue_name, status, count(*)
           FROM procrastinate_jobs
           WHERE status IN ('todo', 'doing')
           GROUP BY queue_name, status"""
    )
    for queue, status, depth in await rows.fetchall():
        QUEUE_DEPTH.labels(queue=str(queue), status=str(status)).set(float(depth))

    rows = await connection.execute(
        """SELECT j.queue_name,
                  COALESCE(
                      EXTRACT(EPOCH FROM (
                          CURRENT_TIMESTAMP - MAX(
                              CASE
                                  WHEN e.type IN ('deferred', 'deferred_for_retry', 'started')
                                  THEN e.at
                              END
                          )
                      )),
                      0
                  )
           FROM procrastinate_jobs j
           LEFT JOIN procrastinate_events e ON e.job_id = j.id
           WHERE j.status IN ('todo', 'doing')
           GROUP BY j.queue_name"""
    )
    for queue, age in await rows.fetchall():
        QUEUE_OLDEST_AGE_SECONDS.labels(queue=str(queue)).set(max(0.0, float(age or 0)))

    rows = await connection.execute(
        """SELECT state, count(*)
           FROM khub_repository_operations
           WHERE state NOT IN ('succeeded', 'failed', 'cancelled')
           GROUP BY state"""
    )
    for state, count in await rows.fetchall():
        OPERATION_BACKLOG.labels(state=str(state)).set(float(count))

    rows = await connection.execute(
        """SELECT count(*)
           FROM khub_repository_operations
           WHERE state IN ('uncertain', 'dispatch_started')"""
    )
    backlog = await rows.fetchone()
    RECONCILIATION_BACKLOG.set(float(backlog[0] or 0))

    rows = await connection.execute(
        """SELECT count(*)
           FROM khub_repository_operations
           WHERE state IN ('succeeded', 'failed', 'cancelled')
             AND finished_at IS NOT NULL
             AND finished_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')""",
        (retention_hours,),
    )
    retention_backlog = await rows.fetchone()
    OPERATION_RETENTION_BACKLOG.set(float(retention_backlog[0] or 0))


def _decode_observation_cursor(value: str | None) -> tuple[str | None, set[str]]:
    """Decode a raw LakeFS cursor plus marker evidence from prior pages."""

    if not value or not value.startswith(OBSERVATION_CURSOR_PREFIX):
        return value, set()
    try:
        payload = json.loads(value[len(OBSERVATION_CURSOR_PREFIX) :])
        after = payload.get("after")
        markers = payload.get("markers") or []
        if not isinstance(after, str) or not all(
            isinstance(marker, str) for marker in markers
        ):
            return None, set()
        return after, set(markers)
    except (TypeError, ValueError):
        return None, set()


def _encode_observation_cursor(after: str, markers: set[str]) -> str:
    return OBSERVATION_CURSOR_PREFIX + json.dumps(
        {"after": after, "markers": sorted(markers)},
        separators=(",", ":"),
        sort_keys=True,
    )


async def _observe_revert_operation_unfenced(
    pool: Any,
    operation_id: Any,
    step_id: int,
) -> bool:
    """Resolve a crashed Revert from its marker or affirmative no-effect state."""

    store = OperationStore(pool)
    async with pool.connection() as connection:
        # This read is only a snapshot for the remote observation.  The
        # connection is not inside a transaction, so FOR UPDATE would not
        # protect anything.  The final transition below is the concurrency
        # boundary and uses a state-conditional update.
        operation = await store.get_operation(connection, operation_id)
        step = await store.get_step(connection, step_id)
    if operation is None or step is None:
        return False
    if (
        operation.observe_not_before is not None
        and operation.observe_not_before > datetime.now(timezone.utc)
    ):
        return False
    payload = dict(step.input_json)
    client = get_lakefs_client()
    try:
        branch = await client.get_branch(
            repository=str(payload["lakefs_repo"]), branch=str(payload["branch"])
        )
        head = str(branch["commit_id"])
    except Exception:
        return False

    marker = step.external_marker
    if not marker:
        return False

    # Freeze the first live head as the immutable log reference. New commits
    # may arrive while observation is paginated, but they must not change the
    # history being searched after a worker restart.
    checkpoint = dict(step.checkpoint_json or {})
    observation_head = checkpoint.get("observation_head")
    if not observation_head:
        observation_head = head
        checkpoint["observation_head"] = observation_head
    after, marker_commit_ids = _decode_observation_cursor(
        checkpoint.get("cursor")
    )

    # Observe an immutable commit ref instead of inspecting only the current
    # HEAD. A later unrelated commit must not hide a successful revert, while
    # an incomplete page must never be treated as proof of no-effect.
    try:
        log = await client.log_commits(
            repository=str(payload["lakefs_repo"]),
            ref=str(observation_head),
            after=after,
            amount=100,
        )
    except Exception:
        return False
    if not isinstance(log, dict):
        return False
    marker_ids: set[str] = set(marker_commit_ids)
    reached_base = False
    for candidate in log.get("results", []):
        candidate_id = candidate.get("id") or candidate.get("commit_id")
        if not candidate_id:
            continue
        if _commit_metadata_matches(candidate, marker):
            marker_ids.add(str(candidate_id))
        else:
            try:
                candidate_detail = await client.get_commit(
                    repository=str(payload["lakefs_repo"]),
                    commit_id=str(candidate_id),
                )
            except Exception:
                return False
            if _commit_metadata_matches(candidate_detail, marker):
                marker_ids.add(str(candidate_id))
        if str(candidate_id) == str(payload.get("base_head")):
            reached_base = True
    if len(marker_ids) > 1:
        return False
    pagination = log.get("pagination") or {}
    if not reached_base and pagination.get("has_more"):
        next_after = pagination.get("next_offset")
        if not next_after or str(next_after) == str(after):
            return False
        checkpoint["cursor"] = _encode_observation_cursor(
            str(next_after), marker_ids
        )
        async with pool.connection() as connection:
            async with connection.transaction():
                return await store.update_observing_checkpoint(
                    connection,
                    step_id,
                    operation_id,
                    checkpoint=checkpoint,
                    error_code="revert_observation_page_saved",
                    error_summary="revert observation will continue from its durable cursor",
                )
    if not reached_base:
        # A complete response must contain the expected base before a missing
        # marker can prove no effect. Once the fixed observation range reaches
        # base_head, older history is outside this operation's search window;
        # requiring has_more=False would restart the same page forever.
        return False
    if len(marker_ids) == 1:
        result = await finalize_revert_commit(
            payload, commit_id=next(iter(marker_ids))
        )
    elif observation_head == str(payload.get("base_head")):
        result = {"success": True, "outcome": "no_effect"}
    else:
        # A complete history with no marker but a moved HEAD is not proof that
        # this operation had no effect; another writer may have moved the ref.
        return False

    async with pool.connection() as connection:
        async with connection.transaction():
            current_operation = await store.get_operation(
                connection, operation_id, for_update=True
            )
            current_step = await store.get_step(connection, step_id, for_update=True)
            if current_operation is None or current_step is None:
                return False
            transitioned = await store.finish_step(
                connection,
                current_operation,
                current_step,
                state="succeeded",
                progress_current=1,
                progress_total=1,
                progress_message="revert observed",
                result_json=result,
                error_code=None,
                error_summary=None,
            )
            if not transitioned:
                return False
    return True


async def _observe_revert_operation(
    pool: Any,
    operation_id: Any,
    step_id: int,
    *,
    service: OperationService,
) -> bool:
    """Observe one Revert while holding the same repository/ref fence as writers."""

    store = OperationStore(pool)
    async with pool.connection() as connection:
        operation = await store.get_operation(connection, operation_id)
        step = await store.get_step(connection, step_id)
    if operation is None or step is None or operation.repository_id is None:
        return False
    branch = str(step.input_json.get("branch") or operation.resource_key)
    async with service.repository_ref_fence(
        operation.repository_id,
        branch,
        exclude_operation_id=operation_id,
    ):
        return await _observe_revert_operation_unfenced(
            pool,
            operation_id,
            step_id,
        )


def _commit_metadata_matches(commit: dict[str, Any], marker: str) -> bool:
    metadata = commit.get("metadata") or {}
    return str(metadata.get("khub_marker", "")) == marker


async def _ensure_observation_head(
    pool: Any,
    intent: Any,
    *,
    service: OperationService | None = None,
) -> Any | None:
    """Freeze the first branch head used by a commit observer.

    LakeFS log pagination against a branch is a moving read. Capture one
    branch head once, then use that commit ID as the immutable log ref for all
    later pages. The durable version check makes concurrent observers
    converge on one head instead of overwriting each other's snapshot.
    """

    existing_head = getattr(intent, "observation_head", None)
    if existing_head:
        return intent
    observe_not_before = getattr(intent, "observe_not_before", None)
    if observe_not_before is not None and observe_not_before > datetime.now(timezone.utc):
        # Do not freeze a branch head while the original remote request may
        # still be alive. A pre-quiet snapshot could hide a delayed success.
        return intent
    if service is not None and getattr(intent, "repository_id", None) is not None:
        # Freeze the head while the same repository/ref fence excludes a live
        # writer. Re-read the intent inside the fence so the CAS and the
        # branch snapshot belong to the same recovery attempt.
        async with service.repository_ref_fence(
            int(intent.repository_id),
            str(intent.ref),
            exclude_intent_id=intent.id,
        ):
            store = OperationStore(pool)
            async with pool.connection() as connection:
                current = await store.get_commit_intent(connection, intent.id)
            if current is None:
                return None
            return await _ensure_observation_head(pool, current)
    client = get_lakefs_client()
    payload = intent.payload_json
    lakefs_repo = payload.get("lakefs_repo")
    if not lakefs_repo:
        return None
    try:
        branch = await client.get_branch(
            repository=lakefs_repo,
            branch=str(intent.ref),
        )
        observation_head = str(branch.get("commit_id") or "")
    except Exception:
        return None
    if not observation_head:
        return None

    version = getattr(intent, "version", None)
    if version is None:
        # Lightweight unit-test snapshots do not have a durable row to CAS.
        return intent

    store = OperationStore(pool)
    async with pool.connection() as connection:
        async with connection.transaction():
            current = await store.get_commit_intent(
                connection, intent.id, for_update=True
            )
            if current is None:
                return None
            if current.observation_head:
                return current
            updated = await store.set_observation_head(
                connection,
                intent.id,
                observation_head,
                expected_version=current.version,
            )
            return updated or await store.get_commit_intent(connection, intent.id)


async def _observe_commit(
    intent: Any,
) -> tuple[str | None, dict[str, Any] | None, str | None]:
    payload = intent.payload_json
    lakefs_repo = payload.get("lakefs_repo")
    if not lakefs_repo:
        return None, None, None
    if (
        intent.lakefs_commit_id is None
        and intent.observe_not_before is not None
        and intent.observe_not_before > datetime.now(timezone.utc)
    ):
        # A negative lookup before the remote request's quiet period is not
        # evidence of no-effect. Keep the intent unresolved for a later pass.
        return None, None, None
    client = get_lakefs_client()
    if intent.lakefs_commit_id:
        try:
            commit = await client.get_commit(
                repository=lakefs_repo, commit_id=intent.lakefs_commit_id
            )
        except Exception:
            return None, None, None
        if _commit_metadata_matches(commit, intent.marker):
            return intent.lakefs_commit_id, commit, None
        return None, None, None

    # The commit API has no server-side marker lookup.  Keep this observation
    # bounded and only accept an exact immutable marker match.  A missing
    # marker is not evidence that dispatch did not happen.
    after, marker_commit_ids = _decode_observation_cursor(
        intent.observation_cursor
    )
    # Unit snapshots from the original helper contract have no durable head;
    # production reconciliation always calls _ensure_observation_head first.
    observation_ref = getattr(intent, "observation_head", None) or intent.ref
    # A page is bounded work.  The observer is re-entered by reconciliation,
    # so a very long history cannot monopolize a worker slot.
    try:
        result = await client.log_commits(
            repository=lakefs_repo,
            ref=str(observation_ref),
            after=after,
            amount=100,
        )
    except Exception:
        return None, None, None
    if not isinstance(result, dict):
        return None, None, None
    for candidate in result.get("results", []):
        commit_id = candidate.get("id") or candidate.get("commit_id")
        if not commit_id:
            continue
        if _commit_metadata_matches(candidate, intent.marker):
            marker_commit_ids.add(str(commit_id))
        else:
            try:
                commit = await client.get_commit(
                    repository=lakefs_repo, commit_id=str(commit_id)
                )
            except Exception:
                # A partial page cannot prove that the marker is absent or
                # that the base head was reached. Keep the intent unresolved.
                return None, None, None
            if _commit_metadata_matches(commit, intent.marker):
                marker_commit_ids.add(str(commit_id))
        if str(commit_id) == str(intent.base_head):
            # Seeing the exact base head after a successful detail lookup is
            # the only affirmative no-effect proof for a marker-less commit.
            if len(marker_commit_ids) > 1:
                return None, None, AMBIGUOUS_MARKER_CURSOR
            if marker_commit_ids:
                marker_commit_id = next(iter(marker_commit_ids))
                try:
                    marker_commit = await client.get_commit(
                        repository=lakefs_repo, commit_id=marker_commit_id
                    )
                except Exception:
                    return None, None, None
                return marker_commit_id, marker_commit, None
            return None, None, NO_EFFECT_CURSOR
    if len(marker_commit_ids) > 1:
        # Multiple commits carrying one operation marker cannot be attributed
        # safely. Keep the intent unresolved for operator/reconciliation work.
        return None, None, AMBIGUOUS_MARKER_CURSOR
    pagination = result.get("pagination") or {}
    if not pagination.get("has_more"):
        # Reaching the end without the exact base head is not proof of
        # no-effect: history may be rewritten, truncated, or only partially
        # visible.  Keep the intent unresolved for operator/reconciliation
        # handling instead of releasing its barrier.
        return None, None, None
    next_after = pagination.get("next_offset")
    if not next_after or str(next_after) == str(after):
        # A missing or non-progressing cursor makes pagination incomplete.
        # Never turn that into a no-effect decision.
        return None, None, None
    # Persist the cursor so a large history is traversed one bounded page at a
    # time instead of monopolising the control worker.
    return None, None, _encode_observation_cursor(str(next_after), marker_commit_ids)


async def _reset_stale_prepared_intent(
    service: OperationService, intent: Any
) -> bool:
    """Clear abandoned staging only while the branch is still at its base.

    A prepared intent has not dispatched a commit, but its API process may have
    left staged objects or deletes behind.  Resetting the unchanged branch is
    idempotent and prevents stale staging from contaminating the next writer.
    If the branch moved, leave the intent blocking and require observation.
    """

    payload = intent.payload_json
    lakefs_repo = payload.get("lakefs_repo")
    if not lakefs_repo:
        return False
    async with service.repository_ref_fence(
        intent.repository_id,
        intent.ref,
        exclude_intent_id=intent.id,
    ) as fence_connection:
        async with fence_connection.transaction():
            current = await service.store.get_commit_intent(
                fence_connection, intent.id, for_update=True
            )
        if current is None or current.state != "prepared":
            return False
        if current.prepared_deadline_at is not None and current.prepared_deadline_at > datetime.now(timezone.utc):
            return False

        client = get_lakefs_client()
        branch = await client.get_branch(repository=lakefs_repo, branch=current.ref)
        if str(branch.get("commit_id")) != str(current.base_head):
            return False
        await mutation_gateway.hard_reset_branch(
            client,
            repository=lakefs_repo,
            branch=current.ref,
            ref=current.base_head,
            force=True,
        )
        async with fence_connection.transaction():
            return await service.abandon_prepared_commit_intent_on_connection(
                fence_connection,
                current.id,
                expected_version=current.version,
                error_code="stale_prepared_cleanup",
            )


async def reconcile_once(
    pool: Any,
    app: Any,
    *,
    registry: OperationRegistry = DEFAULT_REGISTRY,
    limit: int = 100,
    database_url: str | None = None,
    retention_hours: int = 168,
    retention_batch_size: int = 100,
) -> int:
    """Repair durable operation gaps without blindly replaying side effects."""

    started = time.monotonic()
    repaired = 0
    service = OperationService(
        pool,
        app,
        registry,
        database_url=database_url,
        fence_connection_limit=getattr(app, "khub_fence_connection_limit", None),
    )
    store = OperationStore(pool)
    max_attempts = operation_max_attempts()
    stalled_timeout_seconds = float(
        getattr(app, "khub_stalled_worker_timeout_seconds", 30.0)
    )
    if stalled_timeout_seconds <= 0:
        raise ValueError("stalled worker timeout must be positive")
    if retention_hours < 1:
        raise ValueError("retention_hours must be positive")
    if retention_batch_size < 1:
        raise ValueError("retention_batch_size must be positive")
    try:
        await _recover_stalled_deliveries(
            app,
            seconds_since_heartbeat=stalled_timeout_seconds,
            limit=limit,
        )
        async with pool.connection() as connection:
            async with connection.transaction():
                locked = await connection.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                    ("khub-operation-reconciliation:v1",),
                )
                row = await locked.fetchone()
                if not row or not row[0]:
                    RECONCILIATION_RUNS.labels(result="locked").inc()
                    return 0
                repaired += await store.finalize_cancelled_pending_steps(
                    connection, limit=limit
                )
                missing_jobs = await store.list_steps_missing_jobs(
                    connection, limit=limit, for_update=True
                )
                # The connection remains in one transaction while we claim the
                # rows, but external LakeFS calls happen after this short lock
                # window.  A deterministic queueing lock prevents duplicates.
                for item in missing_jobs:
                    spec = registry.get(item["kind"])
                    job_id = await app.tasks[spec.task_name].configure(
                        connection=connection,
                        queue=spec.queue,
                        priority=spec.priority,
                        lock=spec.delivery_lock(item["resource_key"]),
                        queueing_lock=item["delivery_key"],
                    ).defer_async(
                        operation_id=str(item["operation_id"]),
                        step_id=str(item["step_id"]),
                    )
                    await store.set_job_id(connection, item["step_id"], job_id)
                    OPERATION_JOBS_REPAIRED.labels(kind=item["kind"]).inc()
                    repaired += 1

        async with pool.connection() as connection:
            intents = await store.list_recoverable_commit_intents(
                connection, limit=limit
            )
            stale_prepared = await store.list_stale_prepared_commit_intents(
                connection,
                cutoff=datetime.now(timezone.utc),
                limit=limit,
            )

        for intent in stale_prepared:
            try:
                if not await _reset_stale_prepared_intent(service, intent):
                    continue
                await service.sync_commit_observation_operation(intent.id)
                repaired += 1
            except Exception:
                # A cleanup failure must preserve the intent and its barrier;
                # the next bounded reconciliation pass retries it.
                continue

        for intent in intents:
            observed_intent = await _ensure_observation_head(
                pool, intent, service=service
            )
            if observed_intent is None:
                continue
            commit_id, commit, next_cursor = await _observe_commit(observed_intent)
            if next_cursor is not None:
                if next_cursor == NO_EFFECT_CURSOR:
                    await service.abandon_commit_intent(
                        observed_intent.id,
                        error_code="commit_confirmed_no_effect",
                        confirmed_no_effect=True,
                        expected_version=observed_intent.version,
                    )
                    await service.sync_commit_observation_operation(observed_intent.id)
                    repaired += 1
                    continue
                if next_cursor == AMBIGUOUS_MARKER_CURSOR:
                    await service.mark_commit_intent_uncertain(
                        observed_intent.id,
                        error_code="ambiguous_commit_marker",
                        error_summary=(
                            "multiple LakeFS commits carry the same operation marker"
                        ),
                        expected_version=observed_intent.version,
                    )
                    await service.sync_commit_observation_operation(
                        observed_intent.id
                    )
                    repaired += 1
                    continue
                async with pool.connection() as connection:
                    async with connection.transaction():
                        await store.update_observation_cursor(
                            connection,
                            observed_intent.id,
                            next_cursor,
                            expected_version=observed_intent.version,
                        )
                continue
            if not commit_id:
                continue
            payload = dict(observed_intent.payload_json)
            payload["commit_id"] = commit_id
            result_json = dict(intent.result_json or {})
            result_json.setdefault("commitOid", commit_id)
            result_json.setdefault("commitUrl", "")
            await service.mark_commit_intent_committed(
                observed_intent.id, lakefs_commit_id=commit_id, result_json=result_json
            )
            await service.finalize_commit_intent(
                observed_intent.id,
                payload=payload,
                result_json=result_json,
                requested_by_user_id=payload.get("author_id"),
                idempotency_key=f"commit-postprocess:{commit_id}",
            )
            await service.sync_commit_observation_operation(observed_intent.id)
            repaired += 1

        # A stale running step is never blindly replayed.  It is moved into
        # observation, where a business-specific observer can resolve it.
        marked_for_observation: list[tuple[Any, str, int]] = []
        async with pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """SELECT o.id, o.kind, s.id, s.external_marker
                       FROM khub_repository_operations o
                       JOIN khub_operation_steps s ON s.operation_id = o.id
                       WHERE s.state IN ('running', 'observing', 'dispatch_started')
                         AND s.heartbeat_at < CURRENT_TIMESTAMP
                             - (%s * INTERVAL '1 second')
                         AND (
                               s.procrastinate_job_id IS NULL
                               OR NOT EXISTS (
                                   SELECT 1
                                   FROM procrastinate_jobs j
                                   WHERE j.id = s.procrastinate_job_id
                                     AND j.status = 'doing'
                               )
                         )
                       LIMIT %s FOR UPDATE SKIP LOCKED""",
                    (stalled_timeout_seconds, limit),
                )
                stale_rows = await cursor.fetchall()
                for operation_id, kind, step_id, external_marker in stale_rows:
                    spec = registry.get(kind)
                    if external_marker and not spec.replay_safe_after_dispatch:
                        marked_for_observation.append(
                            (operation_id, str(kind), int(step_id))
                        )
                    elif external_marker:
                        await store.requeue_stalled_external_step(
                            connection,
                            step_id,
                            operation_id,
                            max_attempts=max_attempts,
                        )
                    else:
                        await store.requeue_stalled_step(
                            connection,
                            step_id,
                            operation_id,
                            max_attempts=max_attempts,
                        )
                    OPERATION_STALLED.labels(kind=kind).inc()

        for operation_id, kind, step_id in marked_for_observation:
            if kind == "repository.revert.v1":
                try:
                    if await _observe_revert_operation(
                        pool,
                        operation_id,
                        step_id,
                        service=service,
                    ):
                        repaired += 1
                        continue
                except Exception:
                    # Keep the operation in observation until the next
                    # bounded pass; never resend the revert here.
                    pass
            async with pool.connection() as connection:
                async with connection.transaction():
                    await store.mark_step_observing(
                        connection,
                        step_id,
                        operation_id,
                        error_code="stalled_delivery",
                        error_summary="worker delivery heartbeat expired; awaiting observation",
                    )

        async with pool.connection() as connection:
            async with connection.transaction():
                row = await connection.execute(
                    """SELECT COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(updated_at))), 0)
                       FROM khub_repository_operations
                       WHERE state IN ('uncertain', 'dispatch_started')"""
                )
                lag = await row.fetchone()
                RECONCILIATION_LAG_SECONDS.set(float(lag[0] or 0))
                row = await connection.execute(
                    """SELECT count(*) FROM khub_repository_operations
                       WHERE state IN ('uncertain', 'dispatch_started')"""
                )
                uncertain = await row.fetchone()
                OPERATION_UNCERTAIN.set(float(uncertain[0] or 0))
                deleted_operations, _deleted_jobs = (
                    await store.prune_terminal_history(
                        connection,
                        cutoff=datetime.now(timezone.utc)
                        - timedelta(hours=retention_hours),
                        limit=retention_batch_size,
                    )
                )
                if deleted_operations:
                    OPERATION_RETENTION_PRUNED.inc(deleted_operations)
                await _observe_runtime_metrics(
                    connection, retention_hours=retention_hours
                )

                deleted_unlinked_jobs = await store.prune_unlinked_terminal_jobs(
                    connection,
                    cutoff=datetime.now(timezone.utc)
                    - timedelta(hours=retention_hours),
                    limit=retention_batch_size,
                )
                if deleted_unlinked_jobs:
                    OPERATION_RETENTION_PRUNED.inc(deleted_unlinked_jobs)
        RECONCILIATION_RUNS.labels(result="ok").inc()
        return repaired
    except Exception:
        RECONCILIATION_RUNS.labels(result="error").inc()
        raise
    finally:
        RECONCILIATION_DURATION.observe(time.monotonic() - started)

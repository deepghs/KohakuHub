"""Worker-side execution of one bounded operation step."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .store import OperationStore
from .types import RetryableOperationError, StepResult
from .metrics import OPERATION_DELIVERIES, OPERATION_FAILURES
from .service import OperationService


async def _mark_external_dispatch(
    pool: Any,
    operation: Any,
    step: Any,
    *,
    marker: str,
) -> bool:
    """Persist the no-blind-retry boundary before a side-effecting handler."""

    now = datetime.now(timezone.utc)
    remote_deadline = now + timedelta(
        seconds=float(os.getenv("KOHAKU_HUB_OPERATION_REMOTE_SECONDS", "300"))
    )
    observe_not_before = remote_deadline + timedelta(
        seconds=float(os.getenv("KOHAKU_HUB_OPERATION_QUIET_SECONDS", "30"))
    )
    expected_source = str(step.input_json.get("base_head") or operation.expected_head or "")
    expected_target = str(
        step.input_json.get("commit_id")
        or step.input_json.get("target")
        or operation.resource_key
    )
    async with pool.connection() as connection:
        async with connection.transaction():
            current = await connection.execute(
                """SELECT o.state, s.state
                   FROM khub_repository_operations AS o
                   JOIN khub_operation_steps AS s ON s.operation_id = o.id
                   WHERE o.id = %s AND s.id = %s
                   FOR UPDATE OF o, s""",
                (operation.id, step.id),
            )
            row = await current.fetchone()
            if row is None:
                return False
            operation_state, step_state = row
            if operation_state not in {"running", "dispatch_started"}:
                return False
            if step_state in {"succeeded", "failed", "cancelled", "uncertain", "observing"}:
                return False
            await connection.execute(
                """UPDATE khub_operation_steps
                   SET state = 'dispatch_started', external_marker = %s,
                       expected_source = %s, expected_target = %s,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = %s AND operation_id = %s
                     AND state IN ('pending', 'running', 'dispatch_started')""",
                (marker, expected_source, expected_target, step.id, operation.id),
            )
            await connection.execute(
                """UPDATE khub_repository_operations
                   SET state = 'dispatch_started', dispatch_started_at = COALESCE(dispatch_started_at, %s),
                       remote_deadline_at = COALESCE(remote_deadline_at, %s),
                       observe_not_before = COALESCE(observe_not_before, %s),
                       phase = 'dispatching', updated_at = CURRENT_TIMESTAMP,
                       version = version + 1
                   WHERE id = %s AND state IN ('running', 'dispatch_started')""",
                (now, remote_deadline, observe_not_before, operation.id),
            )
    return True


async def execute_operation_step(
    pool: Any,
    operation_id: str,
    step_id: str,
    registry: OperationRegistry = DEFAULT_REGISTRY,
    app: Any | None = None,
) -> None:
    operation_uuid = UUID(operation_id)
    numeric_step_id = int(step_id)
    store = OperationStore(pool)

    async with pool.connection() as connection:
        async with connection.transaction():
            started = await store.mark_running(
                connection, operation_uuid, numeric_step_id
            )
        if started is None:
            return
        operation, step, claimed = started

    if not claimed or operation.state in {"dispatch_started", "uncertain"} or step.state in {
        "dispatch_started",
        "observing",
        "succeeded",
        "failed",
        "cancelled",
        "uncertain",
    }:
        return

    OPERATION_DELIVERIES.labels(kind=operation.kind, state="claimed").inc()

    spec = registry.get(operation.kind)
    external_marker = f"khub:operation:v1:{operation.id}:step:{step.id}"

    heartbeat_stop = asyncio.Event()

    async def refresh_heartbeat() -> None:
        interval = max(
            0.5,
            float(os.getenv("KOHAKU_HUB_OPERATION_HEARTBEAT_SECONDS", "5")),
        )
        while not heartbeat_stop.is_set():
            try:
                await asyncio.wait_for(heartbeat_stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                try:
                    async with pool.connection() as heartbeat_connection:
                        async with heartbeat_connection.transaction():
                            await store.touch_heartbeat(
                                heartbeat_connection, operation_uuid, numeric_step_id
                            )
                except Exception:
                    # The handler result remains authoritative; a transient
                    # heartbeat failure must not turn a successful operation
                    # into a second external dispatch.
                    continue

    heartbeat_task = asyncio.create_task(refresh_heartbeat())
    try:
        if step.step_name != spec.step_name or step.step_version != spec.version:
            result = StepResult(
                state="failed",
                error_code="handler_version_mismatch",
                error_summary="operation checkpoint is not supported by this worker",
            )
        else:
            async def invoke_handler() -> StepResult:
                return await spec.handler(operation, step)

            async def invoke_with_dispatch() -> StepResult:
                if spec.external_side_effect and not await _mark_external_dispatch(
                    pool, operation, step, marker=external_marker
                ):
                    return None  # type: ignore[return-value]
                return await invoke_handler()

            if spec.fence_scope and operation.repository_id is not None:
                database_url = getattr(app, "khub_database_url", None)
                if not database_url:
                    result = StepResult(
                        state="failed",
                        error_code="mutation_fence_unavailable",
                        error_summary="repository mutation fence is unavailable",
                    )
                else:
                    service = OperationService(
                        pool,
                        app,
                        registry,
                        database_url=database_url,
                    )
                    fence_ref = str(
                        step.input_json.get("branch")
                        or step.input_json.get("ref")
                        or operation.resource_key
                    )
                    async with service.repository_ref_fence(
                        operation.repository_id,
                        fence_ref,
                        exclude_operation_id=operation.id,
                        scope=spec.fence_scope,
                    ):
                        result = await invoke_with_dispatch()
            else:
                result = await invoke_with_dispatch()
            if result is None:
                return
    except RetryableOperationError as exc:
        max_attempts = int(os.getenv("KOHAKU_HUB_OPERATION_MAX_RETRIES", "3"))
        if spec.external_side_effect and not spec.replay_safe_after_dispatch:
            # Once an external boundary was crossed, a generic retry could
            # duplicate a mutation.  Keep the operation observable instead;
            # only a handler with an explicit replay-safe contract may use the
            # normal retry path below.
            async with pool.connection() as connection:
                async with connection.transaction():
                    await store.mark_step_observing(
                        connection,
                        numeric_step_id,
                        operation_uuid,
                        error_code=exc.error_code,
                        error_summary=exc.error_summary,
                    )
            OPERATION_FAILURES.labels(kind=operation.kind, code=exc.error_code).inc()
            return
        if spec.external_side_effect and spec.replay_safe_after_dispatch:
            async with pool.connection() as connection:
                async with connection.transaction():
                    requeued = await store.requeue_replay_safe_step(
                        connection,
                        numeric_step_id,
                        operation_uuid,
                        error_code=exc.error_code,
                        error_summary=exc.error_summary,
                    )
            OPERATION_FAILURES.labels(kind=operation.kind, code=exc.error_code).inc()
            if not requeued:
                return
            raise
        if step.attempt >= max_attempts:
            result = StepResult(
                state="failed",
                error_code="retry_exhausted",
                error_summary="retryable operation failed repeatedly",
            )
        else:
            async with pool.connection() as connection:
                async with connection.transaction():
                    await store.requeue_retryable_step(
                        connection,
                        numeric_step_id,
                        operation_uuid,
                        error_code=exc.error_code,
                        error_summary=exc.error_summary,
                    )
            OPERATION_FAILURES.labels(kind=operation.kind, code=exc.error_code).inc()
            raise
    except asyncio.CancelledError:
        # Procrastinate aborts are cooperative. A cancellation before the
        # external dispatch boundary is a terminal KHub cancellation; a
        # cancellation after that boundary must preserve observation state.
        async with pool.connection() as connection:
            async with connection.transaction():
                current_operation = await store.get_operation(
                    connection, operation_uuid, for_update=True
                )
                current_step = await store.get_step(
                    connection, numeric_step_id, for_update=True
                )
                if (
                    current_operation is not None
                    and current_step is not None
                    and current_operation.state == "cancel_requested"
                    and current_step.state == "running"
                ):
                    await store.finish_step(
                        connection,
                        current_operation,
                        current_step,
                        state="cancelled",
                        progress_current=None,
                        progress_total=None,
                        progress_message="cancelled before external dispatch",
                        result_json=None,
                        error_code="cancelled",
                        error_summary="operation cancelled before external dispatch",
                    )
        raise
    except Exception:
        # External details are deliberately omitted from the durable public row.
        result = StepResult(
            state="failed",
            error_code="handler_failed",
            error_summary="registered operation handler failed",
        )
        OPERATION_FAILURES.labels(kind=operation.kind, code="handler_failed").inc()
    finally:
        heartbeat_stop.set()
        await heartbeat_task

    async with pool.connection() as connection:
        async with connection.transaction():
            current_operation = await store.get_operation(
                connection, operation_uuid, for_update=True
            )
            current_step = await store.get_step(
                connection, numeric_step_id, for_update=True
            )
            if current_operation is None or current_step is None:
                return
            external_confirmed = result.external_effect_confirmed and spec.external_side_effect
            blocked_after_dispatch = (
                not external_confirmed
                and (
                    current_operation.state in {"dispatch_started", "uncertain"}
                    or current_step.state
                    in {
                        "dispatch_started",
                        "observing",
                        "succeeded",
                        "failed",
                        "cancelled",
                        "uncertain",
                    }
                )
            )
            if blocked_after_dispatch:
                return
            if current_operation.state == "cancel_requested":
                result = StepResult(state="cancelled")
            if result.next_step_input is not None:
                if app is None:
                    result = StepResult(
                        state="failed",
                        error_code="successor_enqueue_unavailable",
                        error_summary="worker app is required for successor delivery",
                    )
                else:
                    spec = registry.resolve_step(
                        current_operation.kind,
                        current_step.step_name,
                        current_step.step_version,
                    )
                    next_sequence = current_step.sequence + 1
                    next_delivery_key = (
                        f"operation:{current_operation.id}:step:{next_sequence}"
                    )
                    await connection.execute(
                        """UPDATE khub_operation_steps
                           SET state = 'succeeded', finished_at = CURRENT_TIMESTAMP,
                               heartbeat_at = CURRENT_TIMESTAMP,
                               error_code = %s, error_summary = %s,
                               updated_at = CURRENT_TIMESTAMP
                           WHERE id = %s""",
                        (result.error_code, result.error_summary, current_step.id),
                    )
                    from psycopg.types.json import Jsonb

                    cursor = await connection.execute(
                        """INSERT INTO khub_operation_steps
                           (operation_id, sequence, step_name, step_version, state,
                            delivery_key, input_json, checkpoint_json)
                           VALUES (%s, %s, %s, %s, 'pending', %s, %s, '{}'::jsonb)
                           RETURNING id""",
                        (
                            current_operation.id,
                            next_sequence,
                            spec.step_name,
                            spec.version,
                            next_delivery_key,
                            Jsonb(result.next_step_input),
                        ),
                    )
                    next_row = await cursor.fetchone()
                    assert next_row is not None
                    next_step_id = int(next_row[0])
                    next_job_id = await app.tasks[spec.task_name].configure(
                        connection=connection,
                        queue=spec.queue,
                        priority=spec.priority,
                        lock=spec.delivery_lock(current_operation.resource_key),
                        queueing_lock=next_delivery_key,
                    ).defer_async(
                        operation_id=str(current_operation.id),
                        step_id=str(next_step_id),
                    )
                    await store.set_job_id(connection, next_step_id, next_job_id)
                    await connection.execute(
                        """UPDATE khub_repository_operations
                           SET state = 'running', phase = %s,
                               progress_current = COALESCE(%s, progress_current),
                               progress_total = COALESCE(%s, progress_total),
                               progress_message = %s, heartbeat_at = CURRENT_TIMESTAMP,
                               updated_at = CURRENT_TIMESTAMP, version = version + 1
                           WHERE id = %s""",
                        (
                            spec.step_name,
                            result.progress_current,
                            result.progress_total,
                            result.progress_message,
                            current_operation.id,
                        ),
                    )
                    return
            await store.finish_step(
                connection,
                current_operation,
                current_step,
                state=result.state,
                progress_current=result.progress_current,
                progress_total=result.progress_total,
                progress_message=result.progress_message,
                result_json=result.result_json,
                error_code=result.error_code,
                error_summary=result.error_summary,
            )

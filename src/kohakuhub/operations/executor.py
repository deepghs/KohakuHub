"""Worker-side execution of one bounded operation step."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .store import OperationStore
from .types import StepResult


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

    try:
        spec = registry.get(operation.kind)
        if step.step_name != spec.step_name or step.step_version != spec.version:
            result = StepResult(
                state="failed",
                error_code="handler_version_mismatch",
                error_summary="operation checkpoint is not supported by this worker",
            )
        else:
            result = await spec.handler(operation, step)
    except Exception:
        # External details are deliberately omitted from the durable public row.
        result = StepResult(
            state="failed",
            error_code="handler_failed",
            error_summary="registered operation handler failed",
        )

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
            if current_operation.state in {"dispatch_started", "uncertain"} or current_step.state in {
                "dispatch_started",
                "observing",
                "succeeded",
                "failed",
                "cancelled",
                "uncertain",
            }:
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
                               updated_at = CURRENT_TIMESTAMP
                           WHERE id = %s""",
                        (current_step.id,),
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
                        lock=(
                            f"{spec.lock_prefix}:{current_operation.resource_key}"
                            if spec.lock_prefix
                            else None
                        ),
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

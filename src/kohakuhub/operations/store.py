"""Small async PostgreSQL store for operation state."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from .types import OperationRecord, StepRecord


OPERATION_COLUMNS = """
id, kind, handler_version, repository_id, resource_key,
requested_by_user_id, trigger, idempotency_key, request_hash, state, phase,
progress_current, progress_total, progress_message, expected_head,
cancel_requested_at, started_at, heartbeat_at, finished_at,
dispatch_started_at, remote_deadline_at, observe_not_before, result_json,
error_code, error_summary, created_at, updated_at, version
"""

STEP_COLUMNS = """
id, operation_id, sequence, step_name, step_version, state, attempt,
delivery_key, procrastinate_job_id, input_json, checkpoint_json,
external_marker, expected_source, expected_target, artifact_key,
artifact_checksum, artifact_length, error_code, error_summary, started_at,
heartbeat_at, finished_at, created_at, updated_at
"""


def _mapping_row(row: Any, columns: str) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    names = [name.strip() for name in columns.replace("\n", " ").split(",")]
    return dict(zip(names, row, strict=True))


def _operation(row: Any) -> OperationRecord:
    return OperationRecord(**dict(_mapping_row(row, OPERATION_COLUMNS)))


def _step(row: Any) -> StepRecord:
    return StepRecord(**dict(_mapping_row(row, STEP_COLUMNS)))


class OperationStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def get_operation(
        self, connection: psycopg.AsyncConnection, operation_id: UUID, *, for_update: bool = False
    ) -> OperationRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = await connection.execute(
            f"SELECT {OPERATION_COLUMNS} FROM khub_repository_operations WHERE id = %s{suffix}",
            (operation_id,),
        )
        record = await row.fetchone()
        return _operation(record) if record else None

    async def get_operation_by_idempotency(
        self,
        connection: psycopg.AsyncConnection,
        requested_by_user_id: int,
        idempotency_key: str,
        *,
        for_update: bool = False,
    ) -> OperationRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"""SELECT {OPERATION_COLUMNS}
                FROM khub_repository_operations
                WHERE requested_by_user_id = %s AND idempotency_key = %s{suffix}""",
            (requested_by_user_id, idempotency_key),
        )
        row = await cursor.fetchone()
        return _operation(row) if row else None

    async def get_step(
        self, connection: psycopg.AsyncConnection, step_id: int, *, for_update: bool = False
    ) -> StepRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"SELECT {STEP_COLUMNS} FROM khub_operation_steps WHERE id = %s{suffix}",
            (step_id,),
        )
        row = await cursor.fetchone()
        return _step(row) if row else None

    async def get_step_for_operation(
        self, connection: psycopg.AsyncConnection, operation_id: UUID, sequence: int
    ) -> StepRecord | None:
        cursor = await connection.execute(
            f"""SELECT {STEP_COLUMNS}
                FROM khub_operation_steps
                WHERE operation_id = %s AND sequence = %s""",
            (operation_id, sequence),
        )
        row = await cursor.fetchone()
        return _step(row) if row else None

    async def insert_operation(
        self, connection: psycopg.AsyncConnection, values: Mapping[str, Any]
    ) -> None:
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        await connection.execute(
            f"INSERT INTO khub_repository_operations ({columns}) VALUES ({placeholders})",
            tuple(values.values()),
        )

    async def insert_step(
        self, connection: psycopg.AsyncConnection, values: Mapping[str, Any]
    ) -> int:
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        cursor = await connection.execute(
            f"""INSERT INTO khub_operation_steps ({columns})
                VALUES ({placeholders}) RETURNING id""",
            tuple(Jsonb(value) if isinstance(value, (dict, list)) else value for value in values.values()),
        )
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])

    async def set_job_id(
        self, connection: psycopg.AsyncConnection, step_id: int, job_id: int
    ) -> None:
        await connection.execute(
            """UPDATE khub_operation_steps
               SET procrastinate_job_id = %s, updated_at = CURRENT_TIMESTAMP
               WHERE id = %s""",
            (job_id, step_id),
        )

    async def request_cancel(
        self, connection: psycopg.AsyncConnection, operation_id: UUID
    ) -> OperationRecord | None:
        cursor = await connection.execute(
            """UPDATE khub_repository_operations
               SET state = CASE
                   WHEN state IN ('accepted', 'running') THEN 'cancel_requested'
                   ELSE state END,
                   cancel_requested_at = CASE
                   WHEN state IN ('accepted', 'running') THEN CURRENT_TIMESTAMP
                   ELSE cancel_requested_at END,
                   updated_at = CURRENT_TIMESTAMP,
                   version = version + 1
               WHERE id = %s
               RETURNING """ + OPERATION_COLUMNS,
            (operation_id,),
        )
        row = await cursor.fetchone()
        return _operation(row) if row else None

    async def mark_running(
        self,
        connection: psycopg.AsyncConnection,
        operation_id: UUID,
        step_id: int,
    ) -> tuple[OperationRecord, StepRecord, bool] | None:
        operation = await self.get_operation(connection, operation_id, for_update=True)
        step = await self.get_step(connection, step_id, for_update=True)
        if operation is None or step is None or step.operation_id != operation_id:
            return None
        if step.state in {
            "dispatch_started",
            "observing",
            "succeeded",
            "failed",
            "cancelled",
            "uncertain",
        } or operation.state in {"dispatch_started", "uncertain"}:
            return operation, step, False
        if step.state == "running":
            # The first delivery owns the handler until it reaches a terminal
            # state. A redelivery must not run the side effect concurrently.
            return operation, step, False
        if operation.state in {"cancel_requested", "cancelled"}:
            await connection.execute(
                """UPDATE khub_operation_steps
                   SET state = 'cancelled', finished_at = CURRENT_TIMESTAMP,
                       updated_at = CURRENT_TIMESTAMP WHERE id = %s""",
                (step_id,),
            )
            await connection.execute(
                """UPDATE khub_repository_operations
                   SET state = 'cancelled', finished_at = CURRENT_TIMESTAMP,
                       updated_at = CURRENT_TIMESTAMP, version = version + 1
                   WHERE id = %s""",
                (operation_id,),
            )
            return (
                await self.get_operation(connection, operation_id),
                await self.get_step(connection, step_id),
                False,
            )
        await connection.execute(
            """UPDATE khub_operation_steps
               SET state = 'running', attempt = attempt + 1,
                   started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                   heartbeat_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
               WHERE id = %s""",
            (step_id,),
        )
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = 'running', started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                   heartbeat_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP,
                   version = version + 1 WHERE id = %s""",
            (operation_id,),
        )
        return (
            await self.get_operation(connection, operation_id),
            await self.get_step(connection, step_id),
            True,
        )

    async def finish_step(
        self,
        connection: psycopg.AsyncConnection,
        operation: OperationRecord,
        step: StepRecord,
        *,
        state: str,
        progress_current: int | None,
        progress_total: int | None,
        progress_message: str | None,
        result_json: dict[str, Any] | None,
        error_code: str | None,
        error_summary: str | None,
    ) -> None:
        await connection.execute(
            """UPDATE khub_operation_steps
               SET state = %s, finished_at = CURRENT_TIMESTAMP,
                   heartbeat_at = CURRENT_TIMESTAMP, error_code = %s,
                   error_summary = %s, updated_at = CURRENT_TIMESTAMP
               WHERE id = %s""",
            (state, error_code, error_summary, step.id),
        )
        terminal_operation_state = {
            "succeeded": "succeeded",
            "failed": "failed",
            "cancelled": "cancelled",
            "uncertain": "uncertain",
        }[state]
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = %s, phase = %s,
                   progress_current = COALESCE(%s, progress_current),
                   progress_total = COALESCE(%s, progress_total),
                   progress_message = %s, result_json = %s,
                   error_code = %s, error_summary = %s,
                   finished_at = CURRENT_TIMESTAMP, heartbeat_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s""",
            (
                terminal_operation_state,
                state,
                progress_current,
                progress_total,
                progress_message,
                Jsonb(result_json) if result_json is not None else None,
                error_code,
                error_summary,
                operation.id,
            ),
        )

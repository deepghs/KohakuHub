"""Small async PostgreSQL store for operation state."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from .types import CommitIntentRecord, OperationRecord, StepRecord


OPERATION_COLUMNS = """
id, kind, handler_version, repository_id, resource_key,
requested_by_user_id, trigger, idempotency_key, request_hash, state, phase,
progress_current, progress_total, progress_message, expected_head,
cancel_requested_at, started_at, heartbeat_at, finished_at,
dispatch_started_at, remote_deadline_at, observe_not_before,
observation_cursor, result_json, error_code, error_summary, created_at,
updated_at, version
"""

STEP_COLUMNS = """
id, operation_id, sequence, step_name, step_version, state, attempt,
delivery_key, procrastinate_job_id, input_json, checkpoint_json,
external_marker, expected_source, expected_target, artifact_key,
artifact_checksum, artifact_length, error_code, error_summary, started_at,
heartbeat_at, finished_at, created_at, updated_at
"""

COMMIT_INTENT_COLUMNS = """
id, operation_id, observation_operation_id, repository_id, requested_by_user_id, ref, base_head, marker,
idempotency_key, request_hash, prepared_deadline_at, observation_head, observation_cursor, payload_hash, payload_json, state,
dispatch_started_at, remote_deadline_at, observe_not_before, lakefs_commit_id,
result_json, error_code, error_summary, created_at,
updated_at, finalized_at, version
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


def _commit_intent(row: Any) -> CommitIntentRecord:
    return CommitIntentRecord(**dict(_mapping_row(row, COMMIT_INTENT_COLUMNS)))


class OperationStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def insert_commit_intent(
        self, connection: psycopg.AsyncConnection, values: Mapping[str, Any]
    ) -> CommitIntentRecord:
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        cursor = await connection.execute(
            f"""INSERT INTO khub_commit_intents ({columns})
                VALUES ({placeholders})
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            tuple(Jsonb(value) if isinstance(value, (dict, list)) else value for value in values.values()),
        )
        row = await cursor.fetchone()
        assert row is not None
        return _commit_intent(row)

    async def get_commit_intent(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        *,
        for_update: bool = False,
    ) -> CommitIntentRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"SELECT {COMMIT_INTENT_COLUMNS} FROM khub_commit_intents WHERE id = %s{suffix}",
            (intent_id,),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def get_commit_intent_by_marker(
        self,
        connection: psycopg.AsyncConnection,
        marker: str,
        *,
        for_update: bool = False,
    ) -> CommitIntentRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"SELECT {COMMIT_INTENT_COLUMNS} FROM khub_commit_intents WHERE marker = %s{suffix}",
            (marker,),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def get_commit_intent_by_request(
        self,
        connection: psycopg.AsyncConnection,
        *,
        requested_by_user_id: int,
        repository_id: int,
        ref: str,
        idempotency_key: str,
        for_update: bool = False,
    ) -> CommitIntentRecord | None:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"""SELECT {COMMIT_INTENT_COLUMNS}
                FROM khub_commit_intents
                WHERE requested_by_user_id = %s
                  AND repository_id = %s
                  AND ref = %s
                  AND idempotency_key = %s{suffix}""",
            (requested_by_user_id, repository_id, ref, idempotency_key),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def get_blocking_commit_intent(
        self,
        connection: psycopg.AsyncConnection,
        *,
        repository_id: int,
        ref: str,
        exclude_intent_id: UUID | None = None,
    ) -> CommitIntentRecord | None:
        exclusion = "" if exclude_intent_id is None else " AND id <> %s"
        params: tuple[Any, ...] = (repository_id, ref)
        if exclude_intent_id is not None:
            params += (exclude_intent_id,)
        cursor = await connection.execute(
            f"""SELECT {COMMIT_INTENT_COLUMNS}
                FROM khub_commit_intents
                WHERE repository_id = %s AND ref = %s
                  AND state IN (
                      'prepared', 'dispatch_started', 'uncertain',
                      'committed', 'reconciliation_required'
                  ){exclusion}
                ORDER BY CASE state
                    WHEN 'committed' THEN 0
                    WHEN 'reconciliation_required' THEN 1
                    WHEN 'dispatch_started' THEN 2
                    WHEN 'uncertain' THEN 3
                    ELSE 4 END,
                    updated_at
                LIMIT 1""",
            params,
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def get_blocking_repository_operation(
        self,
        connection: psycopg.AsyncConnection,
        *,
        repository_id: int,
        exclude_operation_id: UUID | None = None,
    ) -> OperationRecord | None:
        """Return an active repository mutation that blocks new writers.

        Dangerous operations do not create commit-intent rows, so their
        durable uncertainty must participate in the same writer barrier after
        the worker process or its advisory session disappears.  The registry
        currently namespaces these operations under ``repository.``; using
        that server-owned prefix is intentionally conservative for the fence.
        """

        exclusion = "" if exclude_operation_id is None else " AND id <> %s"
        params: tuple[Any, ...] = (repository_id,)
        if exclude_operation_id is not None:
            params += (exclude_operation_id,)
        cursor = await connection.execute(
            f"""SELECT {OPERATION_COLUMNS}
                FROM khub_repository_operations
                WHERE repository_id = %s
                  AND kind LIKE 'repository.%%'
                  AND state IN (
                      'accepted', 'running', 'cancel_requested',
                      'dispatch_started', 'uncertain', 'cleanup_pending'
                  ){exclusion}
                ORDER BY created_at
                LIMIT 1""",
            params,
        )
        row = await cursor.fetchone()
        return _operation(row) if row else None

    async def get_lakefs_repository(
        self,
        connection: psycopg.AsyncConnection,
        repository_id: int,
    ) -> str | None:
        """Return the LakeFS backing owned by one application repository.

        ``lakefs_repo`` is persisted by migration 016.  The derivation
        fallback keeps old rows readable while the migration runner is
        completing its backfill; the caller still fails closed when no
        repository row exists at all.
        """

        cursor = await connection.execute(
            """SELECT lakefs_repo, repo_type, full_id
               FROM repository
               WHERE id = %s""",
            (repository_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        if row[0]:
            return str(row[0])
        if not row[1] or not row[2]:
            return None
        from kohakuhub.utils.lakefs import lakefs_repo_name

        return lakefs_repo_name(str(row[1]), str(row[2]))

    async def get_blocking_repository_intent(
        self,
        connection: psycopg.AsyncConnection,
        *,
        repository_id: int,
        exclude_intent_id: UUID | None = None,
    ) -> CommitIntentRecord | None:
        """Find any unresolved mutation that blocks a repository cutover."""

        exclusion = "" if exclude_intent_id is None else " AND id <> %s"
        params: tuple[Any, ...] = (repository_id,)
        if exclude_intent_id is not None:
            params += (exclude_intent_id,)
        cursor = await connection.execute(
            f"""SELECT {COMMIT_INTENT_COLUMNS}
                FROM khub_commit_intents
                WHERE repository_id = %s
                  AND state IN (
                      'prepared', 'dispatch_started', 'uncertain',
                      'committed', 'reconciliation_required'
                  ){exclusion}
                ORDER BY CASE state
                    WHEN 'committed' THEN 0
                    WHEN 'reconciliation_required' THEN 1
                    WHEN 'dispatch_started' THEN 2
                    WHEN 'uncertain' THEN 3
                    ELSE 4 END,
                    updated_at
                LIMIT 1""",
            params,
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def update_commit_intent(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        *,
        state: str,
        operation_id: UUID | None = None,
        lakefs_commit_id: str | None = None,
        result_json: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
        finalize: bool = False,
        from_states: tuple[str, ...] = (
            "prepared",
            "committed",
            "reconciliation_required",
        ),
        expected_version: int | None = None,
    ) -> CommitIntentRecord | None:
        state_placeholders = ", ".join(["%s"] * len(from_states))
        version_clause = "" if expected_version is None else " AND version = %s"
        cursor = await connection.execute(
            f"""UPDATE khub_commit_intents
                SET state = %s,
                    operation_id = COALESCE(%s, operation_id),
                    lakefs_commit_id = COALESCE(%s, lakefs_commit_id),
                    result_json = COALESCE(%s, result_json),
                    error_code = %s,
                    error_summary = %s,
                    finalized_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finalized_at END,
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s
                  AND state IN ({state_placeholders})
                  AND (%s::text IS NULL OR lakefs_commit_id IS NULL OR %s::text = lakefs_commit_id)
                  {version_clause}
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            (
                state,
                operation_id,
                lakefs_commit_id,
                Jsonb(result_json) if result_json is not None else None,
                error_code,
                error_summary,
                finalize,
                intent_id,
                *from_states,
                lakefs_commit_id,
                lakefs_commit_id,
                *(() if expected_version is None else (expected_version,)),
            ),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def update_prepared_commit_payload(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        *,
        payload: Mapping[str, Any],
        payload_hash: str,
        expected_version: int,
    ) -> CommitIntentRecord | None:
        cursor = await connection.execute(
            f"""UPDATE khub_commit_intents
                SET payload_json = %s,
                    payload_hash = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s AND state = 'prepared' AND version = %s
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            (Jsonb(dict(payload)), payload_hash, intent_id, expected_version),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def bind_observation_operation(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        operation_id: UUID,
    ) -> CommitIntentRecord | None:
        cursor = await connection.execute(
            f"""UPDATE khub_commit_intents
                SET observation_operation_id = COALESCE(observation_operation_id, %s),
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s
                  AND observation_operation_id IS NULL
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            (operation_id, intent_id),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def refresh_prepared_deadline(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        deadline: Any,
    ) -> CommitIntentRecord | None:
        cursor = await connection.execute(
            f"""UPDATE khub_commit_intents
                SET prepared_deadline_at = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s AND state = 'prepared'
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            (deadline, intent_id),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def update_observation_cursor(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        cursor_value: str | None,
        *,
        expected_version: int | None = None,
    ) -> bool:
        version_clause = "" if expected_version is None else " AND version = %s"
        params: tuple[Any, ...] = (cursor_value, intent_id)
        if expected_version is not None:
            params += (expected_version,)
        cursor = await connection.execute(
            """UPDATE khub_commit_intents
               SET observation_cursor = %s,
                   updated_at = CURRENT_TIMESTAMP,
                   version = version + 1
               WHERE id = %s
                 AND state IN ('dispatch_started', 'committed',
                               'reconciliation_required', 'uncertain')"""
            + version_clause,
            params,
        )
        return cursor.rowcount == 1

    async def set_observation_head(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        observation_head: str,
        *,
        expected_version: int,
    ) -> CommitIntentRecord | None:
        """Persist the first observed branch head exactly once."""

        cursor = await connection.execute(
            f"""UPDATE khub_commit_intents
                SET observation_head = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s
                  AND state IN ('dispatch_started', 'committed',
                                'reconciliation_required', 'uncertain')
                  AND observation_head IS NULL
                  AND version = %s
                RETURNING {COMMIT_INTENT_COLUMNS}""",
            (observation_head, intent_id, expected_version),
        )
        row = await cursor.fetchone()
        return _commit_intent(row) if row else None

    async def finish_observation_operation(
        self,
        connection: psycopg.AsyncConnection,
        operation_id: UUID,
        *,
        state: str,
        result_json: dict[str, Any] | None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> OperationRecord | None:
        if state not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("observation operation must finish in a terminal state")
        from psycopg.types.json import Jsonb

        await connection.execute(
            """UPDATE khub_operation_steps
               SET state = %s,
                   finished_at = CURRENT_TIMESTAMP,
                   error_code = %s,
                   error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE operation_id = %s
                 AND state IN ('pending', 'running', 'observing', 'uncertain')""",
            (state, error_code, error_summary, operation_id),
        )
        cursor = await connection.execute(
            f"""UPDATE khub_repository_operations
                SET state = %s,
                    phase = %s,
                    result_json = %s,
                    error_code = %s,
                    error_summary = %s,
                    finished_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP,
                    version = version + 1
                WHERE id = %s
                  AND state IN ('accepted', 'running', 'uncertain')
                RETURNING {OPERATION_COLUMNS}""",
            (
                state,
                "completed" if state == "succeeded" else state,
                Jsonb(result_json) if result_json is not None else None,
                error_code,
                error_summary,
                operation_id,
            ),
        )
        row = await cursor.fetchone()
        return _operation(row) if row else await self.get_operation(connection, operation_id)

    async def list_quota_reservations(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        *,
        for_update: bool = False,
    ) -> list[dict[str, Any]]:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            """SELECT id, intent_id, scope_type, scope_id, is_private,
                      reserved_bytes, state, created_at, updated_at, finished_at
               FROM khub_quota_reservations
               WHERE intent_id = %s
               ORDER BY scope_type""" + suffix,
            (intent_id,),
        )
        columns = (
            "id", "intent_id", "scope_type", "scope_id", "is_private",
            "reserved_bytes", "state", "created_at", "updated_at", "finished_at",
        )
        return [dict(zip(columns, row, strict=True)) for row in await cursor.fetchall()]

    async def insert_quota_reservation(
        self,
        connection: psycopg.AsyncConnection,
        values: Mapping[str, Any],
    ) -> None:
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        await connection.execute(
            f"""INSERT INTO khub_quota_reservations ({columns})
                VALUES ({placeholders})
                ON CONFLICT (intent_id, scope_type) DO NOTHING""",
            tuple(values.values()),
        )

    async def transition_quota_reservations(
        self,
        connection: psycopg.AsyncConnection,
        intent_id: UUID,
        *,
        from_state: str,
        to_state: str,
    ) -> None:
        await connection.execute(
            """UPDATE khub_quota_reservations
               SET state = %s,
                   finished_at = CASE WHEN %s IN ('consumed', 'released')
                                      THEN CURRENT_TIMESTAMP ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP
               WHERE intent_id = %s AND state = %s""",
            (to_state, to_state, intent_id, from_state),
        )

    async def list_recoverable_commit_intents(
        self,
        connection: psycopg.AsyncConnection,
        *,
        limit: int = 100,
        for_update: bool = False,
    ) -> list[CommitIntentRecord]:
        suffix = " FOR UPDATE SKIP LOCKED" if for_update else ""
        cursor = await connection.execute(
            f"""SELECT {COMMIT_INTENT_COLUMNS}
                FROM khub_commit_intents
                WHERE state IN (
                    'dispatch_started', 'committed',
                    'reconciliation_required', 'uncertain'
                )
                ORDER BY CASE state
                    WHEN 'committed' THEN 0
                    WHEN 'reconciliation_required' THEN 1
                    WHEN 'dispatch_started' THEN 2
                    ELSE 3 END,
                    updated_at
                LIMIT %s{suffix}""",
            (limit,),
        )
        return [_commit_intent(row) for row in await cursor.fetchall()]

    async def list_stale_prepared_commit_intents(
        self,
        connection: psycopg.AsyncConnection,
        *,
        cutoff: Any,
        limit: int = 100,
    ) -> list[CommitIntentRecord]:
        cursor = await connection.execute(
            f"""SELECT {COMMIT_INTENT_COLUMNS}
                FROM khub_commit_intents
                WHERE state = 'prepared'
                  AND COALESCE(
                        prepared_deadline_at,
                        created_at + INTERVAL '5 minutes'
                      ) <= %s
                ORDER BY COALESCE(prepared_deadline_at, created_at), updated_at
                LIMIT %s""",
            (cutoff, limit),
        )
        return [_commit_intent(row) for row in await cursor.fetchall()]

    async def list_steps_missing_jobs(
        self,
        connection: psycopg.AsyncConnection,
        *,
        limit: int = 100,
        for_update: bool = False,
    ) -> list[dict[str, Any]]:
        suffix = " FOR UPDATE OF s SKIP LOCKED" if for_update else ""
        cursor = await connection.execute(
            f"""
            SELECT o.id, o.kind, o.resource_key, o.repository_id,
                   o.requested_by_user_id, o.idempotency_key, o.expected_head,
                   s.id, s.sequence, s.step_name, s.step_version,
                   s.input_json, s.delivery_key
            FROM khub_repository_operations o
            JOIN khub_operation_steps s ON s.operation_id = o.id
            WHERE o.state IN ('accepted', 'running')
              AND s.state = 'pending'
              AND (
                    s.procrastinate_job_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM procrastinate_jobs j
                        WHERE j.id = s.procrastinate_job_id
                          AND j.status NOT IN ('failed', 'aborted', 'cancelled')
                    )
              )
            ORDER BY s.updated_at
            LIMIT %s{suffix}
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "operation_id": row[0],
                "kind": row[1],
                "resource_key": row[2],
                "repository_id": row[3],
                "requested_by_user_id": row[4],
                "idempotency_key": row[5],
                "expected_head": row[6],
                "step_id": row[7],
                "sequence": row[8],
                "step_name": row[9],
                "step_version": row[10],
                "input_json": row[11],
                "delivery_key": row[12],
            }
            for row in rows
        ]

    async def finalize_cancelled_pending_steps(
        self, connection: psycopg.AsyncConnection, *, limit: int = 100
    ) -> int:
        """Finalize queued cancellations after Procrastinate stops delivery."""

        cursor = await connection.execute(
            """SELECT o.id, s.id
               FROM khub_repository_operations o
               JOIN khub_operation_steps s ON s.operation_id = o.id
               WHERE o.state = 'cancel_requested'
                 AND s.state = 'pending'
                 AND (
                       s.procrastinate_job_id IS NULL
                       OR EXISTS (
                           SELECT 1
                           FROM procrastinate_jobs j
                           WHERE j.id = s.procrastinate_job_id
                             AND j.status IN ('cancelled', 'aborted')
                       )
                 )
               ORDER BY s.updated_at
               LIMIT %s
               FOR UPDATE OF s SKIP LOCKED""",
            (limit,),
        )
        rows = await cursor.fetchall()
        for operation_id, step_id in rows:
            await connection.execute(
                """UPDATE khub_operation_steps
                   SET state = 'cancelled', finished_at = CURRENT_TIMESTAMP,
                       error_code = 'cancelled',
                       error_summary = 'operation cancelled before delivery',
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = %s AND state = 'pending'""",
                (step_id,),
            )
            await connection.execute(
                """UPDATE khub_repository_operations
                   SET state = 'cancelled', phase = 'cancelled',
                       finished_at = CURRENT_TIMESTAMP,
                       error_code = 'cancelled',
                       error_summary = 'operation cancelled before delivery',
                       updated_at = CURRENT_TIMESTAMP, version = version + 1
                   WHERE id = %s AND state = 'cancel_requested'""",
                (operation_id,),
            )
        return len(rows)

    async def list_cancel_requested_deliveries(
        self, connection: psycopg.AsyncConnection, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return active deliveries whose durable operation cancellation is pending."""

        if limit < 1:
            raise ValueError("cancellation retry limit must be positive")
        cursor = await connection.execute(
            """SELECT o.id, s.id, s.procrastinate_job_id
               FROM khub_repository_operations o
               JOIN khub_operation_steps s ON s.operation_id = o.id
               WHERE o.state = 'cancel_requested'
                 AND s.state IN ('pending', 'running')
                 AND s.procrastinate_job_id IS NOT NULL
               ORDER BY s.updated_at
               LIMIT %s""",
            (limit,),
        )
        return [
            {
                "operation_id": row[0],
                "step_id": int(row[1]),
                "job_id": int(row[2]),
            }
            for row in await cursor.fetchall()
        ]

    async def mark_step_observing(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        error_code: str,
        error_summary: str,
    ) -> None:
        await connection.execute(
            """UPDATE khub_operation_steps
               SET state = 'observing', error_code = %s, error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s AND state IN ('running', 'dispatch_started')""",
            (error_code, error_summary, step_id, operation_id),
        )
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = 'uncertain', phase = 'observing',
                   error_code = %s, error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s AND state IN ('running', 'dispatch_started')""",
            (error_code, error_summary, operation_id),
        )

    async def update_observing_checkpoint(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        checkpoint: Mapping[str, Any],
        error_code: str,
        error_summary: str,
    ) -> bool:
        """Persist one bounded observer page and keep the operation uncertain."""

        step_cursor = await connection.execute(
            """UPDATE khub_operation_steps
               SET state = 'observing', checkpoint_json = %s,
                   heartbeat_at = CURRENT_TIMESTAMP,
                   error_code = %s, error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s
                 AND state IN ('running', 'dispatch_started', 'observing', 'uncertain')
               RETURNING id""",
            (Jsonb(dict(checkpoint)), error_code, error_summary, step_id, operation_id),
        )
        if await step_cursor.fetchone() is None:
            return False
        operation_cursor = await connection.execute(
            """UPDATE khub_repository_operations
               SET state = 'uncertain', phase = 'observing',
                   heartbeat_at = CURRENT_TIMESTAMP,
                   error_code = %s, error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s
                 AND state IN ('running', 'dispatch_started', 'uncertain')
               RETURNING id""",
            (error_code, error_summary, operation_id),
        )
        return await operation_cursor.fetchone() is not None

    async def requeue_stalled_step(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        max_attempts: int,
    ) -> bool:
        """Return a pre-dispatch stale step to durable delivery."""

        step_cursor = await connection.execute(
            """UPDATE khub_operation_steps
               SET state = CASE WHEN attempt >= %s THEN 'failed' ELSE 'pending' END,
                   procrastinate_job_id = NULL,
                   error_code = CASE WHEN attempt >= %s THEN 'retry_exhausted'
                                     ELSE 'stalled_delivery' END,
                   error_summary = CASE WHEN attempt >= %s
                                        THEN 'stalled operation exceeded its retry budget'
                                        ELSE 'worker delivery heartbeat expired; retrying' END,
                   finished_at = CASE WHEN attempt >= %s THEN CURRENT_TIMESTAMP
                                      ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s AND state = 'running'
               RETURNING state""",
            (max_attempts, max_attempts, max_attempts, max_attempts, step_id, operation_id),
        )
        row = await step_cursor.fetchone()
        if row is None:
            return False
        exhausted = row[0] == "failed"
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = CASE WHEN %s THEN 'failed' ELSE 'running' END,
                   phase = CASE WHEN %s THEN 'failed' ELSE 'queued' END,
                   error_code = CASE WHEN %s THEN 'retry_exhausted'
                                     ELSE 'stalled_delivery' END,
                   error_summary = CASE WHEN %s
                                        THEN 'stalled operation exceeded its retry budget'
                                        ELSE 'worker delivery heartbeat expired; retrying' END,
                   finished_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s AND state = 'running'""",
            (exhausted, exhausted, exhausted, exhausted, exhausted, operation_id),
        )
        return not exhausted

    async def requeue_stalled_external_step(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        max_attempts: int,
    ) -> bool:
        """Requeue an explicitly replay-safe external quantum.

        This is deliberately separate from ``requeue_stalled_step``.  A
        marker proves that the handler crossed an external boundary; only a
        registry entry with an explicit replay-safe contract may use this
        path.  Other marked steps remain in observation.
        """

        step_cursor = await connection.execute(
            """UPDATE khub_operation_steps
               SET state = CASE WHEN attempt >= %s THEN 'failed' ELSE 'pending' END,
                   procrastinate_job_id = NULL,
                   error_code = CASE WHEN attempt >= %s THEN 'retry_exhausted'
                                     ELSE 'stalled_replay_safe_delivery' END,
                   error_summary = CASE WHEN attempt >= %s
                                        THEN 'stalled operation exceeded its retry budget'
                                        ELSE 'replaying an idempotent external quantum' END,
                   finished_at = CASE WHEN attempt >= %s THEN CURRENT_TIMESTAMP
                                      ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s
                 AND state IN ('running', 'dispatch_started', 'observing')
               RETURNING state""",
            (max_attempts, max_attempts, max_attempts, max_attempts, step_id, operation_id),
        )
        row = await step_cursor.fetchone()
        if row is None:
            return False
        exhausted = row[0] == "failed"
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = CASE WHEN %s THEN 'failed' ELSE 'running' END,
                   phase = CASE WHEN %s THEN 'failed' ELSE 'queued' END,
                   error_code = CASE WHEN %s THEN 'retry_exhausted'
                                     ELSE 'stalled_replay_safe_delivery' END,
                   error_summary = CASE WHEN %s
                                        THEN 'stalled operation exceeded its retry budget'
                                        ELSE 'replaying an idempotent external quantum' END,
                   finished_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s
                 AND state IN ('running', 'dispatch_started', 'uncertain')""",
            (exhausted, exhausted, exhausted, exhausted, exhausted, operation_id),
        )
        return not exhausted

    async def requeue_retryable_step(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        error_code: str,
        error_summary: str,
    ) -> None:
        """Return a pre-dispatch handler failure to Procrastinate delivery."""

        await connection.execute(
            """UPDATE khub_operation_steps
               SET state = 'pending', error_code = %s, error_summary = %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s AND state = 'running'""",
            (error_code, error_summary, step_id, operation_id),
        )
        await connection.execute(
            """UPDATE khub_repository_operations
               SET state = 'running', phase = 'queued', error_code = %s,
                   error_summary = %s, updated_at = CURRENT_TIMESTAMP,
                   version = version + 1
               WHERE id = %s AND state = 'running'""",
            (error_code, error_summary, operation_id),
        )

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

    async def requeue_replay_safe_step(
        self,
        connection: psycopg.AsyncConnection,
        step_id: int,
        operation_id: UUID,
        *,
        error_code: str,
        error_summary: str,
        max_attempts: int,
    ) -> bool:
        """Return a marked replay-safe step to durable delivery.

        This transition is separate from ordinary pre-dispatch retry.  A
        replay-safe handler may have crossed its external boundary, so the
        operation and step can be in ``dispatch_started`` when the transient
        exception is raised.
        """

        step_cursor = await connection.execute(
            """UPDATE khub_operation_steps
               SET state = CASE WHEN attempt >= %s THEN 'failed' ELSE 'pending' END,
                   error_code = CASE WHEN attempt >= %s THEN 'retry_exhausted' ELSE %s END,
                   error_summary = CASE WHEN attempt >= %s
                                        THEN 'replay-safe operation exceeded its retry budget'
                                        ELSE %s END,
                   finished_at = CASE WHEN attempt >= %s THEN CURRENT_TIMESTAMP
                                      ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s
                 AND state IN ('running', 'dispatch_started')
               RETURNING state""",
            (
                max_attempts,
                max_attempts,
                error_code,
                max_attempts,
                error_summary,
                max_attempts,
                step_id,
                operation_id,
            ),
        )
        row = await step_cursor.fetchone()
        if row is None:
            return False
        exhausted = row[0] == "failed"
        operation_cursor = await connection.execute(
            """UPDATE khub_repository_operations
               SET state = CASE WHEN %s THEN 'failed' ELSE 'running' END,
                   phase = CASE WHEN %s THEN 'failed' ELSE 'queued' END,
                   error_code = CASE WHEN %s THEN 'retry_exhausted' ELSE %s END,
                   error_summary = CASE WHEN %s
                                        THEN 'replay-safe operation exceeded its retry budget'
                                        ELSE %s END,
                   finished_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finished_at END,
                   updated_at = CURRENT_TIMESTAMP, version = version + 1
               WHERE id = %s AND state IN ('running', 'dispatch_started')
               RETURNING id""",
            (
                exhausted,
                exhausted,
                exhausted,
                error_code,
                exhausted,
                error_summary,
                exhausted,
                operation_id,
            ),
        )
        return not exhausted and await operation_cursor.fetchone() is not None

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

    async def get_active_step_for_operation(
        self, connection: psycopg.AsyncConnection, operation_id: UUID
    ) -> StepRecord | None:
        """Return the current non-terminal step for cancellation/recovery."""

        cursor = await connection.execute(
            f"""SELECT {STEP_COLUMNS}
                FROM khub_operation_steps
                WHERE operation_id = %s
                  AND state IN ('pending', 'running', 'dispatch_started', 'observing')
                ORDER BY sequence
                LIMIT 1""",
            (operation_id,),
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

    async def prune_terminal_history(
        self,
        connection: psycopg.AsyncConnection,
        *,
        cutoff: Any,
        limit: int,
    ) -> tuple[int, int]:
        """Prune old, fully terminal operations and delivery history.

        The operation row is locked before the candidate is returned. Worker
        execution also locks that row before changing a step, so a candidate
        cannot race a new delivery. Unresolved commit intents and non-terminal
        jobs deliberately keep the operation alive for recovery.
        """

        if limit < 1:
            raise ValueError("retention limit must be positive")

        cursor = await connection.execute(
            """
            SELECT o.id
            FROM khub_repository_operations o
            WHERE o.state IN ('succeeded', 'failed', 'cancelled')
              AND o.finished_at IS NOT NULL
              AND o.finished_at < %s
              AND EXISTS (
                  SELECT 1
                  FROM khub_operation_steps s
                  WHERE s.operation_id = o.id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM khub_operation_steps s
                  WHERE s.operation_id = o.id
                    AND s.state NOT IN ('succeeded', 'failed', 'cancelled')
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM khub_operation_steps s
                  LEFT JOIN procrastinate_jobs j
                    ON j.id = s.procrastinate_job_id
                  WHERE s.operation_id = o.id
                    AND s.procrastinate_job_id IS NOT NULL
                    AND j.id IS NOT NULL
                    AND j.status NOT IN (
                        'succeeded', 'failed', 'cancelled', 'aborted'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM khub_commit_intents i
                  WHERE (i.operation_id = o.id
                         OR i.observation_operation_id = o.id)
                    AND i.state NOT IN ('finalized', 'abandoned')
              )
            ORDER BY o.finished_at, o.id
            LIMIT %s
            FOR UPDATE OF o SKIP LOCKED
            """,
            (cutoff, limit),
        )
        operation_ids = [row[0] for row in await cursor.fetchall()]
        if not operation_ids:
            return 0, 0

        job_cursor = await connection.execute(
            """
            SELECT DISTINCT s.procrastinate_job_id
            FROM khub_operation_steps s
            WHERE s.operation_id = ANY(%s)
              AND s.procrastinate_job_id IS NOT NULL
            """,
            (operation_ids,),
        )
        job_ids = [int(row[0]) for row in await job_cursor.fetchall()]

        deleted_jobs = 0
        if job_ids:
            # Periodic defers use a non-cascading FK to jobs. Remove old
            # metadata before deleting the corresponding terminal job.
            await connection.execute(
                "DELETE FROM procrastinate_periodic_defers WHERE job_id = ANY(%s)",
                (job_ids,),
            )
            job_delete = await connection.execute(
                """
                DELETE FROM procrastinate_jobs
                WHERE id = ANY(%s)
                  AND status IN ('succeeded', 'failed', 'cancelled', 'aborted')
                RETURNING id
                """,
                (job_ids,),
            )
            deleted_jobs = len(await job_delete.fetchall())

        await connection.execute(
            """
            DELETE FROM khub_commit_intents
            WHERE (operation_id = ANY(%s) OR observation_operation_id = ANY(%s))
              AND state IN ('finalized', 'abandoned')
            """,
            (operation_ids, operation_ids),
        )
        operation_delete = await connection.execute(
            """
            DELETE FROM khub_repository_operations
            WHERE id = ANY(%s)
            RETURNING id
            """,
            (operation_ids,),
        )
        deleted_operations = len(await operation_delete.fetchall())
        return deleted_operations, deleted_jobs

    async def prune_unlinked_terminal_jobs(
        self,
        connection: psycopg.AsyncConnection,
        *,
        cutoff: Any,
        limit: int,
    ) -> int:
        """Delete a bounded batch of terminal jobs with no KHub step owner.

        Procrastinate's convenience cleanup is intentionally not used here:
        it deletes all matching jobs in one statement and cannot distinguish
        KHub-owned delivery evidence from periodic or other standalone jobs.
        """

        if limit < 1:
            raise ValueError("retention limit must be positive")
        cursor = await connection.execute(
            """
            SELECT j.id
            FROM procrastinate_jobs j
            WHERE j.status IN ('succeeded', 'failed', 'cancelled', 'aborted')
              AND NOT EXISTS (
                  SELECT 1
                  FROM khub_operation_steps s
                  WHERE s.procrastinate_job_id = j.id
              )
              AND (
                  SELECT MAX(e.at)
                  FROM procrastinate_events e
                  WHERE e.job_id = j.id
              ) < %s
            ORDER BY j.id
            LIMIT %s
            FOR UPDATE OF j SKIP LOCKED
            """,
            (cutoff, limit),
        )
        job_ids = [int(row[0]) for row in await cursor.fetchall()]
        if not job_ids:
            return 0

        await connection.execute(
            "DELETE FROM procrastinate_periodic_defers WHERE job_id = ANY(%s)",
            (job_ids,),
        )
        deleted = await connection.execute(
            """
            DELETE FROM procrastinate_jobs
            WHERE id = ANY(%s)
              AND status IN ('succeeded', 'failed', 'cancelled', 'aborted')
            RETURNING id
            """,
            (job_ids,),
        )
        return len(await deleted.fetchall())

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

    async def touch_heartbeat(
        self,
        connection: psycopg.AsyncConnection,
        operation_id: UUID,
        step_id: int,
    ) -> None:
        """Refresh KHub liveness while a registered handler is executing."""

        await connection.execute(
            """UPDATE khub_operation_steps
               SET heartbeat_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s
                 AND state IN ('running', 'dispatch_started')""",
            (step_id, operation_id),
        )
        await connection.execute(
            """UPDATE khub_repository_operations
               SET heartbeat_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND state IN ('running', 'dispatch_started')""",
            (operation_id,),
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
    ) -> bool:
        step_cursor = await connection.execute(
            """UPDATE khub_operation_steps
               SET state = %s, finished_at = CURRENT_TIMESTAMP,
                   heartbeat_at = CURRENT_TIMESTAMP, error_code = %s,
                   error_summary = %s, updated_at = CURRENT_TIMESTAMP
               WHERE id = %s AND operation_id = %s
                 AND state IN ('running', 'dispatch_started', 'observing', 'uncertain')
               RETURNING id""",
            (state, error_code, error_summary, step.id, operation.id),
        )
        if await step_cursor.fetchone() is None:
            return False
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
               WHERE id = %s
                 AND state IN ('accepted', 'running', 'cancel_requested',
                               'dispatch_started', 'uncertain')""",
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
        return True

"""Conservative PostgreSQL schema signatures for migration adoption."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any

from kohakuhub.db import KHUB_MODELS
from kohakuhub.operations.sql import (
    operation_column_contract,
    operation_table_columns,
)


REQUIRED_OPERATION_INDEXES = {
    "khub_operation_idempotency_uidx",
    "khub_operation_active_resource_uidx",
    "khub_operation_owner_idx",
    "khub_operation_repository_idx",
    "khub_operation_steps_operation_idx",
    "khub_operation_steps_state_idx",
    "khub_commit_intent_marker_uidx",
    "khub_commit_intent_operation_uidx",
    "khub_commit_intent_observation_operation_uidx",
    "khub_commit_intent_request_uidx",
    "khub_commit_intent_recovery_idx",
    "khub_commit_intent_repo_ref_idx",
    "khub_quota_reservation_active_idx",
    "khub_quota_reservation_intent_idx",
}
REQUIRED_OPERATION_CONSTRAINTS = {
    "khub_repository_operations_pkey",
    "khub_operation_state_ck",
    "khub_operation_trigger_ck",
    "khub_operation_progress_ck",
    "khub_step_state_ck",
    "khub_step_attempt_ck",
    "khub_step_sequence_uidx",
    "khub_step_delivery_uidx",
    "khub_operation_steps_pkey",
    "khub_operation_steps_operation_id_fkey",
    "khub_commit_intent_marker_ck",
    "khub_commit_intent_payload_hash_ck",
    "khub_commit_intent_state_ck",
    "khub_quota_reservation_scope_ck",
    "khub_quota_reservation_bytes_ck",
    "khub_quota_reservation_state_ck",
    "khub_quota_reservation_unique_scope_uidx",
    "khub_commit_intents_pkey",
    "khub_quota_reservations_pkey",
    "khub_quota_reservations_intent_id_fkey",
}

# The column-only signature is useful for legacy adoption diagnostics, but it
# cannot detect a changed default or a widened nullable column.  Keep the
# durable-kernel contract explicit so API and worker startup reject a schema
# that can silently weaken recovery guarantees.
KERNEL_COLUMN_CONTRACT = operation_column_contract()

# ``pg_get_*`` normalizes SQL expressions, but whitespace and identifier case
# are not part of the durable contract.  These definitions intentionally cover
# partial predicates and FK actions, not just object names.
EXPECTED_OPERATION_INDEX_DEFINITIONS = {
    "khub_operation_idempotency_uidx": "CREATE UNIQUE INDEX khub_operation_idempotency_uidx ON public.khub_repository_operations USING btree (requested_by_user_id, idempotency_key) WHERE (idempotency_key IS NOT NULL)",
    "khub_operation_active_resource_uidx": "CREATE UNIQUE INDEX khub_operation_active_resource_uidx ON public.khub_repository_operations USING btree (resource_key) WHERE (state = ANY (ARRAY['accepted'::text, 'running'::text, 'cancel_requested'::text, 'dispatch_started'::text, 'uncertain'::text]))",
    "khub_operation_owner_idx": "CREATE INDEX khub_operation_owner_idx ON public.khub_repository_operations USING btree (requested_by_user_id, created_at DESC)",
    "khub_operation_repository_idx": "CREATE INDEX khub_operation_repository_idx ON public.khub_repository_operations USING btree (repository_id, created_at DESC)",
    "khub_operation_steps_operation_idx": "CREATE INDEX khub_operation_steps_operation_idx ON public.khub_operation_steps USING btree (operation_id, sequence)",
    "khub_operation_steps_state_idx": "CREATE INDEX khub_operation_steps_state_idx ON public.khub_operation_steps USING btree (state, updated_at)",
    "khub_commit_intent_marker_uidx": "CREATE UNIQUE INDEX khub_commit_intent_marker_uidx ON public.khub_commit_intents USING btree (marker)",
    "khub_commit_intent_operation_uidx": "CREATE UNIQUE INDEX khub_commit_intent_operation_uidx ON public.khub_commit_intents USING btree (operation_id) WHERE (operation_id IS NOT NULL)",
    "khub_commit_intent_observation_operation_uidx": "CREATE UNIQUE INDEX khub_commit_intent_observation_operation_uidx ON public.khub_commit_intents USING btree (observation_operation_id) WHERE (observation_operation_id IS NOT NULL)",
    "khub_commit_intent_request_uidx": "CREATE UNIQUE INDEX khub_commit_intent_request_uidx ON public.khub_commit_intents USING btree (requested_by_user_id, repository_id, ref, idempotency_key) WHERE (idempotency_key IS NOT NULL)",
    "khub_commit_intent_recovery_idx": "CREATE INDEX khub_commit_intent_recovery_idx ON public.khub_commit_intents USING btree (state, updated_at)",
    "khub_commit_intent_repo_ref_idx": "CREATE INDEX khub_commit_intent_repo_ref_idx ON public.khub_commit_intents USING btree (repository_id, ref, created_at DESC)",
    "khub_quota_reservation_active_idx": "CREATE INDEX khub_quota_reservation_active_idx ON public.khub_quota_reservations USING btree (scope_type, scope_id, is_private, state)",
    "khub_quota_reservation_intent_idx": "CREATE INDEX khub_quota_reservation_intent_idx ON public.kohakuhub_quota_reservations USING btree (intent_id, state)",
}

# Correct the table name in the definition above while keeping the mapping
# readable in reviews.
EXPECTED_OPERATION_INDEX_DEFINITIONS["khub_quota_reservation_intent_idx"] = (
    "CREATE INDEX khub_quota_reservation_intent_idx ON public.khub_quota_reservations USING btree (intent_id, state)"
)

EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS = {
    "khub_repository_operations_pkey": ("p", "PRIMARY KEY (id)"),
    "khub_operation_state_ck": (
        "c",
        "CHECK (state = ANY (ARRAY['accepted'::text, 'running'::text, 'cancel_requested'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'dispatch_started'::text, 'uncertain'::text, 'cleanup_pending'::text]))",
    ),
    "khub_operation_trigger_ck": (
        "c",
        "CHECK (trigger = ANY (ARRAY['api'::text, 'schedule'::text, 'reconcile'::text, 'system'::text]))",
    ),
    "khub_operation_progress_ck": (
        "c",
        "CHECK (progress_current >= 0 AND (progress_total IS NULL OR progress_total >= progress_current))",
    ),
    "khub_operation_steps_pkey": ("p", "PRIMARY KEY (id)"),
    "khub_operation_steps_operation_id_fkey": (
        "f",
        "FOREIGN KEY (operation_id) REFERENCES khub_repository_operations(id) ON DELETE CASCADE",
    ),
    "khub_step_state_ck": (
        "c",
        "CHECK (state = ANY (ARRAY['pending'::text, 'running'::text, 'dispatch_started'::text, 'observing'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'uncertain'::text]))",
    ),
    "khub_step_attempt_ck": ("c", "CHECK (attempt >= 0)"),
    "khub_step_sequence_uidx": ("u", "UNIQUE (operation_id, sequence)"),
    "khub_step_delivery_uidx": ("u", "UNIQUE (delivery_key)"),
    "khub_commit_intents_pkey": ("p", "PRIMARY KEY (id)"),
    "khub_commit_intent_marker_ck": ("c", "CHECK (marker ~~ 'khub:v1:%'::text)"),
    "khub_commit_intent_payload_hash_ck": (
        "c",
        "CHECK (payload_hash ~ '^[0-9a-f]{64}$'::text)",
    ),
    "khub_commit_intent_state_ck": (
        "c",
        "CHECK (state = ANY (ARRAY['prepared'::text, 'dispatch_started'::text, 'committed'::text, 'finalized'::text, 'reconciliation_required'::text, 'uncertain'::text, 'abandoned'::text]))",
    ),
    "khub_quota_reservations_pkey": ("p", "PRIMARY KEY (id)"),
    "khub_quota_reservations_intent_id_fkey": (
        "f",
        "FOREIGN KEY (intent_id) REFERENCES khub_commit_intents(id) ON DELETE CASCADE",
    ),
    "khub_quota_reservation_scope_ck": (
        "c",
        "CHECK (scope_type = ANY (ARRAY['repository'::text, 'namespace'::text]))",
    ),
    "khub_quota_reservation_bytes_ck": ("c", "CHECK (reserved_bytes > 0)"),
    "khub_quota_reservation_state_ck": (
        "c",
        "CHECK (state = ANY (ARRAY['reserved'::text, 'consumed'::text, 'released'::text]))",
    ),
    "khub_quota_reservation_unique_scope_uidx": (
        "u",
        "UNIQUE (intent_id, scope_type)",
    ),
}

PROCRASTINATE_TABLE_COLUMNS = {
    "procrastinate_workers": {
        "id",
        "last_heartbeat",
    },
    "procrastinate_jobs": {
        "id",
        "queue_name",
        "task_name",
        "priority",
        "lock",
        "queueing_lock",
        "args",
        "status",
        "scheduled_at",
        "attempts",
        "abort_requested",
        "worker_id",
    },
    "procrastinate_periodic_defers": {
        "id",
        "task_name",
        "defer_timestamp",
        "job_id",
        "periodic_id",
    },
    "procrastinate_events": {
        "id",
        "job_id",
        "type",
        "at",
    },
}
PROCRASTINATE_REQUIRED_INDEXES = {
    "procrastinate_jobs_queueing_lock_idx_v1",
    "procrastinate_jobs_lock_idx_v1",
    "idx_procrastinate_jobs_worker_not_null",
    "procrastinate_jobs_queue_name_idx_v1",
    "procrastinate_jobs_id_lock_idx_v1",
    "procrastinate_jobs_priority_idx_v1",
    "procrastinate_events_job_id_fkey_v1",
    "procrastinate_periodic_defers_job_id_fkey_v1",
    "idx_procrastinate_workers_last_heartbeat",
}
PROCRASTINATE_TYPES = {
    "procrastinate_job_status": (
        "enum",
        (
            "todo",
            "doing",
            "succeeded",
            "failed",
            "cancelled",
            "aborting",
            "aborted",
        ),
    ),
    "procrastinate_job_event_type": (
        "enum",
        (
            "deferred",
            "started",
            "deferred_for_retry",
            "failed",
            "succeeded",
            "cancelled",
            "abort_requested",
            "aborted",
            "scheduled",
            "retried",
        ),
    ),
    "procrastinate_job_to_defer_v1": (
        "composite",
        (
            ("queue_name", "character varying"),
            ("task_name", "character varying"),
            ("priority", "integer"),
            ("lock", "text"),
            ("queueing_lock", "text"),
            ("args", "jsonb"),
            ("scheduled_at", "timestamp with time zone"),
        ),
    ),
}

# Frozen PostgreSQL catalog semantics for Procrastinate 3.9.0.  The table and
# object-name sets above are useful for a fast existence check, but an object
# with the right name can still have the wrong type, predicate, or constraint.
PROCRASTINATE_COLUMN_CONTRACT = {
    "procrastinate_workers": {
        "id": ("bigint", False, "__identity__"),
        "last_heartbeat": ("timestamp with time zone", False, "now()"),
    },
    "procrastinate_jobs": {
        "id": ("bigint", False, "__sequence__"),
        "queue_name": ("character varying(128)", False, None),
        "task_name": ("character varying(128)", False, None),
        "priority": ("integer", False, "0"),
        "lock": ("text", True, None),
        "queueing_lock": ("text", True, None),
        "args": ("jsonb", False, "'{}'::jsonb"),
        "status": ("procrastinate_job_status", False, "'todo'::procrastinate_job_status"),
        "scheduled_at": ("timestamp with time zone", True, None),
        "attempts": ("integer", False, "0"),
        "abort_requested": ("boolean", False, "false"),
        "worker_id": ("bigint", True, None),
    },
    "procrastinate_periodic_defers": {
        "id": ("bigint", False, "__sequence__"),
        "task_name": ("character varying(128)", False, None),
        "defer_timestamp": ("bigint", True, None),
        "job_id": ("bigint", True, None),
        "periodic_id": ("character varying(128)", False, "''::character varying"),
    },
    "procrastinate_events": {
        "id": ("bigint", False, "__sequence__"),
        "job_id": ("bigint", False, None),
        "type": ("procrastinate_job_event_type", True, None),
        "at": ("timestamp with time zone", True, "now()"),
    },
}

PROCRASTINATE_INDEX_DEFINITIONS = {
    "procrastinate_jobs_queueing_lock_idx_v1": "CREATE UNIQUE INDEX procrastinate_jobs_queueing_lock_idx_v1 ON public.procrastinate_jobs USING btree (queueing_lock) WHERE (status = 'todo'::procrastinate_job_status)",
    "procrastinate_jobs_lock_idx_v1": "CREATE UNIQUE INDEX procrastinate_jobs_lock_idx_v1 ON public.procrastinate_jobs USING btree (lock) WHERE (status = 'doing'::procrastinate_job_status)",
    "idx_procrastinate_jobs_worker_not_null": "CREATE INDEX idx_procrastinate_jobs_worker_not_null ON public.procrastinate_jobs USING btree (worker_id) WHERE ((worker_id IS NOT NULL) AND (status = 'doing'::procrastinate_job_status))",
    "procrastinate_jobs_queue_name_idx_v1": "CREATE INDEX procrastinate_jobs_queue_name_idx_v1 ON public.procrastinate_jobs USING btree (queue_name)",
    "procrastinate_jobs_id_lock_idx_v1": "CREATE INDEX procrastinate_jobs_id_lock_idx_v1 ON public.procrastinate_jobs USING btree (id, lock) WHERE (status = ANY (ARRAY['todo'::procrastinate_job_status, 'doing'::procrastinate_job_status]))",
    "procrastinate_jobs_priority_idx_v1": "CREATE INDEX procrastinate_jobs_priority_idx_v1 ON public.procrastinate_jobs USING btree (priority DESC, id) WHERE (status = 'todo'::procrastinate_job_status)",
    "procrastinate_events_job_id_fkey_v1": "CREATE INDEX procrastinate_events_job_id_fkey_v1 ON public.procrastinate_events USING btree (job_id)",
    "procrastinate_periodic_defers_job_id_fkey_v1": "CREATE INDEX procrastinate_periodic_defers_job_id_fkey_v1 ON public.procrastinate_periodic_defers USING btree (job_id)",
    "idx_procrastinate_workers_last_heartbeat": "CREATE INDEX idx_procrastinate_workers_last_heartbeat ON public.procrastinate_workers USING btree (last_heartbeat)",
}

PROCRASTINATE_CONSTRAINT_DEFINITIONS = {
    "procrastinate_events_job_id_fkey": (
        "procrastinate_events",
        "f",
        "FOREIGN KEY (job_id) REFERENCES procrastinate_jobs(id) ON DELETE CASCADE",
    ),
    "procrastinate_events_pkey": (
        "procrastinate_events",
        "p",
        "PRIMARY KEY (id)",
    ),
    "check_not_todo_abort_requested": (
        "procrastinate_jobs",
        "c",
        "CHECK (NOT (status = 'todo'::procrastinate_job_status AND abort_requested = true))",
    ),
    "procrastinate_jobs_pkey": (
        "procrastinate_jobs",
        "p",
        "PRIMARY KEY (id)",
    ),
    "procrastinate_jobs_worker_id_fkey": (
        "procrastinate_jobs",
        "f",
        "FOREIGN KEY (worker_id) REFERENCES procrastinate_workers(id) ON DELETE SET NULL",
    ),
    "procrastinate_periodic_defers_job_id_fkey": (
        "procrastinate_periodic_defers",
        "f",
        "FOREIGN KEY (job_id) REFERENCES procrastinate_jobs(id)",
    ),
    "procrastinate_periodic_defers_pkey": (
        "procrastinate_periodic_defers",
        "p",
        "PRIMARY KEY (id)",
    ),
    "procrastinate_periodic_defers_unique": (
        "procrastinate_periodic_defers",
        "u",
        "UNIQUE (task_name, periodic_id, defer_timestamp)",
    ),
    "procrastinate_workers_pkey": (
        "procrastinate_workers",
        "p",
        "PRIMARY KEY (id)",
    ),
}


def expected_table_columns(
    *, include_operations: bool = True
) -> dict[str, tuple[str, ...]]:
    """Return the current application-owned table/column contract."""

    result = {
        model._meta.table_name: tuple(
            sorted(field.column_name for field in model._meta.sorted_fields)
        )
        for model in KHUB_MODELS
    }
    if include_operations:
        result.update(operation_table_columns())
    return result


def signature_digest(signature: dict[str, Iterable[str]]) -> str:
    normalized = {
        table: sorted(columns) for table, columns in sorted(signature.items())
    }
    payload = json.dumps(normalized, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_table_columns(connection: Any) -> dict[str, tuple[str, ...]]:
    rows = connection.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name NOT LIKE 'procrastinate_%'
          AND table_name <> 'khub_schema_migrations'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    result: dict[str, list[str]] = {}
    for table_name, column_name in rows:
        result.setdefault(table_name, []).append(column_name)
    return {table: tuple(columns) for table, columns in result.items()}


def schema_diff(
    expected: dict[str, Iterable[str]], actual: dict[str, Iterable[str]]
) -> dict[str, Any]:
    expected_normalized = {
        table: set(columns) for table, columns in expected.items()
    }
    actual_normalized = {table: set(columns) for table, columns in actual.items()}
    missing_tables = sorted(set(expected_normalized) - set(actual_normalized))
    extra_tables = sorted(set(actual_normalized) - set(expected_normalized))
    columns = {}
    for table in sorted(set(expected_normalized) & set(actual_normalized)):
        missing = sorted(expected_normalized[table] - actual_normalized[table])
        extra = sorted(actual_normalized[table] - expected_normalized[table])
        if missing or extra:
            columns[table] = {"missing": missing, "extra": extra}
    return {
        "missing_tables": missing_tables,
        "extra_tables": extra_tables,
        "columns": columns,
    }


def _legacy_field_type(field: Any) -> str:
    field_type = type(field).__name__
    if field_type in {"AutoField", "IntegerField", "ForeignKeyField"}:
        return "integer"
    if field_type == "BigIntegerField":
        return "bigint"
    if field_type == "BooleanField":
        return "boolean"
    if field_type == "DateField":
        return "date"
    if field_type == "DateTimeField":
        return "timestamp without time zone"
    if field_type == "BlobField":
        return "bytea"
    if field_type == "TextField":
        return "text"
    if field_type == "CharField":
        return f"character varying({getattr(field, 'max_length', 255)})"
    raise ValueError(f"unsupported legacy Peewee field type: {field_type}")


def _legacy_column_contract() -> dict[str, dict[str, tuple[str, bool, str | None]]]:
    result: dict[str, dict[str, tuple[str, bool, str | None]]] = {}
    for model in KHUB_MODELS:
        table = model._meta.table_name
        result[table] = {}
        for field in model._meta.sorted_fields:
            default = None
            if getattr(field, "primary_key", False) and type(field).__name__ == "AutoField":
                default = "__sequence__"
            result[table][field.column_name] = (
                _legacy_field_type(field),
                bool(field.null),
                default,
            )
    return result


def legacy_schema_semantic_diff(connection: Any) -> dict[str, list[str]]:
    """Validate the legacy KHub tables before a ledger-less adoption."""

    expected = _legacy_column_contract()
    rows = connection.execute(
        """SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
                  a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
           FROM pg_class AS c
           JOIN pg_namespace AS n ON n.oid = c.relnamespace
           JOIN pg_attribute AS a ON a.attrelid = c.oid AND a.attnum > 0
             AND NOT a.attisdropped
           LEFT JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
           WHERE n.nspname = 'public' AND c.relname = ANY(%s)""",
        (sorted(expected),),
    ).fetchall()
    actual = {
        (table, column): (type_name, not not_null, default)
        for table, column, type_name, not_null, default in rows
    }
    errors: list[str] = []
    for table, columns in expected.items():
        for column, expected_value in columns.items():
            actual_value = actual.get((table, column))
            if actual_value is None:
                errors.append(f"missing {table}.{column}")
                continue
            actual_type, actual_nullable, actual_default = actual_value
            expected_type, expected_nullable, expected_default = expected_value
            if (actual_type, actual_nullable) != (expected_type, expected_nullable):
                errors.append(
                    f"{table}.{column} actual={(actual_type, actual_nullable)!r} "
                    f"expected={(expected_type, expected_nullable)!r}"
                )
            if expected_default == "__sequence__":
                if not actual_default or not actual_default.startswith("nextval("):
                    errors.append(f"{table}.{column} is not backed by a sequence")
            elif actual_default is not None:
                errors.append(
                    f"{table}.{column} default={actual_default!r} expected=None"
                )
    return {"columns": errors}


def legacy_schema_object_diff(connection: Any) -> dict[str, list[str]]:
    """Validate model-declared PK/FK and index shape for adoption."""

    constraint_rows = connection.execute(
        """SELECT contype, pg_get_constraintdef(oid, true)
           FROM pg_constraint
           WHERE connamespace = 'public'::regnamespace"""
    ).fetchall()
    actual_constraints = {
        (kind, _normalize_catalog_sql(definition).replace('"', ""))
        for kind, definition in constraint_rows
    }
    expected_constraints: set[tuple[str, str]] = set()
    for model in KHUB_MODELS:
        table = model._meta.table_name
        pk_field = model._meta.primary_key
        expected_constraints.add(("p", f"primary key ({pk_field.column_name})"))
        for field in model._meta.sorted_fields:
            if type(field).__name__ != "ForeignKeyField":
                continue
            action = getattr(field, "on_delete", "NO ACTION")
            expected_constraints.add(
                (
                    "f",
                    _normalize_catalog_sql(
                        f"foreign key ({field.column_name}) references "
                        f"{field.rel_model._meta.table_name}(id) on delete {action}"
                    ),
                )
            )
    missing_constraints = sorted(
        f"{kind}:{definition}"
        for kind, definition in expected_constraints
        if (kind, definition.replace('"', "")) not in actual_constraints
    )

    index_rows = connection.execute(
        """SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"""
    ).fetchall()
    actual_indexes = [_normalize_catalog_sql(row[0]).replace('"', "") for row in index_rows]
    expected_indexes: list[tuple[bool, str]] = []
    for model in KHUB_MODELS:
        table = model._meta.table_name
        for field in model._meta.sorted_fields:
            if getattr(field, "index", False) or getattr(field, "unique", False):
                expected_indexes.append(
                    (
                        bool(field.unique),
                        f" on public.{table} using btree ({field.column_name})",
                    )
                )
        for fields, unique in model._meta.indexes:
            columns = ", ".join(model._meta.fields[name].column_name for name in fields)
            expected_indexes.append(
                (bool(unique), f" on public.{table} using btree ({columns})")
            )
    missing_indexes = []
    for unique, fragment in expected_indexes:
        if not any(
            fragment in actual
            and (not unique or actual.startswith("create unique index"))
            for actual in actual_indexes
        ):
            missing_indexes.append(f"{'unique ' if unique else ''}{fragment.strip()}")
    return {
        "missing_constraints": missing_constraints,
        "missing_indexes": missing_indexes,
    }


def is_exact_current_schema(
    connection: Any, *, include_operations: bool = True
) -> tuple[bool, dict[str, Any]]:
    expected = expected_table_columns(include_operations=include_operations)
    actual = read_table_columns(connection)
    diff = schema_diff(expected, actual)
    legacy_semantics = legacy_schema_semantic_diff(connection)
    legacy_objects = legacy_schema_object_diff(connection)
    diff["legacy_semantics"] = legacy_semantics
    diff["legacy_objects"] = legacy_objects
    exact = (
        not diff["missing_tables"]
        and not diff["extra_tables"]
        and not diff["columns"]
        and not any(legacy_semantics.values())
        and not any(legacy_objects.values())
    )
    return exact, diff


def _normalize_catalog_sql(value: str) -> str:
    return " ".join(value.lower().split())


def operation_schema_object_diff(
    connection: Any,
    *,
    index_definitions: dict[str, str] | None = None,
    constraint_definitions: dict[str, tuple[str, str]] | None = None,
) -> dict[str, list[str]]:
    """Return missing operation indexes and constraints.

    Column presence alone is insufficient for the durable kernel: dropping a
    unique delivery key or state check would silently reintroduce duplicate
    jobs or invalid state transitions while all table-column checks still pass.
    """

    index_definitions = (
        EXPECTED_OPERATION_INDEX_DEFINITIONS
        if index_definitions is None
        else index_definitions
    )
    constraint_definitions = (
        EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS
        if constraint_definitions is None
        else constraint_definitions
    )
    index_rows = connection.execute(
        """
        SELECT p.indexname, p.indexdef, i.indisvalid
        FROM pg_indexes AS p
        JOIN pg_class AS c
          ON c.relname = p.indexname
        JOIN pg_namespace AS n
          ON n.oid = c.relnamespace AND n.nspname = p.schemaname
        JOIN pg_index AS i ON i.indexrelid = c.oid
        WHERE p.schemaname = 'public'
        """
    ).fetchall()
    constraint_rows = connection.execute(
        """
        SELECT conname, contype, convalidated,
               pg_get_constraintdef(oid, true)
           FROM pg_constraint
           WHERE connamespace = 'public'::regnamespace
        """
    ).fetchall()
    indexes = {row[0] for row in index_rows}
    constraints = {row[0] for row in constraint_rows}
    invalid_indexes = sorted(
        name
        for name, definition, valid in index_rows
        if name in index_definitions
        and (
            not valid
            or _normalize_catalog_sql(definition)
            != _normalize_catalog_sql(index_definitions[name])
        )
    )
    invalid_constraints = sorted(
        name
        for name, contype, validated, definition in constraint_rows
        if name in constraint_definitions
        and (
            not validated
            or (contype, _normalize_catalog_sql(definition))
            != (
                constraint_definitions[name][0],
                _normalize_catalog_sql(constraint_definitions[name][1]),
            )
        )
    )
    return {
        "missing_indexes": sorted(set(index_definitions) - indexes),
        "missing_constraints": sorted(set(constraint_definitions) - constraints),
        "invalid_indexes": invalid_indexes,
        "invalid_constraints": invalid_constraints,
    }


def _normalize_procrastinate_index_definition(value: str) -> str:
    normalized = _normalize_catalog_sql(value).replace('"', "")
    normalized = normalized.replace("public.", "")
    return re.sub(
        r"^create\s+(unique\s+)?index\s+[^ ]+\s+on\s+",
        lambda match: f"create {match.group(1) or ''}index on ",
        normalized,
    )


def _normalize_procrastinate_default(
    value: str | None, identity: str | None = None
) -> str | None:
    if identity:
        return "__identity__"
    normalized = None if value is None else _normalize_catalog_sql(value)
    if normalized is not None and normalized.startswith("nextval("):
        return "__sequence__"
    return normalized


def procrastinate_schema_diff(
    connection: Any,
    *,
    table_columns: dict[str, set[str]] | None = None,
    required_indexes: set[str] | None = None,
    types: dict[str, tuple[str, tuple]] | None = None,
    column_contract: dict[str, dict[str, tuple[str, bool, str | None]]] | None = None,
    index_definitions: dict[str, str] | None = None,
    constraint_definitions: dict[str, tuple[str, str, str]] | None = None,
) -> dict[str, list[str]]:
    """Check the durable Procrastinate catalog beyond object names."""

    table_columns = (
        PROCRASTINATE_TABLE_COLUMNS if table_columns is None else table_columns
    )
    required_indexes = (
        PROCRASTINATE_REQUIRED_INDEXES
        if required_indexes is None
        else required_indexes
    )
    types = PROCRASTINATE_TYPES if types is None else types
    column_contract = (
        PROCRASTINATE_COLUMN_CONTRACT if column_contract is None else column_contract
    )
    index_definitions = (
        PROCRASTINATE_INDEX_DEFINITIONS
        if index_definitions is None
        else index_definitions
    )
    constraint_definitions = (
        PROCRASTINATE_CONSTRAINT_DEFINITIONS
        if constraint_definitions is None
        else constraint_definitions
    )

    table_rows = connection.execute(
        """SELECT columns.table_name, columns.column_name,
                  format_type(attribute.atttypid, attribute.atttypmod),
                  attribute.attidentity, NOT attribute.attnotnull,
                  pg_get_expr(default_value.adbin, default_value.adrelid)
           FROM information_schema.columns AS columns
           JOIN pg_class AS relation
             ON relation.relname = columns.table_name
           JOIN pg_namespace AS namespace
             ON namespace.oid = relation.relnamespace
            AND namespace.nspname = columns.table_schema
           JOIN pg_attribute AS attribute
             ON attribute.attrelid = relation.oid
            AND attribute.attname = columns.column_name
           LEFT JOIN pg_attrdef AS default_value
             ON default_value.adrelid = attribute.attrelid
            AND default_value.adnum = attribute.attnum
           WHERE columns.table_schema = 'public'
             AND columns.table_name = ANY(%s)
           ORDER BY columns.table_name, columns.ordinal_position""",
        (sorted(table_columns),),
    ).fetchall()
    actual_columns: dict[str, set[str]] = {}
    actual_column_contract: dict[tuple[str, str], tuple[str, bool, str | None]] = {}
    for row in table_rows:
        table, column = row[:2]
        actual_columns.setdefault(table, set()).add(column)
        if len(row) < 6:
            continue
        _table, _column, type_name, identity, nullable, default = row
        actual_column_contract[(table, column)] = (
            type_name,
            bool(nullable),
            _normalize_procrastinate_default(default, identity),
        )
    missing_columns = [
        f"{table}.{column}"
        for table, columns in table_columns.items()
        for column in sorted(columns - actual_columns.get(table, set()))
    ]
    invalid_columns = [
        f"{table}.{column}"
        for table, columns in column_contract.items()
        for column, expected in columns.items()
        if (table, column) in actual_column_contract
        and actual_column_contract[(table, column)] != expected
    ]

    index_rows = connection.execute(
        """SELECT indexes.tablename, indexes.indexname, indexes.indexdef,
                  index_info.indisvalid
           FROM pg_indexes AS indexes
           JOIN pg_class AS index_relation
             ON index_relation.relname = indexes.indexname
           JOIN pg_namespace AS index_namespace
             ON index_namespace.oid = index_relation.relnamespace
            AND index_namespace.nspname = indexes.schemaname
           JOIN pg_index AS index_info
             ON index_info.indexrelid = index_relation.oid
           WHERE indexes.schemaname = 'public'"""
    ).fetchall()
    actual_index_names: set[str] = set()
    actual_index_definitions: dict[str, tuple[str, bool]] = {}
    for row in index_rows:
        if len(row) == 1:
            actual_index_names.add(row[0])
            continue
        _table, name, definition, valid = row
        actual_index_names.add(name)
        actual_index_definitions[name] = (
            _normalize_procrastinate_index_definition(definition),
            bool(valid),
        )
    expected_index_definitions = {
        name: _normalize_procrastinate_index_definition(definition)
        for name, definition in index_definitions.items()
    }
    actual_index_by_definition = {
        definition: valid
        for definition, valid in actual_index_definitions.values()
    }
    missing_indexes = []
    invalid_indexes = []
    for name in sorted(required_indexes):
        expected_definition = expected_index_definitions.get(name)
        actual = actual_index_definitions.get(name)
        if actual is None:
            if expected_definition and expected_definition in actual_index_by_definition:
                continue
            if name not in actual_index_names:
                missing_indexes.append(name)
            continue
        actual_definition, valid = actual
        if not valid or (
            expected_definition is not None and actual_definition != expected_definition
        ):
            invalid_indexes.append(name)

    type_rows = connection.execute(
        """SELECT t.typname, t.typtype, e.enumlabel
           FROM pg_type AS t
           JOIN pg_namespace AS n ON n.oid = t.typnamespace
           LEFT JOIN pg_enum AS e ON e.enumtypid = t.oid
           WHERE n.nspname = 'public' AND t.typname = ANY(%s)
           ORDER BY t.typname, e.enumsortorder""",
        (sorted(types),),
    ).fetchall()
    actual_types: dict[str, tuple[str, list[str]]] = {}
    for name, type_code, enum_label in type_rows:
        type_name = (
            "enum" if type_code == "e" else "composite" if type_code == "c" else type_code
        )
        entry = actual_types.setdefault(name, (type_name, []))
        if enum_label is not None:
            entry[1].append(enum_label)
    missing_types = sorted(set(types) - set(actual_types))
    composite_names = [
        name for name, (kind, _values) in types.items() if kind == "composite"
    ]
    composite_rows = connection.execute(
        """SELECT type_info.typname, attribute.attname,
                  format_type(attribute.atttypid, attribute.atttypmod)
           FROM pg_type AS type_info
           JOIN pg_namespace AS namespace
             ON namespace.oid = type_info.typnamespace
           JOIN pg_class AS relation
             ON relation.oid = type_info.typrelid
           JOIN pg_attribute AS attribute
             ON attribute.attrelid = relation.oid
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped
           WHERE namespace.nspname = 'public'
             AND type_info.typname = ANY(%s)
           ORDER BY type_info.typname, attribute.attnum""",
        (composite_names,),
    ).fetchall()
    actual_composites: dict[str, list[tuple[str, str]]] = {}
    for name, field, type_name in composite_rows:
        actual_composites.setdefault(name, []).append((field, type_name))
    invalid_types = []
    for name, (expected_kind, expected_values) in types.items():
        if name not in actual_types:
            continue
        actual_kind, actual_values = actual_types[name]
        invalid = actual_kind != expected_kind
        if expected_kind == "enum":
            invalid = invalid or tuple(actual_values) != tuple(expected_values)
        elif expected_kind == "composite":
            invalid = invalid or tuple(actual_composites.get(name, ())) != tuple(
                expected_values
            )
        if invalid:
            invalid_types.append(name)

    constraint_rows = connection.execute(
        """SELECT relation.relname, constraint_info.conname,
                  constraint_info.contype, constraint_info.convalidated,
                  pg_get_constraintdef(constraint_info.oid, true)
           FROM pg_constraint AS constraint_info
           JOIN pg_class AS relation
             ON relation.oid = constraint_info.conrelid
           JOIN pg_namespace AS namespace
             ON namespace.oid = relation.relnamespace
           WHERE namespace.nspname = 'public'
             AND relation.relname = ANY(%s)""",
        (sorted(table_columns),),
    ).fetchall()
    actual_constraints: dict[str, tuple[str, str, bool, str]] = {}
    actual_constraints_by_definition: dict[tuple[str, str, str], bool] = {}
    for row in constraint_rows:
        if len(row) < 5:
            continue
        table, name, kind, valid, definition = row
        normalized = _normalize_catalog_sql(definition).replace('"', "")
        actual_constraints[name] = (table, kind, bool(valid), normalized)
        actual_constraints_by_definition[(table, kind, normalized)] = bool(valid)
    missing_constraints = []
    invalid_constraints = []
    for name, (table, kind, definition) in constraint_definitions.items():
        expected_definition = _normalize_catalog_sql(definition).replace('"', "")
        actual = actual_constraints.get(name)
        semantic_key = (table, kind, expected_definition)
        if actual is None:
            if semantic_key not in actual_constraints_by_definition:
                missing_constraints.append(name)
            elif not actual_constraints_by_definition[semantic_key]:
                invalid_constraints.append(name)
            continue
        actual_table, actual_kind, valid, actual_definition = actual
        if (
            actual_table != table
            or actual_kind != kind
            or not valid
            or actual_definition != expected_definition
        ):
            invalid_constraints.append(name)
    return {
        "missing_columns": sorted(missing_columns),
        "invalid_columns": sorted(invalid_columns),
        "missing_indexes": sorted(missing_indexes),
        "invalid_indexes": sorted(invalid_indexes),
        "missing_types": missing_types,
        "invalid_types": sorted(invalid_types),
        "missing_constraints": sorted(missing_constraints),
        "invalid_constraints": sorted(invalid_constraints),
    }


def kernel_semantic_diff(
    connection: Any,
    *,
    column_contract: dict[str, dict[str, tuple[str, bool, str | None]]] | None = None,
) -> dict[str, list[str]]:
    """Compare durable-kernel types, nullability, and defaults.

    Defaults are normalized by PostgreSQL's ``pg_get_expr`` representation;
    this intentionally reports a changed expression instead of attempting to
    guess whether the change is compatible with an existing deployment.
    """

    column_contract = (
        KERNEL_COLUMN_CONTRACT if column_contract is None else column_contract
    )
    expected_tables = tuple(column_contract)
    rows = connection.execute(
        """
        SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
               NOT a.attnotnull,
               pg_get_expr(d.adbin, d.adrelid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
            AND NOT a.attisdropped
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = 'public'
          AND c.relname = ANY(%s)
        """,
        (list(expected_tables),),
    ).fetchall()
    actual: dict[tuple[str, str], tuple[str, bool, str | None]] = {
        (table, column): (type_name, nullable, default)
        for table, column, type_name, nullable, default in rows
    }
    diffs: list[str] = []
    for table, columns in column_contract.items():
        for column, expected in columns.items():
            value = actual.get((table, column))
            if value is None:
                diffs.append(f"missing {table}.{column}")
                continue
            actual_type, actual_nullable, actual_default = value
            expected_type, expected_nullable, expected_default = expected
            if actual_type != expected_type:
                diffs.append(
                    f"{table}.{column} type={actual_type!r} expected={expected_type!r}"
                )
            if actual_nullable != expected_nullable:
                diffs.append(
                    f"{table}.{column} nullable={actual_nullable!r} expected={expected_nullable!r}"
                )
            if (actual_default or None) != (expected_default or None):
                diffs.append(
                    f"{table}.{column} default={actual_default!r} expected={expected_default!r}"
                )
    actual_tables = {table for table, _ in actual}
    for table in sorted(actual_tables - set(expected_tables)):
        diffs.append(f"unexpected kernel table {table}")
    return {"columns": diffs}

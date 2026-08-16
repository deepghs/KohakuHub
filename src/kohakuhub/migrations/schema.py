"""Conservative PostgreSQL schema signatures for migration adoption."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any

from kohakuhub.db import KHUB_MODELS
from kohakuhub.operations.sql import operation_table_columns


REQUIRED_OPERATION_INDEXES = {
    "khub_operation_idempotency_uidx",
    "khub_operation_active_resource_uidx",
    "khub_operation_owner_idx",
    "khub_operation_repository_idx",
    "khub_operation_steps_operation_idx",
    "khub_operation_steps_state_idx",
}
REQUIRED_OPERATION_CONSTRAINTS = {
    "khub_operation_state_ck",
    "khub_operation_trigger_ck",
    "khub_operation_progress_ck",
    "khub_step_state_ck",
    "khub_step_attempt_ck",
    "khub_step_sequence_uidx",
    "khub_step_delivery_uidx",
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


def is_exact_current_schema(
    connection: Any, *, include_operations: bool = True
) -> tuple[bool, dict[str, Any]]:
    expected = expected_table_columns(include_operations=include_operations)
    actual = read_table_columns(connection)
    diff = schema_diff(expected, actual)
    return not any(diff.values()), diff


def operation_schema_object_diff(connection: Any) -> dict[str, list[str]]:
    """Return missing operation indexes and constraints.

    Column presence alone is insufficient for the durable kernel: dropping a
    unique delivery key or state check would silently reintroduce duplicate
    jobs or invalid state transitions while all table-column checks still pass.
    """

    index_rows = connection.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        """
    ).fetchall()
    constraint_rows = connection.execute(
        """
        SELECT conname
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
        """
    ).fetchall()
    indexes = {row[0] for row in index_rows}
    constraints = {row[0] for row in constraint_rows}
    return {
        "missing_indexes": sorted(REQUIRED_OPERATION_INDEXES - indexes),
        "missing_constraints": sorted(REQUIRED_OPERATION_CONSTRAINTS - constraints),
    }

"""Read-only PostgreSQL checks required before durable operation use."""

from __future__ import annotations

from typing import Any

from kohakuhub.migrations.schema import (
    EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS,
    EXPECTED_OPERATION_INDEX_DEFINITIONS,
    KERNEL_COLUMN_CONTRACT,
    PROCRASTINATE_REQUIRED_INDEXES,
    PROCRASTINATE_TABLE_COLUMNS,
    PROCRASTINATE_TYPES,
    REQUIRED_OPERATION_CONSTRAINTS,
    REQUIRED_OPERATION_INDEXES,
    _normalize_catalog_sql,
)
from kohakuhub.migrations.worker_contract import (
    APPLICATION_SCHEMA_ADOPTION_CHECKSUM_V1,
    WORKER_OPERATION_SCHEMA_CHECKSUM_V1,
    WORKER_OPERATION_SCHEMA_VERSION_V1,
    WORKER_PROCRASTINATE_SCHEMA_CHECKSUM_V390,
)
from kohakuhub.operations.sql import (
    operation_table_columns,
)


PROCRASTINATE_TABLES = {
    "procrastinate_events",
    "procrastinate_jobs",
    "procrastinate_periodic_defers",
    "procrastinate_workers",
}


async def verify_operation_schema(connection: Any) -> None:
    """Raise when the durable worker schema is absent or semantically weak."""

    expected_tables = set(operation_table_columns()) | PROCRASTINATE_TABLES | {
        "khub_schema_migrations"
    }
    table_rows = await connection.execute(
        """SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = 'public' AND table_name = ANY(%s)""",
        (sorted(expected_tables),),
    )
    actual_tables = {row[0] for row in await table_rows.fetchall()}
    missing_tables = sorted(expected_tables - actual_tables)
    if missing_tables:
        raise RuntimeError(f"durable operation schema is incomplete: {missing_tables}")

    ledger_rows = await connection.execute(
        """SELECT migration_name, version, checksum
           FROM khub_schema_migrations
           WHERE migration_name = ANY(%s)""",
        (
            [
                "khub-current-adoption",
                "procrastinate-3.9.0",
                f"khub-operation-kernel-v{WORKER_OPERATION_SCHEMA_VERSION_V1}",
            ],
        ),
    )
    ledger = {
        name: (int(version), checksum)
        for name, version, checksum in await ledger_rows.fetchall()
    }
    expected_ledger = {
        "khub-current-adoption": (
            1,
            APPLICATION_SCHEMA_ADOPTION_CHECKSUM_V1,
        ),
        "procrastinate-3.9.0": (
            1,
            WORKER_PROCRASTINATE_SCHEMA_CHECKSUM_V390,
        ),
        f"khub-operation-kernel-v{WORKER_OPERATION_SCHEMA_VERSION_V1}": (
            WORKER_OPERATION_SCHEMA_VERSION_V1,
            WORKER_OPERATION_SCHEMA_CHECKSUM_V1,
        ),
    }
    ledger_errors = [
        f"{name} actual={ledger.get(name)!r} expected={expected!r}"
        for name, expected in expected_ledger.items()
        if ledger.get(name) != expected
    ]
    if ledger_errors:
        raise RuntimeError("durable schema ledger mismatch: " + "; ".join(ledger_errors))

    expected_columns = operation_table_columns()
    column_rows = await connection.execute(
        """SELECT table_name, column_name
           FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = ANY(%s)""",
        (sorted(expected_columns),),
    )
    actual_columns: dict[str, set[str]] = {}
    for table, column in await column_rows.fetchall():
        actual_columns.setdefault(table, set()).add(column)
    column_diff = {
        table: sorted(set(columns) - actual_columns.get(table, set()))
        for table, columns in expected_columns.items()
        if set(columns) - actual_columns.get(table, set())
    }
    if column_diff:
        raise RuntimeError(f"durable operation columns are incomplete: {column_diff}")

    worker_column_rows = await connection.execute(
        """SELECT table_name, column_name
           FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = ANY(%s)""",
        (sorted(PROCRASTINATE_TABLE_COLUMNS),),
    )
    worker_columns: dict[str, set[str]] = {}
    for table, column in await worker_column_rows.fetchall():
        worker_columns.setdefault(table, set()).add(column)
    missing_worker_columns = [
        f"{table}.{column}"
        for table, columns in PROCRASTINATE_TABLE_COLUMNS.items()
        for column in sorted(columns - worker_columns.get(table, set()))
    ]
    if missing_worker_columns:
        raise RuntimeError(
            "Procrastinate columns are incomplete: "
            f"{missing_worker_columns}"
        )

    worker_index_rows = await connection.execute(
        """SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"""
    )
    missing_worker_indexes = sorted(
        PROCRASTINATE_REQUIRED_INDEXES
        - {row[0] for row in await worker_index_rows.fetchall()}
    )
    if missing_worker_indexes:
        raise RuntimeError(
            "Procrastinate indexes are incomplete: " f"{missing_worker_indexes}"
        )

    worker_type_rows = await connection.execute(
        """SELECT t.typname, t.typtype, e.enumlabel
           FROM pg_type AS t
           JOIN pg_namespace AS n ON n.oid = t.typnamespace
           LEFT JOIN pg_enum AS e ON e.enumtypid = t.oid
           WHERE n.nspname = 'public' AND t.typname = ANY(%s)
           ORDER BY t.typname, e.enumsortorder""",
        (sorted(PROCRASTINATE_TYPES),),
    )
    worker_types: dict[str, tuple[str, list[str]]] = {}
    for name, type_code, enum_label in await worker_type_rows.fetchall():
        kind = "enum" if type_code == "e" else "composite" if type_code == "c" else type_code
        entry = worker_types.setdefault(name, (kind, []))
        if enum_label is not None:
            entry[1].append(enum_label)
    missing_worker_types = sorted(set(PROCRASTINATE_TYPES) - set(worker_types))
    invalid_worker_types = sorted(
        name
        for name, (expected_kind, expected_values) in PROCRASTINATE_TYPES.items()
        if name in worker_types
        and (
            worker_types[name][0] != expected_kind
            or expected_kind == "enum"
            and tuple(worker_types[name][1]) != tuple(expected_values)
        )
    )
    if missing_worker_types or invalid_worker_types:
        raise RuntimeError(
            "Procrastinate types are incompatible: "
            f"missing={missing_worker_types}, invalid={invalid_worker_types}"
        )

    index_rows = await connection.execute(
        """SELECT p.indexname, p.indexdef, i.indisvalid
           FROM pg_indexes AS p
           JOIN pg_class AS c ON c.relname = p.indexname
           JOIN pg_namespace AS n
             ON n.oid = c.relnamespace AND n.nspname = p.schemaname
           JOIN pg_index AS i ON i.indexrelid = c.oid
           WHERE p.schemaname = 'public'"""
    )
    index_definitions = await index_rows.fetchall()
    indexes = {row[0] for row in index_definitions}
    missing_indexes = sorted(REQUIRED_OPERATION_INDEXES - indexes)
    if missing_indexes:
        raise RuntimeError(f"durable operation indexes are incomplete: {missing_indexes}")
    invalid_indexes = sorted(
        name
        for name, definition, valid in index_definitions
        if name in EXPECTED_OPERATION_INDEX_DEFINITIONS
        and (
            not valid
            or _normalize_catalog_sql(definition)
            != _normalize_catalog_sql(EXPECTED_OPERATION_INDEX_DEFINITIONS[name])
        )
    )
    if invalid_indexes:
        raise RuntimeError(f"durable operation index definitions are invalid: {invalid_indexes}")

    constraint_rows = await connection.execute(
        """SELECT conname, contype, convalidated,
                  pg_get_constraintdef(oid, true)
           FROM pg_constraint
           WHERE connamespace = 'public'::regnamespace"""
    )
    constraint_definitions = await constraint_rows.fetchall()
    constraints = {row[0] for row in constraint_definitions}
    missing_constraints = sorted(REQUIRED_OPERATION_CONSTRAINTS - constraints)
    if missing_constraints:
        raise RuntimeError(
            f"durable operation constraints are incomplete: {missing_constraints}"
        )
    invalid_constraints = sorted(
        name
        for name, contype, validated, definition in constraint_definitions
        if name in EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS
        and (
            not validated
            or (contype, _normalize_catalog_sql(definition))
            != (
                EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS[name][0],
                _normalize_catalog_sql(EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS[name][1]),
            )
        )
    )
    if invalid_constraints:
        raise RuntimeError(
            "durable operation constraint definitions are invalid: "
            f"{invalid_constraints}"
        )

    semantic_rows = await connection.execute(
        """SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
                  NOT a.attnotnull,
                  pg_get_expr(d.adbin, d.adrelid)
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
               AND NOT a.attisdropped
           LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
           WHERE n.nspname = 'public' AND c.relname = ANY(%s)""",
        (sorted(KERNEL_COLUMN_CONTRACT),),
    )
    actual_semantics = {
        (table, column): (type_name, nullable, default)
        for table, column, type_name, nullable, default in await semantic_rows.fetchall()
    }
    semantic_errors: list[str] = []
    for table, columns in KERNEL_COLUMN_CONTRACT.items():
        for column, expected in columns.items():
            actual = actual_semantics.get((table, column))
            if actual is None:
                semantic_errors.append(f"missing {table}.{column}")
                continue
            if actual != expected:
                semantic_errors.append(
                    f"{table}.{column} actual={actual!r} expected={expected!r}"
                )
    if semantic_errors:
        raise RuntimeError(
            "durable operation schema semantics mismatch: "
            + "; ".join(semantic_errors)
        )

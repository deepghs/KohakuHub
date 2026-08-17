#!/usr/bin/env python3
"""Apply KHub and Procrastinate schema exactly once before service startup."""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import psycopg
from procrastinate.schema import SchemaManager

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "src"))
# ``khub_migrate.py`` is also imported directly by migration tests.  Keep the
# sibling legacy runner import valid in both invocation modes.
sys.path.insert(0, str(SCRIPT_DIR))

from kohakuhub.config import cfg
from kohakuhub.migrations.schema import (
    expected_table_columns,
    is_exact_current_schema,
    kernel_semantic_diff,
    operation_schema_object_diff,
    procrastinate_schema_diff,
    signature_digest,
)
from kohakuhub.operations.sql import (
    OPERATION_SCHEMA_SQL,
    OPERATION_SCHEMA_VERSION,
    operation_table_columns_for_version,
)

LOCK_KEY = "kohakuhub.schema.lifecycle.v1"
RELEASED_OPERATION_SCHEMA_CHECKSUMS = {
    # 17a225a shipped schema v2 and signed all KHub tables together with the
    # operation tables. Keep the resulting ledger value immutable: deriving
    # it from current Peewee models would break upgrades after model changes.
    2: "edc67a3141b85e4b5dfff264609764e236d192d7086ba2d9b0571b49c381d23e",
}
LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS khub_schema_migrations (
    migration_name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


def _database_url() -> str:
    value = cfg.app.database_url
    if cfg.app.db_backend != "postgres" or not value.startswith(
        ("postgresql://", "postgres://")
    ):
        raise RuntimeError("khub-migrate requires KOHAKU_HUB_DB_BACKEND=postgres")
    return value


def _has_application_tables(connection: psycopg.Connection) -> bool:
    row = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name NOT LIKE 'procrastinate_%'
              AND table_name <> 'khub_schema_migrations'
        )
        """
    ).fetchone()
    return bool(row and row[0])


def _has_ledger(connection: psycopg.Connection) -> bool:
    row = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'khub_schema_migrations'
        )
        """
    ).fetchone()
    return bool(row and row[0])


def _schema_checksum() -> str:
    # Keep this checksum scoped to the operation kernel.  Ordinary KHub model
    # changes must not invalidate a worker migration that did not change.
    from kohakuhub.operations.sql import operation_table_columns

    return signature_digest(operation_table_columns())


def _legacy_schema_checksum() -> str:
    return signature_digest(expected_table_columns(include_operations=False))


def _record(connection: psycopg.Connection, name: str, version: int, checksum: str) -> None:
    connection.execute(
        """
        INSERT INTO khub_schema_migrations (migration_name, version, checksum)
        VALUES (%s, %s, %s)
        ON CONFLICT (migration_name) DO NOTHING
        """,
        (name, version, checksum),
    )
    row = connection.execute(
        """SELECT version, checksum
           FROM khub_schema_migrations
           WHERE migration_name = %s""",
        (name,),
    ).fetchone()
    if row is None or int(row[0]) != int(version) or row[1] != checksum:
        raise RuntimeError(
            f"immutable migration ledger mismatch for {name}: "
            f"recorded={row!r} expected={(version, checksum)!r}"
        )


def _assert_recorded_checksum(
    connection: psycopg.Connection,
    name: str,
    expected_checksum: str,
) -> None:
    row = connection.execute(
        """
        SELECT version, checksum
        FROM khub_schema_migrations
        WHERE migration_name = %s
        """,
        (name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"missing immutable migration ledger record for {name}; "
            "refusing to infer schema history"
        )
    if row[1] != expected_checksum:
        raise RuntimeError(
            f"schema checksum mismatch for {name}: "
            f"recorded={row[1]} expected={expected_checksum}"
        )


def _historical_operation_schema_checksum(version: int) -> str:
    """Return a checksum for a released kernel schema, never infer one."""

    try:
        signature = operation_table_columns_for_version(version)
    except ValueError as exc:
        raise RuntimeError(
            f"unsupported historical operation schema version {version}; "
            "upgrade from a supported durable-kernel release or rebuild explicitly"
        ) from exc
    released_checksum = RELEASED_OPERATION_SCHEMA_CHECKSUMS.get(version)
    if released_checksum is not None:
        return released_checksum
    return signature_digest(signature)


def _apply_procrastinate_schema(connection: psycopg.Connection) -> None:
    expected_tables = {
        "procrastinate_events",
        "procrastinate_jobs",
        "procrastinate_periodic_defers",
        "procrastinate_workers",
    }
    expected_types = {
        "procrastinate_job_status",
        "procrastinate_job_event_type",
        "procrastinate_job_to_defer_v1",
    }
    table_rows = connection.execute(
        """SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = 'public' AND table_name LIKE 'procrastinate_%'"""
    ).fetchall()
    type_rows = connection.execute(
        """SELECT typname
           FROM pg_type
           WHERE typnamespace = 'public'::regnamespace
             AND typname = ANY(%s)""",
        (list(expected_types),),
    ).fetchall()
    actual_tables = {row[0] for row in table_rows}
    actual_types = {row[0] for row in type_rows}
    if actual_tables or actual_types:
        missing_tables = sorted(expected_tables - actual_tables)
        missing_types = sorted(expected_types - actual_types)
        if missing_tables or missing_types:
            raise RuntimeError(
                "partial Procrastinate schema; refusing to guess DDL: "
                f"missing_tables={missing_tables}, missing_types={missing_types}"
            )
        catalog_diff = procrastinate_schema_diff(connection)
        if any(catalog_diff.values()):
            raise RuntimeError(
                "partial or incompatible Procrastinate schema; refusing to guess DDL: "
                f"diagnostic={catalog_diff}"
            )
        checksum = hashlib.sha256(SchemaManager.get_schema().encode("utf-8")).hexdigest()
        _assert_recorded_checksum(connection, "procrastinate-3.9.0", checksum)
        _record(connection, "procrastinate-3.9.0", 1, checksum)
        return
    schema_sql = SchemaManager.get_schema()
    with connection.cursor() as cursor:
        cursor.execute(schema_sql)
    checksum = hashlib.sha256(schema_sql.encode("utf-8")).hexdigest()
    _record(connection, "procrastinate-3.9.0", 1, checksum)


def _apply_operation_schema(connection: psycopg.Connection) -> None:
    current_name = f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION}"
    legacy = connection.execute(
        """SELECT version, checksum
           FROM khub_schema_migrations
           WHERE migration_name = 'khub-operation-kernel'"""
    ).fetchone()
    current = connection.execute(
        """SELECT version, checksum
           FROM khub_schema_migrations
           WHERE migration_name = %s""",
        (current_name,),
    ).fetchone()

    # Once any operation table exists, the table shape must be a known
    # released shape before the migration SQL is allowed to run.  This keeps
    # ``ADD COLUMN IF NOT EXISTS`` from turning an unknown partial schema into
    # something that merely looks current after a failed deployment.
    table_rows = connection.execute(
        """SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = 'public'
             AND table_name LIKE 'khub_%'"""
    ).fetchall()
    operation_tables = {
        row[0]
        for row in table_rows
        if row[0]
        in {
            "khub_repository_operations",
            "khub_operation_steps",
            "khub_commit_intents",
            "khub_quota_reservations",
        }
    }
    if operation_tables:
        recorded = current or legacy
        if recorded is None:
            raise RuntimeError(
                "operation tables exist without a known migration ledger record; "
                "refusing to run operation DDL"
            )
        expected = operation_table_columns_for_version(int(recorded[0]))
        expected_tables = set(expected)
        if operation_tables != expected_tables:
            raise RuntimeError(
                "operation schema tables do not match recorded version: "
                f"actual={sorted(operation_tables)} expected={sorted(expected_tables)}"
            )
        column_rows = connection.execute(
            """SELECT table_name, column_name
               FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = ANY(%s)""",
            (sorted(expected_tables),),
        ).fetchall()
        actual_columns: dict[str, set[str]] = {}
        for table, column in column_rows:
            actual_columns.setdefault(table, set()).add(column)
        column_diff = {
            table: {
                "missing": sorted(set(columns) - actual_columns.get(table, set())),
                "extra": sorted(actual_columns.get(table, set()) - set(columns)),
            }
            for table, columns in expected.items()
            if set(columns) != actual_columns.get(table, set())
        }
        if column_diff:
            raise RuntimeError(
                "operation schema columns do not match recorded version; "
                f"refusing DDL: {column_diff}"
            )
    if legacy is not None:
        expected_legacy = _historical_operation_schema_checksum(int(legacy[0]))
        if legacy[1] != expected_legacy:
            raise RuntimeError(
                "historical operation schema checksum mismatch: "
                f"recorded={legacy[1]} expected={expected_legacy}"
            )
    with connection.cursor() as cursor:
        cursor.execute(OPERATION_SCHEMA_SQL)
    _record(
        connection,
        current_name,
        OPERATION_SCHEMA_VERSION,
        _schema_checksum(),
    )


def migrate() -> None:
    url = _database_url()
    with psycopg.connect(url) as connection:
        connection.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (LOCK_KEY,))
        try:
            ledger_exists = _has_ledger(connection)
            has_tables = _has_application_tables(connection)

            if not ledger_exists and not has_tables:
                from run_migrations import run_migrations

                if not run_migrations():
                    raise RuntimeError("legacy KHub schema bootstrap failed")
                connection.rollback()
                has_tables = _has_application_tables(connection)

            if not ledger_exists and has_tables:
                exact, diff = is_exact_current_schema(
                    connection, include_operations=False
                )
                if not exact:
                    raise RuntimeError(
                        "cannot adopt unknown or partial schema; "
                        f"diagnostic={diff}"
                    )

            if not ledger_exists:
                connection.execute(LEDGER_DDL)
                _record(
                    connection,
                    "khub-current-adoption",
                    1,
                    _legacy_schema_checksum(),
                )
            else:
                _assert_recorded_checksum(
                    connection,
                    "khub-current-adoption",
                    _legacy_schema_checksum(),
                )

            _apply_procrastinate_schema(connection)
            _apply_operation_schema(connection)
            exact, diff = is_exact_current_schema(connection)
            if not exact:
                raise RuntimeError(f"schema mismatch after migration: {diff}")
            object_diff = operation_schema_object_diff(connection)
            if any(object_diff.values()):
                raise RuntimeError(
                    f"operation schema object mismatch after migration: {object_diff}"
                )
            semantic_diff = kernel_semantic_diff(connection)
            if any(semantic_diff.values()):
                raise RuntimeError(
                    f"operation schema semantic mismatch after migration: {semantic_diff}"
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (LOCK_KEY,)
            )


if __name__ == "__main__":
    migrate()

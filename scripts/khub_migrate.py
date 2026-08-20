#!/usr/bin/env python3
"""Durable-worker schema helpers owned by numbered migration 017."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import Any

from procrastinate.schema import SchemaManager

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "src"))
sys.path.insert(0, str(SCRIPT_DIR))

from kohakuhub.migrations.schema import (  # noqa: E402
    kernel_semantic_diff,
    operation_schema_object_diff,
    procrastinate_schema_diff,
    read_table_columns,
    schema_diff,
    signature_digest,
)
from kohakuhub.operations.sql import (  # noqa: E402
    OPERATION_SCHEMA_SQL,
    OPERATION_SCHEMA_VERSION,
    operation_table_columns,
    operation_table_columns_for_version,
)


LOCK_KEY = "kohakuhub.schema.lifecycle.v1"
RELEASED_OPERATION_SCHEMA_CHECKSUMS = {
    # 17a225a shipped schema v2 and signed all KHub tables together with the
    # operation tables. Keep the resulting ledger value immutable: deriving
    # it from current Peewee models would break upgrades after model changes.
    2: "edc67a3141b85e4b5dfff264609764e236d192d7086ba2d9b0571b49c381d23e",
}

# Migration 017 has a fixed post-016 application-schema precondition. It must
# not be recomputed from live Peewee models: model-only tables can be added by
# the runner's final init_db() pass after this migration.
APPLICATION_SCHEMA_CHECKSUM_WORKER = (
    "4e38e499aec1d9ccf55323ab3e4fa9ed58d7cd183dbe07d4bd3d8d8b89e663f8"
)
APPLICATION_TABLE_COLUMNS_WORKER = {
    "commit": (
        "author_id",
        "branch",
        "commit_id",
        "created_at",
        "description",
        "id",
        "message",
        "owner_id",
        "repo_type",
        "repository_id",
        "username",
    ),
    "confirmationtoken": (
        "action_data",
        "action_type",
        "created_at",
        "expires_at",
        "id",
        "token",
    ),
    "dailyrepostats": (
        "anonymous_downloads",
        "authenticated_downloads",
        "created_at",
        "date",
        "download_sessions",
        "id",
        "repository_id",
        "total_files",
    ),
    "downloadsession": (
        "file_count",
        "first_download_at",
        "first_file",
        "id",
        "last_download_at",
        "repository_id",
        "session_id",
        "time_bucket",
        "user_id",
    ),
    "emailverification": ("created_at", "expires_at", "id", "token", "user_id"),
    "file": (
        "created_at",
        "id",
        "is_deleted",
        "lfs",
        "owner_id",
        "path_in_repo",
        "repository_id",
        "sha256",
        "size",
        "updated_at",
    ),
    "fallbacksource": (
        "created_at",
        "enabled",
        "id",
        "name",
        "namespace",
        "priority",
        "source_type",
        "token",
        "updated_at",
        "url",
    ),
    "invitation": (
        "action",
        "created_at",
        "created_by_id",
        "expires_at",
        "id",
        "max_usage",
        "parameters",
        "token",
        "usage_count",
        "used_at",
        "used_by_id",
    ),
    "lfsobjecthistory": (
        "commit_id",
        "created_at",
        "file_id",
        "id",
        "path_in_repo",
        "repository_id",
        "sha256",
        "size",
    ),
    "repository": (
        "created_at",
        "downloads",
        "full_id",
        "id",
        "lakefs_repo",
        "lfs_keep_versions",
        "lfs_suffix_rules",
        "lfs_threshold_bytes",
        "likes_count",
        "name",
        "namespace",
        "owner_id",
        "private",
        "quota_bytes",
        "repo_type",
        "used_bytes",
    ),
    "repositorylike": ("created_at", "id", "repository_id", "user_id"),
    "session": ("created_at", "expires_at", "id", "secret", "session_id", "user_id"),
    "sshkey": (
        "created_at",
        "fingerprint",
        "id",
        "key_type",
        "last_used",
        "public_key",
        "title",
        "user_id",
    ),
    "stagingupload": (
        "created_at",
        "id",
        "lfs",
        "path_in_repo",
        "repo_type",
        "repository_id",
        "revision",
        "sha256",
        "size",
        "storage_key",
        "upload_id",
        "uploader_id",
    ),
    "token": ("created_at", "id", "last_used", "name", "token_hash", "user_id"),
    "user": (
        "avatar",
        "avatar_updated_at",
        "bio",
        "created_at",
        "description",
        "email",
        "email_verified",
        "full_name",
        "id",
        "is_active",
        "is_org",
        "normalized_name",
        "password_hash",
        "private_quota_bytes",
        "private_used_bytes",
        "public_quota_bytes",
        "public_used_bytes",
        "social_media",
        "username",
        "website",
    ),
    "userexternaltoken": (
        "created_at",
        "encrypted_token",
        "id",
        "updated_at",
        "url",
        "user_id",
    ),
    "userorganization": ("created_at", "id", "organization_id", "role", "user_id"),
}
PROCRASTINATE_SCHEMA_CHECKSUM_WORKER = (
    "c70ec4b400a60ad9592787653aae5ae77ae41bf801d56b5bd2712751a07f2009"
)
OPERATION_SCHEMA_VERSION_WORKER = 8
OPERATION_SCHEMA_CHECKSUM_WORKER = (
    "3143de9bba6022a7f4372a3be8cb2c8a7bd7c3e2ecb0a1b46ca046652118afd3"
)

KNOWN_OPERATION_TABLES_WORKER = frozenset(
    table
    for version in (2, 3, 6, 7, OPERATION_SCHEMA_VERSION_WORKER)
    for table in operation_table_columns_for_version(version)
)

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS khub_schema_migrations (
    migration_name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


class _PeeweeConnectionAdapter:
    """Expose the psycopg-like interface required by the schema helpers."""

    def __init__(self, database: Any):
        self.database = database

    def execute(self, query: str, params: Any = None):
        cursor = self.database.cursor()
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)
        return cursor

    def cursor(self):
        return self.database.cursor()


def _schema_checksum() -> str:
    return signature_digest(operation_table_columns())


def _procrastinate_schema_checksum() -> str:
    return hashlib.sha256(SchemaManager.get_schema().encode("utf-8")).hexdigest()


def _worker_application_schema_diff(connection: Any) -> dict[str, Any]:
    # The application adoption record is scoped to the model-owned tables.
    # Known durable-kernel tables may already exist when a pre-release worker
    # schema is being recognized; their exact shape is checked separately by
    # ``_apply_operation_schema`` and ``_verify_worker_schema``.
    actual = {
        table: columns
        for table, columns in read_table_columns(connection).items()
        if table not in KNOWN_OPERATION_TABLES_WORKER
    }
    return schema_diff(APPLICATION_TABLE_COLUMNS_WORKER, actual)


def _assert_worker_application_schema(connection: Any) -> None:
    diff = _worker_application_schema_diff(connection)
    if any(diff.values()):
        raise RuntimeError(
            "migration 017 requires the application schema produced by main; "
            f"diagnostic={diff}"
        )


def _assert_worker_runtime_inputs() -> None:
    operation_checksum = _schema_checksum()
    procrastinate_checksum = _procrastinate_schema_checksum()
    if (
        OPERATION_SCHEMA_VERSION != OPERATION_SCHEMA_VERSION_WORKER
        or operation_checksum != OPERATION_SCHEMA_CHECKSUM_WORKER
        or procrastinate_checksum != PROCRASTINATE_SCHEMA_CHECKSUM_WORKER
    ):
        raise RuntimeError(
            "migration 017 is frozen to its released worker schema; add a new "
            "numbered migration for the current Procrastinate or operation schema"
        )


def _record(connection: Any, name: str, version: int, checksum: str) -> None:
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
    connection: Any,
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
            "unsupported historical operation schema version "
            f"{version}; upgrade from a supported durable-kernel release"
        ) from exc
    released_checksum = RELEASED_OPERATION_SCHEMA_CHECKSUMS.get(version)
    if released_checksum is not None:
        return released_checksum
    return signature_digest(signature)


def _apply_procrastinate_schema(connection: Any) -> None:
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
        _assert_recorded_checksum(
            connection,
            "procrastinate-3.9.0",
            PROCRASTINATE_SCHEMA_CHECKSUM_WORKER,
        )
        _record(
            connection,
            "procrastinate-3.9.0",
            1,
            PROCRASTINATE_SCHEMA_CHECKSUM_WORKER,
        )
        return

    schema_sql = SchemaManager.get_schema()
    with connection.cursor() as cursor:
        cursor.execute(schema_sql)
    _record(
        connection,
        "procrastinate-3.9.0",
        1,
        PROCRASTINATE_SCHEMA_CHECKSUM_WORKER,
    )


def _apply_operation_schema(connection: Any) -> None:
    current_name = f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION}"
    ledger_rows = connection.execute(
        """SELECT migration_name, version, checksum
           FROM khub_schema_migrations
           WHERE migration_name = 'khub-operation-kernel'
              OR migration_name LIKE 'khub-operation-kernel-v%'"""
    ).fetchall()
    operation_records: list[tuple[str, int, str]] = []
    for name, version, checksum in ledger_rows:
        version = int(version)
        if name.startswith("khub-operation-kernel-v"):
            suffix = name.removeprefix("khub-operation-kernel-v")
            if not suffix.isdigit() or int(suffix) != version:
                raise RuntimeError(
                    "operation schema ledger name/version mismatch: "
                    f"name={name!r} version={version}"
                )
        operation_records.append((name, version, checksum))
    recorded = max(operation_records, key=lambda item: item[1], default=None)

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
        if recorded is None:
            raise RuntimeError(
                "operation tables exist without a known migration ledger record; "
                "refusing to run operation DDL"
            )
        expected = operation_table_columns_for_version(recorded[1])
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
    if recorded is not None:
        expected_checksum = _historical_operation_schema_checksum(recorded[1])
        if recorded[2] != expected_checksum:
            raise RuntimeError(
                "operation schema checksum mismatch: "
                f"recorded={recorded[2]} expected={expected_checksum}"
            )
    with connection.cursor() as cursor:
        cursor.execute(OPERATION_SCHEMA_SQL)
    _record(
        connection,
        current_name,
        OPERATION_SCHEMA_VERSION,
        _schema_checksum(),
    )


def _verify_worker_schema(
    connection: Any, *, verify_application_schema: bool = True
) -> None:
    if verify_application_schema:
        _assert_worker_application_schema(connection)
    procrastinate_diff = procrastinate_schema_diff(connection)
    if any(procrastinate_diff.values()):
        raise RuntimeError(
            f"Procrastinate schema mismatch after migration: {procrastinate_diff}"
        )
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


def _expected_worker_ledger_records() -> dict[str, tuple[int, str]]:
    return {
        "khub-current-adoption": (1, APPLICATION_SCHEMA_CHECKSUM_WORKER),
        "procrastinate-3.9.0": (1, PROCRASTINATE_SCHEMA_CHECKSUM_WORKER),
        (f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION_WORKER}"): (
            OPERATION_SCHEMA_VERSION_WORKER,
            OPERATION_SCHEMA_CHECKSUM_WORKER,
        ),
    }


def worker_schema_migration_is_applied(connection: Any) -> bool:
    """Return whether migration 017's immutable ledger records exist."""

    expected = _expected_worker_ledger_records()
    try:
        rows = connection.execute(
            """
            SELECT migration_name, version, checksum
            FROM khub_schema_migrations
            WHERE migration_name = ANY(%s)
            """,
            (list(expected),),
        ).fetchall()
    except Exception:
        return False
    actual = {name: (int(version), checksum) for name, version, checksum in rows}
    return actual == expected


def apply_worker_schema_migration(connection: Any) -> None:
    """Apply migration 017 using the caller-owned transaction."""

    _assert_worker_runtime_inputs()
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (LOCK_KEY,)
    )
    _assert_worker_application_schema(connection)
    connection.execute(LEDGER_DDL)
    _record(
        connection,
        "khub-current-adoption",
        1,
        APPLICATION_SCHEMA_CHECKSUM_WORKER,
    )
    _apply_procrastinate_schema(connection)
    _apply_operation_schema(connection)
    _verify_worker_schema(connection, verify_application_schema=False)


def migrate() -> None:
    """Run the post-main numbered migration stream used by deployment Compose."""

    # This entry point is retained for CI and development callers.  Sending it
    # through the runner keeps fresh bootstraps and main-to-worker upgrades on
    # the identical 017+ migration path.
    from run_migrations import run_migrations

    if not run_migrations():
        raise RuntimeError("KohakuHub numbered migration chain failed")


if __name__ == "__main__":
    migrate()

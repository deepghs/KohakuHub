#!/usr/bin/env python3
"""Durable-worker schema helpers owned by numbered migration 017."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "src"))
sys.path.insert(0, str(SCRIPT_DIR))

from db_migrations._017_schema import (  # noqa: E402
    EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS_V8,
    EXPECTED_OPERATION_INDEX_DEFINITIONS_V8,
    HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
    HISTORICAL_OPERATION_TABLE_COLUMNS_V3,
    OPERATION_COLUMN_CONTRACT_V8,
    OPERATION_SCHEMA_SQL_V8,
    OPERATION_TABLE_COLUMNS_V6,
    OPERATION_TABLE_COLUMNS_V7,
    OPERATION_TABLE_COLUMNS_V8,
    PROCRASTINATE_SCHEMA_SQL_V390,
    PROCRASTINATE_REQUIRED_INDEXES_V390,
    PROCRASTINATE_TABLE_COLUMNS_V390,
    PROCRASTINATE_TYPES_V390,
)
from kohakuhub.migrations.schema import (  # noqa: E402
    kernel_semantic_diff,
    operation_schema_object_diff,
    procrastinate_schema_diff,
    read_table_columns,
    schema_diff,
    signature_digest,
)
from kohakuhub.migrations.worker_contract import (  # noqa: E402
    APPLICATION_SCHEMA_ADOPTION_CHECKSUM_V1,
    WORKER_OPERATION_SCHEMA_CHECKSUM_V1,
    WORKER_OPERATION_SCHEMA_VERSION_V1,
    WORKER_PROCRASTINATE_SCHEMA_CHECKSUM_V390,
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
APPLICATION_SCHEMA_CHECKSUM_WORKER = APPLICATION_SCHEMA_ADOPTION_CHECKSUM_V1
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
PROCRASTINATE_SCHEMA_CHECKSUM_WORKER = WORKER_PROCRASTINATE_SCHEMA_CHECKSUM_V390
OPERATION_SCHEMA_VERSION_WORKER = WORKER_OPERATION_SCHEMA_VERSION_V1
OPERATION_SCHEMA_CHECKSUM_WORKER = WORKER_OPERATION_SCHEMA_CHECKSUM_V1

KNOWN_OPERATION_TABLES_WORKER = frozenset(
    table
    for signature in (
        HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
        HISTORICAL_OPERATION_TABLE_COLUMNS_V3,
        OPERATION_TABLE_COLUMNS_V6,
        OPERATION_TABLE_COLUMNS_V7,
        OPERATION_TABLE_COLUMNS_V8,
    )
    for table in signature
)

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS khub_schema_migrations (
    migration_name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

# ``fallbacksource`` was owned by the application model before it had a
# numbered migration. It is the only model-owned table that a database from
# the early 001-016 compatibility path can legitimately lack at the 017
# boundary. Keep this bootstrap list frozen and explicit: calling the current
# ``init_db()`` here would create tables belonging to a future migration before
# that migration gets a chance to run.
WORKER_APPLICATION_COMPATIBILITY_SQL = (
    """
    CREATE TABLE IF NOT EXISTS fallbacksource (
        id SERIAL PRIMARY KEY,
        namespace VARCHAR(255) NOT NULL DEFAULT '',
        url VARCHAR(255) NOT NULL,
        token VARCHAR(255),
        priority INTEGER NOT NULL DEFAULT 100,
        name VARCHAR(255) NOT NULL,
        source_type VARCHAR(255) NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS fallbacksource_namespace
    ON fallbacksource(namespace)
    """,
    """
    CREATE INDEX IF NOT EXISTS fallbacksource_priority
    ON fallbacksource(priority)
    """,
    """
    CREATE INDEX IF NOT EXISTS fallbacksource_enabled
    ON fallbacksource(enabled)
    """,
    """
    CREATE INDEX IF NOT EXISTS fallbacksource_namespace_priority
    ON fallbacksource(namespace, priority)
    """,
    """
    CREATE INDEX IF NOT EXISTS fallbacksource_enabled_priority
    ON fallbacksource(enabled, priority)
    """,
)


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
    return OPERATION_SCHEMA_CHECKSUM_WORKER


def _procrastinate_schema_checksum() -> str:
    return PROCRASTINATE_SCHEMA_CHECKSUM_WORKER


def bootstrap_worker_application_compatibility(connection: Any) -> None:
    """Create only the known pre-017 model table missing from old releases."""

    with connection.cursor() as cursor:
        for statement in WORKER_APPLICATION_COMPATIBILITY_SQL:
            cursor.execute(statement)


def _worker_application_schema_diff(connection: Any) -> dict[str, Any]:
    # The application adoption record is scoped to the model-owned tables.
    # Known durable-kernel tables may already exist when a pre-release worker
    # schema is being recognized; their exact shape is checked separately by
    # ``_apply_operation_schema`` and ``_verify_worker_schema``.
    # This is an adoption boundary, so it must stay independent of the current
    # Peewee model. Future application tables and columns belong to a later
    # numbered migration and must not be silently accepted by 017.
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
    """Verify the immutable schema snapshot used by migration 017."""

    application_checksum = signature_digest(APPLICATION_TABLE_COLUMNS_WORKER)
    operation_checksum = signature_digest(OPERATION_TABLE_COLUMNS_V8)
    procrastinate_checksum = hashlib.sha256(
        PROCRASTINATE_SCHEMA_SQL_V390.encode("utf-8")
    ).hexdigest()
    if (
        application_checksum != APPLICATION_SCHEMA_CHECKSUM_WORKER
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

    signature = _operation_table_columns_for_version(version)
    released_checksum = RELEASED_OPERATION_SCHEMA_CHECKSUMS.get(version)
    if released_checksum is not None:
        return released_checksum
    return signature_digest(signature)


def _operation_table_columns_for_version(
    version: int,
) -> dict[str, tuple[str, ...]]:
    """Return the frozen operation table contract for a released version."""

    contracts = {
        2: HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
        3: HISTORICAL_OPERATION_TABLE_COLUMNS_V3,
        6: OPERATION_TABLE_COLUMNS_V6,
        7: OPERATION_TABLE_COLUMNS_V7,
        OPERATION_SCHEMA_VERSION_WORKER: OPERATION_TABLE_COLUMNS_V8,
    }
    try:
        return contracts[version]
    except KeyError as exc:
        raise RuntimeError(
            "unsupported historical operation schema version "
            f"{version}; upgrade from a supported durable-kernel release"
        ) from exc


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
        catalog_diff = procrastinate_schema_diff(
            connection,
            table_columns=PROCRASTINATE_TABLE_COLUMNS_V390,
            required_indexes=PROCRASTINATE_REQUIRED_INDEXES_V390,
            types=PROCRASTINATE_TYPES_V390,
        )
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

    schema_sql = PROCRASTINATE_SCHEMA_SQL_V390
    with connection.cursor() as cursor:
        cursor.execute(schema_sql)
    _record(
        connection,
        "procrastinate-3.9.0",
        1,
        PROCRASTINATE_SCHEMA_CHECKSUM_WORKER,
    )


def _apply_operation_schema(connection: Any) -> None:
    current_name = f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION_WORKER}"
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
        expected = _operation_table_columns_for_version(recorded[1])
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
        cursor.execute(OPERATION_SCHEMA_SQL_V8)
    _record(
        connection,
        current_name,
        OPERATION_SCHEMA_VERSION_WORKER,
        _schema_checksum(),
    )


def _verify_worker_schema(
    connection: Any, *, verify_application_schema: bool = True
) -> None:
    if verify_application_schema:
        _assert_worker_application_schema(connection)
    procrastinate_diff = procrastinate_schema_diff(
        connection,
        table_columns=PROCRASTINATE_TABLE_COLUMNS_V390,
        required_indexes=PROCRASTINATE_REQUIRED_INDEXES_V390,
        types=PROCRASTINATE_TYPES_V390,
    )
    if any(procrastinate_diff.values()):
        raise RuntimeError(
            f"Procrastinate schema mismatch after migration: {procrastinate_diff}"
        )
    object_diff = operation_schema_object_diff(
        connection,
        index_definitions=EXPECTED_OPERATION_INDEX_DEFINITIONS_V8,
        constraint_definitions=EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS_V8,
    )
    if any(object_diff.values()):
        raise RuntimeError(
            f"operation schema object mismatch after migration: {object_diff}"
        )
    semantic_diff = kernel_semantic_diff(
        connection,
        column_contract=OPERATION_COLUMN_CONTRACT_V8,
    )
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
    # A missing ledger is the normal first-run state. Query its existence via
    # information_schema first; selecting from a missing table would abort the
    # caller's PostgreSQL transaction before the installer can create it.
    ledger_exists = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'khub_schema_migrations'
        """
    ).fetchone()
    if ledger_exists is None:
        return False
    rows = connection.execute(
        """
        SELECT migration_name, version, checksum
        FROM khub_schema_migrations
        WHERE migration_name = ANY(%s)
        """,
        (list(expected),),
    ).fetchall()
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

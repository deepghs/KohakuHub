#!/usr/bin/env python3
"""
Migration 018: Timeline, progress, cancellation and logs for background tasks.

Changes:
- background_task: stall_seconds, cancel_requested, progress_* columns, checkpoint
- Add background_task_event table (one row per lifecycle event of a task)
- Add background_task_log table (log records captured while a task ran)

The DDL mirrors what Peewee generates for ``BackgroundTask``,
``BackgroundTaskEvent`` and ``BackgroundTaskLog`` so databases created by
init_db() and databases upgraded here end up identical. See issue #111.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
# Add db_migrations to path (for _migration_utils)
sys.path.insert(0, os.path.dirname(__file__))

from kohakuhub.config import cfg
from kohakuhub.db import db
from _migration_utils import (
    check_column_exists,
    check_table_exists,
    should_skip_due_to_future_migrations,
)

MIGRATION_NUMBER = 18

# (column, postgres type, sqlite type). NOT NULL columns need a default so
# existing rows can be upgraded; Peewee itself creates them without one.
TASK_COLUMNS = [
    ("stall_seconds", "INTEGER", "INTEGER"),
    ("cancel_requested", "BOOLEAN NOT NULL DEFAULT FALSE", "INTEGER NOT NULL DEFAULT 0"),
    ("progress_done", "BIGINT", "INTEGER"),
    ("progress_total", "BIGINT", "INTEGER"),
    ("progress_stage", "VARCHAR(255)", "VARCHAR(255)"),
    ("progress_at", "TIMESTAMP", "DATETIME"),
    ("progress_base_done", "BIGINT", "INTEGER"),
    ("progress_base_at", "TIMESTAMP", "DATETIME"),
    ("checkpoint", "TEXT", "TEXT"),
]

EVENT_COLUMNS = """
    "task_id" {bigint} NOT NULL,
    "at" {ts} NOT NULL,
    "type" VARCHAR(32) NOT NULL,
    "attempt" INTEGER NOT NULL,
    "worker" VARCHAR(255),
    "detail" TEXT,
    FOREIGN KEY ("task_id") REFERENCES "background_task" ("id") ON DELETE CASCADE
"""

LOG_COLUMNS = """
    "task_id" {bigint} NOT NULL,
    "attempt" INTEGER NOT NULL,
    "at" {ts} NOT NULL,
    "level" VARCHAR(16) NOT NULL,
    "message" TEXT NOT NULL,
    FOREIGN KEY ("task_id") REFERENCES "background_task" ("id") ON DELETE CASCADE
"""

INDEXES = [
    'CREATE INDEX IF NOT EXISTS "backgroundtaskevent_task_id" '
    'ON "background_task_event" ("task_id")',
    'CREATE INDEX IF NOT EXISTS "backgroundtasklog_task_id" '
    'ON "background_task_log" ("task_id")',
]


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Returns True once the last object it creates, background_task_log, exists.
    """
    return check_table_exists(db, "background_task_log")


def _migrate(postgres: bool):
    cursor = db.cursor()
    id_column = (
        '"id" BIGSERIAL NOT NULL PRIMARY KEY' if postgres else '"id" INTEGER NOT NULL PRIMARY KEY'
    )
    types = {
        "bigint": "BIGINT" if postgres else "INTEGER",
        "ts": "TIMESTAMP" if postgres else "DATETIME",
    }

    print("Adding background_task columns...")
    for column, postgres_type, sqlite_type in TASK_COLUMNS:
        if check_column_exists(db, cfg, "background_task", column):
            continue
        column_type = postgres_type if postgres else sqlite_type
        cursor.execute(f'ALTER TABLE "background_task" ADD COLUMN "{column}" {column_type}')
    print("  ✓ Added background_task columns")

    print("Creating background_task_event and background_task_log tables...")
    cursor.execute(
        f'CREATE TABLE IF NOT EXISTS "background_task_event" ({id_column},'
        f"{EVENT_COLUMNS.format(**types)})"
    )
    cursor.execute(
        f'CREATE TABLE IF NOT EXISTS "background_task_log" ({id_column},'
        f"{LOG_COLUMNS.format(**types)})"
    )
    for statement in INDEXES:
        cursor.execute(statement)
    print("  ✓ Created tables and indexes")


def migrate_postgres():
    """Upgrade the background task schema in PostgreSQL."""
    _migrate(postgres=True)


def migrate_sqlite():
    """Upgrade the background task schema in SQLite."""
    _migrate(postgres=False)


def run():
    """Run migration 018.

    Returns:
        True if successful or already applied, False otherwise
    """
    db.connect(reuse_if_open=True)

    try:
        # Check if should skip due to future migrations
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print(f"Migration {MIGRATION_NUMBER}: Skipped (superseded by future migration)")
            return True

        # Check if already applied
        if is_applied(db, cfg):
            print(
                f"Migration {MIGRATION_NUMBER}: Already applied "
                "(background_task_log table exists)"
            )
            return True

        print("=" * 70)
        print(f"Migration {MIGRATION_NUMBER}: Background task timeline, progress and logs")
        print("=" * 70)

        # Run migration in transaction
        with db.atomic():
            if cfg.app.db_backend == "postgres":
                migrate_postgres()
            else:
                migrate_sqlite()

        print("\n" + "=" * 70)
        print(f"Migration {MIGRATION_NUMBER}: ✓ Completed Successfully")
        print("=" * 70)
        print("\nSummary:")
        print("  • Added progress, cancellation and checkpoint columns to background_task")
        print("  • Added background_task_event and background_task_log tables")
        return True

    except Exception as e:
        print(f"\n✗ Migration {MIGRATION_NUMBER} failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    run()

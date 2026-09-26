#!/usr/bin/env python3
"""
Migration 017: Add the background_task table for the khub-worker task queue.

Changes:
- Add background_task table (durable background tasks, see kohakuhub.tasks)
- Add the unique dedupe_key index and the claim/recovery/retention indexes

The DDL mirrors what Peewee generates for ``BackgroundTask`` so databases
created by init_db() and databases upgraded here end up identical.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
# Add db_migrations to path (for _migration_utils)
sys.path.insert(0, os.path.dirname(__file__))

from kohakuhub.config import cfg
from kohakuhub.db import db
from _migration_utils import check_table_exists, should_skip_due_to_future_migrations

MIGRATION_NUMBER = 17

COLUMNS = """
    "kind" VARCHAR(255) NOT NULL,
    "queue" VARCHAR(64) NOT NULL,
    "payload" TEXT NOT NULL,
    "status" VARCHAR(16) NOT NULL,
    "priority" INTEGER NOT NULL,
    "dedupe_key" VARCHAR(255),
    "run_after" {ts} NOT NULL,
    "attempts" INTEGER NOT NULL,
    "max_attempts" INTEGER NOT NULL,
    "locked_by" VARCHAR(255),
    "locked_until" {ts},
    "last_error" TEXT,
    "created_at" {ts} NOT NULL,
    "started_at" {ts},
    "finished_at" {ts}
"""

INDEXES = [
    'CREATE UNIQUE INDEX IF NOT EXISTS "backgroundtask_dedupe_key" '
    'ON "background_task" ("dedupe_key")',
    'CREATE INDEX IF NOT EXISTS "backgroundtask_status_queue_run_after" '
    'ON "background_task" ("status", "queue", "run_after")',
    'CREATE INDEX IF NOT EXISTS "backgroundtask_status_locked_until" '
    'ON "background_task" ("status", "locked_until")',
    'CREATE INDEX IF NOT EXISTS "backgroundtask_status_finished_at" '
    'ON "background_task" ("status", "finished_at")',
]


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Returns True if the background_task table exists.
    """
    return check_table_exists(db, "background_task")


def _create(id_column, timestamp_type):
    cursor = db.cursor()

    print("Creating background_task table...")
    cursor.execute(
        f'CREATE TABLE IF NOT EXISTS "background_task" ({id_column},'
        f"{COLUMNS.format(ts=timestamp_type)})"
    )
    print("  ✓ Created background_task table")

    print("Creating indexes...")
    for statement in INDEXES:
        cursor.execute(statement)
    print("  ✓ Created indexes")


def migrate_postgres():
    """Create background_task table in PostgreSQL."""
    _create('"id" BIGSERIAL NOT NULL PRIMARY KEY', "TIMESTAMP")


def migrate_sqlite():
    """Create background_task table in SQLite."""
    _create('"id" INTEGER NOT NULL PRIMARY KEY', "DATETIME")


def run():
    """Run migration 017.

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
            print(f"Migration {MIGRATION_NUMBER}: Already applied (background_task table exists)")
            return True

        print("=" * 70)
        print(f"Migration {MIGRATION_NUMBER}: Add background_task table")
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
        print("  • Added background_task table for the khub-worker task queue")
        return True

    except Exception as e:
        print(f"\n✗ Migration {MIGRATION_NUMBER} failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    run()

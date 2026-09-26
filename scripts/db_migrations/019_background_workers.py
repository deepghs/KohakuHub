#!/usr/bin/env python3
"""
Migration 019: Add the background_worker table (the worker roster).

Changes:
- Add background_worker table: one row per khub-worker process, registered
  at startup and refreshed by heartbeats, so the admin panel can list the
  workers and tell which are alive (see kohakuhub.tasks.worker_status)

The DDL mirrors what Peewee generates for ``BackgroundWorker`` so databases
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

MIGRATION_NUMBER = 19

COLUMNS = """
    "id" VARCHAR(255) NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "hostname" VARCHAR(255) NOT NULL,
    "pid" INTEGER NOT NULL,
    "queues" TEXT NOT NULL,
    "concurrency" INTEGER NOT NULL,
    "state" VARCHAR(16) NOT NULL,
    "succeeded" {bigint} NOT NULL,
    "failed" {bigint} NOT NULL,
    "started_at" {ts} NOT NULL,
    "last_heartbeat_at" {ts} NOT NULL,
    "stopped_at" {ts}
"""


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Returns True if the background_worker table exists.
    """
    return check_table_exists(db, "background_worker")


def _create(bigint, timestamp_type):
    cursor = db.cursor()
    print("Creating background_worker table...")
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS "background_worker" ('
        f"{COLUMNS.format(bigint=bigint, ts=timestamp_type)})"
    )
    print("  ✓ Created background_worker table")


def migrate_postgres():
    """Create background_worker table in PostgreSQL."""
    _create("BIGINT", "TIMESTAMP")


def migrate_sqlite():
    """Create background_worker table in SQLite."""
    _create("INTEGER", "DATETIME")


def run():
    """Run migration 019.

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
            print(f"Migration {MIGRATION_NUMBER}: Already applied (background_worker table exists)")
            return True

        print("=" * 70)
        print(f"Migration {MIGRATION_NUMBER}: Add background_worker table")
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
        print("  • Added background_worker table (the khub-worker roster)")
        return True

    except Exception as e:
        print(f"\n✗ Migration {MIGRATION_NUMBER} failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    run()

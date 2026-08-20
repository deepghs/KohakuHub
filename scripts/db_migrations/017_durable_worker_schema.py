#!/usr/bin/env python3
"""Migration 017: install the PostgreSQL durable-worker schema.

This migration intentionally does not repair or reinterpret older application
schemas. Databases already at the released ``main`` application schema reach it
after no-op historical checks. Pre-main databases retain the historical
001-016 compatibility path. The one known model-only table absent from that
numbered path is bootstrapped explicitly immediately before the frozen
migration-017 precondition check.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from _migration_utils import should_skip_due_to_future_migrations
from khub_migrate import (  # noqa: E402
    _PeeweeConnectionAdapter,
    apply_worker_schema_migration,
    bootstrap_worker_application_compatibility,
    worker_schema_migration_is_applied,
)
from kohakuhub.config import cfg  # noqa: E402
from kohakuhub.db import db  # noqa: E402


MIGRATION_NUMBER = 17


def is_applied(database, config) -> bool:
    """Return whether migration 017 has recorded its immutable PostgreSQL DDL."""

    if config.app.db_backend != "postgres":
        return False
    return worker_schema_migration_is_applied(_PeeweeConnectionAdapter(database))


def check_migration_needed() -> bool:
    return not is_applied(db, cfg)


def run() -> bool:
    """Apply the durable-worker schema without changing legacy migrations."""

    db.connect(reuse_if_open=True)
    try:
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print("Migration 017: Skipped (superseded by future migration)")
            return True
        if cfg.app.db_backend != "postgres":
            print("Migration 017: Not applicable to SQLite")
            return True
        if not check_migration_needed():
            # Once the immutable worker ledger exists, later numbered
            # migrations own any schema extensions. Re-validating against the
            # current application/kernel definitions here would let a future
            # release make an already-applied 017 fail before its own code
            # runs.
            print("Migration 017: Already applied")
            return True
        # Some application tables present on main were introduced through the
        # normal model bootstrap rather than a numbered legacy migration. Keep
        # the explicit compatibility bootstrap in the same PostgreSQL
        # transaction as the frozen boundary check and worker DDL, so a
        # rejected schema cannot leave behind partial application tables.
        with db.atomic():
            bootstrap_worker_application_compatibility(_PeeweeConnectionAdapter(db))
            print("Migration 017: Installing durable PostgreSQL worker schema...")
            apply_worker_schema_migration(_PeeweeConnectionAdapter(db))
        print("Migration 017: Completed")
        return True
    except Exception as exc:
        print(f"Migration 017: Failed - {exc}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    sys.exit(0 if run() else 1)

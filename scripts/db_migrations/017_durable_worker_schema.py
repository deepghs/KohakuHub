#!/usr/bin/env python3
"""Migration 017: install the PostgreSQL durable-worker schema.

This migration intentionally does not repair or reinterpret older application
schemas. Databases already at the released ``main`` application schema reach it
after no-op historical checks. Pre-main databases retain the historical
001-016 compatibility path. Model-only tables absent from that numbered path
are created immediately before the frozen migration-017 precondition check.
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
    _verify_worker_schema,
    apply_worker_schema_migration,
    worker_schema_migration_is_applied,
)
from kohakuhub.config import cfg  # noqa: E402
from kohakuhub.db import db, init_db  # noqa: E402


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
        # Some application tables present on main were introduced through the
        # normal model bootstrap rather than a numbered legacy migration. The
        # 001-016 chain has now finished, so it is safe to create those missing
        # tables before checking the frozen worker-schema boundary.
        init_db()
        if not check_migration_needed():
            # The frozen application boundary is only a precondition for the
            # first 017 install. Later numbered migrations may extend the
            # application schema while this worker schema remains applied.
            _verify_worker_schema(
                _PeeweeConnectionAdapter(db), verify_application_schema=False
            )
            print("Migration 017: Already applied")
            return True

        print("Migration 017: Installing durable PostgreSQL worker schema...")
        with db.atomic():
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

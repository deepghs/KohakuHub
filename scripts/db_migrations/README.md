# Database Migrations

This directory contains database migration scripts for KohakuHub.

## Upgrade Boundary

Migration `017` adds the durable worker and operation schema. It is appended
to the original numbered upgrade path. The runner discovers the whole numbered
chain in order; on a database already at `main`, migrations `001`-`016` verify
their signatures and return without changing data, then `017` installs the new
schema. Their numbering and order remain the historical compatibility contract;
the few corrections described below preserve that contract while making the
old path executable on real data.

There is no `018` migration in the current repository. That number remains
available for the next independent schema change. The released `main` schema
is the pre-worker boundary: historical checks are no-ops there and the first
effective change is `017`.

The numbering and upgrade order of `001`-`016` are unchanged. A small number
of existing scripts contain compatibility corrections required to complete
their originally documented upgrade on real SQLite/PostgreSQL data (for
example, the `008` organization-ID remap and SQLite's `012` table rebuild);
these corrections do not introduce a new migration boundary.

## How Migrations Work

1. **Auto-detection**: Each migration checks if it needs to run by verifying its schema/ledger signature
2. **Sequential execution**: The runner evaluates every numbered migration in order; already-applied migrations return without mutation
3. **Idempotent**: Safe to run multiple times - already-applied migrations are skipped
4. **Auto-run**: Compose runs the one-shot `khub-migrate` service before API and worker startup
5. **Compatibility boundary**: A released `main` schema makes the historical
   checks no-ops, so its first effective change is `017`; older schemas use the
   numbered compatibility path.

## Migration Order

| # | Name | Description | Notes |
|---|------|-------------|-------|
| 001-016 | historical application changes | Compatibility path for pre-main deployments | Run when needed |
| 017 | durable_worker_schema | Install PostgreSQL durable worker and operation schema | Requires `main` application schema |

## Migration 008 Schema Refactoring

Migration 008 is the major refactoring in the preserved pre-`main` upgrade
path.

**Migration 008 is a major schema refactoring that:**
- Merges the Organization table into User table (adds `is_org` flag)
- Converts all integer ID references to proper ForeignKey constraints
- Adds denormalized owner fields for performance

**If you have an existing database:** back it up and run the normal migration
runner. Databases already at `main` pass through no-op checks before `017`;
older databases use the preserved `001`-`016` compatibility path first.

**For fresh/new databases:** run the same numbered runner. It creates the
current application tables, then applies migration `017` on PostgreSQL; no
special migration path is required.

## Creating New Migrations

1. Create a new file: `scripts/db_migrations/00X_name.py`
2. Implement these functions:
   - `MIGRATION_NUMBER` - Constant with migration number (e.g., 9)
   - `is_applied(db, cfg)` - Check if THIS migration has been applied (for future migrations to detect)
   - `check_migration_needed()` - Returns True if migration should run
   - `migrate_sqlite()` - SQLite migration logic
   - `migrate_postgres()` - PostgreSQL migration logic
   - `run()` - Main entry point that uses `should_skip_due_to_future_migrations()`

3. Template:
```python
#!/usr/bin/env python3
"""Migration 00X: Description"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
# Add db_migrations to path (for _migration_utils)
sys.path.insert(0, os.path.dirname(__file__))

from kohakuhub.db import db
from kohakuhub.config import cfg
from _migration_utils import should_skip_due_to_future_migrations, check_column_exists, check_table_exists

# IMPORTANT: Do NOT import Peewee models (User, Repository, etc.)
# Models may be renamed/deleted in future versions, breaking old migrations.
# Use raw SQL queries instead.

MIGRATION_NUMBER = X  # Replace X with actual number


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    This function is called by older migrations to detect if this migration
    has already applied their changes. Choose a unique signature column/table.

    Returns True if this migration is applied, False otherwise.
    A known-absent signature may return False. Unexpected database or loader
    errors must propagate so the migration cannot silently skip required work.
    """
    # Example: Check if a signature column/table exists
    return check_column_exists(db, cfg, "mytable", "mycolumn")


def check_migration_needed():
    """Check if this migration needs to run."""
    cursor = db.cursor()
    if cfg.app.db_backend == "postgres":
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='mytable' AND column_name='mycolumn'
        """)
        return cursor.fetchone() is None
    else:
        cursor.execute("PRAGMA table_info(mytable)")
        columns = [row[1] for row in cursor.fetchall()]
        return 'mycolumn' not in columns


def migrate_sqlite():
    cursor = db.cursor()
    cursor.execute("ALTER TABLE mytable ADD COLUMN mycolumn INTEGER")
    db.commit()


def migrate_postgres():
    cursor = db.cursor()
    cursor.execute("ALTER TABLE mytable ADD COLUMN mycolumn BIGINT")
    db.commit()


def run():
    db.connect(reuse_if_open=True)
    try:
        # Check the explicit migration-008 supersession boundary.
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print("Migration 00X: Skipped (superseded by future migration)")
            return True

        if not check_migration_needed():
            print("Migration 00X: Already applied")
            return True

        print("Migration 00X: Running...")
        if cfg.app.db_backend == "postgres":
            migrate_postgres()
        else:
            migrate_sqlite()
        print("Migration 00X: ✓ Completed")
        return True
    except Exception as e:
        print(f"Migration 00X: ✗ Failed - {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
```

## Running Migrations

**Automatic (in Docker):**
- The one-shot `khub-migrate` service runs the numbered migration chain before API and worker startup

**Manual:**
```bash
# Run all pending migrations
python scripts/run_migrations.py

# Run the current migration directly
python scripts/db_migrations/017_durable_worker_schema.py
```

## Best Practices

### For Fresh Databases
Run the same numbered migration service used for upgrades. It creates the
application schema and, on PostgreSQL, installs the durable worker schema at
`017`:

```bash
docker compose run --rm khub-migrate
```

Do not delete an existing database as a migration strategy; the runner is the
supported path for both fresh and existing deployments.

### For Existing Databases
1. **Backup first!** Always backup before running migrations
2. Keep the database on a consistent released schema
3. Run migrations via the automatic startup process
4. Monitor logs for migration `017` and later post-main migrations

### For Development
```bash
# Run all pending migrations
python scripts/run_migrations.py

# Run the current migration directly
python scripts/db_migrations/017_durable_worker_schema.py
```

## Migration System Design

The `001`-`016` files remain available for pre-main databases. The runner loads
the complete numbered chain in order. Each historical migration checks its own
signature, while `001`-`007` may also be superseded by a complete `008`. Thus a
released `main` database reaches `017` without historical mutation, and an older
database receives the missing changes before reaching `017`.

### Historical Supersession Detection

Migrations `001`-`007` can be superseded by a complete migration `008`, because
`008` contains the earlier application-schema changes. The helper deliberately
does not treat migrations `009`-`016` as interchangeable: they are independent
changes and must run when their own schema is missing.

**How it works:**
1. Migration 003 is about to run
2. It checks whether migration 008's complete post-refactoring signature exists
3. If that signature is complete, migration 003 is skipped
4. Otherwise migration 003 runs and reports its own result

**Benefits:**
- Keeps the only known historical supersession boundary explicit
- Prevents an unrelated later migration from hiding a failed upgrade
- Treats a known-absent signature as "not applied"; errors loading or
  evaluating the supersession check abort the migration run

**Each migration must implement:**
```python
def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Choose a unique signature (table or column) that this migration creates.
    Return False only for a known-absent signature. Propagate unexpected
    database errors so the migration is attempted visibly and can fail.
    """
    return check_column_exists(db, cfg, "mytable", "my_signature_column")
```

### Important Guidelines

#### DO NOT Import Peewee Models
**Never import models like `User`, `Repository`, `Organization`, etc. in migrations!**

```python
# ❌ BAD - Will break if model is renamed/deleted
from kohakuhub.db import db, User, Organization
db.create_tables([User], safe=True)

# ✅ GOOD - Use raw SQL instead
from kohakuhub.db import db
cursor = db.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS user (...)")
```

**Why?**
- Migrations are permanent historical records
- Models may be renamed, deleted, or refactored in future versions
- Importing models creates tight coupling that breaks old migrations
- Example: Migration 002 imported `Organization`, which no longer exists after migration 008

**Use raw SQL for all schema changes:**
- Table creation: `CREATE TABLE IF NOT EXISTS`
- Column addition: `ALTER TABLE ... ADD COLUMN`
- Index creation: `CREATE INDEX IF NOT EXISTS`

## Notes

- Migrations are idempotent - safe to re-run
- Failed migrations will prevent server startup
- Only `001`-`007` may be superseded by a complete `008` signature
- Use raw SQL queries instead of importing Peewee models
- Errors in the historical supersession check abort the migration run
- Old migration scripts in `scripts/migrate_*.py` are kept for reference

## Utilities (`_migration_utils.py`)

Common helper functions available to all migrations:

```python
from _migration_utils import (
    should_skip_due_to_future_migrations,  # Check if future migrations applied
    check_table_exists,                     # Check if table exists
    check_column_exists,                    # Check if column exists
)

# Usage in migration
if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
    print("Migration skipped - superseded by future migration")
    return True
```

## Troubleshooting

### Error: "column repository_id does not exist"

This error indicates the database is in an inconsistent state (mix of old and new schema).

**Cause:** The database has some tables created with the new schema (post-migration 008), but migrations never ran to update the data properly.

**Solution:** Stop API/worker startup, take a database backup, and rerun the
numbered migration runner. It will either complete the 008 compatibility
conversion atomically or stop with a diagnostic before `017`; it does not
silently continue on a partial schema.

**Prevention:** Always run migrations before application starts (handled automatically in Docker)

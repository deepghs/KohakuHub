#!/usr/bin/env python3
"""
Main migration runner for KohakuHub database migrations.

This script automatically discovers and runs all migration scripts in the
db_migrations/ directory in numerical order (001, 002, 003, etc.).

Each migration script should:
1. Have a run() function that returns True on success, False on failure
2. Auto-detect if it needs to run (check if columns exist)
3. Handle both SQLite and PostgreSQL

Usage:
    python scripts/run_migrations.py
"""

import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path

# Add src to path
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "src"))

# Import after path setup
from kohakuhub.db import db, init_db  # noqa: E402
from kohakuhub.config import cfg  # noqa: E402


MIGRATION_LOCK_KEY = "kohakuhub.schema.lifecycle.v1"


def discover_migrations():
    """Discover all migration scripts in db_migrations/ directory.

    Returns:
        List of (name, path) tuples sorted by name
    """
    migrations_dir = SCRIPT_DIR / "db_migrations"
    if not migrations_dir.exists():
        return []

    migrations = []
    for file_path in migrations_dir.glob("*.py"):
        if file_path.name.startswith("_"):
            continue  # Skip __init__.py and private files
        number, separator, _ = file_path.stem.partition("_")
        if not separator or not number.isdecimal():
            continue
        migrations.append((file_path.stem, file_path))

    # Sort by the numeric prefix so both ``017_...`` and an unpadded future
    # ``17_...`` remain in the same migration order.
    migrations.sort(key=lambda x: (int(x[0].partition("_")[0]), x[0]))
    return migrations


def load_migration_module(name, path):
    """Dynamically load a migration module.

    Args:
        name: Module name
        path: Path to migration file

    Returns:
        Loaded module or None if failed
    """
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"  [ERROR] Failed to load {name}: {e}")
        return None


def is_database_initialized():
    """Check if database is initialized (has User table).

    Returns:
        True if User table exists (database is initialized)
        False if User table doesn't exist (fresh database)
    """
    db.connect(reuse_if_open=True)
    cursor = db.cursor()

    if cfg.app.db_backend == "postgres":
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name='user'
            """
        )
        return cursor.fetchone() is not None

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user'")
    return cursor.fetchone() is not None


@contextmanager
def migration_lock():
    """Serialize the complete PostgreSQL migration lifecycle."""

    if cfg.app.db_backend != "postgres":
        yield
        return

    db.connect(reuse_if_open=True)
    cursor = db.cursor()
    cursor.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
        (MIGRATION_LOCK_KEY,),
    )
    try:
        yield
    finally:
        cursor = db.cursor()
        cursor.execute(
            "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
            (MIGRATION_LOCK_KEY,),
        )


def run_migrations():
    """Run all pending migrations."""
    with migration_lock():
        return _run_migrations_locked()


def _run_migrations_locked():
    """Run the original numbered migration chain while holding its DB lock."""
    print("=" * 70)
    print("KohakuHub Database Migrations")
    print("=" * 70)
    print(f"Database backend: {cfg.app.db_backend}")
    print(f"Database URL: {cfg.app.database_url}")
    print()

    # A fresh database still runs numbered migrations. This is required for
    # PostgreSQL-only migrations such as 017, which init_db() cannot install.
    if not is_database_initialized():
        print("Database is uninitialized (User table doesn't exist)")
        print("Initializing the current schema before numbered migrations")
        print("\nInitializing database (creating all tables)...")
        init_db()
        print("✓ Database initialized with current schema\n")
    else:
        print("Database is initialized, checking for pending migrations...\n")

    # Discover migrations
    migrations = discover_migrations()
    if not migrations:
        print("No migrations found in db_migrations/")
        print("\nEnsuring database schema is up-to-date...")
        init_db()
        print("✓ Database schema verified\n")
        return True

    print(f"Found {len(migrations)} migration(s):\n")

    # Run each migration
    all_success = True
    for name, path in migrations:
        print(f"Running {name}...")

        # Load migration module
        module = load_migration_module(name, path)
        if not module:
            all_success = False
            break

        # Check if module has run() function
        if not hasattr(module, "run"):
            print(f"  [ERROR] Migration {name} missing run() function")
            all_success = False
            break

        # Run migration
        try:
            success = module.run()
            if not success:
                all_success = False
                print(f"  [ERROR] Stopping after failed migration {name}")
                break
        except Exception as e:
            print(f"  [ERROR] Migration {name} crashed: {e}")
            import traceback

            traceback.print_exc()
            all_success = False
            print(f"  [ERROR] Stopping after crashed migration {name}")
            break

        print()

    # Initialize database AFTER migrations (create tables/indexes if needed)
    if all_success:
        print("\nFinalizing database schema (ensuring all tables/indexes exist)...")
        try:
            init_db()
            print("[OK] Database schema finalized\n")
        except Exception as e:
            print(f"[ERROR] Failed to finalize database schema: {e}")
            import traceback

            traceback.print_exc()
            all_success = False

    # Summary
    print("=" * 70)
    if all_success:
        print("[OK] All migrations completed successfully!")
    else:
        print("[ERROR] Some migrations failed - please check errors above")
    print("=" * 70)

    return all_success


def main():
    try:
        success = run_migrations()
        return 0 if success else 1
    except Exception as e:
        print(f"\n[ERROR] Migration runner crashed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

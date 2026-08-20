#!/usr/bin/env python3
"""
Shared utilities for database migrations.

This module provides common functionality for checking migration status
without importing any Peewee models (to avoid breaking old migrations).
"""

import importlib.util
import sys
from pathlib import Path


def should_skip_due_to_future_migrations(
    current_migration_number: int, db, cfg
) -> bool:
    """Skip only migrations genuinely superseded by migration 008.

    Args:
        current_migration_number: The number of the current migration (e.g., 3 for migration 003)
        db: Database connection object
        cfg: Config object with db_backend property

    Migrations 001-007 feed the schema consolidation performed by 008, so a
    completed 008 can safely supersede them. Migrations 009 onward are
    independent changes: seeing any one later column/table must not suppress
    another migration. The old broad scan caused partially upgraded databases
    to skip required migrations merely because, for example, 016's model
    column already existed.
    """
    if not 1 <= current_migration_number < 8:
        return False

    module_name = "_migration_008_supersession_check"
    migration_path = Path(__file__).parent / "008_foreignkey_refactoring.py"
    try:
        spec = importlib.util.spec_from_file_location(module_name, migration_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(
                f"unable to load migration-008 supersession check from {migration_path}"
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return bool(module.is_applied(db, cfg))
    finally:
        sys.modules.pop(module_name, None)


def check_table_exists(db, table_name: str) -> bool:
    """Check if a table exists in the database.

    Args:
        db: Database connection object
        table_name: Name of the table to check

    Returns:
        True if table exists, False otherwise
    """
    try:
        return db.table_exists(table_name)
    except Exception:
        return False


def check_column_exists(db, cfg, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table.

    Args:
        db: Database connection object
        cfg: Config object with db_backend property
        table_name: Name of the table
        column_name: Name of the column to check

    Returns:
        True if column exists, False otherwise
    """
    try:
        cursor = db.cursor()
        if cfg.app.db_backend == "postgres":
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s
            """,
                (table_name, column_name),
            )
            return cursor.fetchone() is not None
        else:
            # SQLite
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            return column_name in columns
    except Exception:
        return False

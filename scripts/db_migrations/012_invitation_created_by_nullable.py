#!/usr/bin/env python3
"""
Migration 012: Make Invitation.created_by nullable.

This allows system/admin-generated invitations to have created_by=NULL
instead of requiring a fake user ID.

Changes:
- Invitation.created_by: Make nullable
- Update any invitations with invalid created_by to NULL
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
# Add db_migrations to path (for _migration_utils)
sys.path.insert(0, os.path.dirname(__file__))

from kohakuhub.db import db
from kohakuhub.config import cfg
from _migration_utils import should_skip_due_to_future_migrations, check_table_exists

MIGRATION_NUMBER = 12


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Returns True if Invitation.created_by is already nullable.
    """
    try:
        cursor = db.cursor()
        if cfg.app.db_backend == "postgres":
            cursor.execute(
                """
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name='invitation' AND column_name='created_by_id'
                """
            )
            result = cursor.fetchone()
            # is_nullable = 'YES' means it's nullable
            return result and result[0] == "YES"
        else:
            # SQLite doesn't have a direct way to check nullability
            # We'll check if table has been recreated by checking for the field
            # If migration hasn't run, we assume not nullable
            cursor.execute("PRAGMA table_info(invitation)")
            columns = {row[1]: row for row in cursor.fetchall()}
            if "created_by_id" in columns:
                # Column exists, check notnull flag (0=nullable, 1=not null)
                notnull = columns["created_by_id"][3]
                return notnull == 0  # 0 means nullable
            return False
    except Exception:
        # An uncertain signature must be retried and fail visibly.
        return False


def migrate_postgres():
    """Make Invitation.created_by nullable in PostgreSQL."""
    cursor = db.cursor()

    print("Making Invitation.created_by_id nullable...")

    try:
        cursor.execute(
            "ALTER TABLE invitation ALTER COLUMN created_by_id DROP NOT NULL"
        )
        print("  ✓ Made Invitation.created_by_id nullable")
    except Exception as e:
        if "does not exist" in str(e).lower() or "column" in str(e).lower():
            print(f"  - Column might not exist or already nullable: {e}")
        else:
            raise

    # Update any invitations with invalid created_by to NULL
    print("  Cleaning up invalid created_by references...")
    cursor.execute(
        """
        UPDATE invitation
        SET created_by_id = NULL
        WHERE created_by_id NOT IN (SELECT id FROM "user")
        """
    )
    affected = cursor.rowcount
    if affected > 0:
        print(
            f"    ✓ Set created_by_id=NULL for {affected} invitation(s) with invalid references"
        )
    else:
        print("    - No invalid references found")

    return True


def migrate_sqlite():
    """Make Invitation.created_by nullable in SQLite.

    SQLite requires table recreation to change column constraints.
    """
    cursor = db.cursor()
    foreign_keys = cursor.execute("PRAGMA foreign_keys").fetchone()[0]
    legacy_alter_table = cursor.execute("PRAGMA legacy_alter_table").fetchone()[0]
    cursor.execute("PRAGMA foreign_keys = OFF")
    cursor.execute("PRAGMA legacy_alter_table = ON")
    try:
        cursor.execute("BEGIN IMMEDIATE")
        _sqlite_invitation_rebuild(cursor)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.execute(f"PRAGMA legacy_alter_table = {legacy_alter_table}")
        cursor.execute(f"PRAGMA foreign_keys = {foreign_keys}")
    return True


def _sqlite_invitation_rebuild(cursor):
    """Rebuild Invitation with nullable creator while preserving its contract."""

    cursor.execute("ALTER TABLE invitation RENAME TO __khub_012_old_invitation")
    for row in cursor.execute(
        'PRAGMA index_list("__khub_012_old_invitation")'
    ).fetchall():
        index_name = row[1]
        if not index_name.startswith("sqlite_autoindex_"):
            escaped = index_name.replace('"', '""')
            cursor.execute(f'DROP INDEX "{escaped}"')
    cursor.execute(
        """
        CREATE TABLE invitation (
            id INTEGER NOT NULL PRIMARY KEY,
            token VARCHAR(255) NOT NULL UNIQUE,
            action VARCHAR(255) NOT NULL,
            parameters TEXT NOT NULL,
            created_by_id INTEGER REFERENCES "user"(id) ON DELETE CASCADE,
            expires_at DATETIME NOT NULL,
            max_usage INTEGER,
            usage_count INTEGER NOT NULL DEFAULT 0,
            used_at DATETIME,
            used_by_id INTEGER REFERENCES "user"(id) ON DELETE SET NULL,
            created_at DATETIME NOT NULL
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO invitation (
            id, token, action, parameters, created_by_id, expires_at,
            max_usage, usage_count, used_at, used_by_id, created_at
        )
        SELECT
            old.id,
            old.token,
            old.action,
            old.parameters,
            CASE WHEN creator.id IS NULL THEN NULL ELSE old.created_by_id END,
            old.expires_at,
            old.max_usage,
            old.usage_count,
            old.used_at,
            CASE WHEN used.id IS NULL THEN NULL ELSE old.used_by_id END,
            old.created_at
        FROM __khub_012_old_invitation old
        LEFT JOIN "user" creator ON creator.id = old.created_by_id
        LEFT JOIN "user" used ON used.id = old.used_by_id
        """
    )
    cursor.execute("CREATE INDEX invitation_token ON invitation(token)")
    cursor.execute("CREATE INDEX invitation_action ON invitation(action)")
    cursor.execute("CREATE INDEX invitation_created_by_id ON invitation(created_by_id)")
    cursor.execute("CREATE INDEX invitation_used_by_id ON invitation(used_by_id)")
    cursor.execute(
        "CREATE INDEX invitation_action_created_by_id "
        "ON invitation(action, created_by_id)"
    )
    cursor.execute("DROP TABLE __khub_012_old_invitation")
    violations = cursor.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise RuntimeError(
            f"foreign key violations after migration 012: {violations[:5]}"
        )


def run():
    """Run this migration.

    Returns:
        True if successful, False otherwise
    """
    db.connect(reuse_if_open=True)

    try:
        # Check if should skip due to future migrations
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print("Migration 012: Skipped (superseded by future migration)")
            return True

        # Check if table exists
        if not check_table_exists(db, "invitation"):
            print("Migration 012: Skipped (invitation table doesn't exist yet)")
            return True

        # Check if already applied
        if is_applied(db, cfg):
            print("Migration 012: Already applied (Invitation.created_by is nullable)")
            return True

        print("=" * 70)
        print("Migration 012: Make Invitation.created_by nullable")
        print("=" * 70)

        if cfg.app.db_backend == "postgres":
            with db.atomic():
                result = migrate_postgres()
        else:
            result = migrate_sqlite()

        if result:
            print("\n" + "=" * 70)
            print("Migration 012: ✓ Completed Successfully")
            print("=" * 70)

        return result

    except Exception as e:
        print(f"\nMigration 012: ✗ Failed - {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""Compatibility repair for the immutable pre-017 invitation schema."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from kohakuhub.config import cfg  # noqa: E402
from kohakuhub.db import db  # noqa: E402


def _is_nullable(cursor) -> bool:
    if cfg.app.db_backend == "postgres":
        cursor.execute(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'invitation'
              AND column_name = 'created_by_id'
            """
        )
        row = cursor.fetchone()
        return row is not None and row[0] == "YES"

    columns = {
        row[1]: row
        for row in cursor.execute("PRAGMA table_info(invitation)").fetchall()
    }
    return "created_by_id" in columns and columns["created_by_id"][3] == 0


def _rebuild_sqlite_invitation(cursor) -> None:
    cursor.execute("ALTER TABLE invitation RENAME TO __khub_012_old_invitation")
    for row in cursor.execute(
        'PRAGMA index_list("__khub_012_old_invitation")'
    ).fetchall():
        index_name = row[1]
        if not index_name.startswith("sqlite_autoindex_"):
            cursor.execute(f'DROP INDEX "{index_name.replace(chr(34), chr(34) * 2)}"')

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
    for statement in (
        "CREATE INDEX invitation_token ON invitation(token)",
        "CREATE INDEX invitation_action ON invitation(action)",
        "CREATE INDEX invitation_created_by_id ON invitation(created_by_id)",
        "CREATE INDEX invitation_used_by_id ON invitation(used_by_id)",
        "CREATE INDEX invitation_action_created_by_id "
        "ON invitation(action, created_by_id)",
    ):
        cursor.execute(statement)
    cursor.execute("DROP TABLE __khub_012_old_invitation")
    violations = cursor.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise RuntimeError(
            f"foreign key violations after invitation compatibility repair: "
            f"{violations[:5]}"
        )


def repair_legacy_invitation_schema() -> bool:
    """Make ``Invitation.created_by_id`` nullable and normalize bad refs."""

    db.connect(reuse_if_open=True)
    cursor = db.cursor()
    if not db.table_exists("invitation") or _is_nullable(cursor):
        return True

    if cfg.app.db_backend == "postgres":
        with db.atomic():
            cursor.execute(
                'ALTER TABLE invitation ALTER COLUMN created_by_id DROP NOT NULL'
            )
            cursor.execute(
                """
                UPDATE invitation
                SET created_by_id = NULL
                WHERE created_by_id NOT IN (SELECT id FROM "user")
                """
            )
        return True

    foreign_keys = cursor.execute("PRAGMA foreign_keys").fetchone()[0]
    legacy_alter_table = cursor.execute("PRAGMA legacy_alter_table").fetchone()[0]
    cursor.execute("PRAGMA foreign_keys = OFF")
    cursor.execute("PRAGMA legacy_alter_table = ON")
    try:
        cursor.execute("BEGIN IMMEDIATE")
        _rebuild_sqlite_invitation(cursor)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.execute(f"PRAGMA legacy_alter_table = {legacy_alter_table}")
        cursor.execute(f"PRAGMA foreign_keys = {foreign_keys}")
    return True

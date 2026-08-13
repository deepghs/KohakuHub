#!/usr/bin/env python3
"""
Migration 016: Persist the LakeFS repository id on Repository.

Adds:
- Repository: lakefs_repo (NULL = derive from repo_type + full_id, legacy behaviour)

Why: the LakeFS repository id is currently derived from the repo id on every
call (utils/lakefs.py: lakefs_repo_name). Because the derivation is
deterministic, a repo id that is freed by a rename maps straight back to the
LakeFS repository the rename just deleted. LakeFS deletes repositories
asynchronously, so re-creating the freed name inside the cleanup window fails
with `409 not unique` (see issue #93). Storing the id lets it be allocated
instead of derived, so a new repository under a just-freed name can take a
fresh id and never collide with a pending deletion.

Existing rows are backfilled with the currently-derived value, so the stored id
keeps pointing at the LakeFS repository that already holds the data.

NOTE: the derivation below is a FROZEN COPY of lakefs_repo_name() as of this
migration, deliberately not an import. A future change to that function must not
retroactively change what this backfill writes for repositories that were
created under the old scheme.
"""

import hashlib
import os
import re
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
# Add db_migrations to path (for _migration_utils)
sys.path.insert(0, os.path.dirname(__file__))

from kohakuhub.db import db
from kohakuhub.config import cfg
from _migration_utils import should_skip_due_to_future_migrations, check_column_exists

MIGRATION_NUMBER = 16

_BASE36_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"


def _frozen_base36(num: int) -> str:
    """Base36-encode with the 0-9a-z alphabet (matches numpy.base_repr(...).lower())."""
    if num == 0:
        return "0"
    out = []
    while num:
        num, rem = divmod(num, 36)
        out.append(_BASE36_DIGITS[rem])
    return "".join(reversed(out))


def _frozen_hash_to_112bit(data: str) -> int:
    """SHA3-224 folded to 112 bits by XORing its two halves."""
    digest = hashlib.sha3_224(data.encode()).digest()  # 28 bytes
    return int.from_bytes(digest[:14], "big") ^ int.from_bytes(digest[14:], "big")


def _frozen_sanitize_repo_id(repo_id: str) -> str:
    """Reduce a repo id to LakeFS-safe characters (a-z, 0-9, hyphen)."""
    safe = repo_id.replace("/", "-").replace("_", "-").replace(".", "-")
    safe = re.sub(r"[^a-z0-9-]", "-", safe.lower())
    safe = re.sub(r"-+", "-", safe)
    return safe.strip("-")


def frozen_lakefs_repo_name(repo_type: str, repo_id: str) -> str:
    """Frozen copy of lakefs_repo_name: {type_char}-{safe_id[:38]}-{hash_b36:>022}."""
    type_char = {"model": "m", "dataset": "d", "space": "s"}.get(repo_type, "m")
    safe_id = _frozen_sanitize_repo_id(repo_id)[:38]
    hash_suffix = _frozen_base36(_frozen_hash_to_112bit(repo_id)).zfill(22)
    return f"{type_char}-{safe_id}-{hash_suffix}"


def is_applied(db, cfg):
    """Check if THIS migration has been applied.

    Returns True if Repository.lakefs_repo column exists.
    """
    return check_column_exists(db, cfg, "repository", "lakefs_repo")


def check_migration_needed():
    """Check if this migration needs to run by checking if the column exists."""
    cursor = db.cursor()

    if cfg.app.db_backend == "postgres":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='repository' AND column_name='lakefs_repo'
        """
        )
        return cursor.fetchone() is None
    else:
        # SQLite: Check via PRAGMA
        cursor.execute("PRAGMA table_info(repository)")
        columns = [row[1] for row in cursor.fetchall()]
        return "lakefs_repo" not in columns


def _backfill_lakefs_repo():
    """Fill lakefs_repo for rows that do not have it yet.

    Runs inside the caller's transaction. Values are computed from the frozen
    derivation so they match the LakeFS repositories that already exist.
    """
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, repo_type, namespace, name FROM repository WHERE lakefs_repo IS NULL"
    )
    rows = cursor.fetchall()

    if not rows:
        print("  - No rows to backfill")
        return

    placeholder = "%s" if cfg.app.db_backend == "postgres" else "?"
    update_sql = (
        f"UPDATE repository SET lakefs_repo = {placeholder} "
        f"WHERE id = {placeholder} AND lakefs_repo IS NULL"
    )

    seen = {}
    duplicates = 0
    for repo_id, repo_type, namespace, name in rows:
        derived = frozen_lakefs_repo_name(repo_type, f"{namespace}/{name}")
        if derived in seen:
            # Two rows deriving the same id would mean the existing scheme already
            # had them sharing one LakeFS repository. Report instead of failing:
            # the column is nullable and reads fall back to the derivation, so a
            # duplicate is no worse than today's behaviour.
            duplicates += 1
            print(
                f"  ! Duplicate derived id {derived}: "
                f"row {repo_id} collides with row {seen[derived]}"
            )
        else:
            seen[derived] = repo_id
        cursor.execute(update_sql, (derived, repo_id))

    print(f"  ✓ Backfilled Repository.lakefs_repo for {len(rows)} row(s)")
    if duplicates:
        print(
            f"  ! {duplicates} duplicate derived id(s) found — do NOT add a unique "
            f"constraint until these are resolved"
        )
    else:
        print(f"  ✓ All {len(rows)} backfilled id(s) are distinct")


def migrate_sqlite():
    """Migrate SQLite database.

    Note: This function runs inside a transaction (db.atomic()).
    Do NOT call db.commit() or db.rollback() inside this function.
    """
    cursor = db.cursor()

    try:
        cursor.execute(
            "ALTER TABLE repository ADD COLUMN lakefs_repo VARCHAR(255) DEFAULT NULL"
        )
        print("  ✓ Added Repository.lakefs_repo")
    except Exception as e:
        if "duplicate column" in str(e).lower():
            print("  - Repository.lakefs_repo already exists")
        else:
            raise

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS repository_lakefs_repo ON repository (lakefs_repo)"
    )
    print("  ✓ Ensured index repository_lakefs_repo")

    _backfill_lakefs_repo()


def migrate_postgres():
    """Migrate PostgreSQL database.

    Note: This function runs inside a transaction (db.atomic()).
    Do NOT call db.commit() or db.rollback() inside this function.
    """
    cursor = db.cursor()

    try:
        cursor.execute(
            "ALTER TABLE repository ADD COLUMN lakefs_repo VARCHAR(255) DEFAULT NULL"
        )
        print("  ✓ Added Repository.lakefs_repo")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  - Repository.lakefs_repo already exists")
            # Don't need to rollback - the exception will propagate and rollback
            # the entire transaction
        else:
            raise

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS repository_lakefs_repo ON repository (lakefs_repo)"
    )
    print("  ✓ Ensured index repository_lakefs_repo")

    _backfill_lakefs_repo()


def run():
    """Run this migration.

    IMPORTANT: Do NOT call db.close() in finally block!
    The db connection is managed by run_migrations.py and should stay open
    across all migrations to avoid stdout/stderr closure issues on Windows.
    """
    db.connect(reuse_if_open=True)

    try:
        # Pre-flight checks (outside transaction for performance)
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print("Migration 016: Skipped (superseded by future migration)")
            return True

        if not check_migration_needed():
            print("Migration 016: Already applied (column exists)")
            return True

        print("Migration 016: Adding Repository.lakefs_repo...")

        # Run migration in a transaction - will auto-rollback on exception
        with db.atomic():
            if cfg.app.db_backend == "postgres":
                migrate_postgres()
            else:
                migrate_sqlite()

        print("Migration 016: ✓ Completed")
        return True

    except Exception as e:
        # Transaction automatically rolled back if we reach here
        print(f"Migration 016: ✗ Failed - {e}")
        print("  All changes have been rolled back")
        import traceback

        traceback.print_exc()
        return False
    # NOTE: No finally block - db connection stays open


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)

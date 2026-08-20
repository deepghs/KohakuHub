#!/usr/bin/env python3
"""Migration 008: merge users/organizations and install relation fields.

The released migration could leave a database half migrated: SQLite inserted
organizations before making credentials nullable, and neither backend
installed the complete relation schema used by the application. This version
recognizes only the complete contract as applied and converts atomically.
"""

from __future__ import annotations

import os
import hashlib
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from _migration_utils import should_skip_due_to_future_migrations  # noqa: E402
from kohakuhub.config import cfg  # noqa: E402
from kohakuhub.db import db  # noqa: E402


MIGRATION_NUMBER = 8

_BASE36_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"


def _frozen_base36(num: int) -> str:
    if num == 0:
        return "0"
    out = []
    while num:
        num, remainder = divmod(num, 36)
        out.append(_BASE36_DIGITS[remainder])
    return "".join(reversed(out))


def _frozen_hash_to_112bit(data: str) -> int:
    digest = hashlib.sha3_224(data.encode()).digest()
    return int.from_bytes(digest[:14], "big") ^ int.from_bytes(digest[14:], "big")


def _frozen_sanitize_repo_id(repo_id: str) -> str:
    safe = repo_id.replace("/", "-").replace("_", "-").replace(".", "-")
    safe = re.sub(r"[^a-z0-9-]", "-", safe.lower())
    safe = re.sub(r"-+", "-", safe)
    return safe.strip("-")


def frozen_lakefs_repo_name(repo_type: str, repo_id: str) -> str:
    """Return the migration-016 LakeFS id derivation without live imports."""

    type_char = {"model": "m", "dataset": "d", "space": "s"}.get(repo_type, "m")
    safe_id = _frozen_sanitize_repo_id(repo_id)[:38]
    hash_suffix = _frozen_base36(_frozen_hash_to_112bit(repo_id)).zfill(22)
    return f"{type_char}-{safe_id}-{hash_suffix}"


RELATION_CONTRACT = (
    ("emailverification", "user_id", "user", "CASCADE"),
    ("session", "user_id", "user", "CASCADE"),
    ("token", "user_id", "user", "CASCADE"),
    ("repository", "owner_id", "user", "CASCADE"),
    ("file", "repository_id", "repository", "CASCADE"),
    ("file", "owner_id", "user", "CASCADE"),
    ("stagingupload", "repository_id", "repository", "CASCADE"),
    ("stagingupload", "uploader_id", "user", "SET NULL"),
    ("userorganization", "user_id", "user", "CASCADE"),
    ("userorganization", "organization_id", "user", "CASCADE"),
    ("commit", "repository_id", "repository", "CASCADE"),
    ("commit", "author_id", "user", "CASCADE"),
    ("commit", "owner_id", "user", "CASCADE"),
    ("lfsobjecthistory", "repository_id", "repository", "CASCADE"),
    ("lfsobjecthistory", "file_id", "file", "SET NULL"),
    ("sshkey", "user_id", "user", "CASCADE"),
    ("invitation", "created_by_id", "user", "CASCADE"),
    ("invitation", "used_by_id", "user", "SET NULL"),
)

SQLITE_TABLES = {
    "user": """
        CREATE TABLE "user" (
            id INTEGER NOT NULL PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            normalized_name VARCHAR(255) NOT NULL UNIQUE,
            is_org INTEGER NOT NULL DEFAULT 0,
            email VARCHAR(255) UNIQUE,
            password_hash VARCHAR(255),
            email_verified INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            private_quota_bytes INTEGER,
            public_quota_bytes INTEGER,
            private_used_bytes INTEGER NOT NULL DEFAULT 0,
            public_used_bytes INTEGER NOT NULL DEFAULT 0,
            full_name VARCHAR(255),
            bio TEXT,
            description TEXT,
            website VARCHAR(255),
            social_media TEXT,
            avatar BLOB,
            avatar_updated_at DATETIME,
            created_at DATETIME NOT NULL
        )
    """,
    "repository": """
        CREATE TABLE repository (
            id INTEGER NOT NULL PRIMARY KEY,
            repo_type VARCHAR(255) NOT NULL,
            namespace VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            full_id VARCHAR(255) NOT NULL,
            lakefs_repo VARCHAR(255),
            private INTEGER NOT NULL DEFAULT 0,
            owner_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            quota_bytes INTEGER,
            used_bytes INTEGER NOT NULL DEFAULT 0,
            lfs_threshold_bytes INTEGER,
            lfs_keep_versions INTEGER,
            lfs_suffix_rules TEXT,
            downloads INTEGER NOT NULL DEFAULT 0,
            likes_count INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            UNIQUE(repo_type, namespace, name)
        )
    """,
    "emailverification": """
        CREATE TABLE emailverification (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            token VARCHAR(255) NOT NULL UNIQUE,
            expires_at DATETIME NOT NULL,
            created_at DATETIME NOT NULL
        )
    """,
    "session": """
        CREATE TABLE session (
            id INTEGER NOT NULL PRIMARY KEY,
            session_id VARCHAR(255) NOT NULL UNIQUE,
            user_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            secret VARCHAR(255) NOT NULL,
            expires_at DATETIME NOT NULL,
            created_at DATETIME NOT NULL
        )
    """,
    "token": """
        CREATE TABLE token (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            token_hash VARCHAR(255) NOT NULL UNIQUE,
            name VARCHAR(255) NOT NULL,
            last_used DATETIME,
            created_at DATETIME NOT NULL
        )
    """,
    "file": """
        CREATE TABLE file (
            id INTEGER NOT NULL PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES repository(id) ON DELETE CASCADE,
            path_in_repo VARCHAR(255) NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            sha256 VARCHAR(255) NOT NULL,
            lfs INTEGER NOT NULL DEFAULT 0,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            owner_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE(repository_id, path_in_repo)
        )
    """,
    "stagingupload": """
        CREATE TABLE stagingupload (
            id INTEGER NOT NULL PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES repository(id) ON DELETE CASCADE,
            repo_type VARCHAR(255) NOT NULL,
            revision VARCHAR(255) NOT NULL,
            path_in_repo VARCHAR(255) NOT NULL,
            sha256 VARCHAR(255) NOT NULL DEFAULT '',
            size INTEGER NOT NULL DEFAULT 0,
            upload_id VARCHAR(255),
            storage_key VARCHAR(255) NOT NULL,
            lfs INTEGER NOT NULL DEFAULT 0,
            uploader_id INTEGER REFERENCES "user"(id) ON DELETE SET NULL,
            created_at DATETIME NOT NULL
        )
    """,
    "userorganization": """
        CREATE TABLE userorganization (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            organization_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            role VARCHAR(255) NOT NULL DEFAULT 'member',
            created_at DATETIME NOT NULL,
            UNIQUE(user_id, organization_id)
        )
    """,
    "commit": """
        CREATE TABLE "commit" (
            id INTEGER NOT NULL PRIMARY KEY,
            commit_id VARCHAR(255) NOT NULL,
            repository_id INTEGER NOT NULL REFERENCES repository(id) ON DELETE CASCADE,
            repo_type VARCHAR(255) NOT NULL,
            branch VARCHAR(255) NOT NULL,
            author_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            owner_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            username VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            created_at DATETIME NOT NULL,
            UNIQUE(commit_id, repository_id)
        )
    """,
    "lfsobjecthistory": """
        CREATE TABLE lfsobjecthistory (
            id INTEGER NOT NULL PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES repository(id) ON DELETE CASCADE,
            path_in_repo VARCHAR(255) NOT NULL,
            sha256 VARCHAR(255) NOT NULL,
            size INTEGER NOT NULL,
            commit_id VARCHAR(255) NOT NULL,
            file_id INTEGER REFERENCES file(id) ON DELETE SET NULL,
            created_at DATETIME NOT NULL
        )
    """,
    "sshkey": """
        CREATE TABLE sshkey (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
            key_type VARCHAR(255) NOT NULL,
            public_key TEXT NOT NULL,
            fingerprint VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL,
            last_used DATETIME,
            created_at DATETIME NOT NULL,
            UNIQUE(user_id, fingerprint)
        )
    """,
    "invitation": """
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
    """,
}

SQLITE_INDEXES = (
    'CREATE INDEX user_username ON "user"(username)',
    'CREATE INDEX user_normalized_name ON "user"(normalized_name)',
    'CREATE INDEX user_is_org ON "user"(is_org)',
    'CREATE INDEX user_email ON "user"(email)',
    "CREATE INDEX repository_repo_type ON repository(repo_type)",
    "CREATE INDEX repository_namespace ON repository(namespace)",
    "CREATE INDEX repository_name ON repository(name)",
    "CREATE INDEX repository_full_id ON repository(full_id)",
    "CREATE INDEX repository_lakefs_repo ON repository(lakefs_repo)",
    "CREATE INDEX repository_owner_id ON repository(owner_id)",
    "CREATE INDEX emailverification_user_id ON emailverification(user_id)",
    "CREATE INDEX emailverification_token ON emailverification(token)",
    "CREATE INDEX session_session_id ON session(session_id)",
    "CREATE INDEX session_user_id ON session(user_id)",
    "CREATE INDEX token_user_id ON token(user_id)",
    "CREATE INDEX token_token_hash ON token(token_hash)",
    "CREATE INDEX file_repository_id ON file(repository_id)",
    "CREATE INDEX file_path_in_repo ON file(path_in_repo)",
    "CREATE INDEX file_sha256 ON file(sha256)",
    "CREATE INDEX file_is_deleted ON file(is_deleted)",
    "CREATE INDEX file_owner_id ON file(owner_id)",
    "CREATE INDEX stagingupload_repository_id ON stagingupload(repository_id)",
    "CREATE INDEX stagingupload_repo_type ON stagingupload(repo_type)",
    "CREATE INDEX stagingupload_revision ON stagingupload(revision)",
    "CREATE INDEX stagingupload_uploader_id ON stagingupload(uploader_id)",
    "CREATE INDEX userorganization_user_id ON userorganization(user_id)",
    "CREATE INDEX userorganization_organization_id ON userorganization(organization_id)",
    'CREATE INDEX commit_commit_id ON "commit"(commit_id)',
    'CREATE INDEX commit_repository_id ON "commit"(repository_id)',
    'CREATE INDEX commit_repo_type ON "commit"(repo_type)',
    'CREATE INDEX commit_branch ON "commit"(branch)',
    'CREATE INDEX commit_author_id ON "commit"(author_id)',
    'CREATE INDEX commit_owner_id ON "commit"(owner_id)',
    'CREATE INDEX commit_username ON "commit"(username)',
    'CREATE INDEX commit_repository_id_branch ON "commit"(repository_id, branch)',
    "CREATE INDEX lfsobjecthistory_repository_id ON lfsobjecthistory(repository_id)",
    "CREATE INDEX lfsobjecthistory_path_in_repo ON lfsobjecthistory(path_in_repo)",
    "CREATE INDEX lfsobjecthistory_sha256 ON lfsobjecthistory(sha256)",
    "CREATE INDEX lfsobjecthistory_commit_id ON lfsobjecthistory(commit_id)",
    "CREATE INDEX lfsobjecthistory_file_id ON lfsobjecthistory(file_id)",
    "CREATE INDEX sshkey_user_id ON sshkey(user_id)",
    "CREATE INDEX sshkey_fingerprint ON sshkey(fingerprint)",
    "CREATE INDEX invitation_token ON invitation(token)",
    "CREATE INDEX invitation_action ON invitation(action)",
    "CREATE INDEX invitation_created_by_id ON invitation(created_by_id)",
    "CREATE INDEX invitation_used_by_id ON invitation(used_by_id)",
    "CREATE INDEX invitation_action_created_by_id ON invitation(action, created_by_id)",
)


def _normalize(value: str) -> str:
    return value.lower().replace("-", "").replace("_", "")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_rows(cursor, table: str) -> list[dict]:
    columns = [
        row[1]
        for row in cursor.execute(
            f"PRAGMA table_info({_quote_identifier(table)})"
        ).fetchall()
    ]
    if not columns:
        return []
    selected = ", ".join(_quote_identifier(column) for column in columns)
    rows = cursor.execute(
        f"SELECT {selected} FROM {_quote_identifier(table)} ORDER BY id"
    ).fetchall()
    return [dict(zip(columns, row)) for row in rows]


def _sqlite_contract_complete(cursor) -> bool:
    if cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='organization'"
    ).fetchone():
        return False
    user_columns = {
        row[1]: row for row in cursor.execute('PRAGMA table_info("user")').fetchall()
    }
    required = {"is_org", "normalized_name", "email", "password_hash"}
    if not required.issubset(user_columns):
        return False
    if user_columns["email"][3] or user_columns["password_hash"][3]:
        return False
    for table, column, parent, on_delete in RELATION_CONTRACT:
        foreign_keys = cursor.execute(
            f"PRAGMA foreign_key_list({_quote_identifier(table)})"
        ).fetchall()
        if not any(
            row[2] == parent
            and row[3] == column
            and row[4] == "id"
            and row[6].upper() == on_delete
            for row in foreign_keys
        ):
            return False
    return True


def _postgres_contract_complete(cursor) -> bool:
    cursor.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = 'organization'
        """
    )
    if cursor.fetchone() is not None:
        return False
    cursor.execute(
        """
        SELECT column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'user'
        """
    )
    user_columns = dict(cursor.fetchall())
    required = {"is_org", "normalized_name", "email", "password_hash"}
    if not required.issubset(user_columns):
        return False
    if user_columns["email"] != "YES" or user_columns["password_hash"] != "YES":
        return False
    for table, column, parent, on_delete in RELATION_CONTRACT:
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage child
              ON child.constraint_schema = rc.constraint_schema
             AND child.constraint_name = rc.constraint_name
            JOIN information_schema.constraint_column_usage parent
              ON parent.constraint_schema = rc.unique_constraint_schema
             AND parent.constraint_name = rc.unique_constraint_name
            WHERE child.table_schema = current_schema()
              AND child.table_name = %s
              AND child.column_name = %s
              AND parent.table_name = %s
              AND parent.column_name = 'id'
              AND rc.delete_rule = %s
            """,
            (table, column, parent, on_delete),
        )
        if cursor.fetchone() is None:
            return False
    return True


def is_applied(database, config) -> bool:
    """Return true only for the complete relation contract."""

    try:
        cursor = database.cursor()
        if config.app.db_backend == "postgres":
            return _postgres_contract_complete(cursor)
        return _sqlite_contract_complete(cursor)
    except Exception:
        return False


def check_migration_needed() -> bool:
    cursor = db.cursor()
    if cfg.app.db_backend == "postgres":
        cursor.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name = 'user'
            """
        )
    else:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='user'")
    return cursor.fetchone() is not None and not is_applied(db, cfg)


def _sqlite_replace_table(cursor, table: str) -> tuple[str, list[dict]]:
    rows = _sqlite_rows(cursor, table)
    old_table = f"__khub_008_old_{table}"
    cursor.execute(
        f"ALTER TABLE {_quote_identifier(table)} RENAME TO "
        f"{_quote_identifier(old_table)}"
    )
    indexes = cursor.execute(
        f"PRAGMA index_list({_quote_identifier(old_table)})"
    ).fetchall()
    for row in indexes:
        index_name = row[1]
        if not index_name.startswith("sqlite_autoindex_"):
            cursor.execute(f"DROP INDEX {_quote_identifier(index_name)}")
    cursor.execute(SQLITE_TABLES[table])
    return old_table, rows


def _sqlite_insert(cursor, table: str, columns: tuple[str, ...], values: tuple) -> None:
    names = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    cursor.execute(
        f"INSERT INTO {_quote_identifier(table)} ({names}) VALUES ({placeholders})",
        values,
    )


def _validate_name_contract(
    user_rows: list[dict], organization_rows: list[dict]
) -> None:
    seen: dict[str, tuple[str, bool]] = {}
    for row in user_rows:
        normalized = row.get("normalized_name") or _normalize(row["username"])
        current = (row["username"], bool(row.get("is_org", False)))
        previous = seen.get(normalized)
        if previous and previous != current:
            raise RuntimeError(
                "legacy names collide after normalization: "
                f"{previous[0]!r} and {current[0]!r}"
            )
        seen[normalized] = current
    for row in organization_rows:
        normalized = _normalize(row["name"])
        previous = seen.get(normalized)
        if previous and not previous[1]:
            raise RuntimeError(
                "legacy names collide after normalization: "
                f"{previous[0]!r} and {row['name']!r}"
            )


def _repository_lookup(repository_rows: list[dict]):
    by_full_id = defaultdict(list)
    by_type_and_full_id = {}
    by_id = {}
    for row in repository_rows:
        by_id[row["id"]] = row
        by_full_id[row["full_id"]].append(row)
        by_type_and_full_id[(row["repo_type"], row["full_id"])] = row

    def resolve(row: dict) -> dict:
        if row.get("repository_id") is not None:
            return by_id[row["repository_id"]]
        full_id = row.get("repo_full_id")
        if row.get("repo_type") is not None:
            match = by_type_and_full_id.get((row["repo_type"], full_id))
            if match is not None:
                return match
        matches = by_full_id.get(full_id, ())
        if len(matches) != 1:
            raise RuntimeError(
                f"cannot infer a unique legacy repository for {full_id!r}"
            )
        return matches[0]

    return resolve


def _migrate_sqlite_schema(cursor) -> None:
    legacy_tables = []
    source = {}
    for table in SQLITE_TABLES:
        old_table, rows = _sqlite_replace_table(cursor, table)
        legacy_tables.append(old_table)
        source[table] = rows

    organizations = _sqlite_rows(cursor, "organization")
    _validate_name_contract(source["user"], organizations)

    user_columns = (
        "id",
        "username",
        "normalized_name",
        "is_org",
        "email",
        "password_hash",
        "email_verified",
        "is_active",
        "private_quota_bytes",
        "public_quota_bytes",
        "private_used_bytes",
        "public_used_bytes",
        "full_name",
        "bio",
        "description",
        "website",
        "social_media",
        "avatar",
        "avatar_updated_at",
        "created_at",
    )
    users_by_normalized = {}
    for row in source["user"]:
        normalized = row.get("normalized_name") or _normalize(row["username"])
        _sqlite_insert(
            cursor,
            "user",
            user_columns,
            (
                row["id"],
                row["username"],
                normalized,
                bool(row.get("is_org", False)),
                row.get("email"),
                row.get("password_hash"),
                row.get("email_verified", False),
                row.get("is_active", True),
                row.get("private_quota_bytes"),
                row.get("public_quota_bytes"),
                row.get("private_used_bytes", 0),
                row.get("public_used_bytes", 0),
                row.get("full_name"),
                row.get("bio"),
                row.get("description"),
                row.get("website"),
                row.get("social_media"),
                row.get("avatar"),
                row.get("avatar_updated_at"),
                row["created_at"],
            ),
        )
        users_by_normalized[normalized] = (
            row["id"],
            bool(row.get("is_org", False)),
        )

    organization_mapping = {}
    for row in organizations:
        normalized = _normalize(row["name"])
        existing = users_by_normalized.get(normalized)
        if existing:
            if not existing[1]:
                raise RuntimeError(
                    f"organization {row['name']!r} conflicts with a user"
                )
            organization_mapping[row["id"]] = existing[0]
            continue
        _sqlite_insert(
            cursor,
            "user",
            user_columns[1:],
            (
                row["name"],
                normalized,
                True,
                None,
                None,
                False,
                True,
                row.get("private_quota_bytes"),
                row.get("public_quota_bytes"),
                row.get("private_used_bytes", 0),
                row.get("public_used_bytes", 0),
                None,
                row.get("bio"),
                row.get("description"),
                row.get("website"),
                row.get("social_media"),
                row.get("avatar"),
                row.get("avatar_updated_at"),
                row["created_at"],
            ),
        )
        new_id = cursor.lastrowid
        organization_mapping[row["id"]] = new_id
        users_by_normalized[normalized] = (new_id, True)

    repository_columns = (
        "id",
        "repo_type",
        "namespace",
        "name",
        "full_id",
        "lakefs_repo",
        "private",
        "owner_id",
        "quota_bytes",
        "used_bytes",
        "lfs_threshold_bytes",
        "lfs_keep_versions",
        "lfs_suffix_rules",
        "downloads",
        "likes_count",
        "created_at",
    )
    for row in source["repository"]:
        _sqlite_insert(
            cursor,
            "repository",
            repository_columns,
            (
                row["id"],
                row["repo_type"],
                row["namespace"],
                row["name"],
                row["full_id"],
                row.get("lakefs_repo")
                or frozen_lakefs_repo_name(row["repo_type"], row["full_id"]),
                row.get("private", False),
                row["owner_id"],
                row.get("quota_bytes"),
                row.get("used_bytes", 0),
                row.get("lfs_threshold_bytes"),
                row.get("lfs_keep_versions"),
                row.get("lfs_suffix_rules"),
                row.get("downloads", 0),
                row.get("likes_count", 0),
                row["created_at"],
            ),
        )

    resolve_repository = _repository_lookup(source["repository"])
    simple_tables = (
        (
            "emailverification",
            ("id", "user_id", "token", "expires_at", "created_at"),
        ),
        (
            "session",
            ("id", "session_id", "user_id", "secret", "expires_at", "created_at"),
        ),
        (
            "token",
            ("id", "user_id", "token_hash", "name", "last_used", "created_at"),
        ),
        (
            "sshkey",
            (
                "id",
                "user_id",
                "key_type",
                "public_key",
                "fingerprint",
                "title",
                "last_used",
                "created_at",
            ),
        ),
    )
    for table, columns in simple_tables:
        for row in source[table]:
            values = tuple(
                row.get("user_id", row.get("user"))
                if column == "user_id"
                else row.get(column)
                for column in columns
            )
            _sqlite_insert(cursor, table, columns, values)

    file_repository = {}
    for row in source["file"]:
        repository = resolve_repository(row)
        repository_id = repository["id"]
        file_repository[(repository_id, row["path_in_repo"])] = row["id"]
        _sqlite_insert(
            cursor,
            "file",
            (
                "id",
                "repository_id",
                "path_in_repo",
                "size",
                "sha256",
                "lfs",
                "is_deleted",
                "owner_id",
                "created_at",
                "updated_at",
            ),
            (
                row["id"],
                repository_id,
                row["path_in_repo"],
                row.get("size", 0),
                row["sha256"],
                row.get("lfs", False),
                row.get("is_deleted", False),
                row.get("owner_id", repository["owner_id"]),
                row["created_at"],
                row["updated_at"],
            ),
        )

    for row in source["stagingupload"]:
        repository = resolve_repository(row)
        _sqlite_insert(
            cursor,
            "stagingupload",
            (
                "id",
                "repository_id",
                "repo_type",
                "revision",
                "path_in_repo",
                "sha256",
                "size",
                "upload_id",
                "storage_key",
                "lfs",
                "uploader_id",
                "created_at",
            ),
            (
                row["id"],
                repository["id"],
                row["repo_type"],
                row["revision"],
                row["path_in_repo"],
                row.get("sha256", ""),
                row.get("size", 0),
                row.get("upload_id"),
                row["storage_key"],
                row.get("lfs", False),
                row.get("uploader_id"),
                row["created_at"],
            ),
        )

    memberships = {}
    for row in source["userorganization"]:
        organization_id = organization_mapping.get(
            row["organization_id"], row["organization_id"]
        )
        key = (row["user_id"], organization_id)
        current = memberships.get(key)
        if current is None or row["id"] < current["id"]:
            memberships[key] = {**row, "organization_id": organization_id}
    for row in memberships.values():
        _sqlite_insert(
            cursor,
            "userorganization",
            ("id", "user_id", "organization_id", "role", "created_at"),
            (
                row["id"],
                row["user_id"],
                row["organization_id"],
                row["role"],
                row["created_at"],
            ),
        )

    for row in source["commit"]:
        repository = resolve_repository(row)
        _sqlite_insert(
            cursor,
            "commit",
            (
                "id",
                "commit_id",
                "repository_id",
                "repo_type",
                "branch",
                "author_id",
                "owner_id",
                "username",
                "message",
                "description",
                "created_at",
            ),
            (
                row["id"],
                row["commit_id"],
                repository["id"],
                row["repo_type"],
                row["branch"],
                row.get("author_id", row.get("user_id")),
                row.get("owner_id", repository["owner_id"]),
                row["username"],
                row["message"],
                row.get("description", ""),
                row["created_at"],
            ),
        )

    for row in source["lfsobjecthistory"]:
        repository = resolve_repository(row)
        file_id = row.get("file_id")
        if file_id is None:
            file_id = file_repository.get((repository["id"], row["path_in_repo"]))
        _sqlite_insert(
            cursor,
            "lfsobjecthistory",
            (
                "id",
                "repository_id",
                "path_in_repo",
                "sha256",
                "size",
                "commit_id",
                "file_id",
                "created_at",
            ),
            (
                row["id"],
                repository["id"],
                row["path_in_repo"],
                row["sha256"],
                row["size"],
                row["commit_id"],
                file_id,
                row["created_at"],
            ),
        )

    valid_user_ids = {item[0] for item in users_by_normalized.values()}
    for row in source["invitation"]:
        created_by = row.get("created_by_id", row.get("created_by"))
        if created_by not in valid_user_ids:
            created_by = None
        used_by = row.get("used_by_id", row.get("used_by"))
        if used_by not in valid_user_ids:
            used_by = None
        _sqlite_insert(
            cursor,
            "invitation",
            (
                "id",
                "token",
                "action",
                "parameters",
                "created_by_id",
                "expires_at",
                "max_usage",
                "usage_count",
                "used_at",
                "used_by_id",
                "created_at",
            ),
            (
                row["id"],
                row["token"],
                row["action"],
                row["parameters"],
                created_by,
                row["expires_at"],
                row.get("max_usage"),
                row.get("usage_count", 0),
                row.get("used_at"),
                used_by,
                row["created_at"],
            ),
        )

    for statement in SQLITE_INDEXES:
        cursor.execute(statement)
    cursor.execute("DROP TABLE IF EXISTS organization")
    for table in reversed(legacy_tables):
        cursor.execute(f"DROP TABLE {_quote_identifier(table)}")
    violations = cursor.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise RuntimeError(
            f"foreign key violations after migration 008: {violations[:5]}"
        )


def migrate_sqlite() -> bool:
    """Run the complete SQLite rebuild in one explicit transaction."""

    cursor = db.cursor()
    foreign_keys = cursor.execute("PRAGMA foreign_keys").fetchone()[0]
    legacy_alter_table = cursor.execute("PRAGMA legacy_alter_table").fetchone()[0]
    cursor.execute("PRAGMA foreign_keys = OFF")
    cursor.execute("PRAGMA legacy_alter_table = ON")
    try:
        cursor.execute("BEGIN IMMEDIATE")
        _migrate_sqlite_schema(cursor)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.execute(f"PRAGMA legacy_alter_table = {legacy_alter_table}")
        cursor.execute(f"PRAGMA foreign_keys = {foreign_keys}")
    return True


def _postgres_columns(cursor, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        """,
        (table,),
    )
    return {row[0] for row in cursor.fetchall()}


def _postgres_add_column(cursor, table: str, column: str, definition: str) -> None:
    cursor.execute(
        f"ALTER TABLE {_quote_identifier(table)} ADD COLUMN IF NOT EXISTS "
        f"{_quote_identifier(column)} {definition}"
    )


def _postgres_drop_foreign_keys(cursor, table: str, column: str) -> None:
    cursor.execute(
        """
        SELECT constraint_name
        FROM information_schema.key_column_usage usage
        JOIN information_schema.table_constraints constraint_info
          USING (constraint_schema, constraint_name, table_schema, table_name)
        WHERE usage.table_schema = current_schema()
          AND usage.table_name = %s
          AND usage.column_name = %s
          AND constraint_info.constraint_type = 'FOREIGN KEY'
        """,
        (table, column),
    )
    for (constraint_name,) in cursor.fetchall():
        cursor.execute(
            f"ALTER TABLE {_quote_identifier(table)} DROP CONSTRAINT "
            f"{_quote_identifier(constraint_name)}"
        )


def _postgres_add_foreign_key(
    cursor, table: str, column: str, parent: str, on_delete: str
) -> None:
    _postgres_drop_foreign_keys(cursor, table, column)
    constraint = f"{table}_{column}_fk_008"
    cursor.execute(
        f"ALTER TABLE {_quote_identifier(table)} ADD CONSTRAINT "
        f"{_quote_identifier(constraint)} FOREIGN KEY "
        f"({_quote_identifier(column)}) REFERENCES "
        f"{_quote_identifier(parent)}(id) ON DELETE {on_delete}"
    )


def _postgres_rename_legacy_column(cursor, table: str, old: str, new: str) -> None:
    columns = _postgres_columns(cursor, table)
    if old in columns and new not in columns:
        cursor.execute(
            f"ALTER TABLE {_quote_identifier(table)} RENAME COLUMN "
            f"{_quote_identifier(old)} TO {_quote_identifier(new)}"
        )


def migrate_postgres() -> bool:
    """Convert the PostgreSQL schema inside the caller's transaction."""

    cursor = db.cursor()
    _postgres_add_column(cursor, "user", "normalized_name", "VARCHAR(255)")
    _postgres_add_column(cursor, "user", "is_org", "BOOLEAN NOT NULL DEFAULT FALSE")
    _postgres_add_column(cursor, "user", "description", "TEXT")
    cursor.execute('ALTER TABLE "user" ALTER COLUMN email DROP NOT NULL')
    cursor.execute('ALTER TABLE "user" ALTER COLUMN password_hash DROP NOT NULL')
    cursor.execute(
        """
        UPDATE "user"
        SET normalized_name = lower(replace(replace(username, '-', ''), '_', ''))
        WHERE normalized_name IS NULL
        """
    )
    cursor.execute(
        """
        SELECT normalized_name FROM "user"
        GROUP BY normalized_name HAVING count(*) > 1
        """
    )
    collision = cursor.fetchone()
    if collision:
        raise RuntimeError(
            f"legacy names collide after normalization: {collision[0]!r}"
        )

    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = 'organization'"
    )
    organization_exists = cursor.fetchone() is not None
    organization_mapping = {}
    if organization_exists:
        cursor.execute(
            "SELECT id, name, description, private_quota_bytes, "
            "public_quota_bytes, private_used_bytes, public_used_bytes, bio, "
            "website, social_media, avatar, avatar_updated_at, created_at "
            "FROM organization ORDER BY id"
        )
        for row in cursor.fetchall():
            org_id, name = row[0], row[1]
            normalized = _normalize(name)
            cursor.execute(
                'SELECT id, is_org FROM "user" WHERE normalized_name = %s',
                (normalized,),
            )
            existing = cursor.fetchone()
            if existing:
                if not existing[1]:
                    raise RuntimeError(f"organization {name!r} conflicts with a user")
                organization_mapping[org_id] = existing[0]
                continue
            cursor.execute(
                """
                INSERT INTO "user" (
                    username, normalized_name, is_org, email, password_hash,
                    email_verified, is_active, private_quota_bytes,
                    public_quota_bytes, private_used_bytes, public_used_bytes,
                    full_name, bio, description, website, social_media, avatar,
                    avatar_updated_at, created_at
                ) VALUES (
                    %s, %s, TRUE, NULL, NULL, FALSE, TRUE, %s, %s, %s, %s,
                    NULL, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """,
                (
                    name,
                    normalized,
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[2],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                ),
            )
            organization_mapping[org_id] = cursor.fetchone()[0]

    _postgres_rename_legacy_column(cursor, "emailverification", "user", "user_id")
    _postgres_rename_legacy_column(cursor, "invitation", "created_by", "created_by_id")
    _postgres_rename_legacy_column(cursor, "invitation", "used_by", "used_by_id")

    _postgres_add_column(cursor, "file", "repository_id", "INTEGER")
    _postgres_add_column(cursor, "file", "owner_id", "INTEGER")
    _postgres_add_column(cursor, "file", "is_deleted", "BOOLEAN NOT NULL DEFAULT FALSE")
    _postgres_add_column(cursor, "stagingupload", "repository_id", "INTEGER")
    _postgres_add_column(cursor, "stagingupload", "uploader_id", "INTEGER")
    _postgres_add_column(cursor, "commit", "repository_id", "INTEGER")
    _postgres_add_column(cursor, "commit", "author_id", "INTEGER")
    _postgres_add_column(cursor, "commit", "owner_id", "INTEGER")
    _postgres_add_column(cursor, "lfsobjecthistory", "repository_id", "INTEGER")
    _postgres_add_column(cursor, "lfsobjecthistory", "file_id", "INTEGER")

    for table in ("file", "lfsobjecthistory"):
        if "repo_full_id" in _postgres_columns(cursor, table):
            cursor.execute(
                f"""
                SELECT source.repo_full_id
                FROM {_quote_identifier(table)} source
                JOIN repository ON repository.full_id = source.repo_full_id
                GROUP BY source.repo_full_id
                HAVING count(DISTINCT repository.id) <> 1
                LIMIT 1
                """
            )
            ambiguous = cursor.fetchone()
            if ambiguous:
                raise RuntimeError(
                    f"cannot infer a unique legacy repository for {ambiguous[0]!r}"
                )
            cursor.execute(
                f"""
                UPDATE {_quote_identifier(table)} source
                SET repository_id = repository.id
                FROM repository
                WHERE source.repository_id IS NULL
                  AND repository.full_id = source.repo_full_id
                """
            )

    for table in ("stagingupload", "commit"):
        if "repo_full_id" in _postgres_columns(cursor, table):
            cursor.execute(
                f"""
                UPDATE {_quote_identifier(table)} source
                SET repository_id = repository.id
                FROM repository
                WHERE source.repository_id IS NULL
                  AND repository.full_id = source.repo_full_id
                  AND repository.repo_type = source.repo_type
                """
            )

    cursor.execute(
        'UPDATE "file" SET owner_id = repository.owner_id FROM repository '
        'WHERE "file".repository_id = repository.id '
        'AND "file".owner_id IS NULL'
    )
    cursor.execute(
        'UPDATE "commit" SET owner_id = repository.owner_id FROM repository '
        'WHERE "commit".repository_id = repository.id '
        'AND "commit".owner_id IS NULL'
    )
    if "user_id" in _postgres_columns(cursor, "commit"):
        cursor.execute(
            'UPDATE "commit" SET author_id = user_id WHERE author_id IS NULL'
        )
    cursor.execute(
        """
        UPDATE lfsobjecthistory history
        SET file_id = file.id
        FROM file
        WHERE history.file_id IS NULL
          AND history.repository_id = file.repository_id
          AND history.path_in_repo = file.path_in_repo
        """
    )

    if organization_exists:
        _postgres_drop_foreign_keys(cursor, "userorganization", "organization_id")
        for old_id, new_id in organization_mapping.items():
            cursor.execute(
                "UPDATE userorganization SET organization_id = %s "
                "WHERE organization_id = %s",
                (new_id, old_id),
            )
        cursor.execute("DROP TABLE organization CASCADE")

    not_null_columns = (
        ("file", "repository_id"),
        ("file", "owner_id"),
        ("stagingupload", "repository_id"),
        ("commit", "repository_id"),
        ("commit", "author_id"),
        ("commit", "owner_id"),
        ("lfsobjecthistory", "repository_id"),
    )
    for table, column in not_null_columns:
        cursor.execute(
            f"ALTER TABLE {_quote_identifier(table)} ALTER COLUMN "
            f"{_quote_identifier(column)} SET NOT NULL"
        )

    old_columns = (
        ("file", "repo_full_id"),
        ("stagingupload", "repo_full_id"),
        ("commit", "repo_full_id"),
        ("commit", "user_id"),
        ("lfsobjecthistory", "repo_full_id"),
    )
    for table, old_column in old_columns:
        if old_column in _postgres_columns(cursor, table):
            cursor.execute(
                f"ALTER TABLE {_quote_identifier(table)} DROP COLUMN "
                f"{_quote_identifier(old_column)} CASCADE"
            )

    cursor.execute('ALTER TABLE "user" ALTER COLUMN normalized_name SET NOT NULL')
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS user_normalized_name "
        'ON "user"(normalized_name)'
    )
    for table, column, parent, on_delete in RELATION_CONTRACT:
        _postgres_add_foreign_key(cursor, table, column, parent, on_delete)
    return True


def _confirm_postgres_migration() -> bool:
    print("Migration 008 changes the legacy application schema.")
    auto = os.environ.get("KOHAKU_HUB_AUTO_MIGRATE", "").lower()
    if auto in {"true", "1", "yes"}:
        print("Auto-confirmation enabled (KOHAKU_HUB_AUTO_MIGRATE=true)")
        return True
    return input("Type 'yes' to continue: ").lower() == "yes"


def run() -> bool:
    db.connect(reuse_if_open=True)
    try:
        if should_skip_due_to_future_migrations(MIGRATION_NUMBER, db, cfg):
            print("Migration 008: Skipped (superseded by future migration)")
            return True
        if not check_migration_needed():
            print("Migration 008: Already applied")
            return True

        print("=" * 70)
        print("Migration 008: Merge users/organizations and install relations")
        print("=" * 70)
        if cfg.app.db_backend == "postgres":
            if not _confirm_postgres_migration():
                print("Migration 008: Cancelled")
                return False
            with db.atomic():
                result = migrate_postgres()
        else:
            result = migrate_sqlite()
        print("Migration 008: Completed")
        return result
    except Exception as exc:
        print(f"Migration 008: Failed - {exc}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    sys.exit(0 if run() else 1)

"""Regression coverage for legacy databases at the 008 compatibility boundary."""

from __future__ import annotations

import sqlite3
from importlib.util import module_from_spec, spec_from_file_location

import pytest

from test.kohakuhub.support.migration_history_database import (
    ROOT_DIR,
    _initialize_archived_schema,
    _run_archived_migrations,
    _insert_row,
    _run_script,
    archive_parent,
    archive_release,
    create_sqlite_database,
    seed_database,
    snapshot_database,
)
from test.kohakuhub.support.migration_history_manifest import HISTORICAL_RELEASES


SQLITE_FOREIGN_KEY_CONTRACT = (
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


def _historical_sqlite(tmp_path, slug):
    release = next(release for release in HISTORICAL_RELEASES if release.slug == slug)
    database = create_sqlite_database(tmp_path, release)
    archive_root = archive_release(release, tmp_path / "archive")
    base_root = archive_parent(release, tmp_path / "base")
    _initialize_archived_schema(base_root, database)
    seed_database(database)
    result = _run_archived_migrations(
        archive_root,
        database,
        release.migration_numbers[-1],
    )
    assert result.succeeded, result.diagnostic()
    return database


def _run_current_runner(database):
    return _run_script(
        ROOT_DIR / "scripts" / "run_migrations.py",
        database,
        cwd=ROOT_DIR,
    )


def _load_migration_008():
    spec = spec_from_file_location(
        "test_migration_008_repair",
        ROOT_DIR / "scripts" / "db_migrations" / "008_foreignkey_refactoring.py",
    )
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_migration_012():
    spec = spec_from_file_location(
        "test_migration_012_rollback",
        ROOT_DIR
        / "scripts"
        / "db_migrations"
        / "012_invitation_created_by_nullable.py",
    )
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_migration_016():
    spec = spec_from_file_location(
        "test_migration_016_frozen_derivation",
        ROOT_DIR / "scripts" / "db_migrations" / "016_repository_lakefs_repo.py",
    )
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sqlite_008_uses_the_frozen_016_lakefs_derivation():
    migration_008 = _load_migration_008()
    migration_016 = _load_migration_016()

    for repo_type, repo_id in (
        ("model", "migration-owner/preserved-repository"),
        ("dataset", "name_with.special/chars"),
        ("space", "Mixed/Case"),
    ):
        assert migration_008.frozen_lakefs_repo_name(repo_type, repo_id) == (
            migration_016.frozen_lakefs_repo_name(repo_type, repo_id)
        )


def _sqlite_schema_snapshot(database):
    connection = sqlite3.connect(database.sqlite_path)
    try:
        return tuple(
            connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall()
        )
    finally:
        connection.close()


def test_sqlite_012_normalizes_invalid_creator_during_rebuild(tmp_path):
    database = _historical_sqlite(tmp_path, "v011")
    connection = sqlite3.connect(database.sqlite_path)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("UPDATE invitation SET created_by_id = 999 WHERE id = 1")
        connection.commit()
    finally:
        connection.close()

    result = _run_current_runner(database)

    assert result.succeeded, result.diagnostic()
    connection = sqlite3.connect(database.sqlite_path)
    try:
        assert connection.execute(
            "SELECT created_by_id FROM invitation WHERE id = 1"
        ).fetchone() == (None,)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()


def test_sqlite_012_rebuild_failure_rolls_back_and_can_be_retried(
    tmp_path, monkeypatch
):
    database = _historical_sqlite(tmp_path, "v011")
    migration = _load_migration_012()

    class DatabaseAdapter:
        def __init__(self, connection):
            self.connection = connection

        def cursor(self):
            return self.connection.cursor()

        def commit(self):
            self.connection.commit()

        def rollback(self):
            self.connection.rollback()

        def execute_sql(self, statement):
            return self.connection.execute(statement)

    connection = sqlite3.connect(database.sqlite_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        before_rows = snapshot_database(database)
        before_schema = _sqlite_schema_snapshot(database)
        adapter = DatabaseAdapter(connection)
        monkeypatch.setattr(migration, "db", adapter)
        original_rebuild = migration._sqlite_invitation_rebuild

        def fail_after_rebuild(cursor):
            original_rebuild(cursor)
            raise RuntimeError("injected invitation rebuild failure")

        monkeypatch.setattr(migration, "_sqlite_invitation_rebuild", fail_after_rebuild)
        with pytest.raises(RuntimeError, match="injected invitation rebuild failure"):
            migration.migrate_sqlite()

        assert snapshot_database(database) == before_rows
        assert _sqlite_schema_snapshot(database) == before_schema
        assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
    finally:
        connection.close()


def test_sqlite_001_removes_legacy_full_id_unique_constraint(tmp_path):
    database = _historical_sqlite(tmp_path, "v001")
    connection = sqlite3.connect(database.sqlite_path)
    try:
        connection.execute("DROP INDEX IF EXISTS repository_full_id")
        connection.execute(
            "CREATE UNIQUE INDEX legacy_repository_full_id ON repository(full_id)"
        )
        connection.commit()
    finally:
        connection.close()

    result = _run_current_runner(database)

    assert result.succeeded, result.diagnostic()
    connection = sqlite3.connect(database.sqlite_path)
    try:
        for _sequence, index_name, unique, _origin, _partial in connection.execute(
            "PRAGMA index_list(repository)"
        ).fetchall():
            if not unique:
                continue
            columns = tuple(
                row[2]
                for row in sorted(
                    connection.execute(
                        f'PRAGMA index_info("{index_name.replace(chr(34), chr(34) * 2)}")'
                    ).fetchall(),
                    key=lambda row: row[0],
                )
            )
            assert columns != ("full_id",)
    finally:
        connection.close()


def test_sqlite_008_rejects_ambiguous_legacy_repository_reference(tmp_path):
    database = _historical_sqlite(tmp_path, "v007")
    connection = sqlite3.connect(database.sqlite_path)
    try:
        _insert_row(
            connection,
            database,
            "repository",
            {
                "id": 2,
                "repo_type": "dataset",
                "namespace": "migration-owner",
                "name": "preserved-repository",
                "full_id": "migration-owner/preserved-repository",
                "private": False,
                "owner_id": 1,
                "used_bytes": 505,
                "created_at": "2025-01-02 03:04:05",
            },
        )
        _insert_row(
            connection,
            database,
            "file",
            {
                "id": 2,
                "repo_full_id": "migration-owner/preserved-repository",
                "path_in_repo": "ambiguous.bin",
                "size": 1,
                "sha256": "ambiguous-file",
                "lfs": False,
                "created_at": "2025-01-02 03:04:05",
                "updated_at": "2025-01-02 03:04:05",
            },
        )
        connection.commit()
    finally:
        connection.close()

    before_rows = snapshot_database(database)
    before_schema = _sqlite_schema_snapshot(database)
    result = _run_current_runner(database)

    assert not result.succeeded
    assert "cannot infer a unique legacy repository" in result.stdout
    assert "Running 009_repo_lfs_settings..." not in result.stdout
    assert snapshot_database(database) == before_rows
    assert _sqlite_schema_snapshot(database) == before_schema


def test_sqlite_008_rejects_normalized_name_conflicts_before_mutation(tmp_path):
    database = _historical_sqlite(tmp_path, "v007")
    connection = sqlite3.connect(database.sqlite_path)
    try:
        connection.execute(
            "INSERT INTO user (username, email, password_hash, email_verified, "
            "is_active, private_used_bytes, public_used_bytes, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "migration_owner",
                "conflict@example.test",
                "password",
                0,
                1,
                0,
                0,
                "2025-01-02 03:04:05",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    before_rows = snapshot_database(database)
    before_schema = _sqlite_schema_snapshot(database)
    result = _run_current_runner(database)

    assert not result.succeeded
    assert "legacy names collide" in result.stdout
    assert "Running 009_repo_lfs_settings..." not in result.stdout
    assert snapshot_database(database) == before_rows
    assert _sqlite_schema_snapshot(database) == before_schema


def test_sqlite_008_deduplicates_memberships_and_installs_full_fk_contract(tmp_path):
    database = _historical_sqlite(tmp_path, "v007")
    connection = sqlite3.connect(database.sqlite_path)
    try:
        connection.execute(
            "INSERT INTO userorganization "
            "(id, user_id, organization_id, role, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (2, 1, 20, "visitor", "2025-01-02 03:04:05"),
        )
        connection.commit()
    finally:
        connection.close()

    result = _run_current_runner(database)

    assert result.succeeded, result.diagnostic()
    connection = sqlite3.connect(database.sqlite_path)
    try:
        organization_user_id = connection.execute(
            "SELECT id FROM user WHERE username = ?", ("migration-org",)
        ).fetchone()[0]
        assert connection.execute(
            "SELECT user_id, organization_id, role FROM userorganization"
        ).fetchall() == [(1, organization_user_id, "admin")]
        unique_indexes = []
        for _sequence, index_name, unique, _origin, _partial in connection.execute(
            "PRAGMA index_list(userorganization)"
        ).fetchall():
            if not unique:
                continue
            columns = tuple(
                row[2]
                for row in sorted(
                    connection.execute(
                        f'PRAGMA index_info("{index_name.replace(chr(34), chr(34) * 2)}")'
                    ).fetchall(),
                    key=lambda row: row[0],
                )
            )
            unique_indexes.append(columns)
        assert ("user_id", "organization_id") in unique_indexes

        for table, column, parent, on_delete in SQLITE_FOREIGN_KEY_CONTRACT:
            rows = connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
            assert any(
                row[2] == parent
                and row[3] == column
                and row[4] == "id"
                and row[6].upper() == on_delete
                for row in rows
            ), (table, column, parent, on_delete, rows)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()

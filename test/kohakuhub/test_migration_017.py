"""Tests for migration 017 (background_task table)."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from peewee import SqliteDatabase

from kohakuhub.db import BackgroundTask, db

MIGRATION_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "db_migrations" / "017_background_tasks.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location("migration_017", MIGRATION_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema(database):
    columns = {
        column.name: (column.data_type.lower(), column.null, column.primary_key)
        for column in database.get_columns("background_task")
    }
    indexes = {
        (index.name, tuple(index.columns), index.unique)
        for index in database.get_indexes("background_task")
        if not index.name.endswith("_pkey")
    }
    return columns, indexes


def test_migration_017_matches_init_db_on_postgres(prepared_backend_test_state):
    expected = _schema(db)  # created by init_db()
    db.execute_sql('DROP TABLE "background_task"')
    try:
        migration = _load_migration()
        assert migration.is_applied(db, migration.cfg) is False

        assert migration.run() is True
        assert _schema(db) == expected
        assert migration.run() is True  # re-running is a no-op
    finally:
        BackgroundTask.create_table(safe=True)


def test_migration_017_matches_init_db_on_sqlite(tmp_path, monkeypatch):
    migration = _load_migration()
    migrated = SqliteDatabase(str(tmp_path / "migrated.db"))
    monkeypatch.setattr(migration, "db", migrated)
    monkeypatch.setattr(migration, "cfg", SimpleNamespace(app=SimpleNamespace(db_backend="sqlite")))

    assert migration.run() is True
    assert migration.run() is True

    reference = SqliteDatabase(str(tmp_path / "reference.db"))
    with BackgroundTask.bind_ctx(reference):
        reference.create_tables([BackgroundTask])
    assert _schema(migrated) == _schema(reference)


def test_migration_017_reports_failure(tmp_path, monkeypatch):
    migration = _load_migration()
    monkeypatch.setattr(migration, "db", SqliteDatabase(str(tmp_path / "broken.db")))
    monkeypatch.setattr(migration, "cfg", SimpleNamespace(app=SimpleNamespace(db_backend="sqlite")))

    def explode():
        raise RuntimeError("disk full")

    monkeypatch.setattr(migration, "migrate_sqlite", explode)

    assert migration.run() is False

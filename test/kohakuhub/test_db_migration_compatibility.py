"""Focused coverage for the migration-017 boundary."""

from contextlib import nullcontext
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

import pytest

from kohakuhub.migrations.schema import expected_table_columns, signature_digest
from scripts import run_migrations
from scripts import khub_migrate
from scripts.db_migrations import _migration_utils
from scripts.khub_migrate import (
    APPLICATION_TABLE_COLUMNS_WORKER,
    APPLICATION_SCHEMA_CHECKSUM_WORKER,
    _expected_worker_ledger_records,
)
from scripts.generate_docker_compose import generate_khub_migrate_service


ROOT_DIR = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT_DIR / "scripts" / "db_migrations" / "017_durable_worker_schema.py"


def _load_migration_017():
    spec = spec_from_file_location("test_migration_017", MIGRATION_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Database:
    def __init__(self, rows=()):
        self.cursor_instance = _Cursor(rows)
        self.connect_calls = 0

    def cursor(self):
        return self.cursor_instance

    def connect(self, **_kwargs):
        self.connect_calls += 1


def _config(backend):
    return SimpleNamespace(
        app=SimpleNamespace(
            db_backend=backend,
            database_url="postgresql://migration-test",
        )
    )


def test_017_uses_the_worker_ledger_records_for_postgres():
    migration = _load_migration_017()
    expected = _expected_worker_ledger_records()
    rows = [(name, version, checksum) for name, (version, checksum) in expected.items()]

    assert migration.MIGRATION_NUMBER == 17
    # The durable ledger records the worker schema contract, not this filename.
    # A pre-release execution of the old 018 file therefore remains detectable.
    assert migration.is_applied(_Database(rows), _config("postgres")) is True

    incomplete_rows = rows[:-1]
    assert (
        migration.is_applied(_Database(incomplete_rows), _config("postgres")) is False
    )


def test_017_treats_a_missing_worker_ledger_as_not_applied():
    migration = _load_migration_017()
    database = _Database()

    assert migration.is_applied(database, _config("postgres")) is False
    assert not any(
        "FROM khub_schema_migrations" in query
        for query, _params in database.cursor_instance.executed
    )


def test_017_application_schema_checksum_is_frozen():
    assert signature_digest(expected_table_columns(include_operations=False)) == (
        APPLICATION_SCHEMA_CHECKSUM_WORKER
    )
    assert set(APPLICATION_TABLE_COLUMNS_WORKER) == set(
        expected_table_columns(include_operations=False)
    )


def test_017_rejects_extra_application_schema_objects(monkeypatch):
    actual = dict(APPLICATION_TABLE_COLUMNS_WORKER)
    actual["user"] = (*actual["user"], "unexpected_column")
    actual["unexpected_application_table"] = ("id",)
    monkeypatch.setattr(khub_migrate, "read_table_columns", lambda _connection: actual)

    with pytest.raises(RuntimeError, match="extra"):
        khub_migrate._assert_worker_application_schema(object())


def test_017_application_adoption_allows_known_operation_tables(monkeypatch):
    actual = dict(APPLICATION_TABLE_COLUMNS_WORKER)
    actual.update(
        khub_migrate.operation_table_columns_for_version(
            khub_migrate.OPERATION_SCHEMA_VERSION_WORKER
        )
    )
    monkeypatch.setattr(khub_migrate, "read_table_columns", lambda _connection: actual)

    khub_migrate._assert_worker_application_schema(object())


def test_017_application_adoption_allows_current_model_extensions(monkeypatch):
    actual = dict(APPLICATION_TABLE_COLUMNS_WORKER)
    actual["user"] = (*actual["user"], "future_model_column")
    actual["future_model_table"] = ("id",)

    current = dict(expected_table_columns(include_operations=False))
    current["user"] = (*current["user"], "future_model_column")
    current["future_model_table"] = ("id",)
    monkeypatch.setattr(khub_migrate, "expected_table_columns", lambda **_: current)
    monkeypatch.setattr(khub_migrate, "read_table_columns", lambda _connection: actual)

    khub_migrate._assert_worker_application_schema(object())


def test_historical_supersession_check_does_not_mask_loader_errors(monkeypatch):
    def fail_to_load(*_args, **_kwargs):
        raise RuntimeError("migration loader unavailable")

    monkeypatch.setattr(
        _migration_utils.importlib.util,
        "spec_from_file_location",
        fail_to_load,
    )

    with pytest.raises(RuntimeError, match="loader unavailable"):
        _migration_utils.should_skip_due_to_future_migrations(1, None, None)


def test_017_is_a_sqlite_no_op(monkeypatch):
    migration = _load_migration_017()
    database = _Database()
    monkeypatch.setattr(migration, "cfg", _config("sqlite"))
    monkeypatch.setattr(migration, "db", database)

    assert migration.run() is True
    assert database.connect_calls == 1
    assert database.cursor_instance.executed == []


def test_runner_discovers_the_complete_numbered_chain(tmp_path, monkeypatch):
    migrations_dir = tmp_path / "db_migrations"
    migrations_dir.mkdir()
    for name in (
        "001_repository_schema.py",
        "016_repository_lakefs_repo.py",
        "017_durable_worker_schema.py",
        "018_follow_up.py",
        "019_follow_up.py",
        "not_a_migration.py",
    ):
        (migrations_dir / name).touch()

    monkeypatch.setattr(run_migrations, "SCRIPT_DIR", tmp_path)

    assert [name for name, _path in run_migrations.discover_migrations()] == [
        "001_repository_schema",
        "016_repository_lakefs_repo",
        "017_durable_worker_schema",
        "018_follow_up",
        "019_follow_up",
    ]


def test_runner_sorts_unpadded_numeric_migration_prefixes(tmp_path, monkeypatch):
    migrations_dir = tmp_path / "db_migrations"
    migrations_dir.mkdir()
    for name in ("10_later.py", "2_earlier.py", "001_first.py"):
        (migrations_dir / name).touch()

    monkeypatch.setattr(run_migrations, "SCRIPT_DIR", tmp_path)

    assert [name for name, _path in run_migrations.discover_migrations()] == [
        "001_first",
        "2_earlier",
        "10_later",
    ]


def test_fresh_database_continues_to_numbered_migration_017(monkeypatch):
    """The fresh-schema shortcut must not bypass the worker schema migration."""

    calls = []
    migration = SimpleNamespace(run=lambda: calls.append("017") or True)

    monkeypatch.setattr(run_migrations, "cfg", _config("postgres"))
    monkeypatch.setattr(run_migrations, "migration_lock", nullcontext)
    monkeypatch.setattr(run_migrations, "is_database_initialized", lambda: False)
    monkeypatch.setattr(run_migrations, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(
        run_migrations,
        "discover_migrations",
        lambda: [("017_durable_worker_schema", MIGRATION_PATH)],
    )
    monkeypatch.setattr(
        run_migrations,
        "load_migration_module",
        lambda _name, _path: migration,
    )

    assert run_migrations.run_migrations() is True
    assert calls == ["init_db", "017", "init_db"]


def test_initialized_main_database_uses_the_numbered_chain(monkeypatch):
    calls = []
    migrations = {
        "001_repository_schema": SimpleNamespace(
            run=lambda: calls.append("001") or True
        ),
        "017_durable_worker_schema": SimpleNamespace(
            run=lambda: calls.append("017") or True
        ),
    }

    monkeypatch.setattr(run_migrations, "cfg", _config("postgres"))
    monkeypatch.setattr(run_migrations, "migration_lock", nullcontext)
    monkeypatch.setattr(run_migrations, "is_database_initialized", lambda: True)
    monkeypatch.setattr(run_migrations, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(
        run_migrations,
        "discover_migrations",
        lambda: [
            ("001_repository_schema", MIGRATION_PATH),
            ("017_durable_worker_schema", MIGRATION_PATH),
        ],
    )
    monkeypatch.setattr(
        run_migrations,
        "load_migration_module",
        lambda name, _path: migrations[name],
    )

    assert run_migrations.run_migrations() is True
    assert calls == ["001", "017", "init_db"]


def test_runner_stops_before_017_and_finalization_after_failure(monkeypatch):
    calls = []
    migrations = {
        "008_foreignkey_refactoring": SimpleNamespace(
            run=lambda: calls.append("008") or False
        ),
        "017_durable_worker_schema": SimpleNamespace(
            run=lambda: calls.append("017") or True
        ),
    }

    monkeypatch.setattr(run_migrations, "cfg", _config("postgres"))
    monkeypatch.setattr(run_migrations, "migration_lock", nullcontext)
    monkeypatch.setattr(run_migrations, "is_database_initialized", lambda: True)
    monkeypatch.setattr(run_migrations, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(
        run_migrations,
        "discover_migrations",
        lambda: [
            ("008_foreignkey_refactoring", MIGRATION_PATH),
            ("017_durable_worker_schema", MIGRATION_PATH),
        ],
    )
    monkeypatch.setattr(
        run_migrations,
        "load_migration_module",
        lambda name, _path: migrations[name],
    )

    assert run_migrations.run_migrations() is False
    assert calls == ["008"]


def test_postgres_migration_lock_covers_the_runner(monkeypatch):
    events = []

    class Cursor:
        def execute(self, query, params=None):
            events.append(("execute", " ".join(query.split()), params))

    class Database:
        def connect(self, **_kwargs):
            events.append(("connect",))

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(run_migrations, "cfg", _config("postgres"))
    monkeypatch.setattr(run_migrations, "db", Database())
    monkeypatch.setattr(
        run_migrations,
        "_run_migrations_locked",
        lambda: events.append(("runner",)) or True,
    )

    assert run_migrations.run_migrations() is True
    assert "pg_advisory_lock" in events[1][1]
    assert events[2] == ("runner",)
    assert "pg_advisory_unlock" in events[3][1]


def test_generated_migration_service_is_noninteractive_and_uses_numbered_runner():
    service = generate_khub_migrate_service(
        {
            "postgres_builtin": True,
            "postgres_user": "db-user",
            "postgres_password": "db-password",
            "postgres_db": "kohakuhub",
        }
    )

    assert 'command: ["python", "scripts/run_migrations.py"]' in service
    assert "KOHAKU_HUB_AUTO_MIGRATE=true" in service

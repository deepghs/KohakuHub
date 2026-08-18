import pytest

import kohakuhub.worker.config as worker_config
from kohakuhub.worker.config import (
    CONTROL_LANE,
    CONTROL_QUEUE,
    WORK_LANE,
    WorkerSettings,
)


def test_default_lanes_reserve_control_capacity():
    assert CONTROL_LANE.queues == (CONTROL_QUEUE,)
    assert CONTROL_LANE.concurrency == 1
    assert WORK_LANE.concurrency == 3
    assert "bulk-v1" in WORK_LANE.queues


def test_worker_requires_postgres(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "sqlite")
    monkeypatch.setenv("KOHAKU_HUB_DATABASE_URL", "sqlite:///hub.db")

    with pytest.raises(ValueError, match="requires.*postgres"):
        WorkerSettings.from_env()


def test_worker_rejects_invalid_pool_order(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv(
        "KOHAKU_HUB_DATABASE_URL", "postgresql://user:pass@localhost/db"
    )
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MIN", "9")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MAX", "8")

    with pytest.raises(ValueError, match="max size"):
        WorkerSettings.from_env()


def test_worker_pool_split_must_fit_aggregate_budget(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv(
        "KOHAKU_HUB_DATABASE_URL", "postgresql://user:pass@localhost/db"
    )
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MAX", "8")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONTROL_POOL_MAX", "3")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_WORK_POOL_MAX", "6")

    with pytest.raises(ValueError, match="aggregate pool max"):
        WorkerSettings.from_env()


def test_worker_loads_valid_postgres_environment(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv(
        "KOHAKU_HUB_DATABASE_URL", "postgresql://user:pass@localhost/db"
    )

    settings = WorkerSettings.from_env()

    assert settings.database_url.endswith("/db")


def test_worker_rejects_non_positive_api_operation_pool_after_parsing(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv(
        "KOHAKU_HUB_DATABASE_URL", "postgresql://user:pass@localhost/db"
    )
    original_positive_int = worker_config._positive_int

    def fake_positive_int(name, default):
        if name == "KOHAKU_HUB_OPERATION_POOL_MAX":
            return 0
        return original_positive_int(name, default)

    monkeypatch.setattr(worker_config, "_positive_int", fake_positive_int)

    with pytest.raises(ValueError, match="API operation pool max size"):
        WorkerSettings.from_env()


def test_worker_connection_budget_covers_api_and_worker_reservations():
    settings = WorkerSettings(database_url="postgresql://user:pass@localhost/db")

    assert settings.configured_connection_budget == (
        4 * (4 + 4) + 2 + 6 + 4 + 2 + 2
    )

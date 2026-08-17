from procrastinate.testing import InMemoryConnector
import pytest

import kohakuhub.worker.app as worker_app
from kohakuhub.worker.app import build_worker_app, build_worker_apps
from kohakuhub.worker.config import WorkerSettings


def test_worker_app_registers_only_code_owned_tasks():
    app = build_worker_app(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        connector=InMemoryConnector(),
    )

    assert "khub:worker:probe.v1" in app.tasks
    task = app.tasks["khub:worker:probe.v1"]
    assert task.queue == "control-v1"
    assert task.priority == 100


def test_worker_app_carries_worker_fence_connection_limit():
    settings = WorkerSettings(
        database_url="postgresql://user:pass@localhost/db",
        worker_fence_max_connections=2,
    )

    app = build_worker_app(settings, connector=InMemoryConnector())

    assert app.khub_fence_connection_limit == 2


def test_worker_apps_reserve_periodic_registry_for_control_lane():
    control, work = build_worker_apps(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db")
    )

    assert control.periodic_registry.periodic_tasks
    assert not work.periodic_registry.periodic_tasks
    assert "khub:operation:execute.v1" in control.tasks
    assert "khub:operation:execute.v1" in work.tasks
    assert control.khub_fence_limiter_owner is work.khub_fence_limiter_owner


@pytest.mark.asyncio
async def test_operation_task_delegates_to_executor_with_lane_app(monkeypatch):
    connector = InMemoryConnector()
    connector.pool = object()
    app = build_worker_app(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        connector=connector,
        include_periodic=False,
    )
    calls = []

    async def execute(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(worker_app, "execute_operation_step", execute)
    await app.tasks["khub:operation:execute.v1"].func("operation", "step")

    assert calls[0][0] == (app.connector.pool, "operation", "step")
    assert calls[0][1]["app"] is app
    assert calls[0][1]["registry"] is worker_app.DEFAULT_REGISTRY


@pytest.mark.asyncio
async def test_reconciliation_task_passes_durable_retention_settings(monkeypatch):
    settings = WorkerSettings(
        database_url="postgresql://user:pass@localhost/db",
        operation_retention_hours=12,
        operation_retention_batch_size=7,
    )
    connector = InMemoryConnector()
    connector.pool = object()
    app = build_worker_app(
        settings,
        connector=connector,
        include_periodic=False,
    )
    calls = []

    async def reconcile(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(worker_app, "reconcile_once", reconcile)
    await app.tasks["khub:worker:reconcile.v1"].func(timestamp=123)

    assert calls[0][0] == (app.connector.pool, app)
    assert calls[0][1]["database_url"] == settings.database_url
    assert calls[0][1]["retention_hours"] == 12
    assert calls[0][1]["retention_batch_size"] == 7

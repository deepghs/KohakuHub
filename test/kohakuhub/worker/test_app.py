from procrastinate.testing import InMemoryConnector

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

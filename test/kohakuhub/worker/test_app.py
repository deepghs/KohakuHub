from procrastinate.testing import InMemoryConnector

from kohakuhub.worker.app import build_worker_app
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

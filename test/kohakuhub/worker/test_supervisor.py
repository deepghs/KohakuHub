import pytest

from kohakuhub.worker.config import CONTROL_LANE, WORK_LANE, WorkerSettings
from kohakuhub.worker.supervisor import WorkerSupervisor


class FakeWorker:
    instances = []

    def __init__(self, app, **kwargs):
        self.app = app
        self.kwargs = kwargs
        self.stopped = False
        self.instances.append(self)

    async def run(self):
        await self.app.worker_started(self)

    def stop(self):
        self.stopped = True


class FakeAppContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeApp:
    def __init__(self):
        self.stop_callback = None

    def open_async(self):
        return FakeAppContext()

    async def worker_started(self, worker):
        worker.stop()
        if self.stop_callback is not None:
            self.stop_callback()


@pytest.mark.asyncio
async def test_supervisor_constructs_reserved_lanes_and_stops_both():
    FakeWorker.instances = []
    app = FakeApp()
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        app_factory=lambda settings: app,
        worker_factory=FakeWorker,
    )
    app.stop_callback = supervisor.stop

    await supervisor.run()

    assert [worker.kwargs["concurrency"] for worker in FakeWorker.instances] == [1, 3]
    assert FakeWorker.instances[0].kwargs["queues"] == list(CONTROL_LANE.queues)
    assert FakeWorker.instances[1].kwargs["queues"] == list(WORK_LANE.queues)
    assert all(worker.stopped for worker in FakeWorker.instances)

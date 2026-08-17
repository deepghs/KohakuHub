import asyncio

import pytest

import kohakuhub.worker.supervisor as supervisor_module
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


@pytest.mark.asyncio
async def test_supervisor_does_not_wait_forever_when_a_lane_exits_before_readiness():
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db")
    )
    supervisor._workers = [object(), object()]

    async def exited():
        return None

    async def blocked():
        await asyncio.Event().wait()

    exited_task = asyncio.create_task(exited())
    blocked_task = asyncio.create_task(blocked())
    try:
        with pytest.raises(RuntimeError, match="exited before readiness"):
            await supervisor._wait_for_workers_started([exited_task, blocked_task])
    finally:
        blocked_task.cancel()
        await asyncio.gather(exited_task, blocked_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_supervisor_closes_http_when_app_construction_fails(monkeypatch):
    runner = object()
    closed = False

    async def serve(*_args):
        return runner

    async def close(value):
        nonlocal closed
        assert value is runner
        closed = True

    monkeypatch.setattr(supervisor_module, "serve_worker_http", serve)
    monkeypatch.setattr(supervisor_module, "close_worker_http", close)

    def fail(_settings):
        raise RuntimeError("app construction failed")

    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        app_factory=fail,
    )

    with pytest.raises(RuntimeError, match="app construction failed"):
        await supervisor.run()

    assert closed


@pytest.mark.asyncio
async def test_supervisor_cleans_pre_readiness_worker_failure(monkeypatch):
    runner = object()
    closed = False
    installed = 0
    removed = 0

    async def serve(*_args):
        return runner

    async def close(value):
        nonlocal closed
        assert value is runner
        closed = True

    class ExplodingWorker:
        worker_id = None

        def __init__(self, *_args, **_kwargs):
            self.stopped = False

        async def run(self):
            raise RuntimeError("worker startup failed")

        def stop(self):
            self.stopped = True

    def install():
        nonlocal installed
        installed += 1

    def remove():
        nonlocal removed
        removed += 1

    monkeypatch.setattr(supervisor_module, "serve_worker_http", serve)
    monkeypatch.setattr(supervisor_module, "close_worker_http", close)

    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        app_factory=lambda _settings: FakeApp(),
        worker_factory=ExplodingWorker,
    )
    supervisor._install_signal_handlers = install
    supervisor._remove_signal_handlers = remove

    with pytest.raises(RuntimeError, match="worker startup failed"):
        await supervisor.run()

    assert closed
    assert installed == removed == 1


@pytest.mark.asyncio
async def test_supervisor_stops_partially_constructed_workers(monkeypatch):
    class Runner:
        pass

    runner = Runner()
    closed = False

    async def serve(*_args):
        return runner

    async def close(value):
        nonlocal closed
        assert value is runner
        closed = True

    class PartiallyConstructedWorker:
        instances = []

        def __init__(self, _app, **_kwargs):
            if self.instances:
                raise RuntimeError("second worker failed")
            self.stopped = False
            self.instances.append(self)

        async def run(self):
            await asyncio.Event().wait()

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(supervisor_module, "serve_worker_http", serve)
    monkeypatch.setattr(supervisor_module, "close_worker_http", close)
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        app_factory=lambda _settings: FakeApp(),
        worker_factory=PartiallyConstructedWorker,
    )

    with pytest.raises(RuntimeError, match="second worker failed"):
        await supervisor.run()

    assert closed
    assert PartiallyConstructedWorker.instances[0].stopped

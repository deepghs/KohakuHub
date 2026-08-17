import asyncio
from types import SimpleNamespace

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


@pytest.mark.asyncio
async def test_supervisor_readiness_checks_connection_budget_and_schema(monkeypatch):
    checked = []

    class Result:
        def __init__(self, row):
            self.row = row

        async def fetchone(self):
            return self.row

    class Connection:
        async def execute(self, query, *_args):
            query = str(query)
            if "SELECT 1" in query:
                return Result((1,))
            if "max_connections" in query:
                return Result((100,))
            return Result((4,))

    class Pool:
        def connection(self):
            class Context:
                async def __aenter__(self):
                    return Connection()

                async def __aexit__(self, *_args):
                    return False

            return Context()

    async def verify(_connection):
        checked.append(True)

    monkeypatch.setattr(supervisor_module, "verify_operation_schema", verify)
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
    )
    await supervisor._verify_readiness(SimpleNamespace(connector=SimpleNamespace(pool=Pool())))

    assert checked == [True]


@pytest.mark.asyncio
async def test_supervisor_rejects_connection_budget_over_headroom(monkeypatch):
    class Result:
        def __init__(self, row):
            self.row = row

        async def fetchone(self):
            return self.row

    class Connection:
        async def execute(self, query, *_args):
            if "SELECT 1" in str(query):
                return Result((1,))
            if "max_connections" in str(query):
                return Result((100,))
            return Result((1,))

    class Pool:
        def connection(self):
            class Context:
                async def __aenter__(self):
                    return Connection()

                async def __aexit__(self, *_args):
                    return False

            return Context()

    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONNECTION_BUDGET", "81")
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
    )

    with pytest.raises(RuntimeError, match="exceeds 80% headroom"):
        await supervisor._verify_readiness(SimpleNamespace(connector=SimpleNamespace(pool=Pool())))


@pytest.mark.asyncio
async def test_supervisor_exports_process_health_and_pool_pressure(monkeypatch):
    stop = asyncio.Event()
    calls = 0

    async def wait_for(awaitable, timeout):
        nonlocal calls
        calls += 1
        awaitable.close()
        stop.set()
        raise asyncio.TimeoutError

    class Pool:
        def get_stats(self):
            return {"pool_size": 3, "requests_waiting": 2}

    monkeypatch.setattr(supervisor_module.asyncio, "wait_for", wait_for)
    monkeypatch.setattr(
        supervisor_module.resource,
        "getrusage",
        lambda _kind: SimpleNamespace(ru_maxrss=1024),
    )
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
    )
    supervisor._apps = [
        SimpleNamespace(connector=SimpleNamespace(pool=Pool())),
        SimpleNamespace(connector=SimpleNamespace(pool=Pool())),
    ]

    await supervisor._observe_process_health(stop)

    assert calls == 1


def test_supervisor_tolerates_platforms_without_signal_handlers(monkeypatch):
    class Loop:
        def add_signal_handler(self, *_args):
            raise NotImplementedError

        def remove_signal_handler(self, *_args):
            raise RuntimeError("embedded loop")

    monkeypatch.setattr(supervisor_module.asyncio, "get_running_loop", lambda: Loop())
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
    )

    supervisor._install_signal_handlers()
    supervisor._remove_signal_handlers()


@pytest.mark.asyncio
async def test_supervisor_reports_unexpected_ready_lane_exit(monkeypatch):
    class Runner:
        pass

    class ReadyWorker:
        def __init__(self, *_args, **_kwargs):
            self.worker_id = "ready"
            self.stopped = False

        async def run(self):
            return None

        def stop(self):
            self.stopped = True

    class App(FakeApp):
        pass

    runner = Runner()
    monkeypatch.setattr(supervisor_module, "serve_worker_http", lambda *_args: _async_value(runner))
    closed = []

    async def close(value):
        closed.append(value)

    monkeypatch.setattr(supervisor_module, "close_worker_http", close)
    supervisor = WorkerSupervisor(
        WorkerSettings(database_url="postgresql://user:pass@localhost/db"),
        app_factory=lambda _settings: (App(), App()),
        worker_factory=ReadyWorker,
    )
    supervisor._install_signal_handlers = lambda: None
    supervisor._remove_signal_handlers = lambda: None

    with pytest.raises(RuntimeError, match="exited unexpectedly"):
        await supervisor.run()

    assert closed == [runner]


async def _async_value(value):
    return value

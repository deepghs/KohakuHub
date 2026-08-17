"""Supervise the control and work Procrastinate lanes in one service."""

from __future__ import annotations

import asyncio
import signal
from contextlib import AsyncExitStack
from collections.abc import Callable
from typing import Any

from procrastinate.worker import Worker

from .app import build_worker_apps
from .config import CONTROL_LANE, WORK_LANE, WorkerLane, WorkerSettings
from .http import close_worker_http, serve_worker_http
from kohakuhub.operations.metrics import METRICS_REGISTRY
from kohakuhub.operations.readiness import verify_operation_schema


WorkerFactory = Callable[..., Worker]


class WorkerSupervisor:
    """Run both lanes and fail the service if either lane fails."""

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        app_factory: Callable[..., Any] = build_worker_apps,
        worker_factory: WorkerFactory = Worker,
        lanes: tuple[WorkerLane, ...] = (CONTROL_LANE, WORK_LANE),
    ) -> None:
        self.settings = settings
        self.app_factory = app_factory
        self.worker_factory = worker_factory
        self.lanes = lanes
        self._workers: list[Worker] = []
        self._apps: tuple[Any, ...] = ()
        self._stop_requested = False

    async def run(self) -> None:
        ready = asyncio.Event()
        http_runner = await serve_worker_http(
            self.settings.metrics_host, self.settings.metrics_port, ready
        )
        built = self.app_factory(self.settings)
        if isinstance(built, (tuple, list)):
            apps = tuple(built)
        else:
            # Backward-compatible test and embedding hook. Production uses
            # build_worker_apps and therefore gets one App per lane.
            apps = tuple(built for _ in self.lanes)
        if len(apps) != len(self.lanes):
            raise RuntimeError("worker app count must match lane count")
        self._apps = apps
        try:
            async with AsyncExitStack() as stack:
                opened: set[int] = set()
                for app in apps:
                    if id(app) in opened:
                        continue
                    await stack.enter_async_context(app.open_async())
                    opened.add(id(app))
                for app in apps:
                    await self._verify_readiness(app)
                self._workers = [
                    self.worker_factory(
                        app,
                        # Procrastinate binds queue filters as a PostgreSQL array;
                        # psycopg3 accepts a list here, while our policy stays immutable.
                        queues=list(lane.queues),
                        name=f"khub-{lane.name}",
                        concurrency=lane.concurrency,
                        wait=True,
                        fetch_job_polling_interval=self.settings.polling_interval_seconds,
                        abort_job_polling_interval=self.settings.polling_interval_seconds,
                        shutdown_graceful_timeout=self.settings.graceful_shutdown_seconds,
                        install_signal_handlers=False,
                        update_heartbeat_interval=self.settings.heartbeat_interval_seconds,
                        stalled_worker_timeout=self.settings.stalled_worker_timeout_seconds,
                    )
                    for app, lane in zip(apps, self.lanes)
                ]
                self._install_signal_handlers()
                tasks = [asyncio.create_task(worker.run()) for worker in self._workers]
                # Let both worker coroutines register their listeners before
                # advertising readiness.  Production Worker instances expose a
                # worker_id after PostgreSQL registration; lightweight fakes do
                # not, so they are considered started after one event-loop turn.
                await self._wait_for_workers_started(tasks)
                ready.set()
                try:
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    failures = [task.exception() for task in done if not task.cancelled()]
                    if failures and failures[0] is not None:
                        raise failures[0]
                    if not self._stop_requested:
                        raise RuntimeError("khub-worker lane exited unexpectedly")
                finally:
                    ready.clear()
                    self.stop()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self._remove_signal_handlers()
                    self._workers.clear()
        finally:
            self._apps = ()
            ready.clear()
            await close_worker_http(http_runner)

    async def _wait_for_workers_started(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Wait for registration and fail promptly when a lane exits early."""

        while True:
            if all(getattr(worker, "worker_id", None) is not None for worker in self._workers):
                return
            done_tasks = [task for task in tasks if task.done()]
            if done_tasks:
                if self._stop_requested:
                    return
                for task in done_tasks:
                    if task.cancelled():
                        raise RuntimeError("khub-worker lane was cancelled before readiness")
                    error = task.exception()
                    if error is not None:
                        raise error
                raise RuntimeError("khub-worker lane exited before readiness")
            await asyncio.sleep(0)

    async def _verify_readiness(self, app: Any) -> None:
        """Fail startup before ready if the durable control plane is absent."""

        connector = getattr(app, "connector", None)
        pool = getattr(connector, "pool", None)
        if pool is None:
            # Lightweight fake apps used by supervisor unit tests do not expose
            # a connector. Production Procrastinate apps always do.
            return
        async with pool.connection() as connection:
            row = await connection.execute("SELECT 1")
            if await row.fetchone() != (1,):
                raise RuntimeError("worker database readiness check failed")
            await verify_operation_schema(connection)

    def stop(self) -> None:
        self._stop_requested = True
        for worker in self._workers:
            worker.stop()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, self.stop)
            except (NotImplementedError, RuntimeError):
                # Windows and embedded loops can lack signal-handler support.
                continue

    def _remove_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.remove_signal_handler(signum)
            except (NotImplementedError, RuntimeError):
                continue


def run_worker() -> None:
    """Synchronous process entrypoint used by Docker and local smoke tests."""

    settings = WorkerSettings.from_env()
    asyncio.run(WorkerSupervisor(settings).run())

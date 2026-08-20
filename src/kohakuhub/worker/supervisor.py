"""Supervise the control and work Procrastinate lanes in one service."""

from __future__ import annotations

import asyncio
import os
import resource
import signal
from contextlib import AsyncExitStack
from collections.abc import Awaitable, Callable
from typing import Any

from procrastinate.worker import Worker

from .app import build_worker_apps
from .config import CONTROL_LANE, WORK_LANE, WorkerLane, WorkerSettings
from .http import close_worker_http, serve_worker_http
from kohakuhub.operations.metrics import (
    DB_POOL_SIZE,
    DB_POOL_WAITING,
    WORKER_EVENT_LOOP_LAG_SECONDS,
    WORKER_RSS_BYTES,
)
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
        self._stop_requested = False
        ready = asyncio.Event()
        startup_deadline = (
            asyncio.get_running_loop().time() + self.settings.startup_timeout_seconds
        )
        http_runner: Any | None = None
        tasks: list[asyncio.Task[Any]] = []
        health_stop: asyncio.Event | None = None
        health_task: asyncio.Task[Any] | None = None
        signal_handlers_installed = False
        try:
            http_runner = await self._await_startup(
                serve_worker_http(
                    self.settings.metrics_host, self.settings.metrics_port, ready
                ),
                startup_deadline,
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
            async with AsyncExitStack() as stack:
                opened: set[int] = set()
                for app in apps:
                    if id(app) in opened:
                        continue
                    await self._await_startup(
                        stack.enter_async_context(app.open_async()), startup_deadline
                    )
                    opened.add(id(app))
                for app in apps:
                    await self._await_startup(
                        self._verify_readiness(app), startup_deadline
                    )
                self._workers = []
                for app, lane in zip(apps, self.lanes):
                    self._workers.append(
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
                    )
                signal_handlers_installed = True
                self._install_signal_handlers()
                tasks = [asyncio.create_task(worker.run()) for worker in self._workers]
                health_stop = asyncio.Event()
                health_task = asyncio.create_task(
                    self._observe_process_health(health_stop)
                )
                # Let both worker coroutines register their listeners before
                # advertising readiness.  Production Worker instances expose a
                # worker_id after PostgreSQL registration; lightweight fakes do
                # not, so they are considered started after one event-loop turn.
                await self._wait_for_workers_started(
                    tasks, deadline=startup_deadline
                )
                ready.set()
                done, _ = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                failures = [task.exception() for task in done if not task.cancelled()]
                failure = next((error for error in failures if error is not None), None)
                if failure is not None:
                    raise failure
                if not self._stop_requested:
                    raise RuntimeError("khub-worker lane exited unexpectedly")
        finally:
            ready.clear()
            self.stop()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if health_stop is not None:
                health_stop.set()
            if health_task is not None:
                await asyncio.gather(health_task, return_exceptions=True)
            if signal_handlers_installed:
                self._remove_signal_handlers()
            self._workers.clear()
            self._apps = ()
            if http_runner is not None:
                await close_worker_http(http_runner)

    async def _await_startup(
        self, awaitable: Awaitable[Any], deadline: float
    ) -> Any:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise RuntimeError(
                "khub-worker startup did not complete within "
                f"{self.settings.startup_timeout_seconds:g} seconds"
            )
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                "khub-worker startup did not complete within "
                f"{self.settings.startup_timeout_seconds:g} seconds"
            ) from exc

    async def _wait_for_workers_started(
        self,
        tasks: list[asyncio.Task[Any]],
        *,
        deadline: float | None = None,
    ) -> None:
        """Wait for registration and fail promptly when a lane exits early."""

        loop = asyncio.get_running_loop()
        deadline = deadline or loop.time() + self.settings.startup_timeout_seconds
        while True:
            if all(
                getattr(worker, "worker_id", None) is not None
                for worker in self._workers
            ):
                return
            done_tasks = [task for task in tasks if task.done()]
            if done_tasks:
                if self._stop_requested:
                    return
                for task in done_tasks:
                    if task.cancelled():
                        raise RuntimeError(
                            "khub-worker lane was cancelled before readiness"
                        )
                    error = task.exception()
                    if error is not None:
                        raise error
                raise RuntimeError("khub-worker lane exited before readiness")

            remaining = deadline - loop.time()
            if remaining <= 0:
                self.stop()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise RuntimeError(
                    "khub-worker did not become ready within "
                    f"{self.settings.startup_timeout_seconds:g} seconds"
                )
            await asyncio.sleep(min(0.05, remaining))

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
            row = await connection.execute(
                "SELECT current_setting('max_connections')::integer"
            )
            max_connections = int((await row.fetchone())[0])
            row = await connection.execute(
                "SELECT count(*) FROM pg_stat_activity"
            )
            active_connections = int((await row.fetchone())[0])
            configured_budget = int(
                os.getenv(
                    "KOHAKU_HUB_WORKER_CONNECTION_BUDGET",
                    str(self.settings.configured_connection_budget),
                )
            )
            if configured_budget < 1:
                raise RuntimeError("worker connection budget must be positive")
            required_budget = self.settings.configured_connection_budget
            if configured_budget < required_budget:
                raise RuntimeError(
                    "worker connection budget is below configured allocation: "
                    f"configured={configured_budget} required={required_budget}"
                )
            headroom_limit = max_connections * 0.8
            if configured_budget > headroom_limit:
                raise RuntimeError(
                    "worker PostgreSQL connection budget exceeds 80% headroom: "
                    f"active={active_connections} configured={configured_budget} "
                    f"max={max_connections}"
                )
            if active_connections > headroom_limit:
                raise RuntimeError(
                    "current PostgreSQL connections exceed 80% headroom: "
                    f"active={active_connections} max={max_connections}"
                )
            await verify_operation_schema(connection)

    async def _observe_process_health(self, stop: asyncio.Event) -> None:
        """Export process scheduling, RSS, and database-pool pressure."""

        loop = asyncio.get_running_loop()
        interval = 1.0
        deadline = loop.time() + interval
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                continue
            except asyncio.TimeoutError:
                pass

            now = loop.time()
            WORKER_EVENT_LOOP_LAG_SECONDS.set(max(0.0, now - deadline))
            deadline = now + interval
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Linux reports KiB; macOS reports bytes.  Docker deployments use
            # Linux, while the fallback keeps local development meaningful.
            WORKER_RSS_BYTES.set(float(rss * 1024 if rss < 10**9 else rss))
            for app, lane in zip(self._apps, self.lanes):
                connector = getattr(app, "connector", None)
                pool = getattr(connector, "pool", None)
                get_stats = getattr(pool, "get_stats", None)
                if get_stats is None:
                    continue
                stats = get_stats()
                DB_POOL_SIZE.labels(lane=lane.name).set(
                    float(stats.get("pool_size", 0))
                )
                DB_POOL_WAITING.labels(lane=lane.name).set(
                    float(stats.get("requests_waiting", 0))
                )

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

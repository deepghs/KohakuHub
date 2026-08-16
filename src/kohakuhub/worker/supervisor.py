"""Supervise the control and work Procrastinate lanes in one service."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from typing import Any

from procrastinate.worker import Worker

from .app import build_worker_app
from .config import CONTROL_LANE, WORK_LANE, WorkerLane, WorkerSettings


WorkerFactory = Callable[..., Worker]


class WorkerSupervisor:
    """Run both lanes and fail the service if either lane fails."""

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        app_factory: Callable[..., Any] = build_worker_app,
        worker_factory: WorkerFactory = Worker,
        lanes: tuple[WorkerLane, ...] = (CONTROL_LANE, WORK_LANE),
    ) -> None:
        self.settings = settings
        self.app_factory = app_factory
        self.worker_factory = worker_factory
        self.lanes = lanes
        self._workers: list[Worker] = []
        self._stop_requested = False

    async def run(self) -> None:
        app = self.app_factory(self.settings)
        async with app.open_async():
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
                for lane in self.lanes
            ]
            self._install_signal_handlers()
            tasks = [asyncio.create_task(worker.run()) for worker in self._workers]
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
                self.stop()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._remove_signal_handlers()
                self._workers.clear()

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

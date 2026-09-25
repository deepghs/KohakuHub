"""Background task worker: ``python -m kohakuhub.worker``.

Claims rows from ``background_task`` and runs their registered handlers with
bounded concurrency. Delivery semantics live in ``kohakuhub.tasks``.
The worker never runs migrations; it waits until hub-api has created the table.
"""

import asyncio
import json
import os
import signal
import socket
import uuid

from kohakuhub import tasks
from kohakuhub.config import cfg
from kohakuhub.db import BackgroundTask, db
from kohakuhub.lakefs_rest_client import close_lakefs_rest_client
from kohakuhub.logger import get_logger

logger = get_logger("WORKER")


def _reset_connection() -> None:
    """Drop a possibly broken connection; Peewee reconnects on next use."""
    try:
        db.close()
    except Exception as e:
        logger.warning(f"Failed to close database connection: {e}")


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


class Worker:
    def __init__(
        self,
        *,
        worker_id: str | None = None,
        concurrency: int | None = None,
        lease_seconds: float | None = None,
        poll_interval: float | None = None,
        shutdown_grace: float | None = None,
        queues: list[str] | None = None,
    ):
        options = cfg.worker
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.concurrency = concurrency or options.concurrency
        self.lease_seconds = lease_seconds or options.lease_seconds
        self.poll_interval = poll_interval or options.poll_interval_seconds
        self.shutdown_grace = shutdown_grace or options.shutdown_grace_seconds
        self.queues = options.queues if queues is None else queues
        self._running: set[asyncio.Task] = set()

    async def wait_for_schema(self, stop: asyncio.Event) -> bool:
        """Block until the task table exists; ``False`` if stopped first."""
        while not stop.is_set():
            try:
                if BackgroundTask.table_exists():
                    return True
                logger.info("Waiting for the background_task table (migrations run on hub-api)")
            except Exception as e:
                logger.warning(f"Database not ready: {e}")
                _reset_connection()
            await _sleep_or_stop(stop, self.poll_interval)
        return False

    async def run(self, stop: asyncio.Event) -> None:
        """Claim and run tasks until ``stop`` is set, then drain."""
        tasks.ensure_periodic_tasks()
        logger.info(
            f"Worker {self.worker_id} started "
            f"(concurrency={self.concurrency}, queues={self.queues or 'all'})"
        )
        while not stop.is_set():
            if len(self._running) < self.concurrency and (row := self._claim()) is not None:
                self._start(row)
                continue
            await self._wait(stop)
        await self._drain()
        logger.info(f"Worker {self.worker_id} stopped")

    def _claim(self) -> BackgroundTask | None:
        try:
            return tasks.claim_next(
                self.worker_id, lease_seconds=self.lease_seconds, queues=self.queues or None
            )
        except Exception as e:
            logger.exception("Failed to claim a task", e)
            _reset_connection()
            return None

    def _start(self, row: BackgroundTask) -> None:
        running = asyncio.create_task(self._execute(row))
        self._running.add(running)
        running.add_done_callback(self._running.discard)

    async def _wait(self, stop: asyncio.Event) -> None:
        """Sleep until stop, a handler finishing, or the next poll."""
        stop_waiter = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait(
                {stop_waiter, *self._running},
                timeout=self.poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            stop_waiter.cancel()

    async def _drain(self) -> None:
        if not self._running:
            return
        logger.info(f"Waiting up to {self.shutdown_grace}s for {len(self._running)} task(s)")
        _, pending = await asyncio.wait(set(self._running), timeout=self.shutdown_grace)
        # Cancelled tasks keep status=running; their lease expires and
        # another worker claims them again.
        for running in pending:
            running.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _execute(self, row: BackgroundTask) -> None:
        spec = tasks.get_spec(row.kind)
        run = asyncio.create_task(
            asyncio.wait_for(spec.handler(json.loads(row.payload)), timeout=spec.timeout)
        )
        heartbeat = asyncio.create_task(self._heartbeat(row, run))
        try:
            await run
        except asyncio.CancelledError:
            if not (heartbeat.done() and heartbeat.result()):
                raise  # worker shutdown
            logger.warning(f"Lost the lease on task {row.id} ({row.kind}); abandoned it")
        except asyncio.TimeoutError:
            self._record_failure(row, f"Timed out after {spec.timeout}s")
        except tasks.PermanentTaskError as e:
            self._record_failure(row, f"PermanentTaskError: {e}", permanent=True)
        except Exception as e:
            self._record_failure(row, f"{type(e).__name__}: {e}")
        else:
            self._record(lambda: tasks.complete_task(row))
        finally:
            heartbeat.cancel()
            run.cancel()

    async def _heartbeat(self, row: BackgroundTask, run: asyncio.Task) -> bool:
        """Renew the lease; cancel the handler and return ``True`` if it is lost."""
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            try:
                owned = tasks.renew_lease(row, lease_seconds=self.lease_seconds)
            except Exception as e:
                logger.warning(f"Failed to renew the lease on task {row.id}: {e}")
                _reset_connection()
                continue
            if not owned:
                run.cancel()
                return True

    def _record_failure(self, row: BackgroundTask, error: str, *, permanent: bool = False) -> None:
        status = self._record(lambda: tasks.fail_task(row, error, permanent=permanent))
        logger.warning(
            f"Task {row.id} ({row.kind}) attempt {row.attempts} failed -> {status}: {error}"
        )

    def _record(self, write):
        try:
            return write()
        except Exception as e:
            # The lease expires and another worker retries the task.
            logger.exception("Failed to record a task outcome", e)
            _reset_connection()
            return None


async def serve(worker: Worker | None = None) -> None:
    """Run a worker until SIGINT/SIGTERM."""
    worker = worker or Worker()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        if await worker.wait_for_schema(stop):
            await worker.run(stop)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await close_lakefs_rest_client()
        _reset_connection()


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":  # pragma: no cover
    main()

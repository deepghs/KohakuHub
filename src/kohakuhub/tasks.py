"""Durable background tasks stored in the application database.

A task is one ``BackgroundTask`` row. ``enqueue`` inserts it through the shared
Peewee ``db``, so calling it inside ``db.atomic()`` commits or rolls back
together with the caller's business writes. ``python -m kohakuhub.worker``
claims and runs tasks.

Delivery is at-least-once: a worker holds a lease on the task it runs and
renews it while the handler is alive. When a worker dies, its lease expires
and another worker claims the task again, so every handler must be idempotent.

Handlers share the event loop thread's database connection with the worker's
bookkeeping (as hub-api request handlers do), so never ``await`` inside
``db.atomic()``.
"""

import inspect
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from peewee import PostgresqlDatabase

from kohakuhub.config import cfg
from kohakuhub.db import BackgroundTask, utcnow
from kohakuhub.logger import get_logger

logger = get_logger("TASKS")

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
STATUSES = (QUEUED, RUNNING, SUCCEEDED, FAILED)

MAX_ERROR_LENGTH = 4000
RETRY_BASE_SECONDS = 5
RETRY_MAX_SECONDS = 3600
CLEANUP_KIND = "tasks.cleanup"

TaskHandler = Callable[[dict[str, Any]], Awaitable[None]]


class PermanentTaskError(Exception):
    """Raised by a handler to fail its task without further retries."""


@dataclass(frozen=True)
class TaskSpec:
    kind: str
    handler: TaskHandler
    queue: str
    max_attempts: int
    timeout: float
    every: timedelta | None


_registry: dict[str, TaskSpec] = {}


def task(
    kind: str,
    *,
    queue: str = "default",
    max_attempts: int = 5,
    timeout: float = 3600.0,
    every: timedelta | None = None,
) -> Callable[[TaskHandler], TaskHandler]:
    """Register an async handler for ``kind``.

    Only registered kinds can be enqueued or claimed; rows never name code.
    ``every`` makes the kind periodic (see ``ensure_periodic_tasks``).
    """

    def decorator(handler: TaskHandler) -> TaskHandler:
        if not inspect.iscoroutinefunction(handler):
            raise TypeError(f"Task handler for {kind!r} must be an async function")
        if kind in _registry:
            raise ValueError(f"Task kind already registered: {kind}")
        _registry[kind] = TaskSpec(kind, handler, queue, max_attempts, timeout, every)
        return handler

    return decorator


def get_spec(kind: str) -> TaskSpec | None:
    return _registry.get(kind)


def periodic_key(kind: str) -> str:
    return f"periodic:{kind}"


def enqueue(
    kind: str,
    payload: dict[str, Any] | None = None,
    *,
    dedupe_key: str | None = None,
    run_after: datetime | None = None,
    priority: int = 0,
    queue: str | None = None,
) -> int | None:
    """Insert a task and return its id.

    Returns ``None`` when a pending task with the same ``dedupe_key`` already
    exists; that task will do the work. Keep payloads to IDs and small
    parameters: never credentials, presigned URLs, or file content.
    """
    spec = _registry.get(kind)
    if spec is None:
        raise ValueError(f"Unknown task kind: {kind}")
    if run_after is not None and run_after.tzinfo is not None:
        # Columns hold naive UTC; see kohakuhub.db.utcnow.
        run_after = run_after.astimezone(timezone.utc).replace(tzinfo=None)
    now = utcnow()
    rows = list(
        BackgroundTask.insert(
            kind=kind,
            queue=queue or spec.queue,
            payload=json.dumps(payload or {}),
            priority=priority,
            dedupe_key=dedupe_key,
            run_after=run_after or now,
            max_attempts=spec.max_attempts,
            created_at=now,
        )
        .on_conflict_ignore()
        .returning(BackgroundTask.id)
        .tuples()
        .execute()
    )
    return rows[0][0] if rows else None


def ensure_periodic_tasks() -> None:
    """Make sure every periodic kind has its pending occurrence."""
    for spec in _registry.values():
        if spec.every is not None:
            enqueue(spec.kind, dedupe_key=periodic_key(spec.kind))


def _select_candidate(now: datetime, queues: list[str] | None) -> BackgroundTask | None:
    T = BackgroundTask
    eligible = ((T.status == QUEUED) & (T.run_after <= now)) | (
        (T.status == RUNNING) & (T.locked_until < now)
    )
    condition = eligible & T.kind.in_(list(_registry))
    if queues:
        condition &= T.queue.in_(queues)
    query = T.select().where(condition).order_by(T.priority.desc(), T.run_after, T.id)
    if isinstance(T._meta.database, PostgresqlDatabase):
        # SQLite serializes writers, so it needs no row lock.
        query = query.for_update(skip_locked=True)
    return query.first()


def claim_next(
    worker_id: str, *, lease_seconds: int, queues: list[str] | None = None
) -> BackgroundTask | None:
    """Claim the next eligible task for ``worker_id``, or return ``None``.

    Eligible tasks are queued tasks that are due, and running tasks whose
    lease expired. Claiming releases the dedupe key and, for periodic kinds,
    schedules the next occurrence in the same transaction.
    """
    if not _registry:
        return None
    T = BackgroundTask
    now = utcnow()
    with T._meta.database.atomic():
        while (candidate := _select_candidate(now, queues)) is not None:
            if candidate.status == RUNNING and candidate.attempts >= candidate.max_attempts:
                # The worker died on its last attempt; don't loop on a task
                # that keeps killing workers.
                _finish(candidate, FAILED, now, "lease expired on the final attempt")
                continue
            # Compare-and-set on the observed state: under SQLite a concurrent
            # worker may have claimed the same row since we read it.
            claimed = (
                T.update(
                    status=RUNNING,
                    attempts=T.attempts + 1,
                    locked_by=worker_id,
                    locked_until=now + timedelta(seconds=lease_seconds),
                    started_at=now,
                    dedupe_key=None,
                )
                .where(
                    (T.id == candidate.id)
                    & (T.status == candidate.status)
                    & (T.attempts == candidate.attempts)
                )
                .execute()
            )
            if not claimed:
                return None
            spec = _registry[candidate.kind]
            if spec.every is not None:
                enqueue(spec.kind, dedupe_key=periodic_key(spec.kind), run_after=now + spec.every)
            return T.get_by_id(candidate.id)
    return None


def _owned(task_row: BackgroundTask):
    T = BackgroundTask
    return (
        (T.id == task_row.id)
        & (T.status == RUNNING)
        & (T.locked_by == task_row.locked_by)
        & (T.attempts == task_row.attempts)
    )


def _finish(task_row: BackgroundTask, status: str, now: datetime, error: str | None) -> int:
    return (
        BackgroundTask.update(
            status=status,
            finished_at=now,
            locked_by=None,
            locked_until=None,
            last_error=error,
        )
        .where(_owned(task_row))
        .execute()
    )


def renew_lease(task_row: BackgroundTask, *, lease_seconds: int) -> bool:
    """Extend the lease; ``False`` means this worker no longer owns the task."""
    until = utcnow() + timedelta(seconds=lease_seconds)
    return bool(BackgroundTask.update(locked_until=until).where(_owned(task_row)).execute())


def complete_task(task_row: BackgroundTask) -> bool:
    return bool(_finish(task_row, SUCCEEDED, utcnow(), None))


def release_task(task_row: BackgroundTask) -> bool:
    """Hand an interrupted task back to the queue without spending the attempt."""
    T = BackgroundTask
    return bool(
        T.update(
            status=QUEUED,
            attempts=T.attempts - 1,
            run_after=utcnow(),
            locked_by=None,
            locked_until=None,
        )
        .where(_owned(task_row))
        .execute()
    )


def retry_delay(attempts: int) -> float:
    """Exponential backoff with up to 25% jitter."""
    delay = min(RETRY_BASE_SECONDS * 2 ** (attempts - 1), RETRY_MAX_SECONDS)
    return delay * random.uniform(1.0, 1.25)


def fail_task(task_row: BackgroundTask, error: str, *, permanent: bool = False) -> str | None:
    """Record a failed attempt; return the new status, or ``None`` if not owned."""
    error = error[:MAX_ERROR_LENGTH]
    now = utcnow()
    if permanent or task_row.attempts >= task_row.max_attempts:
        return FAILED if _finish(task_row, FAILED, now, error) else None
    requeued = (
        BackgroundTask.update(
            status=QUEUED,
            run_after=now + timedelta(seconds=retry_delay(task_row.attempts)),
            locked_by=None,
            locked_until=None,
            last_error=error,
        )
        .where(_owned(task_row))
        .execute()
    )
    return QUEUED if requeued else None


def retry_failed_task(task_id: int) -> bool:
    """Requeue a failed task with a fresh attempt budget."""
    T = BackgroundTask
    return bool(
        T.update(status=QUEUED, attempts=0, run_after=utcnow(), finished_at=None)
        .where((T.id == task_id) & (T.status == FAILED))
        .execute()
    )


def discard_task(task_id: int) -> bool:
    """Delete a queued or failed task. Running tasks cannot be discarded."""
    T = BackgroundTask
    return bool(T.delete().where((T.id == task_id) & T.status.in_([QUEUED, FAILED])).execute())


@task(CLEANUP_KIND, every=timedelta(hours=1), max_attempts=3)
async def cleanup_finished_tasks(payload: dict[str, Any]) -> None:
    """Delete finished tasks older than the configured retention."""
    T = BackgroundTask
    now = utcnow()
    succeeded_before = now - timedelta(days=cfg.worker.succeeded_retention_days)
    failed_before = now - timedelta(days=cfg.worker.failed_retention_days)
    # ponytail: one unbounded DELETE; batch it if the table ever holds millions of rows.
    deleted = (
        T.delete()
        .where(
            ((T.status == SUCCEEDED) & (T.finished_at < succeeded_before))
            | ((T.status == FAILED) & (T.finished_at < failed_before))
        )
        .execute()
    )
    if deleted:
        logger.info(f"Deleted {deleted} finished background task(s)")

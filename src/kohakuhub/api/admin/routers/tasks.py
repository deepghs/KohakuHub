"""Background task inspection endpoints for admin API."""

import json
import math
from operator import itemgetter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from peewee import fn

from kohakuhub.db import (
    BackgroundTask,
    BackgroundTaskEvent,
    BackgroundTaskLog,
    BackgroundWorker,
    utcnow,
)
from kohakuhub.logger import get_logger
from kohakuhub.tasks import (
    CANCELLED,
    FAILED,
    QUEUED,
    RUNNING,
    STATUSES,
    SUCCEEDED,
    WORKER_INACTIVE_AFTER,
    WORKER_LOST_AFTER,
    discard_task,
    estimate_eta_seconds,
    is_stalled,
    request_cancel,
    retry_failed_task,
    worker_status,
)
from kohakuhub.api.admin.utils import verify_admin_token

logger = get_logger("ADMIN")
router = APIRouter()

# Dashboard windows: label -> (span, number of buckets in the activity series).
STATS_WINDOWS = {
    "15m": (timedelta(minutes=15), 30),
    "1h": (timedelta(hours=1), 60),
    "6h": (timedelta(hours=6), 72),
    "24h": (timedelta(hours=24), 96),
    "7d": (timedelta(days=7), 84),
}
BACKLOG_WARN_SECONDS = 60
BACKLOG_CRITICAL_SECONDS = 300
FAILURE_RATE_WARN = 0.10
FAILURE_RATE_CRITICAL = 0.50
MIN_RATE_SAMPLES = 5
TOP_ERRORS = 8
LOG_PAGE_MAX = 2000
LOG_DOWNLOAD_BATCH = 1000
# Events that end an attempt, for the per-attempt summary.
ATTEMPT_OUTCOMES = {
    "succeeded": "succeeded",
    "failed": "failed",
    "retry_scheduled": "failed",
    "cancelled": "cancelled",
    "released": "released",
    "lease_expired": "lease expired",
}


def _iso(value: datetime | None) -> str | None:
    # Stored as naive UTC; tag it so the admin UI renders local time correctly.
    return value.replace(tzinfo=timezone.utc).isoformat() if value else None


def _payload(task: BackgroundTask):
    try:
        return json.loads(task.payload)
    except ValueError:
        return task.payload  # corrupt rows stay visible so they can be discarded


def _progress(task: BackgroundTask, now: datetime) -> dict | None:
    if task.progress_done is None and task.progress_stage is None:
        return None
    return {
        "done": task.progress_done,
        "total": task.progress_total,
        "stage": task.progress_stage,
        "updated_at": _iso(task.progress_at),
        "eta_seconds": estimate_eta_seconds(task, now),
    }


def _serialize(task: BackgroundTask) -> dict:
    now = utcnow()
    return {
        "id": task.id,
        "kind": task.kind,
        "queue": task.queue,
        "payload": _payload(task),
        "status": task.status,
        "priority": task.priority,
        "dedupe_key": task.dedupe_key,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "run_after": _iso(task.run_after),
        "locked_by": task.locked_by,
        "locked_until": _iso(task.locked_until),
        "lease_expired": task.status == RUNNING
        and task.locked_until is not None
        and task.locked_until < now,
        "cancel_requested": task.cancel_requested,
        "stalled": is_stalled(task, now),
        "stall_seconds": task.stall_seconds,
        "progress": _progress(task, now),
        "last_error": task.last_error,
        "created_at": _iso(task.created_at),
        "started_at": _iso(task.started_at),
        "finished_at": _iso(task.finished_at),
    }


def _get_task(task_id: int) -> BackgroundTask:
    task = BackgroundTask.get_or_none(BackgroundTask.id == task_id)
    if task is None:
        raise HTTPException(404, detail={"error": f"Task not found: {task_id}"})
    return task


@router.get("/tasks")
async def list_tasks(
    status: str | None = None,
    kind: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _admin: bool = Depends(verify_admin_token),
):
    """List background tasks, newest first, with per-status counts.

    Args:
        status: Filter by status (queued, running, succeeded, failed, cancelled)
        kind: Filter by task kind
        limit: Maximum number to return
        offset: Offset for pagination
    """
    if status is not None and status not in STATUSES:
        raise HTTPException(400, detail={"error": f"Invalid status: {status}"})

    query = BackgroundTask.select()
    if status:
        query = query.where(BackgroundTask.status == status)
    if kind:
        query = query.where(BackgroundTask.kind == kind)
    total = query.count()
    rows = query.order_by(BackgroundTask.id.desc()).limit(limit).offset(offset)

    counts = dict.fromkeys(STATUSES, 0)
    for row_status, count in (
        BackgroundTask.select(BackgroundTask.status, fn.COUNT(BackgroundTask.id))
        .group_by(BackgroundTask.status)
        .tuples()
    ):
        counts[row_status] = count
    kinds = [
        row_kind
        for (row_kind,) in BackgroundTask.select(BackgroundTask.kind)
        .distinct()
        .order_by(BackgroundTask.kind)
        .tuples()
    ]

    return {
        "tasks": [_serialize(row) for row in rows],
        "total": total,
        "counts": counts,
        "kinds": kinds,
        "limit": limit,
        "offset": offset,
    }


def _percentile(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile; ``None`` for an empty sample."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def _format_age(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    return f"{int(seconds // 3600)}h"


def _first_line(error: str | None) -> str | None:
    return error.splitlines()[0][:200] if error else None


def _error_class(error: str | None) -> str:
    """Group errors by the exception name before the first colon."""
    line = _first_line(error)
    if not line:
        return "Unknown error"
    head, sep, _ = line.partition(":")
    head = head.strip()
    return head if sep and head and " " not in head else line[:80]


def _health(
    *,
    finished: int,
    failed: int,
    stuck: int,
    oldest_due: float | None,
    retrying: int,
    busy: bool,
    stalled: int = 0,
    due: int = 0,
    workers_online: int | None = None,
) -> tuple[str, list[dict]]:
    """Classify queue health and explain why, most severe signals first."""
    reasons: list[dict] = []

    def add(level: str, message: str) -> None:
        reasons.append({"level": level, "message": message})

    if due and workers_online == 0:
        add("unhealthy", f"No worker is online; {due} due task(s) are waiting")
    if stuck:
        add(
            "unhealthy",
            f"{stuck} running task(s) have an expired lease and were not reclaimed; "
            "is a worker running?",
        )
    if oldest_due is not None and oldest_due >= BACKLOG_CRITICAL_SECONDS:
        add(
            "unhealthy",
            f"Oldest due task has waited {_format_age(oldest_due)}; the queue is not draining",
        )
    elif oldest_due is not None and oldest_due >= BACKLOG_WARN_SECONDS:
        add("degraded", f"Oldest due task has waited {_format_age(oldest_due)}")
    if finished >= MIN_RATE_SAMPLES:
        rate = failed / finished
        if rate >= FAILURE_RATE_CRITICAL:
            add("unhealthy", f"{rate:.0%} of tasks finished in the window failed")
        elif rate >= FAILURE_RATE_WARN:
            add("degraded", f"{rate:.0%} of tasks finished in the window failed")
    elif failed:
        add("degraded", f"{failed} task(s) failed in the window")
    if retrying:
        add("degraded", f"{retrying} task(s) are waiting to retry after an error")
    if stalled:
        add("degraded", f"{stalled} running task(s) have made no progress for a while")

    if any(reason["level"] == "unhealthy" for reason in reasons):
        return "unhealthy", reasons
    if reasons:
        return "degraded", reasons
    return ("healthy" if busy or finished else "idle"), reasons


def build_task_stats(window: str, now: datetime) -> dict:
    """Aggregate queue health for the dashboard over ``window`` ending at ``now``."""
    span, bucket_count = STATS_WINDOWS[window]
    since = now - span
    bucket_seconds = span.total_seconds() / bucket_count
    T = BackgroundTask
    # ponytail: aggregates in Python over the window's rows; move to SQL
    # GROUP BY if a 7d window ever holds hundreds of thousands of tasks.
    rows = (
        T.select(
            T.kind,
            T.status,
            T.attempts,
            T.created_at,
            T.started_at,
            T.finished_at,
            T.run_after,
            T.locked_by,
            T.locked_until,
            T.last_error,
            T.stall_seconds,
            T.cancel_requested,
            T.progress_at,
        )
        # Rows created in the window are either still queued/running or
        # finished after creation, so these two clauses cover them too. The
        # finished clause matches the (status, finished_at) index.
        .where(
            T.status.in_([QUEUED, RUNNING])
            | (T.status.in_([SUCCEEDED, FAILED, CANCELLED]) & (T.finished_at >= since))
        ).dicts()
    )

    def bucket_of(moment: datetime) -> int:
        return min(bucket_count - 1, int((moment - since).total_seconds() // bucket_seconds))

    series = [{"succeeded": 0, "failed": 0, "enqueued": 0} for _ in range(bucket_count)]
    kinds: dict[str, dict] = {}
    errors: dict[str, dict] = {}
    durations: list[float] = []
    succeeded = failed = cancelled = after_retry = enqueued = 0
    due = scheduled = retrying = running = stuck = stalled = cancelling = 0
    oldest_due: datetime | None = None

    def kind_stats(kind: str) -> dict:
        return kinds.setdefault(
            kind,
            {
                "kind": kind,
                "succeeded": 0,
                "failed": 0,
                "queued": 0,
                "running": 0,
                "stuck": 0,
                "cancelled": 0,
                "durations": [],
                "timeline": [{"succeeded": 0, "failed": 0} for _ in range(bucket_count)],
                "failures": [],  # (finished_at, first error line)
            },
        )

    def record_error(row: dict, seen_at: datetime, field: str) -> None:
        group = errors.setdefault(
            _error_class(row["last_error"]),
            {"failed": 0, "retrying": 0, "kinds": set(), "seen": []},
        )
        group[field] += 1
        group["kinds"].add(row["kind"])
        group["seen"].append((seen_at, _first_line(row["last_error"])))

    # Newest-wins picks use max() so the result does not depend on row order.
    newest = itemgetter(0)

    for row in rows:
        stats = kind_stats(row["kind"])
        if row["created_at"] and row["created_at"] >= since:
            enqueued += 1
            series[bucket_of(row["created_at"])]["enqueued"] += 1
        finished_at = row["finished_at"]
        match row["status"]:
            case "succeeded" | "failed":
                status = row["status"]
                index = bucket_of(finished_at)
                series[index][status] += 1
                stats["timeline"][index][status] += 1
                stats[status] += 1
                if row["started_at"]:
                    duration = (finished_at - row["started_at"]).total_seconds()
                    durations.append(duration)
                    stats["durations"].append(duration)
                if status == SUCCEEDED:
                    succeeded += 1
                    after_retry += row["attempts"] > 1
                else:
                    failed += 1
                    record_error(row, finished_at, "failed")
                    stats["failures"].append((finished_at, _first_line(row["last_error"])))
            case "cancelled":
                # An admin decision, not a failure: counted, but kept out of
                # the failure rate and the success/failure series.
                cancelled += 1
                stats["cancelled"] += 1
            case "queued":
                stats["queued"] += 1
                if row["run_after"] <= now:
                    due += 1
                    oldest_due = min(oldest_due or row["run_after"], row["run_after"])
                else:
                    scheduled += 1
                if row["attempts"] > 0 and row["last_error"]:
                    retrying += 1
                    record_error(row, row["started_at"] or row["created_at"], "retrying")
            case _:  # running: the query selects no other status
                running += 1
                stats["running"] += 1
                if row["locked_until"] is None or row["locked_until"] < now:
                    stuck += 1
                    stats["stuck"] += 1
                else:
                    stalled += is_stalled(SimpleNamespace(**row), now)
                cancelling += row["cancel_requested"]

    finished = succeeded + failed
    W = BackgroundWorker
    # Online or draining: not signed off and heard from recently.
    active_workers = (
        W.select()
        .where((W.state != "stopped") & (W.last_heartbeat_at >= now - WORKER_LOST_AFTER))
        .count()
    )
    oldest_due_seconds = (now - oldest_due).total_seconds() if oldest_due else None
    for kind in kinds.values():
        kind["last_failed_at"], kind["last_error"] = max(
            kind.pop("failures"), key=newest, default=(None, None)
        )
    for group in errors.values():
        group["last_seen"], group["example"] = max(group.pop("seen"), key=newest)

    status, reasons = _health(
        finished=finished,
        failed=failed,
        stuck=stuck,
        oldest_due=oldest_due_seconds,
        retrying=retrying,
        busy=bool(due or scheduled or running),
        stalled=stalled,
        due=due,
        workers_online=active_workers,
    )

    return {
        "window": window,
        "window_seconds": int(span.total_seconds()),
        "bucket_seconds": bucket_seconds,
        "generated_at": _iso(now),
        "health": {"status": status, "reasons": reasons},
        "thresholds": {
            "backlog_warn_seconds": BACKLOG_WARN_SECONDS,
            "backlog_critical_seconds": BACKLOG_CRITICAL_SECONDS,
            "failure_rate_warn": FAILURE_RATE_WARN,
            "failure_rate_critical": FAILURE_RATE_CRITICAL,
        },
        "summary": {
            "finished": finished,
            "succeeded": succeeded,
            "failed": failed,
            "cancelled": cancelled,
            "succeeded_after_retry": after_retry,
            "failure_rate": failed / finished if finished else None,
            "throughput_per_minute": finished / (span.total_seconds() / 60),
            "duration_p50": _percentile(durations, 0.5),
            "duration_p95": _percentile(durations, 0.95),
            "enqueued": enqueued,
        },
        "backlog": {
            "due": due,
            "scheduled": scheduled,
            "retrying": retrying,
            "oldest_due_seconds": oldest_due_seconds,
            "running": running,
            "stuck": stuck,
            "stalled": stalled,
            "cancel_requested": cancelling,
            "active_workers": active_workers,
        },
        "series": [
            {"start": _iso(since + timedelta(seconds=i * bucket_seconds)), **bucket}
            for i, bucket in enumerate(series)
        ],
        "kinds": [
            {
                **{k: v for k, v in kind.items() if k != "durations"},
                "failure_rate": (
                    kind["failed"] / (kind["succeeded"] + kind["failed"])
                    if kind["succeeded"] + kind["failed"]
                    else None
                ),
                "duration_p95": _percentile(kind["durations"], 0.95),
                "last_failed_at": _iso(kind["last_failed_at"]),
            }
            for kind in sorted(
                kinds.values(),
                key=lambda k: (
                    -k["failed"],
                    -(k["succeeded"] + k["queued"] + k["running"]),
                    k["kind"],
                ),
            )
        ],
        "errors": [
            {
                "error": name,
                "failed": group["failed"],
                "retrying": group["retrying"],
                "kinds": sorted(group["kinds"]),
                "example": group["example"],
                "last_seen": _iso(group["last_seen"]),
            }
            for name, group in sorted(
                errors.items(),
                key=lambda item: (-(item[1]["failed"] + item[1]["retrying"]), item[0]),
            )[:TOP_ERRORS]
        ],
    }


@router.get("/tasks/stats")
def task_stats(window: str = "1h", _admin: bool = Depends(verify_admin_token)):
    """Queue health for the admin dashboard over a recent window.

    A plain ``def`` so FastAPI runs the query and aggregation in its
    threadpool instead of blocking the event loop.

    Args:
        window: One of 15m, 1h, 6h, 24h, 7d
    """
    if window not in STATS_WINDOWS:
        raise HTTPException(
            400, detail={"error": f"Invalid window: {window}. Use one of {list(STATS_WINDOWS)}"}
        )
    return build_task_stats(window, utcnow())


def _event_detail(event: BackgroundTaskEvent) -> dict:
    try:
        return json.loads(event.detail) if event.detail else {}
    except ValueError:
        return {"raw": event.detail}


def build_runs(events: list[dict]) -> list[dict]:
    """One entry per run, in claim order: where it ran, how it ended and why.

    A run handed back at shutdown does not spend its attempt, so the next run
    reuses the number; events attach to the latest run with their number.
    """
    runs: list[dict] = []
    latest: dict[int, dict] = {}
    for event in events:
        if event["type"] == "claimed":
            run = {
                "attempt": event["attempt"],
                "worker": event["worker"],
                "started_at": event["at"],
                "finished_at": None,
                "outcome": "running",
                "error": None,
                "stages": [],
            }
            runs.append(run)
            latest[event["attempt"]] = run
            continue
        run = latest.get(event["attempt"])
        if run is None or run["outcome"] != "running":
            continue  # not tied to a claim, or after the run already ended
        if event["type"] == "stage":
            run["stages"].append({"at": event["at"], "stage": event["detail"].get("stage")})
        elif event["type"] in ATTEMPT_OUTCOMES:
            run["finished_at"] = event["at"]
            run["outcome"] = ATTEMPT_OUTCOMES[event["type"]]
            run["error"] = event["detail"].get("error")
    return runs


WORKER_STATUS_ORDER = {"online": 0, "draining": 1, "lost": 2, "stopped": 3}


@router.get("/tasks/workers")
def list_workers(include_inactive: bool = False, _admin: bool = Depends(verify_admin_token)):
    """The worker roster: every worker process and whether it is alive.

    Workers are never deleted. Lost or stopped workers with no heartbeat for
    ``inactive_after_seconds`` are left out unless ``include_inactive``.

    Args:
        include_inactive: Also list long-inactive lost and stopped workers
    """
    now = utcnow()
    inactive_before = now - WORKER_INACTIVE_AFTER
    shown: list[tuple[BackgroundWorker, str]] = []
    hidden = 0
    for worker in BackgroundWorker.select().order_by(BackgroundWorker.started_at.desc()):
        status = worker_status(worker, now)
        inactive = status in ("lost", "stopped") and worker.last_heartbeat_at < inactive_before
        if inactive and not include_inactive:
            hidden += 1
            continue
        shown.append((worker, status))
    shown.sort(key=lambda item: WORKER_STATUS_ORDER[item[1]])  # stable: newest first within

    T = BackgroundTask
    running: dict[str, list[dict]] = {}
    ids = [worker.id for worker, _ in shown]
    for task in (
        T.select().where((T.status == RUNNING) & T.locked_by.in_(ids)).order_by(T.id) if ids else []
    ):
        running.setdefault(task.locked_by, []).append(
            {"id": task.id, "kind": task.kind, "progress": _progress(task, now)}
        )

    counts = dict.fromkeys(WORKER_STATUS_ORDER, 0)
    for _, status in shown:
        counts[status] += 1
    return {
        "workers": [
            {
                "id": worker.id,
                "name": worker.name,
                "hostname": worker.hostname,
                "pid": worker.pid,
                "queues": json.loads(worker.queues),
                "concurrency": worker.concurrency,
                "status": status,
                # Live from the task table, not the worker's last report, so a
                # lost worker whose tasks were reclaimed shows none.
                "running": len(running.get(worker.id, [])),
                "succeeded": worker.succeeded,
                "failed": worker.failed,
                "started_at": _iso(worker.started_at),
                "last_heartbeat_at": _iso(worker.last_heartbeat_at),
                "heartbeat_age_seconds": (now - worker.last_heartbeat_at).total_seconds(),
                "stopped_at": _iso(worker.stopped_at),
                "tasks": running.get(worker.id, []),
            }
            for worker, status in shown
        ],
        "counts": counts,
        "hidden": hidden,
        "lost_after_seconds": int(WORKER_LOST_AFTER.total_seconds()),
        "inactive_after_seconds": int(WORKER_INACTIVE_AFTER.total_seconds()),
    }


@router.get("/tasks/{task_id}")
async def get_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Get one task with its timeline, one summary per run and log sizes."""
    task = _get_task(task_id)
    events = [
        {
            "id": event.id,
            "at": _iso(event.at),
            "type": event.type,
            "attempt": event.attempt,
            "worker": event.worker,
            "detail": _event_detail(event),
        }
        for event in BackgroundTaskEvent.select()
        .where(BackgroundTaskEvent.task == task_id)
        .order_by(BackgroundTaskEvent.id)
    ]
    L = BackgroundTaskLog
    log_lines = {
        attempt: count
        for attempt, count in L.select(L.attempt, fn.COUNT(L.id))
        .where(L.task == task_id)
        .group_by(L.attempt)
        .tuples()
    }
    runs = build_runs(events)
    for run in runs:
        run["log_lines"] = log_lines.get(run["attempt"], 0)
    try:
        checkpoint = json.loads(task.checkpoint) if task.checkpoint else None
    except ValueError:
        checkpoint = task.checkpoint
    return {
        **_serialize(task),
        "checkpoint": checkpoint,
        "events": events,
        "runs": runs,
        "log_lines": sum(log_lines.values()),
    }


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Requeue a failed or cancelled task with a fresh attempt budget."""
    task = _get_task(task_id)
    if not retry_failed_task(task_id):
        raise HTTPException(
            409,
            detail={
                "error": f"Only failed or cancelled tasks can be retried (task is {task.status})"
            },
        )
    logger.info(f"Admin requeued background task {task_id} ({task.kind})")
    return _serialize(_get_task(task_id))


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Cancel a queued task, or ask the worker running a task to stop it."""
    task = _get_task(task_id)
    outcome = request_cancel(task_id)
    if outcome is None:
        reason = (
            "cancellation was already requested"
            if task.status == RUNNING and task.cancel_requested
            else f"task is {task.status}"
        )
        raise HTTPException(
            409, detail={"error": f"Only queued or running tasks can be cancelled ({reason})"}
        )
    logger.info(
        f"Admin {'cancelled' if outcome == CANCELLED else 'requested cancellation of'} "
        f"background task {task_id} ({task.kind})"
    )
    return _serialize(_get_task(task_id))


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Delete a failed or cancelled task with its timeline and logs."""
    task = _get_task(task_id)
    if not discard_task(task_id):
        raise HTTPException(
            409,
            detail={
                "error": "Only failed or cancelled tasks can be discarded; cancel queued or "
                f"running tasks instead (task is {task.status})"
            },
        )
    logger.info(f"Admin discarded background task {task_id} ({task.kind})")
    return {"success": True, "id": task_id}


def _log_query(task_id: int, attempt: int | None):
    query = BackgroundTaskLog.select().where(BackgroundTaskLog.task == task_id)
    if attempt is not None:
        query = query.where(BackgroundTaskLog.attempt == attempt)
    return query


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: int,
    attempt: int | None = None,
    after_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=LOG_PAGE_MAX),
    _admin: bool = Depends(verify_admin_token),
):
    """Page through a task's log, oldest first.

    Pass the returned ``next_after_id`` back as ``after_id`` to tail new
    records while the task runs.

    Args:
        attempt: Only this attempt's records
        after_id: Only records after this id
        limit: Maximum number of records
    """
    task = _get_task(task_id)
    rows = list(
        _log_query(task_id, attempt)
        .where(BackgroundTaskLog.id > after_id)
        .order_by(BackgroundTaskLog.id)
        .limit(limit + 1)
    )
    has_more, rows = len(rows) > limit, rows[:limit]
    return {
        "lines": [
            {
                "id": row.id,
                "attempt": row.attempt,
                "at": _iso(row.at),
                "level": row.level,
                "message": row.message,
            }
            for row in rows
        ],
        "next_after_id": rows[-1].id if rows else after_id,
        "has_more": has_more,
        "status": task.status,
    }


def _format_log_line(row: BackgroundTaskLog) -> str:
    stamp = row.at.replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds")
    return f"{stamp} [attempt {row.attempt}] {row.level:<8} {row.message}\n"


@router.get("/tasks/{task_id}/logs/download")
def download_task_logs(
    task_id: int, attempt: int | None = None, _admin: bool = Depends(verify_admin_token)
):
    """Download a task's log (or one attempt's) as plain text.

    A plain ``def`` so the query runs in FastAPI's threadpool.

    Args:
        attempt: Only this attempt's records
    """
    task = _get_task(task_id)

    def lines():
        yield f"# Task {task_id} ({task.kind}), status {task.status}\n"
        after_id = 0
        # Batched by id so a large log is never held in memory at once.
        while batch := list(
            _log_query(task_id, attempt)
            .where(BackgroundTaskLog.id > after_id)
            .order_by(BackgroundTaskLog.id)
            .limit(LOG_DOWNLOAD_BATCH)
        ):
            for row in batch:
                yield _format_log_line(row)
            after_id = batch[-1].id

    suffix = f"-attempt-{attempt}" if attempt is not None else ""
    return StreamingResponse(
        lines(),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="task-{task_id}{suffix}.log"'},
    )

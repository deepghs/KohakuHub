"""Background task inspection endpoints for admin API."""

import json
import math
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from peewee import fn

from kohakuhub.db import BackgroundTask, utcnow
from kohakuhub.logger import get_logger
from kohakuhub.tasks import (
    FAILED,
    QUEUED,
    RUNNING,
    STATUSES,
    SUCCEEDED,
    discard_task,
    retry_failed_task,
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


def _iso(value: datetime | None) -> str | None:
    # Stored as naive UTC; tag it so the admin UI renders local time correctly.
    return value.replace(tzinfo=timezone.utc).isoformat() if value else None


def _payload(task: BackgroundTask):
    try:
        return json.loads(task.payload)
    except ValueError:
        return task.payload  # corrupt rows stay visible so they can be discarded


def _serialize(task: BackgroundTask) -> dict:
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
        and task.locked_until < utcnow(),
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
        status: Filter by status (queued, running, succeeded, failed)
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
) -> tuple[str, list[dict]]:
    """Classify queue health and explain why, most severe signals first."""
    reasons: list[dict] = []

    def add(level: str, message: str) -> None:
        reasons.append({"level": level, "message": message})

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
        )
        .where(T.status.in_([QUEUED, RUNNING]) | (T.finished_at >= since) | (T.created_at >= since))
        .order_by(T.id)
        .dicts()
    )

    def bucket_of(moment: datetime) -> int:
        return min(bucket_count - 1, int((moment - since).total_seconds() // bucket_seconds))

    series = [{"succeeded": 0, "failed": 0, "enqueued": 0} for _ in range(bucket_count)]
    kinds: dict[str, dict] = {}
    errors: dict[str, dict] = {}
    durations: list[float] = []
    succeeded = failed = after_retry = enqueued = 0
    due = scheduled = retrying = running = stuck = 0
    oldest_due: datetime | None = None
    workers: set[str] = set()

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
                "durations": [],
                "timeline": [{"succeeded": 0, "failed": 0} for _ in range(bucket_count)],
                "last_error": None,
                "last_failed_at": None,
            },
        )

    def note_failure(stats: dict, row: dict, finished_at: datetime) -> None:
        if stats["last_failed_at"] is None or finished_at > stats["last_failed_at"]:
            stats["last_failed_at"] = finished_at
            stats["last_error"] = _first_line(row["last_error"])

    def record_error(row: dict, seen_at: datetime, field: str) -> None:
        group = errors.setdefault(
            _error_class(row["last_error"]),
            {"failed": 0, "retrying": 0, "kinds": set(), "example": None, "last_seen": None},
        )
        group[field] += 1
        group["kinds"].add(row["kind"])
        if group["last_seen"] is None or seen_at > group["last_seen"]:
            group["last_seen"] = seen_at
            group["example"] = _first_line(row["last_error"])

    for row in rows:
        stats = kind_stats(row["kind"])
        if row["created_at"] and row["created_at"] >= since:
            enqueued += 1
            series[bucket_of(row["created_at"])]["enqueued"] += 1
        finished_at = row["finished_at"]
        match row["status"]:
            case "succeeded" | "failed" if finished_at and finished_at >= since:
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
                    note_failure(stats, row, finished_at)
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
            case "running":
                running += 1
                stats["running"] += 1
                if row["locked_until"] is None or row["locked_until"] < now:
                    stuck += 1
                    stats["stuck"] += 1
                else:
                    workers.add(row["locked_by"])

    # A kind whose only row was enqueued without finishing, queueing or
    # running (an anomalous row) says nothing about this window.
    active_kinds = [
        k for k in kinds.values() if k["succeeded"] + k["failed"] + k["queued"] + k["running"]
    ]
    finished = succeeded + failed
    oldest_due_seconds = (now - oldest_due).total_seconds() if oldest_due else None
    status, reasons = _health(
        finished=finished,
        failed=failed,
        stuck=stuck,
        oldest_due=oldest_due_seconds,
        retrying=retrying,
        busy=bool(due or scheduled or running),
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
            "active_workers": len(workers),
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
                active_kinds,
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
async def task_stats(window: str = "1h", _admin: bool = Depends(verify_admin_token)):
    """Queue health for the admin dashboard over a recent window.

    Args:
        window: One of 15m, 1h, 6h, 24h, 7d
    """
    if window not in STATS_WINDOWS:
        raise HTTPException(
            400, detail={"error": f"Invalid window: {window}. Use one of {list(STATS_WINDOWS)}"}
        )
    return build_task_stats(window, utcnow())


@router.get("/tasks/{task_id}")
async def get_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Get one background task including payload and last error."""
    return _serialize(_get_task(task_id))


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Requeue a failed task with a fresh attempt budget."""
    task = _get_task(task_id)
    if not retry_failed_task(task_id):
        raise HTTPException(
            409, detail={"error": f"Only failed tasks can be retried (task is {task.status})"}
        )
    logger.info(f"Admin requeued background task {task_id} ({task.kind})")
    return _serialize(_get_task(task_id))


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: int, _admin: bool = Depends(verify_admin_token)):
    """Discard a queued or failed task."""
    task = _get_task(task_id)
    if not discard_task(task_id):
        raise HTTPException(
            409,
            detail={
                "error": f"Only queued or failed tasks can be discarded (task is {task.status})"
            },
        )
    logger.info(f"Admin discarded background task {task_id} ({task.kind})")
    return {"success": True, "id": task_id}

"""Background task inspection endpoints for admin API."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from peewee import fn

from kohakuhub.db import BackgroundTask, utcnow
from kohakuhub.logger import get_logger
from kohakuhub.tasks import RUNNING, STATUSES, discard_task, retry_failed_task
from kohakuhub.api.admin.utils import verify_admin_token

logger = get_logger("ADMIN")
router = APIRouter()


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

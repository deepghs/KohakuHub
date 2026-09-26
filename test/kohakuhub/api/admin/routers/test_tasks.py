"""API tests for admin background task routes."""

from datetime import timedelta

import pytest

from kohakuhub import tasks
from kohakuhub.db import BackgroundTask, BackgroundTaskEvent, BackgroundTaskLog


@pytest.fixture(autouse=True)
def task_rows(prepared_backend_test_state, monkeypatch):
    monkeypatch.setattr(tasks, "_registry", {})

    @tasks.task("admin.demo")
    async def demo(payload):
        return None

    @tasks.task("admin.other")
    async def other(payload):
        return None

    BackgroundTask.delete().execute()
    yield
    BackgroundTask.delete().execute()


def _failed_task(kind="admin.demo"):
    task_id = tasks.enqueue(kind, {"repo_id": 1}, priority=100)
    row = tasks.claim_next("w-admin", lease_seconds=60)
    tasks.fail_task(row, "RuntimeError: boom", permanent=True)
    return task_id


async def test_task_routes_require_admin_token(client):
    assert (await client.get("/admin/api/tasks")).status_code in (401, 403)
    assert (await client.delete("/admin/api/tasks/1")).status_code in (401, 403)


async def test_list_tasks_returns_rows_counts_and_kinds(admin_client):
    queued = tasks.enqueue("admin.other", {"n": 1}, priority=3)
    failed = _failed_task()

    response = await admin_client.get("/admin/api/tasks")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [row["id"] for row in body["tasks"]] == [failed, queued]
    assert body["counts"] == {
        "queued": 1,
        "running": 0,
        "succeeded": 0,
        "failed": 1,
        "cancelled": 0,
    }
    assert body["kinds"] == ["admin.demo", "admin.other"]
    assert (body["limit"], body["offset"]) == (50, 0)
    failed_row = body["tasks"][0]
    assert failed_row["payload"] == {"repo_id": 1}
    assert failed_row["last_error"] == "RuntimeError: boom"
    assert failed_row["finished_at"].endswith("+00:00")
    assert failed_row["lease_expired"] is False
    assert (failed_row["cancel_requested"], failed_row["stalled"]) == (False, False)
    assert failed_row["progress"] is None
    assert failed_row["stall_seconds"] == 600
    assert body["tasks"][1]["priority"] == 3
    assert body["tasks"][1]["started_at"] is None


async def test_list_tasks_filters_and_paginates(admin_client):
    for _ in range(3):
        tasks.enqueue("admin.demo")
    tasks.enqueue("admin.other")
    _failed_task("admin.other")

    by_kind = (await admin_client.get("/admin/api/tasks", params={"kind": "admin.demo"})).json()
    assert by_kind["total"] == 3
    by_status = (await admin_client.get("/admin/api/tasks", params={"status": "failed"})).json()
    assert [row["kind"] for row in by_status["tasks"]] == ["admin.other"]
    page = (await admin_client.get("/admin/api/tasks", params={"limit": 2, "offset": 4})).json()
    assert page["total"] == 5
    assert len(page["tasks"]) == 1
    # Counts always describe the whole table, not the filtered page.
    assert by_status["counts"]["queued"] == 4


async def test_list_tasks_rejects_invalid_filters(admin_client):
    assert (
        await admin_client.get("/admin/api/tasks", params={"status": "bogus"})
    ).status_code == 400
    assert (await admin_client.get("/admin/api/tasks", params={"limit": 0})).status_code == 422


async def test_get_task_reports_expired_leases(admin_client):
    task_id = tasks.enqueue("admin.demo")
    tasks.claim_next("w-gone", lease_seconds=60)
    BackgroundTask.update(locked_until=tasks.utcnow() - timedelta(seconds=5)).where(
        BackgroundTask.id == task_id
    ).execute()

    response = await admin_client.get(f"/admin/api/tasks/{task_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "running"
    assert body["locked_by"] == "w-gone"
    assert body["lease_expired"] is True
    assert (await admin_client.get("/admin/api/tasks/999999")).status_code == 404


async def test_retry_task_requeues_failed_or_cancelled_tasks_only(admin_client):
    failed = _failed_task()
    queued = tasks.enqueue("admin.demo")

    response = await admin_client.post(f"/admin/api/tasks/{failed}/retry")
    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["attempts"] == 0

    conflict = await admin_client.post(f"/admin/api/tasks/{queued}/retry")
    assert conflict.status_code == 409
    assert "queued" in conflict.json()["detail"]["error"]
    assert (await admin_client.post("/admin/api/tasks/999999/retry")).status_code == 404


async def test_delete_task_discards_failed_and_cancelled_tasks_only(admin_client):
    failed = _failed_task()
    running = tasks.enqueue("admin.demo")
    tasks.claim_next("w-admin", lease_seconds=60)
    queued = tasks.enqueue("admin.demo")
    cancelled = tasks.enqueue("admin.other")
    tasks.request_cancel(cancelled)

    assert (await admin_client.delete(f"/admin/api/tasks/{failed}")).json() == {
        "success": True,
        "id": failed,
    }
    assert (await admin_client.delete(f"/admin/api/tasks/{cancelled}")).status_code == 200
    for task_id, status in ((running, "running"), (queued, "queued")):
        conflict = await admin_client.delete(f"/admin/api/tasks/{task_id}")
        assert conflict.status_code == 409
        assert "cancel" in conflict.json()["detail"]["error"]
        assert status in conflict.json()["detail"]["error"]
    assert (await admin_client.delete(f"/admin/api/tasks/{failed}")).status_code == 404


async def test_cancel_task_cancels_queued_and_flags_running_tasks(admin_client):
    queued = tasks.enqueue("admin.demo")
    running = tasks.enqueue("admin.other", priority=10)
    tasks.claim_next("w-admin", lease_seconds=60)

    response = await admin_client.post(f"/admin/api/tasks/{queued}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"

    response = await admin_client.post(f"/admin/api/tasks/{running}/cancel")
    assert response.status_code == 200
    assert (response.json()["status"], response.json()["cancel_requested"]) == ("running", True)

    again = await admin_client.post(f"/admin/api/tasks/{running}/cancel")
    assert again.status_code == 409
    assert "already requested" in again.json()["detail"]["error"]
    finished = await admin_client.post(f"/admin/api/tasks/{queued}/cancel")
    assert finished.status_code == 409
    assert "task is cancelled" in finished.json()["detail"]["error"]
    assert (await admin_client.post("/admin/api/tasks/999999/cancel")).status_code == 404


async def test_get_task_returns_timeline_attempts_progress_and_log_counts(
    admin_client, monkeypatch
):
    monkeypatch.setattr(tasks, "retry_delay", lambda attempts: 0)
    task_id = tasks.enqueue("admin.demo", {"repo_id": 9})
    first = tasks.claim_next("w-1", lease_seconds=60)
    tasks.record_stage(first, "listing")
    tasks.write_logs(first, [{"at": tasks.utcnow(), "level": "INFO", "message": "one"}])
    tasks.fail_task(first, "RuntimeError: flaky")
    second = tasks.claim_next("w-2", lease_seconds=60)
    ctx = tasks.TaskContext(second, log_limit_bytes=1000)
    ctx.checkpoint({"cursor": "b/7"})
    ctx.progress(40, 100)
    tasks.heartbeat(second, lease_seconds=60, progress=ctx.pending_progress())
    tasks.write_logs(
        second, [{"at": tasks.utcnow(), "level": "INFO", "message": m} for m in ("a", "b")]
    )

    body = (await admin_client.get(f"/admin/api/tasks/{task_id}")).json()

    assert [event["type"] for event in body["events"]] == [
        "created",
        "claimed",
        "stage",
        "retry_scheduled",
        "claimed",
    ]
    assert body["events"][2]["detail"] == {"stage": "listing"}
    assert body["checkpoint"] == {"cursor": "b/7"}
    assert body["log_lines"] == 3
    first_attempt, second_attempt = body["runs"]
    assert (first_attempt["worker"], first_attempt["outcome"]) == ("w-1", "failed")
    assert first_attempt["error"] == "RuntimeError: flaky"
    assert [s["stage"] for s in first_attempt["stages"]] == ["listing"]
    assert (first_attempt["log_lines"], second_attempt["log_lines"]) == (1, 2)
    assert (second_attempt["outcome"], second_attempt["finished_at"]) == ("running", None)
    progress = body["progress"]
    assert (progress["done"], progress["total"], progress["stage"]) == (40, 100, "listing")
    assert progress["eta_seconds"] is None  # a single report has no rate yet
    assert progress["updated_at"].endswith("+00:00")


async def test_get_task_tolerates_corrupt_checkpoints_and_event_details(admin_client):
    task_id = tasks.enqueue("admin.demo")
    BackgroundTask.update(checkpoint="{bad").where(BackgroundTask.id == task_id).execute()
    BackgroundTaskEvent.update(detail="{bad").where(BackgroundTaskEvent.task == task_id).execute()

    body = (await admin_client.get(f"/admin/api/tasks/{task_id}")).json()

    assert body["checkpoint"] == "{bad"
    assert body["events"][0]["detail"] == {"raw": "{bad"}
    assert body["runs"] == [] and body["log_lines"] == 0


async def test_list_tasks_reports_progress_eta_and_stalls(admin_client):
    now = tasks.utcnow()
    moving = tasks.enqueue("admin.demo")
    tasks.claim_next("w-1", lease_seconds=60)
    BackgroundTask.update(
        progress_done=50,
        progress_total=100,
        progress_at=now,
        progress_base_done=0,
        progress_base_at=now - timedelta(seconds=50),
    ).where(BackgroundTask.id == moving).execute()
    stuck = tasks.enqueue("admin.other")
    tasks.claim_next("w-1", lease_seconds=60)
    BackgroundTask.update(
        started_at=now - timedelta(minutes=30), progress_stage="waiting on LakeFS"
    ).where(BackgroundTask.id == stuck).execute()

    rows = {row["id"]: row for row in (await admin_client.get("/admin/api/tasks")).json()["tasks"]}

    assert rows[moving]["progress"]["eta_seconds"] == pytest.approx(50, abs=2)
    assert rows[moving]["stalled"] is False
    assert rows[stuck]["stalled"] is True
    assert rows[stuck]["progress"] == {
        "done": None,
        "total": None,
        "stage": "waiting on LakeFS",
        "updated_at": None,
        "eta_seconds": None,
    }


def _logs(task_id, attempt, count, prefix="line"):
    BackgroundTaskLog.insert_many(
        [
            {
                "task": task_id,
                "attempt": attempt,
                "at": tasks.utcnow(),
                "level": "INFO",
                "message": f"{prefix} {i}",
            }
            for i in range(count)
        ]
    ).execute()


async def test_task_logs_page_filter_and_tail(admin_client):
    task_id = tasks.enqueue("admin.demo")
    _logs(task_id, 1, 3, "first")
    _logs(task_id, 2, 2, "second")

    page = (await admin_client.get(f"/admin/api/tasks/{task_id}/logs", params={"limit": 2})).json()
    assert [line["message"] for line in page["lines"]] == ["first 0", "first 1"]
    assert page["has_more"] is True
    assert page["status"] == "queued"
    rest = (
        await admin_client.get(
            f"/admin/api/tasks/{task_id}/logs", params={"after_id": page["next_after_id"]}
        )
    ).json()
    assert [line["message"] for line in rest["lines"]] == [
        "first 2",
        "second 0",
        "second 1",
    ]
    assert rest["has_more"] is False
    assert rest["lines"][0]["at"].endswith("+00:00")

    tail = (
        await admin_client.get(
            f"/admin/api/tasks/{task_id}/logs", params={"after_id": rest["next_after_id"]}
        )
    ).json()
    assert tail["lines"] == [] and tail["next_after_id"] == rest["next_after_id"]

    second = (
        await admin_client.get(f"/admin/api/tasks/{task_id}/logs", params={"attempt": 2})
    ).json()
    assert [line["attempt"] for line in second["lines"]] == [2, 2]
    assert (await admin_client.get("/admin/api/tasks/999999/logs")).status_code == 404


async def test_task_logs_download_streams_plain_text_in_batches(admin_client, monkeypatch):
    monkeypatch.setattr(tasks_router, "LOG_DOWNLOAD_BATCH", 2)
    task_id = tasks.enqueue("admin.demo")
    _logs(task_id, 1, 3, "first")
    _logs(task_id, 2, 2, "second")

    response = await admin_client.get(f"/admin/api/tasks/{task_id}/logs/download")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert f'filename="task-{task_id}.log"' in response.headers["content-disposition"]
    lines = response.text.splitlines()
    assert lines[0] == f"# Task {task_id} (admin.demo), status queued"
    assert len(lines) == 6
    assert lines[1].endswith("first 0") and "[attempt 1] INFO" in lines[1]
    assert lines[-1].endswith("second 1")

    one = await admin_client.get(f"/admin/api/tasks/{task_id}/logs/download", params={"attempt": 2})
    assert f'filename="task-{task_id}-attempt-2.log"' in one.headers["content-disposition"]
    assert len(one.text.splitlines()) == 3
    assert (await admin_client.get("/admin/api/tasks/999999/logs/download")).status_code == 404


def test_build_runs_ignores_events_outside_a_run():
    at = "2026-01-01T00:00:00+00:00"

    def event(type_, attempt, **detail):
        return {"type": type_, "attempt": attempt, "worker": "w", "at": at, "detail": detail}

    attempts = tasks_router.build_runs(
        [
            event("created", 0),
            event("cancel_requested", 1),  # before any claim of attempt 1
            event("claimed", 1),
            event("lease_expired", 1),
            event("succeeded", 1),  # after the attempt already ended
            event("claimed", 2),
            event("released", 2),  # shutdown: the next run reuses attempt 2
            event("claimed", 2),
            event("cancel_requested", 2),  # neither a stage nor an outcome
            event("cancelled", 2),
        ]
    )

    assert [(a["attempt"], a["outcome"]) for a in attempts] == [
        (1, "lease expired"),
        (2, "released"),
        (2, "cancelled"),
    ]


async def test_list_tasks_tolerates_corrupt_payloads(admin_client):
    task_id = tasks.enqueue("admin.demo")
    BackgroundTask.update(payload="{not json").where(BackgroundTask.id == task_id).execute()

    response = await admin_client.get("/admin/api/tasks")

    assert response.status_code == 200
    assert response.json()["tasks"][0]["payload"] == "{not json"


# ----- /admin/api/tasks/stats -----------------------------------------------

from kohakuhub.api.admin.routers import tasks as tasks_router  # noqa: E402


def _row(
    kind="admin.demo",
    status="succeeded",
    *,
    finished_ago=None,
    created_ago=5,
    duration=None,
    attempts=1,
    run_after_ago=None,
    lease_in=None,
    worker=None,
    error=None,
    stall_seconds=None,
    cancel_requested=False,
):
    now = tasks.utcnow()

    def ago(minutes):
        return None if minutes is None else now - timedelta(minutes=minutes)

    finished = ago(finished_ago)
    started = finished - timedelta(seconds=duration) if finished and duration is not None else None
    return BackgroundTask.insert(
        kind=kind,
        status=status,
        attempts=attempts,
        created_at=ago(created_ago),
        started_at=started or (ago(1) if status == "running" else None),
        finished_at=finished,
        run_after=ago(run_after_ago) if run_after_ago is not None else ago(created_ago),
        locked_by=worker,
        locked_until=None if lease_in is None else now + timedelta(seconds=lease_in),
        last_error=error,
        stall_seconds=stall_seconds,
        cancel_requested=cancel_requested,
    ).execute()


async def test_stats_rejects_unknown_window(admin_client):
    response = await admin_client.get("/admin/api/tasks/stats", params={"window": "3d"})
    assert response.status_code == 400
    assert (await admin_client.get("/admin/api/tasks/stats")).status_code == 200


async def test_stats_idle_when_nothing_happened(admin_client):
    body = (await admin_client.get("/admin/api/tasks/stats")).json()

    assert body["window"] == "1h"
    assert body["window_seconds"] == 3600
    assert len(body["series"]) == 60
    assert body["health"] == {"status": "idle", "reasons": []}
    assert body["thresholds"]["backlog_critical_seconds"] == 300
    assert body["summary"]["finished"] == 0
    assert body["summary"]["failure_rate"] is None
    assert body["summary"]["duration_p50"] is None
    assert body["kinds"] == [] and body["errors"] == []


async def test_stats_summarises_window_activity(admin_client):
    for minutes, duration in ((50, 2), (30, 4), (10, 6), (5, 8)):
        _row(finished_ago=minutes, created_ago=minutes + 1, duration=duration)
    _row(finished_ago=20, created_ago=21, duration=10, attempts=3)  # recovered after retries
    _row(
        status="failed",
        finished_ago=15,
        created_ago=16,
        duration=1,
        error="RuntimeError: LakeFS returned 503\n  detail line",
    )
    _row(finished_ago=120, created_ago=121, duration=1)  # outside the 1h window

    body = (await admin_client.get("/admin/api/tasks/stats", params={"window": "1h"})).json()

    summary = body["summary"]
    assert (summary["finished"], summary["succeeded"], summary["failed"]) == (6, 5, 1)
    assert summary["succeeded_after_retry"] == 1
    assert summary["failure_rate"] == pytest.approx(1 / 6)
    assert summary["throughput_per_minute"] == pytest.approx(6 / 60)
    assert summary["duration_p50"] == pytest.approx(4)
    assert summary["duration_p95"] == pytest.approx(10)
    assert summary["enqueued"] == 6
    assert sum(b["succeeded"] for b in body["series"]) == 5
    assert sum(b["failed"] for b in body["series"]) == 1
    # Newest bucket last: the task finished 5 minutes ago lands 5-6 buckets
    # from the end (the exact one depends on sub-second timing).
    assert body["series"][-6]["succeeded"] + body["series"][-5]["succeeded"] == 1

    (kind,) = body["kinds"]
    assert kind["kind"] == "admin.demo"
    assert (kind["succeeded"], kind["failed"]) == (5, 1)
    assert len(kind["timeline"]) == 60
    assert kind["last_error"] == "RuntimeError: LakeFS returned 503"

    (error,) = body["errors"]
    assert error["error"] == "RuntimeError"
    assert (error["failed"], error["retrying"]) == (1, 0)
    assert error["kinds"] == ["admin.demo"]
    assert error["example"] == "RuntimeError: LakeFS returned 503"
    # 1 failure out of 6 is above the warning threshold.
    assert body["health"]["status"] == "degraded"


async def test_stats_reports_backlog_stuck_tasks_and_workers(admin_client):
    _row(status="queued", created_ago=10, run_after_ago=8)  # due, waiting 8 minutes
    _row(status="queued", created_ago=1, run_after_ago=-30)  # scheduled in the future
    _row(
        status="queued",
        created_ago=3,
        run_after_ago=-1,
        attempts=2,
        error="ConnectError: connection refused",
    )  # waiting to retry
    _row(status="running", created_ago=2, lease_in=60, worker="w-1")
    _row(status="running", created_ago=2, lease_in=60, worker="w-1")
    _row(status="running", created_ago=30, lease_in=-120, worker="w-dead")  # stuck

    body = (await admin_client.get("/admin/api/tasks/stats", params={"window": "15m"})).json()

    backlog = body["backlog"]
    assert (backlog["due"], backlog["scheduled"], backlog["retrying"]) == (1, 2, 1)
    assert backlog["oldest_due_seconds"] == pytest.approx(480, abs=5)
    assert (backlog["running"], backlog["stuck"], backlog["active_workers"]) == (3, 1, 1)
    assert body["errors"][0] == {
        "error": "ConnectError",
        "failed": 0,
        "retrying": 1,
        "kinds": ["admin.demo"],
        "example": "ConnectError: connection refused",
        "last_seen": body["errors"][0]["last_seen"],
    }
    assert body["health"]["status"] == "unhealthy"
    messages = " ".join(r["message"] for r in body["health"]["reasons"])
    assert "expired lease" in messages
    assert "waited" in messages
    assert "retry" in messages


def test_health_rules_cover_each_threshold():
    health = tasks_router._health

    assert health(finished=0, failed=0, stuck=0, oldest_due=None, retrying=0, busy=False) == (
        "idle",
        [],
    )
    assert health(finished=3, failed=0, stuck=0, oldest_due=None, retrying=0, busy=True)[0] == (
        "healthy"
    )
    status, reasons = health(finished=2, failed=1, stuck=0, oldest_due=10, retrying=0, busy=True)
    assert status == "degraded"  # too few samples for a rate, but failures are reported
    assert reasons == [{"level": "degraded", "message": "1 task(s) failed in the window"}]
    assert health(finished=10, failed=6, stuck=0, oldest_due=None, retrying=0, busy=True)[0] == (
        "unhealthy"
    )
    assert health(finished=0, failed=0, stuck=0, oldest_due=90, retrying=0, busy=True)[0] == (
        "degraded"
    )
    assert health(finished=0, failed=0, stuck=0, oldest_due=400, retrying=0, busy=True)[0] == (
        "unhealthy"
    )


def test_format_age_and_error_class():
    assert tasks_router._format_age(45) == "45s"
    assert tasks_router._format_age(300) == "5m"
    assert tasks_router._format_age(7200) == "2h"
    assert tasks_router._error_class("KeyError: 'x'") == "KeyError"
    assert tasks_router._error_class("something broke") == "something broke"
    assert tasks_router._error_class(None) == "Unknown error"


def test_percentile_handles_small_samples():
    assert tasks_router._percentile([], 0.95) is None
    assert tasks_router._percentile([3.0], 0.5) == 3.0
    assert tasks_router._percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.0


async def test_stats_edge_cases_keep_latest_error_and_ignore_anomalies(admin_client):
    # Two failures of the same kind and error class: the newest one wins.
    _row(status="failed", finished_ago=5, duration=1, error="RuntimeError: newest")
    _row(status="failed", finished_ago=40, duration=1, error="RuntimeError: older")
    # Finished without a recorded start: counted, but not in durations.
    _row(kind="admin.other", finished_ago=10, duration=None)
    # Anomalous row (finished status, no finished_at): outside every window.
    BackgroundTask.insert(
        kind="admin.ghost", status="succeeded", created_at=tasks.utcnow()
    ).execute()

    body = (await admin_client.get("/admin/api/tasks/stats")).json()

    assert body["summary"]["finished"] == 3
    assert body["summary"]["enqueued"] == 3
    assert body["summary"]["duration_p50"] == pytest.approx(1)
    demo = next(k for k in body["kinds"] if k["kind"] == "admin.demo")
    assert demo["last_error"] == "RuntimeError: newest"
    assert body["errors"][0]["example"] == "RuntimeError: newest"
    assert body["errors"][0]["failed"] == 2
    assert "admin.ghost" not in {k["kind"] for k in body["kinds"]}
    other = next(k for k in body["kinds"] if k["kind"] == "admin.other")
    assert other["duration_p95"] is None


def test_health_tolerates_failure_rate_below_threshold():
    status, reasons = tasks_router._health(
        finished=20, failed=1, stuck=0, oldest_due=None, retrying=0, busy=True
    )
    assert (status, reasons) == ("healthy", [])


async def test_stats_counts_cancellations_and_stalls_without_calling_them_failures(
    admin_client,
):
    _row(finished_ago=5, created_ago=6, duration=1)
    _row(status="cancelled", finished_ago=4, created_ago=6)
    # Started a minute ago with no progress since, past a 30 s threshold.
    _row(status="running", created_ago=2, lease_in=60, worker="w-1", stall_seconds=30)
    _row(status="running", created_ago=2, lease_in=60, worker="w-1", cancel_requested=True)

    body = (await admin_client.get("/admin/api/tasks/stats", params={"window": "15m"})).json()

    assert (body["summary"]["finished"], body["summary"]["cancelled"]) == (1, 1)
    assert body["summary"]["failure_rate"] == 0
    assert (body["backlog"]["stalled"], body["backlog"]["cancel_requested"]) == (1, 1)
    demo = next(k for k in body["kinds"] if k["kind"] == "admin.demo")
    assert demo["cancelled"] == 1
    assert body["health"]["status"] == "degraded"
    assert "no progress" in body["health"]["reasons"][0]["message"]

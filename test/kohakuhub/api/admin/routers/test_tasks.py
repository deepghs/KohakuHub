"""API tests for admin background task routes."""

from datetime import timedelta

import pytest

from kohakuhub import tasks
from kohakuhub.db import BackgroundTask


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
    assert body["counts"] == {"queued": 1, "running": 0, "succeeded": 0, "failed": 1}
    assert body["kinds"] == ["admin.demo", "admin.other"]
    assert (body["limit"], body["offset"]) == (50, 0)
    failed_row = body["tasks"][0]
    assert failed_row["payload"] == {"repo_id": 1}
    assert failed_row["last_error"] == "RuntimeError: boom"
    assert failed_row["finished_at"].endswith("+00:00")
    assert failed_row["lease_expired"] is False
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


async def test_retry_task_requeues_failed_tasks_only(admin_client):
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


async def test_delete_task_discards_queued_and_failed_but_not_running(admin_client):
    failed = _failed_task()
    running = tasks.enqueue("admin.demo")
    tasks.claim_next("w-admin", lease_seconds=60)

    assert (await admin_client.delete(f"/admin/api/tasks/{failed}")).json() == {
        "success": True,
        "id": failed,
    }
    conflict = await admin_client.delete(f"/admin/api/tasks/{running}")
    assert conflict.status_code == 409
    assert "running" in conflict.json()["detail"]["error"]
    assert (await admin_client.delete(f"/admin/api/tasks/{failed}")).status_code == 404


async def test_list_tasks_tolerates_corrupt_payloads(admin_client):
    task_id = tasks.enqueue("admin.demo")
    BackgroundTask.update(payload="{not json").where(BackgroundTask.id == task_id).execute()

    response = await admin_client.get("/admin/api/tasks")

    assert response.status_code == 200
    assert response.json()["tasks"][0]["payload"] == "{not json"

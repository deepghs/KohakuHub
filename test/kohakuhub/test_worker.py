"""Tests for the background task worker runtime."""

import asyncio
import os
import signal
from datetime import timedelta

import pytest

from kohakuhub import tasks, worker as worker_module
from kohakuhub.db import BackgroundTask
from kohakuhub.worker import Worker


@pytest.fixture(autouse=True)
def clean_tasks(prepared_backend_test_state):
    BackgroundTask.delete().execute()
    yield
    BackgroundTask.delete().execute()


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    monkeypatch.setattr(tasks, "_registry", {})
    return tasks._registry


def _worker(**options):
    defaults = dict(
        worker_id="w-test", concurrency=2, lease_seconds=3, poll_interval=0.05, shutdown_grace=2
    )
    return Worker(**(defaults | options))


async def _run_until(worker, predicate, timeout=5.0):
    """Run ``worker`` until ``predicate()`` holds, then stop it."""
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while not predicate():
            assert loop.time() < deadline, "worker did not reach the expected state"
            await asyncio.sleep(0.02)
    finally:
        stop.set()
        await runner


def _status(task_id):
    return BackgroundTask.get_by_id(task_id).status


def test_worker_defaults_come_from_config(monkeypatch):
    monkeypatch.setattr(worker_module.cfg.worker, "queues", ["bulk"])
    worker = Worker()

    assert worker.worker_id.count(":") == 2
    assert worker.concurrency == worker_module.cfg.worker.concurrency
    assert worker.queues == ["bulk"]
    assert Worker(queues=[]).queues == []


async def test_worker_runs_task_to_success():
    seen = []

    @tasks.task("test.ok")
    async def handler(payload):
        seen.append(payload)

    task_id = tasks.enqueue("test.ok", {"n": 1})
    await _run_until(_worker(), lambda: _status(task_id) == tasks.SUCCEEDED)

    assert seen == [{"n": 1}]


async def test_worker_retries_failures_and_fails_permanent_errors(monkeypatch):
    monkeypatch.setattr(tasks, "retry_delay", lambda attempts: 3600)

    @tasks.task("test.flaky")
    async def flaky(payload):
        raise RuntimeError("temporarily broken")

    @tasks.task("test.fatal")
    async def fatal(payload):
        raise tasks.PermanentTaskError("bad input")

    flaky_id = tasks.enqueue("test.flaky")
    fatal_id = tasks.enqueue("test.fatal")
    await _run_until(
        _worker(),
        lambda: BackgroundTask.get_by_id(flaky_id).last_error and _status(fatal_id) == tasks.FAILED,
    )

    flaky_row = BackgroundTask.get_by_id(flaky_id)
    assert flaky_row.status == tasks.QUEUED
    assert flaky_row.last_error == "RuntimeError: temporarily broken"
    assert BackgroundTask.get_by_id(fatal_id).last_error == "PermanentTaskError: bad input"


async def test_worker_times_out_slow_handlers():
    @tasks.task("test.slow", timeout=0.05, max_attempts=1)
    async def slow(payload):
        await asyncio.sleep(10)

    task_id = tasks.enqueue("test.slow")
    await _run_until(_worker(), lambda: _status(task_id) == tasks.FAILED)

    assert BackgroundTask.get_by_id(task_id).last_error == "Timed out after 0.05s"


async def test_worker_heartbeat_keeps_long_task_owned():
    # The handler outlives the lease, so only heartbeats keep it owned. The
    # lease leaves ~0.6s of slack for slow CI runners between renewals.
    @tasks.task("test.long")
    async def long_task(payload):
        await asyncio.sleep(2.0)

    task_id = tasks.enqueue("test.long")
    await _run_until(_worker(lease_seconds=0.9), lambda: _status(task_id) == tasks.SUCCEEDED)

    assert BackgroundTask.get_by_id(task_id).attempts == 1


async def test_worker_abandons_task_when_lease_is_lost():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @tasks.task("test.stolen")
    async def stolen(payload):
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task_id = tasks.enqueue("test.stolen")

    async def steal():
        await started.wait()
        BackgroundTask.update(locked_by="someone-else").where(
            BackgroundTask.id == task_id
        ).execute()

    stealer = asyncio.create_task(steal())
    await _run_until(_worker(lease_seconds=0.15), cancelled.is_set)
    await stealer

    row = BackgroundTask.get_by_id(task_id)
    assert (row.status, row.locked_by) == (tasks.RUNNING, "someone-else")


async def test_worker_survives_heartbeat_errors(monkeypatch):
    calls = {"renew": 0}
    real_renew = tasks.renew_lease

    def flaky_renew(row, **kwargs):
        calls["renew"] += 1
        if calls["renew"] == 1:
            raise RuntimeError("db hiccup")
        return real_renew(row, **kwargs)

    monkeypatch.setattr(tasks, "renew_lease", flaky_renew)

    @tasks.task("test.hiccup")
    async def hiccup(payload):
        await asyncio.sleep(1.5)

    # First renewal (0.3s) fails, the second (0.6s) must land before the
    # 0.9s lease expires; the handler outlives the lease.
    task_id = tasks.enqueue("test.hiccup")
    await _run_until(_worker(lease_seconds=0.9), lambda: _status(task_id) == tasks.SUCCEEDED)

    assert calls["renew"] >= 2


async def test_worker_respects_concurrency_limit():
    release = asyncio.Event()
    active = {"now": 0, "max": 0}

    @tasks.task("test.blocking")
    async def blocking(payload):
        active["now"] += 1
        active["max"] = max(active["max"], active["now"])
        await release.wait()
        active["now"] -= 1

    ids = [tasks.enqueue("test.blocking") for _ in range(3)]
    worker = _worker(concurrency=2)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.3)
    assert active["max"] == 2
    assert sorted(_status(i) for i in ids) == [tasks.QUEUED, tasks.RUNNING, tasks.RUNNING]

    release.set()
    while any(_status(i) != tasks.SUCCEEDED for i in ids):
        await asyncio.sleep(0.02)
    stop.set()
    await runner


async def test_worker_drains_running_tasks_on_stop():
    started = asyncio.Event()

    @tasks.task("test.drain")
    async def drain(payload):
        started.set()
        await asyncio.sleep(0.2)

    task_id = tasks.enqueue("test.drain")
    stop = asyncio.Event()
    runner = asyncio.create_task(_worker().run(stop))
    await started.wait()
    stop.set()
    await runner

    assert _status(task_id) == tasks.SUCCEEDED


async def test_worker_starts_periodic_tasks():
    runs = []

    @tasks.task("test.tick", every=timedelta(hours=1))
    async def tick(payload):
        runs.append(1)

    await _run_until(_worker(), lambda: runs)

    pending = BackgroundTask.get(BackgroundTask.status == tasks.QUEUED)
    assert pending.dedupe_key == tasks.periodic_key("test.tick")


async def test_worker_survives_claim_and_record_errors(monkeypatch):
    @tasks.task("test.ok")
    async def handler(payload):
        return None

    task_id = tasks.enqueue("test.ok")
    real_claim, real_complete = tasks.claim_next, tasks.complete_task
    failures = {"claim": 1, "complete": 1}

    def flaky_claim(*args, **kwargs):
        if failures["claim"]:
            failures["claim"] -= 1
            raise RuntimeError("connection reset")
        return real_claim(*args, **kwargs)

    def flaky_complete(row):
        if failures["complete"]:
            failures["complete"] -= 1
            raise RuntimeError("connection reset")
        return real_complete(row)

    monkeypatch.setattr(tasks, "claim_next", flaky_claim)
    monkeypatch.setattr(tasks, "complete_task", flaky_complete)

    # The first outcome write fails; the task stays running until its lease
    # expires, then the worker claims and completes it again.
    await _run_until(_worker(lease_seconds=0.3), lambda: _status(task_id) == tasks.SUCCEEDED)

    assert BackgroundTask.get_by_id(task_id).attempts == 2


def test_reset_connection_logs_close_errors(monkeypatch):
    def broken_close():
        raise RuntimeError("already gone")

    monkeypatch.setattr(worker_module.db, "close", broken_close)
    worker_module._reset_connection()  # does not raise


async def test_wait_for_schema_retries_until_table_exists(monkeypatch):
    answers = iter([RuntimeError("db down"), False, True])

    def table_exists():
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(BackgroundTask, "table_exists", table_exists)

    assert await _worker(poll_interval=0.01).wait_for_schema(asyncio.Event()) is True


async def test_wait_for_schema_returns_false_when_stopped(monkeypatch):
    monkeypatch.setattr(BackgroundTask, "table_exists", lambda: False)
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)

    assert await _worker(poll_interval=10).wait_for_schema(stop) is False


async def test_serve_stops_on_sigterm():
    ran = []

    class FakeWorker:
        async def wait_for_schema(self, stop):
            return True

        async def run(self, stop):
            ran.append("run")
            os.kill(os.getpid(), signal.SIGTERM)
            await stop.wait()

    await worker_module.serve(FakeWorker())

    assert ran == ["run"]


async def test_serve_skips_run_when_stopped_before_schema():
    class FakeWorker:
        async def wait_for_schema(self, stop):
            return False

        async def run(self, stop):
            raise AssertionError("must not run")

    await worker_module.serve(FakeWorker())


def test_main_runs_serve(monkeypatch):
    calls = []

    async def fake_serve():
        calls.append("serve")

    monkeypatch.setattr(worker_module, "serve", fake_serve)
    worker_module.main()

    assert calls == ["serve"]


async def test_worker_restores_discarded_periodic_occurrence(monkeypatch):
    monkeypatch.setattr(worker_module, "PERIODIC_RESYNC_SECONDS", 0.1)

    @tasks.task("test.hourly", every=timedelta(hours=1))
    async def hourly(payload):
        return None

    def pending():
        return BackgroundTask.get_or_none(
            (BackgroundTask.status == tasks.QUEUED)
            & (BackgroundTask.dedupe_key == tasks.periodic_key("test.hourly"))
        )

    discarded = []

    def discard_then_wait_for_resync():
        row = pending()
        if row is not None and not discarded:
            tasks.discard_task(row.id)  # e.g. an admin discarded it
            discarded.append(row.id)
            return False
        return bool(discarded) and row is not None and row.id not in discarded

    await _run_until(_worker(), discard_then_wait_for_resync)


async def test_worker_survives_periodic_resync_errors(monkeypatch):
    monkeypatch.setattr(worker_module, "PERIODIC_RESYNC_SECONDS", 0.05)
    calls = []
    real_ensure = tasks.ensure_periodic_tasks

    def flaky_ensure():
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("db hiccup")
        real_ensure()

    monkeypatch.setattr(tasks, "ensure_periodic_tasks", flaky_ensure)

    await _run_until(_worker(), lambda: len(calls) >= 3)


async def test_worker_fails_tasks_with_corrupt_payload():
    @tasks.task("test.corrupt", max_attempts=1)
    async def corrupt(payload):
        raise AssertionError("handler must not run")

    task_id = tasks.enqueue("test.corrupt")
    BackgroundTask.update(payload="{not json").where(BackgroundTask.id == task_id).execute()

    await _run_until(_worker(), lambda: _status(task_id) == tasks.FAILED)

    assert BackgroundTask.get_by_id(task_id).last_error.startswith("JSONDecodeError")


async def test_worker_releases_tasks_cancelled_at_shutdown():
    started = asyncio.Event()

    @tasks.task("test.interrupted", max_attempts=1)
    async def interrupted(payload):
        started.set()
        await asyncio.sleep(10)

    task_id = tasks.enqueue("test.interrupted")
    stop = asyncio.Event()
    runner = asyncio.create_task(_worker(shutdown_grace=0.1).run(stop))
    await started.wait()
    stop.set()
    await runner

    # Handed back without spending the attempt, so the next worker runs it.
    row = BackgroundTask.get_by_id(task_id)
    assert (row.status, row.attempts, row.locked_by) == (tasks.QUEUED, 0, None)
    assert row.run_after <= tasks.utcnow()


async def test_worker_keeps_handler_timeout_errors_distinct():
    @tasks.task("test.socket-timeout", max_attempts=1, timeout=30)
    async def socket_timeout(payload):
        raise asyncio.TimeoutError("read timed out")

    task_id = tasks.enqueue("test.socket-timeout")
    await _run_until(_worker(), lambda: _status(task_id) == tasks.FAILED)

    assert BackgroundTask.get_by_id(task_id).last_error == "TimeoutError: read timed out"

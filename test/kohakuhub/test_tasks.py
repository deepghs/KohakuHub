"""Tests for the durable background task queue."""

import json
import threading
from datetime import timedelta

import pytest

from kohakuhub import tasks
from kohakuhub.config import cfg
from peewee import SqliteDatabase

from kohakuhub.db import BackgroundTask, db

WORKER = "worker-a"
LEASE = 60


@pytest.fixture(autouse=True)
def clean_tasks(prepared_backend_test_state):
    BackgroundTask.delete().execute()
    yield
    BackgroundTask.delete().execute()


@pytest.fixture
def registry(monkeypatch):
    """Isolate handler registrations made by a test."""
    monkeypatch.setattr(tasks, "_registry", {})
    return tasks._registry


def _register(kind="test.noop", **options):
    @tasks.task(kind, **options)
    async def handler(payload):
        return None

    return handler


def _age(task_id, **fields):
    BackgroundTask.update(**fields).where(BackgroundTask.id == task_id).execute()


def test_task_decorator_registers_spec(registry):
    handler = _register(
        "test.spec", queue="bulk", max_attempts=2, timeout=5, every=timedelta(minutes=1)
    )

    spec = tasks.get_spec("test.spec")
    assert spec.handler is handler
    assert (spec.queue, spec.max_attempts, spec.timeout) == ("bulk", 2, 5)
    assert spec.every == timedelta(minutes=1)
    assert tasks.get_spec("test.missing") is None


def test_task_decorator_rejects_sync_handlers_and_duplicates(registry):
    with pytest.raises(TypeError):

        @tasks.task("test.sync")
        def sync_handler(payload):
            return None

    _register("test.dup")
    with pytest.raises(ValueError):
        _register("test.dup")


def test_builtin_cleanup_task_is_registered():
    spec = tasks.get_spec(tasks.CLEANUP_KIND)
    assert spec is not None
    assert spec.every is not None


def test_enqueue_persists_row(registry):
    _register("test.enqueue", queue="bulk", max_attempts=3)

    task_id = tasks.enqueue("test.enqueue", {"repo_id": 7}, priority=5)

    row = BackgroundTask.get_by_id(task_id)
    assert row.kind == "test.enqueue"
    assert row.queue == "bulk"
    assert json.loads(row.payload) == {"repo_id": 7}
    assert row.status == tasks.QUEUED
    assert row.priority == 5
    assert row.max_attempts == 3
    assert row.attempts == 0
    assert row.run_after <= tasks.utcnow()


def test_enqueue_defaults_payload_and_accepts_queue_and_delay(registry):
    _register("test.delay")
    later = tasks.utcnow() + timedelta(hours=1)

    row = BackgroundTask.get_by_id(tasks.enqueue("test.delay", run_after=later, queue="other"))

    assert json.loads(row.payload) == {}
    assert row.queue == "other"
    assert row.run_after == later


def test_enqueue_rejects_unknown_kind(registry):
    with pytest.raises(ValueError):
        tasks.enqueue("test.unknown")


def test_enqueue_rolls_back_with_caller_transaction(registry):
    _register("test.tx")

    with pytest.raises(RuntimeError):
        with db.atomic():
            tasks.enqueue("test.tx")
            raise RuntimeError("business write failed")

    assert BackgroundTask.select().count() == 0


def test_enqueue_dedupe_coalesces_pending_work(registry):
    _register("test.dedupe")

    first = tasks.enqueue("test.dedupe", {"repo_id": 1}, dedupe_key="repo:1")
    second = tasks.enqueue("test.dedupe", {"repo_id": 1}, dedupe_key="repo:1")

    assert first is not None
    assert second is None
    assert BackgroundTask.select().count() == 1


def test_claim_releases_dedupe_key(registry):
    _register("test.dedupe")
    tasks.enqueue("test.dedupe", dedupe_key="repo:1")

    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert claimed.dedupe_key is None
    assert tasks.enqueue("test.dedupe", dedupe_key="repo:1") is not None


def test_claim_marks_task_running_with_lease(registry):
    _register("test.claim")
    task_id = tasks.enqueue("test.claim")

    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert claimed.id == task_id
    assert claimed.status == tasks.RUNNING
    assert claimed.attempts == 1
    assert claimed.locked_by == WORKER
    assert claimed.locked_until > tasks.utcnow() + timedelta(seconds=LEASE - 5)
    assert claimed.started_at is not None
    assert tasks.claim_next(WORKER, lease_seconds=LEASE) is None


def test_claim_returns_none_without_registered_kinds(registry):
    assert tasks.claim_next(WORKER, lease_seconds=LEASE) is None


def test_claim_skips_future_unknown_and_other_queue_tasks(registry):
    _register("test.known", queue="bulk")
    tasks.enqueue("test.known", run_after=tasks.utcnow() + timedelta(hours=1))
    other_queue = tasks.enqueue("test.known")
    BackgroundTask.insert(kind="test.not-registered", queue="bulk").execute()

    assert tasks.claim_next(WORKER, lease_seconds=LEASE, queues=["default"]) is None
    assert tasks.claim_next(WORKER, lease_seconds=LEASE, queues=["bulk"]).id == other_queue


def test_claim_orders_by_priority_then_run_after(registry):
    _register("test.order")
    now = tasks.utcnow()
    old_low = tasks.enqueue("test.order", run_after=now - timedelta(minutes=2))
    new_high = tasks.enqueue("test.order", priority=10, run_after=now - timedelta(minutes=1))
    old_high = tasks.enqueue("test.order", priority=10, run_after=now - timedelta(minutes=3))

    order = [tasks.claim_next(WORKER, lease_seconds=LEASE).id for _ in range(3)]

    assert order == [old_high, new_high, old_low]


def test_claim_reclaims_expired_lease(registry):
    _register("test.reclaim")
    task_id = tasks.enqueue("test.reclaim")
    stale = tasks.claim_next("worker-dead", lease_seconds=LEASE)
    _age(task_id, locked_until=tasks.utcnow() - timedelta(seconds=1))

    reclaimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert reclaimed.id == task_id
    assert reclaimed.locked_by == WORKER
    assert reclaimed.attempts == 2
    # The stale owner can no longer touch the row.
    assert tasks.renew_lease(stale, lease_seconds=LEASE) is False
    assert tasks.complete_task(stale) is False
    assert tasks.fail_task(stale, "late") is None


def test_claim_fails_expired_task_that_exhausted_attempts(registry):
    _register("test.crashy", max_attempts=1)
    crashy = tasks.enqueue("test.crashy")
    tasks.claim_next("worker-dead", lease_seconds=LEASE)
    _age(crashy, locked_until=tasks.utcnow() - timedelta(seconds=1))
    healthy = tasks.enqueue("test.crashy")

    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert claimed.id == healthy
    row = BackgroundTask.get_by_id(crashy)
    assert row.status == tasks.FAILED
    assert "lease expired" in row.last_error
    assert row.finished_at is not None
    assert row.locked_by is None


def test_claim_backs_off_when_race_is_lost(registry, monkeypatch):
    _register("test.race")
    task_id = tasks.enqueue("test.race")
    real_select = tasks._select_candidate

    def select_then_steal(*args, **kwargs):
        candidate = real_select(*args, **kwargs)
        _age(task_id, attempts=5)  # another process claimed it meanwhile
        return candidate

    monkeypatch.setattr(tasks, "_select_candidate", select_then_steal)

    assert tasks.claim_next(WORKER, lease_seconds=LEASE) is None


def test_concurrent_claims_never_share_a_task(registry):
    _register("test.concurrent")
    for _ in range(20):
        tasks.enqueue("test.concurrent")
    claimed: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(4)

    def claim_all(worker_id):
        barrier.wait()
        try:
            while (row := tasks.claim_next(worker_id, lease_seconds=LEASE)) is not None:
                with lock:
                    claimed.append(row.id)
        finally:
            db.close()

    threads = [threading.Thread(target=claim_all, args=(f"w{i}",)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(claimed) == 20
    assert len(set(claimed)) == 20


def test_periodic_claim_schedules_exactly_one_next_occurrence(registry):
    _register("test.one-shot")
    _register("test.periodic", every=timedelta(minutes=5))
    tasks.ensure_periodic_tasks()
    tasks.ensure_periodic_tasks()
    assert BackgroundTask.select().count() == 1

    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    pending = BackgroundTask.get(BackgroundTask.status == tasks.QUEUED)
    assert pending.id != claimed.id
    assert pending.dedupe_key == tasks.periodic_key("test.periodic")
    assert pending.run_after >= claimed.started_at + timedelta(minutes=5)
    tasks.ensure_periodic_tasks()
    assert BackgroundTask.select().where(BackgroundTask.status == tasks.QUEUED).count() == 1


def test_renew_lease_extends_owned_task(registry):
    _register("test.renew")
    tasks.enqueue("test.renew")
    claimed = tasks.claim_next(WORKER, lease_seconds=1)

    assert tasks.renew_lease(claimed, lease_seconds=LEASE) is True
    assert BackgroundTask.get_by_id(claimed.id).locked_until > claimed.locked_until


def test_complete_task_marks_success(registry):
    _register("test.complete")
    tasks.enqueue("test.complete")
    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert tasks.complete_task(claimed) is True

    row = BackgroundTask.get_by_id(claimed.id)
    assert row.status == tasks.SUCCEEDED
    assert row.finished_at is not None
    assert (row.locked_by, row.locked_until) == (None, None)


def test_fail_task_requeues_with_backoff_until_exhausted(registry, monkeypatch):
    _register("test.retry", max_attempts=2)
    monkeypatch.setattr(tasks, "retry_delay", lambda attempts: 30.0)
    tasks.enqueue("test.retry")

    first = tasks.claim_next(WORKER, lease_seconds=LEASE)
    assert tasks.fail_task(first, "boom") == tasks.QUEUED
    row = BackgroundTask.get_by_id(first.id)
    assert row.last_error == "boom"
    assert row.run_after >= tasks.utcnow() + timedelta(seconds=25)
    assert row.locked_by is None

    _age(first.id, run_after=tasks.utcnow())
    second = tasks.claim_next(WORKER, lease_seconds=LEASE)
    assert tasks.fail_task(second, "boom again") == tasks.FAILED
    row = BackgroundTask.get_by_id(first.id)
    assert row.status == tasks.FAILED
    assert row.finished_at is not None


def test_fail_task_permanent_skips_retries_and_truncates_error(registry):
    _register("test.permanent", max_attempts=5)
    tasks.enqueue("test.permanent")
    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert tasks.fail_task(claimed, "x" * 10_000, permanent=True) == tasks.FAILED
    assert len(BackgroundTask.get_by_id(claimed.id).last_error) == tasks.MAX_ERROR_LENGTH


def test_retry_delay_grows_exponentially_with_bounded_jitter(monkeypatch):
    monkeypatch.setattr(tasks.random, "uniform", lambda low, high: high)
    assert tasks.retry_delay(1) == tasks.RETRY_BASE_SECONDS * 1.25
    assert tasks.retry_delay(3) == tasks.RETRY_BASE_SECONDS * 4 * 1.25
    assert tasks.retry_delay(100) == tasks.RETRY_MAX_SECONDS * 1.25

    monkeypatch.setattr(tasks.random, "uniform", lambda low, high: low)
    assert tasks.retry_delay(2) == tasks.RETRY_BASE_SECONDS * 2


def test_retry_failed_task_requeues_only_failed(registry):
    _register("test.admin-retry")
    task_id = tasks.enqueue("test.admin-retry")
    assert tasks.retry_failed_task(task_id) is False

    claimed = tasks.claim_next(WORKER, lease_seconds=LEASE)
    tasks.fail_task(claimed, "boom", permanent=True)
    assert tasks.retry_failed_task(task_id) is True

    row = BackgroundTask.get_by_id(task_id)
    assert row.status == tasks.QUEUED
    assert row.attempts == 0
    assert row.finished_at is None
    assert row.run_after <= tasks.utcnow()
    assert tasks.retry_failed_task(999_999) is False


def test_discard_task_rejects_running_and_finished_tasks(registry):
    _register("test.discard")
    queued = tasks.enqueue("test.discard")
    running = tasks.enqueue("test.discard", priority=1)
    tasks.claim_next(WORKER, lease_seconds=LEASE)

    assert tasks.discard_task(running) is False
    assert tasks.discard_task(queued) is True
    assert tasks.discard_task(queued) is False
    assert BackgroundTask.select().count() == 1


async def test_cleanup_task_deletes_expired_finished_rows(registry, monkeypatch):
    monkeypatch.setattr(cfg.worker, "succeeded_retention_days", 7)
    monkeypatch.setattr(cfg.worker, "failed_retention_days", 30)
    now = tasks.utcnow()

    def row(status, age_days):
        return BackgroundTask.insert(
            kind="test.row", status=status, finished_at=now - timedelta(days=age_days)
        ).execute()

    old_success = row(tasks.SUCCEEDED, 8)
    fresh_success = row(tasks.SUCCEEDED, 6)
    old_failure = row(tasks.FAILED, 31)
    fresh_failure = row(tasks.FAILED, 8)
    queued = BackgroundTask.insert(kind="test.row").execute()

    await tasks.cleanup_finished_tasks({})

    remaining = {r.id for r in BackgroundTask.select(BackgroundTask.id)}
    assert remaining == {fresh_success, fresh_failure, queued}
    assert old_success not in remaining and old_failure not in remaining

    await tasks.cleanup_finished_tasks({})  # nothing left to delete
    assert BackgroundTask.select().count() == 3


def test_queue_lifecycle_on_sqlite(registry, tmp_path):
    """SQLite has no SKIP LOCKED; the compare-and-set claim still works."""
    _register("test.sqlite")
    _register("test.sqlite-periodic", every=timedelta(minutes=1))
    sqlite_db = SqliteDatabase(str(tmp_path / "tasks.db"))
    with BackgroundTask.bind_ctx(sqlite_db):
        sqlite_db.create_tables([BackgroundTask])
        task_id = tasks.enqueue("test.sqlite", {"n": 1}, dedupe_key="k")
        assert tasks.enqueue("test.sqlite", dedupe_key="k") is None
        tasks.ensure_periodic_tasks()

        first = tasks.claim_next(WORKER, lease_seconds=LEASE)
        second = tasks.claim_next(WORKER, lease_seconds=LEASE)
        assert {first.kind, second.kind} == {"test.sqlite", "test.sqlite-periodic"}
        assert tasks.claim_next(WORKER, lease_seconds=LEASE) is None
        assert tasks.complete_task(first) is True
        assert tasks.fail_task(second, "boom") == tasks.QUEUED
        assert BackgroundTask.get_by_id(task_id).dedupe_key is None
        # The periodic kind left exactly one next occurrence behind.
        assert (
            BackgroundTask.select()
            .where(BackgroundTask.dedupe_key == tasks.periodic_key("test.sqlite-periodic"))
            .count()
            == 1
        )
    sqlite_db.close()

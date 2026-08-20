"""Adversarial unit tests for one durable operation delivery."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from kohakuhub.operations import executor
from kohakuhub.operations.registry import HandlerSpec, OperationRegistry
from kohakuhub.operations.types import RetryableOperationError, StepResult


class _Cursor:
    def __init__(self, row=None):
        self.row = row

    async def fetchone(self):
        return self.row


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Connection:
    def __init__(self):
        self.queries = []
        self.next_step_id = 100

    def transaction(self):
        return _Transaction()

    async def execute(self, query, params=None):
        self.queries.append((" ".join(str(query).split()), params))
        if "INSERT INTO khub_operation_steps" in str(query):
            return _Cursor((self.next_step_id,))
        return _Cursor()


class _Pool:
    def __init__(self):
        self.connection_value = _Connection()

    def connection(self):
        connection = self.connection_value

        class Context:
            async def __aenter__(self):
                return connection

            async def __aexit__(self, *_args):
                return False

        return Context()


def _records(*, operation_state="accepted", step_state="pending", attempt=1):
    operation_id = uuid4()
    operation = SimpleNamespace(
        id=operation_id,
        kind="test.operation.v1",
        state=operation_state,
        expected_head="base",
        resource_key="resource",
        repository_id=None,
    )
    step = SimpleNamespace(
        id=1,
        operation_id=operation_id,
        state=step_state,
        attempt=attempt,
        step_name="test.operation.v1",
        step_version="1",
        input_json={},
        checkpoint_json={},
        external_marker=None,
        sequence=0,
        procrastinate_job_id=10,
    )
    return operation, step


class _Store:
    def __init__(self, pool, operation, step):
        self.operation = operation
        self.step = step
        self.pool = pool
        self.finished = []
        self.observing = []
        self.requeued = []
        self.running = True

    async def mark_running(self, _connection, _operation_id, _step_id):
        if not self.running:
            return None
        return self.operation, self.step, True

    async def touch_heartbeat(self, *_args):
        return None

    async def get_operation(self, _connection, *_args, **_kwargs):
        return self.operation

    async def get_step(self, _connection, *_args, **_kwargs):
        return self.step

    async def finish_step(self, _connection, _operation, _step, **kwargs):
        self.finished.append(kwargs)
        return True

    async def mark_step_observing(self, _connection, _step_id, _operation_id, **kwargs):
        self.observing.append(kwargs)
        return True

    async def requeue_retryable_step(self, _connection, *_args, **kwargs):
        self.requeued.append(("ordinary", kwargs))
        return True

    async def requeue_replay_safe_step(self, _connection, *_args, **kwargs):
        self.requeued.append(("replay-safe", kwargs))
        return True

    async def set_job_id(self, *_args):
        return None


def _registry(spec: HandlerSpec):
    return OperationRegistry((spec,))


@pytest.mark.asyncio
async def test_executor_rejects_stale_handler_checkpoint(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    step.step_name = "old.operation.v1"
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=lambda *_args: None,
        )
    )

    await executor.execute_operation_step(
        pool, str(operation.id), str(step.id), registry=registry
    )

    assert store.finished[0]["state"] == "failed"
    assert store.finished[0]["error_code"] == "handler_version_mismatch"


@pytest.mark.asyncio
async def test_executor_redacts_unexpected_handler_failure(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def fail(_operation, _step):
        raise RuntimeError("secret remote response")

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=fail,
        )
    )

    await executor.execute_operation_step(
        pool, str(operation.id), str(step.id), registry=registry
    )

    assert store.finished[0]["state"] == "failed"
    assert store.finished[0]["error_code"] == "handler_failed"
    assert "secret" not in str(store.finished[0])


@pytest.mark.asyncio
async def test_executor_does_not_turn_connection_failure_into_handler_failure(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def fail(_operation, _step):
        raise ConnectionError("database connection dropped")

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=fail,
        )
    )

    with pytest.raises(ConnectionError, match="database connection dropped"):
        await executor.execute_operation_step(
            pool, str(operation.id), str(step.id), registry=registry
        )

    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_moves_non_replayable_external_failure_to_observing(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setattr(executor, "_mark_external_dispatch", lambda *_args, **_kwargs: _true())

    async def fail(_operation, _step):
        raise RetryableOperationError(error_code="remote_timeout")

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=fail,
            external_side_effect=True,
            replay_safe_after_dispatch=False,
        )
    )

    await executor.execute_operation_step(
        pool, str(operation.id), str(step.id), registry=registry
    )

    assert store.observing[0]["error_code"] == "remote_timeout"
    assert store.requeued == []


@pytest.mark.asyncio
async def test_executor_requeues_replay_safe_external_failure(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setattr(executor, "_mark_external_dispatch", lambda *_args, **_kwargs: _true())

    async def fail(_operation, _step):
        raise RetryableOperationError(error_code="temporary_storage_failure")

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=fail,
            external_side_effect=True,
            replay_safe_after_dispatch=True,
        )
    )

    with pytest.raises(RetryableOperationError):
        await executor.execute_operation_step(
            pool, str(operation.id), str(step.id), registry=registry
        )

    assert store.requeued[0][0] == "replay-safe"


@pytest.mark.asyncio
async def test_executor_exhausts_ordinary_retry_without_requeue(monkeypatch):
    pool = _Pool()
    operation, step = _records(attempt=3)
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def fail(_operation, _step):
        raise RetryableOperationError(error_code="temporary_failure")

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=fail,
        )
    )

    await executor.execute_operation_step(
        pool, str(operation.id), str(step.id), registry=registry
    )

    assert store.finished[0]["error_code"] == "retry_exhausted"
    assert store.requeued == []


@pytest.mark.asyncio
async def test_executor_cancellation_finishes_pre_dispatch_step(monkeypatch):
    pool = _Pool()
    operation, step = _records(operation_state="cancel_requested", step_state="running")
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    started = asyncio.Event()

    async def blocked(_operation, _step):
        started.set()
        await asyncio.Event().wait()

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=1,
            handler=blocked,
        )
    )
    task = asyncio.create_task(
        executor.execute_operation_step(
            pool, str(operation.id), str(step.id), registry=registry
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.finished[0]["state"] == "cancelled"
    assert store.finished[0]["error_code"] == "cancelled"


@pytest.mark.asyncio
async def test_executor_enqueues_deterministic_successor_in_same_connection(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _Store(pool, operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    configured = []

    class Task:
        def configure(self, **kwargs):
            configured.append(kwargs)
            return self

        async def defer_async(self, **kwargs):
            configured.append(kwargs)
            return 99

    app = SimpleNamespace(tasks={"test.task": Task()})

    async def continue_handler(_operation, _step):
        return StepResult.continue_with(
            input_json={"cursor": "next"},
            progress_current=1,
            progress_total=2,
        )

    registry = _registry(
        HandlerSpec(
            kind=operation.kind,
            version="1",
            task_name="test.task",
            queue="control-v1",
            priority=7,
            handler=continue_handler,
            lock_prefix="test-lock",
        )
    )

    await executor.execute_operation_step(
        pool, str(operation.id), str(step.id), registry=registry, app=app
    )

    assert configured[0]["queue"] == "control-v1"
    assert configured[0]["queueing_lock"] == f"operation:{operation.id}:step:1"
    assert configured[1] == {"operation_id": str(operation.id), "step_id": "100"}
    assert any("INSERT INTO khub_operation_steps" in query for query, _ in pool.connection_value.queries)


async def _true():
    return True

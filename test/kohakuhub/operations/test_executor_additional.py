"""High-value boundary tests for operation execution and finalization."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from kohakuhub.operations import executor, handlers
from kohakuhub.operations.finalizer import (
    finalize_commit_domain,
    mark_commit_intent_committed,
)
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
    def __init__(self, *, select_row=None, next_step_id=100):
        self.select_row = select_row
        self.next_step_id = next_step_id
        self.queries = []

    def transaction(self):
        return _Transaction()

    async def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.queries.append((normalized, params))
        if "SELECT o.state, s.state" in normalized:
            return _Cursor(self.select_row)
        if "INSERT INTO khub_operation_steps" in normalized:
            return _Cursor((self.next_step_id,))
        return _Cursor()


class _Pool:
    def __init__(self, connection=None):
        self.connection_value = connection or _Connection()

    def connection(self):
        connection = self.connection_value

        class _Context:
            async def __aenter__(self):
                return connection

            async def __aexit__(self, *_args):
                return False

        return _Context()


class _HeartbeatFailurePool(_Pool):
    """Fail one heartbeat connection while leaving delivery connections usable."""

    def __init__(self, connection=None):
        super().__init__(connection)
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        if self.connection_calls == 2:
            class _FailingContext:
                async def __aenter__(self):
                    raise RuntimeError("heartbeat connection unavailable")

                async def __aexit__(self, *_args):
                    return False

            return _FailingContext()
        return super().connection()


def _records(
    *,
    operation_state="accepted",
    step_state="pending",
    attempt=1,
    repository_id=None,
):
    operation_id = uuid4()
    operation = SimpleNamespace(
        id=operation_id,
        kind="test.operation.v1",
        state=operation_state,
        expected_head="expected-base",
        resource_key="resource",
        repository_id=repository_id,
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


class _ExecutorStore:
    def __init__(
        self,
        operation,
        step,
        *,
        claimed=True,
        started=True,
        ordinary_requeue=True,
        replay_safe_requeue=True,
        final_operation=None,
        final_step=None,
    ):
        self.operation = operation
        self.step = step
        self.claimed = claimed
        self.started = started
        self.ordinary_requeue = ordinary_requeue
        self.replay_safe_requeue = replay_safe_requeue
        self.final_operation = final_operation
        self.final_step = final_step
        self.finished = []
        self.observing = []
        self.requeued = []
        self.heartbeat_calls = 0

    async def mark_running(self, _connection, _operation_id, _step_id):
        if not self.started:
            return None
        return self.operation, self.step, self.claimed

    async def touch_heartbeat(self, *_args):
        self.heartbeat_calls += 1

    async def get_operation(self, _connection, *_args, **_kwargs):
        return self.final_operation if self.final_operation is not None else self.operation

    async def get_step(self, _connection, *_args, **_kwargs):
        return self.final_step if self.final_step is not None else self.step

    async def finish_step(self, _connection, _operation, _step, **kwargs):
        self.finished.append(kwargs)
        return True

    async def mark_step_observing(self, _connection, _step_id, _operation_id, **kwargs):
        self.observing.append(kwargs)
        return True

    async def requeue_retryable_step(self, _connection, *_args, **kwargs):
        self.requeued.append(("ordinary", kwargs))
        return self.ordinary_requeue

    async def requeue_replay_safe_step(self, _connection, *_args, **kwargs):
        self.requeued.append(("replay-safe", kwargs))
        return self.replay_safe_requeue

    async def set_job_id(self, *_args):
        return None


def _registry(operation, handler, **overrides):
    return OperationRegistry(
        (
            HandlerSpec(
                kind=operation.kind,
                version="1",
                task_name="test.task",
                queue="control-v1",
                priority=1,
                handler=handler,
                **overrides,
            ),
        )
    )


@pytest.mark.asyncio
async def test_mark_external_dispatch_persists_marker_expectations_and_deadlines(monkeypatch):
    connection = _Connection(select_row=("running", "pending"))
    pool = _Pool(connection)
    operation, step = _records()
    step.input_json = {"base_head": "input-base", "commit_id": "target-commit"}
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_REMOTE_SECONDS", "12")
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_QUIET_SECONDS", "4")

    assert await executor._mark_external_dispatch(
        pool,
        operation,
        step,
        marker="dispatch-marker",
    ) is True

    step_update = connection.queries[1]
    assert "SET state = 'dispatch_started'" in step_update[0]
    assert step_update[1][:3] == (
        "dispatch-marker",
        "input-base",
        "target-commit",
    )

    operation_update = connection.queries[2]
    dispatch_now, remote_deadline, observe_not_before, operation_id = operation_update[1]
    assert operation_id == operation.id
    assert remote_deadline - dispatch_now == timedelta(seconds=12)
    assert observe_not_before - remote_deadline == timedelta(seconds=4)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_state", "step_state"),
    (
        pytest.param("cancel_requested", "running", id="cancel-requested"),
        pytest.param("running", "observing", id="observing"),
        pytest.param("running", "uncertain", id="uncertain"),
    ),
)
async def test_mark_external_dispatch_refuses_cancelled_or_observed_rows(
    operation_state, step_state
):
    connection = _Connection(select_row=(operation_state, step_state))
    operation, step = _records()

    assert await executor._mark_external_dispatch(
        _Pool(connection), operation, step, marker="must-not-dispatch"
    ) is False
    assert len(connection.queries) == 1


@pytest.mark.asyncio
async def test_mark_external_dispatch_returns_false_when_row_disappears():
    connection = _Connection(select_row=None)
    operation, step = _records()

    assert await executor._mark_external_dispatch(
        _Pool(connection), operation, step, marker="missing-row"
    ) is False
    assert len(connection.queries) == 1


@pytest.mark.asyncio
async def test_executor_returns_when_delivery_claim_disappears(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step, started=False)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, lambda *_args: StepResult.succeeded()),
    )

    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_ignores_delivery_that_was_not_claimed(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step, claimed=False)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    called = []

    async def handler(_operation, _step):
        called.append(True)
        return StepResult.succeeded()

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler),
    )

    assert called == []
    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_does_not_reenter_dispatch_started_delivery(monkeypatch):
    pool = _Pool()
    operation, step = _records(
        operation_state="dispatch_started", step_state="dispatch_started"
    )
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    called = []

    async def handler(_operation, _step):
        called.append(True)
        return StepResult.succeeded()

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler, external_side_effect=True),
    )

    assert called == []
    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_aborts_handler_when_dispatch_boundary_is_lost(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setattr(executor, "_mark_external_dispatch", _false_async)
    called = []

    async def handler(_operation, _step):
        called.append(True)
        return StepResult.succeeded()

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler, external_side_effect=True),
    )

    assert called == []
    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_preserves_observation_when_cancelled_after_dispatch(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    started = asyncio.Event()

    async def mark_dispatch(*_args, **_kwargs):
        operation.state = "dispatch_started"
        step.state = "dispatch_started"
        return True

    monkeypatch.setattr(executor, "_mark_external_dispatch", mark_dispatch)

    async def handler(_operation, _step):
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        executor.execute_operation_step(
            pool,
            str(operation.id),
            str(step.id),
            registry=_registry(operation, handler, external_side_effect=True),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.finished == []
    assert store.observing == []


@pytest.mark.asyncio
async def test_executor_turns_completion_after_cancel_request_into_cancelled(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def handler(_operation, _step):
        operation.state = "cancel_requested"
        return StepResult.succeeded(result_json={"must_not": "escape"})

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler),
    )

    assert store.finished[0]["state"] == "cancelled"
    assert store.finished[0]["result_json"] is None


@pytest.mark.asyncio
async def test_executor_does_not_terminalize_unconfirmed_external_failure(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def mark_dispatch(*_args, **_kwargs):
        operation.state = "dispatch_started"
        step.state = "dispatch_started"
        return True

    monkeypatch.setattr(executor, "_mark_external_dispatch", mark_dispatch)

    async def handler(_operation, _step):
        raise RuntimeError("secret dependency details")

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler, external_side_effect=True),
    )

    assert store.finished == []
    assert store.observing == []


@pytest.mark.asyncio
async def test_executor_requeues_pre_dispatch_retryable_failure_and_reraises(monkeypatch):
    pool = _Pool()
    operation, step = _records(attempt=1)
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_MAX_RETRIES", "3")

    async def handler(_operation, _step):
        raise RetryableOperationError(
            error_code="temporary_dependency",
            error_summary="retry from checkpoint",
        )

    with pytest.raises(RetryableOperationError) as exc_info:
        await executor.execute_operation_step(
            pool,
            str(operation.id),
            str(step.id),
            registry=_registry(operation, handler),
        )

    assert exc_info.value.error_code == "temporary_dependency"
    assert store.requeued == [
        (
            "ordinary",
            {
                "error_code": "temporary_dependency",
                "error_summary": "retry from checkpoint",
            },
        )
    ]
    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_stops_replay_safe_retry_when_store_rejects_requeue(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step, replay_safe_requeue=False)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setattr(executor, "_mark_external_dispatch", _mark_dispatch_state)

    async def handler(_operation, _step):
        raise RetryableOperationError(error_code="remote_timeout")

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(
            operation,
            handler,
            external_side_effect=True,
            replay_safe_after_dispatch=True,
        ),
    )

    assert store.requeued[0][0] == "replay-safe"
    assert store.finished == []


@pytest.mark.asyncio
async def test_executor_finishes_confirmed_external_success_after_dispatch(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setattr(executor, "_mark_external_dispatch", _mark_dispatch_state)

    async def handler(_operation, _step):
        return StepResult(
            state="succeeded",
            result_json={"remote": "confirmed"},
            external_effect_confirmed=True,
        )

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler, external_side_effect=True),
    )

    assert store.finished[0]["state"] == "succeeded"
    assert store.finished[0]["result_json"] == {"remote": "confirmed"}


@pytest.mark.asyncio
async def test_executor_reports_missing_app_for_successor_delivery(monkeypatch):
    pool = _Pool()
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)

    async def handler(_operation, _step):
        return StepResult.continue_with(input_json={"cursor": "next"})

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler),
    )

    assert store.finished[0]["state"] == "failed"
    assert store.finished[0]["error_code"] == "successor_enqueue_unavailable"


@pytest.mark.asyncio
async def test_executor_rejects_fenced_handler_when_database_url_is_unavailable(monkeypatch):
    pool = _Pool()
    operation, step = _records(repository_id=9)
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    called = []

    async def handler(_operation, _step):
        called.append(True)
        return StepResult.succeeded()

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler, fence_scope="repository"),
        app=SimpleNamespace(),
    )

    assert called == []
    assert store.finished[0]["error_code"] == "mutation_fence_unavailable"


@pytest.mark.asyncio
async def test_executor_keeps_handler_result_when_heartbeat_connection_fails(
    monkeypatch,
):
    connection = _Connection()
    pool = _HeartbeatFailurePool(connection)
    operation, step = _records()
    store = _ExecutorStore(operation, step)
    monkeypatch.setattr(executor, "OperationStore", lambda _pool: store)
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_HEARTBEAT_SECONDS", "0.5")

    async def handler(_operation, _step):
        await asyncio.sleep(0.75)
        return StepResult.succeeded()

    await executor.execute_operation_step(
        pool,
        str(operation.id),
        str(step.id),
        registry=_registry(operation, handler),
    )

    assert pool.connection_calls >= 3
    assert store.finished[0]["state"] == "succeeded"


def _finalizer_payload(**overrides):
    payload = {
        "repository_id": 7,
        "owner_id": 3,
        "commit_id": "commit-1",
        "repo_type": "model",
        "branch": "main",
        "author_id": 2,
        "username": "owner",
        "file_mutations": [],
        "quota_delta": 0,
        "is_private": False,
    }
    payload.update(overrides)
    return payload


def _query(connection, prefix):
    return next(
        (entry for entry in connection.queries if entry[0].lower().startswith(prefix)),
        None,
    )


@pytest.mark.asyncio
async def test_finalize_commit_domain_upserts_main_file_on_supplied_connection():
    connection = _Connection()
    await finalize_commit_domain(
        connection,
        payload=_finalizer_payload(
            file_mutations=[
                {
                    "path": "README.md",
                    "size": 11,
                    "sha256": "sha-1",
                    "lfs": True,
                    "action": "upsert",
                }
            ]
        ),
    )

    file_insert = _query(connection, "insert into file")
    assert file_insert is not None
    assert file_insert[1] == (7, "README.md", 11, "sha-1", True, 3)


@pytest.mark.asyncio
async def test_finalize_commit_domain_marks_deleted_main_file_on_supplied_connection():
    connection = _Connection()
    await finalize_commit_domain(
        connection,
        payload=_finalizer_payload(
            file_mutations=[{"path": "removed.bin", "action": "delete"}]
        ),
    )

    file_update = _query(connection, "update file")
    assert file_update is not None
    assert file_update[1] == (7, "removed.bin")


@pytest.mark.asyncio
async def test_finalize_commit_domain_batches_distinct_main_file_upserts():
    connection = _Connection()
    mutations = [
        {
            "path": f"items/file-{index}.txt",
            "size": index + 1,
            "sha256": f"sha-{index}",
            "lfs": False,
            "action": "upsert",
        }
        for index in range(4)
    ]

    await finalize_commit_domain(
        connection,
        payload=_finalizer_payload(file_mutations=mutations),
    )

    file_inserts = [
        entry for entry in connection.queries if entry[0].lower().startswith("insert into file")
    ]
    assert len(file_inserts) == 1
    assert file_inserts[0][1] == (
        7,
        "items/file-0.txt",
        1,
        "sha-0",
        False,
        3,
        7,
        "items/file-1.txt",
        2,
        "sha-1",
        False,
        3,
        7,
        "items/file-2.txt",
        3,
        "sha-2",
        False,
        3,
        7,
        "items/file-3.txt",
        4,
        "sha-3",
        False,
        3,
    )


@pytest.mark.asyncio
async def test_finalize_commit_domain_chunks_large_file_upsert_batches():
    connection = _Connection()
    mutations = [
        {
            "path": f"items/file-{index}.txt",
            "size": index + 1,
            "sha256": f"sha-{index}",
            "lfs": False,
            "action": "upsert",
        }
        for index in range(1001)
    ]

    await finalize_commit_domain(
        connection,
        payload=_finalizer_payload(file_mutations=mutations),
    )

    file_inserts = [
        entry
        for entry in connection.queries
        if entry[0].lower().startswith("insert into file")
    ]
    assert len(file_inserts) == 2
    assert len(file_inserts[0][1]) == 1000 * 6
    assert len(file_inserts[1][1]) == 1 * 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("is_private", "expected_namespace_params"),
    (
        pytest.param(True, (True, 5, True, 5, 3), id="private"),
        pytest.param(False, (False, 5, False, 5, 3), id="public"),
    ),
)
async def test_finalize_commit_domain_routes_main_quota_delta_to_namespace_counter(
    is_private, expected_namespace_params
):
    connection = _Connection()
    await finalize_commit_domain(
        connection,
        payload=_finalizer_payload(quota_delta=5, is_private=is_private),
    )

    repository_update = _query(connection, "update repository")
    user_update = _query(connection, 'update "user"')
    assert repository_update is not None
    assert repository_update[1] == (5, 7)
    assert user_update is not None
    assert user_update[1] == expected_namespace_params


@pytest.mark.asyncio
async def test_finalize_commit_domain_requires_owner_before_writing_commit():
    connection = _Connection()
    payload = _finalizer_payload()
    payload.pop("owner_id")

    with pytest.raises(ValueError, match="missing owner_id"):
        await finalize_commit_domain(connection, payload=payload)

    assert connection.queries == []


@pytest.mark.asyncio
async def test_finalize_commit_domain_rejects_unsupported_main_file_mutation():
    connection = _Connection()

    with pytest.raises(ValueError, match="unsupported commit file mutation"):
        await finalize_commit_domain(
            connection,
            payload=_finalizer_payload(
                file_mutations=[{"path": "x", "action": "rename"}]
            ),
        )

    assert _query(connection, "update file") is None


@pytest.mark.asyncio
async def test_mark_commit_intent_committed_persists_confirmed_id_and_result():
    connection = _Connection()

    await mark_commit_intent_committed(
        connection,
        intent_id="intent-1",
        lakefs_commit_id="lake-commit",
        result_json={"new_commit_id": "lake-commit"},
    )

    query, params = connection.queries[0]
    assert "state = 'committed'" in query
    assert params[0] == "lake-commit"
    assert params[1].obj == {"new_commit_id": "lake-commit"}
    assert params[2] == "intent-1"


def _postprocess_payload(**overrides):
    payload = {
        "repository_id": 7,
        "namespace": "owner",
        "repo_type": "model",
        "name": "repo",
        "commit_id": "commit-1",
        "lfs_tracking": [],
        "allow_destructive_gc": False,
    }
    payload.update(overrides)
    return payload


def test_incremental_usage_rejects_missing_namespace(monkeypatch):
    class RepositoryRecord:
        private = False
        used_bytes = 0

        @classmethod
        def get_by_id(cls, _repository_id):
            return cls()

    monkeypatch.setattr(handlers, "Repository", RepositoryRecord)
    monkeypatch.setattr(handlers, "get_organization", lambda _namespace: None)

    with pytest.raises(ValueError, match="namespace no longer exists"):
        handlers._read_incremental_usage(7, "missing-owner", True)


@pytest.mark.asyncio
async def test_postprocess_gc_dependency_failure_is_retryable(monkeypatch):
    monkeypatch.setattr(
        handlers,
        "_read_incremental_usage",
        lambda *_args: {
            "repository_used_bytes": 0,
            "namespace_used_bytes": 0,
            "namespace_total_used_bytes": 0,
        },
    )
    monkeypatch.setattr(handlers, "track_lfs_object", lambda **_kwargs: None)

    def fail_gc(**_kwargs):
        raise OSError("object cleanup unavailable")

    monkeypatch.setattr(handlers, "run_gc_for_file", fail_gc)
    monkeypatch.setattr(handlers.cfg.app, "lfs_auto_gc", True)

    with pytest.raises(RetryableOperationError) as exc_info:
        await handlers.perform_commit_postprocess(
            _postprocess_payload(
                allow_destructive_gc=True,
                lfs_tracking=[
                    {
                        "path": "replaced.bin",
                        "sha256": "new-sha",
                        "size": 1,
                        "old_sha256": "old-sha",
                    }
                ],
            )
        )

    assert exc_info.value.error_code == "postprocess_dependency_unavailable"
    assert "cleanup dependency" in exc_info.value.error_summary


@pytest.mark.asyncio
async def test_finalize_revert_rejects_missing_repository_or_author(monkeypatch):
    monkeypatch.setattr(handlers, "get_repository", lambda *_args: None)
    monkeypatch.setattr(handlers.User, "get_by_id", lambda _user_id: None)

    with pytest.raises(ValueError, match="repository or author no longer exists"):
        await handlers.finalize_revert_commit(
            {
                "lakefs_repo": "lakefs-repo",
                "branch": "main",
                "ref": "old-commit",
                "repo_type": "model",
                "namespace": "owner",
                "name": "repo",
                "author_id": 1,
                "username": "owner",
            },
            commit_id="new-commit",
        )


@pytest.mark.asyncio
async def test_revert_handler_reuses_persisted_marker_on_redelivery(monkeypatch):
    calls = []

    async def perform(payload, *, marker):
        calls.append((payload, marker))
        return {"success": True}

    monkeypatch.setattr(handlers, "perform_revert_operation", perform)
    operation = SimpleNamespace(id="operation-id")
    step = SimpleNamespace(
        id=9,
        external_marker="persisted-marker",
        input_json={"ref": "target"},
    )

    result = await handlers.revert_operation_handler(operation, step)

    assert calls == [({"ref": "target"}, "persisted-marker")]
    assert result.external_effect_confirmed is True


async def _false_async(*_args, **_kwargs):
    return False


async def _mark_dispatch_state(_pool, operation, step, *, marker):
    del marker
    operation.state = "dispatch_started"
    step.state = "dispatch_started"
    return True

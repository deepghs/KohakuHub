"""High-value isolated tests for the OperationService contract."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from kohakuhub import lakefs_mutation_gateway as mutation_gateway
import kohakuhub.operations.service as service_module
from kohakuhub.operations.registry import HandlerSpec, OperationRegistry
from kohakuhub.operations.service import (
    CommitInProgress,
    IdempotencyConflict,
    OperationInProgress,
    OperationNotCancellable,
    OperationService,
    _canonical_payload,
)


_MISSING = object()


class _Cursor:
    def __init__(self, row=None):
        self.row = row
        self.rowcount = 0

    async def fetchone(self):
        return self.row


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, _exc_type, _exc, _tb):
        return False


class _Connection:
    def __init__(self, *, operation=None, step=None, intent=None):
        self.operation = operation
        self.step = step
        self.intent = intent
        self.queries: list[tuple[str, object]] = []

    def transaction(self):
        return _AsyncContext(self)

    async def execute(self, query, params=None):
        normalized = " ".join(str(query).split()).lower()
        self.queries.append((normalized, params))
        if self.operation is not None and "update khub_operation_steps" in normalized:
            if "state = 'dispatch_started'" in normalized:
                self.step.state = "dispatch_started"
        if (
            self.operation is not None
            and "update khub_repository_operations" in normalized
        ):
            if "state = 'dispatch_started'" in normalized:
                self.operation.state = "dispatch_started"
        if self.intent is not None and "set dispatch_started_at" in normalized:
            self.intent.dispatch_started_at = params[0]
            self.intent.remote_deadline_at = params[1]
            self.intent.observe_not_before = params[2]
            self.intent.version += 1
        return _Cursor()


class _Pool:
    def __init__(self, connection=None):
        self._connection = connection or _Connection()
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        return _AsyncContext(self._connection)


class _Task:
    def __init__(self):
        self.configure_calls: list[dict[str, object]] = []
        self.defer_calls: list[dict[str, object]] = []

    def configure(self, **kwargs):
        self.configure_calls.append(kwargs)
        return self

    async def defer_async(self, **kwargs):
        self.defer_calls.append(kwargs)
        return 91


class _JobManager:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.calls: list[tuple[int, bool]] = []

    async def cancel_job_by_id_async(self, job_id, *, abort):
        self.calls.append((job_id, abort))
        if self.error is not None:
            raise self.error


def _app(*, task=None, job_manager=None):
    return SimpleNamespace(
        tasks={"khub:operation:execute.v1": task or _Task()},
        job_manager=job_manager,
    )


def _operation(
    *,
    state="accepted",
    kind="maintenance.noop.v1",
    operation_id=None,
    request_hash="request-hash",
    repository_id=7,
):
    return SimpleNamespace(
        id=operation_id or uuid4(),
        kind=kind,
        state=state,
        request_hash=request_hash,
        repository_id=repository_id,
    )


def _step(*, operation_id, state="pending", job_id=42):
    return SimpleNamespace(
        id=11,
        operation_id=operation_id,
        state=state,
        procrastinate_job_id=job_id,
    )


def _intent(
    *,
    state="prepared",
    intent_id=None,
    version=3,
    payload=None,
    payload_hash="payload-hash",
    request_hash="request-hash",
    observation_operation_id=None,
    lakefs_commit_id=None,
):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=intent_id or uuid4(),
        operation_id=None,
        observation_operation_id=observation_operation_id,
        repository_id=7,
        requested_by_user_id=3,
        ref="main",
        base_head="base-head",
        marker="marker",
        idempotency_key="intent-key",
        request_hash=request_hash,
        prepared_deadline_at=None,
        dispatch_started_at=None,
        remote_deadline_at=None,
        observe_not_before=None,
        observation_head=None,
        observation_cursor=None,
        payload_hash=payload_hash,
        payload_json=payload or {"repository_id": 7},
        state=state,
        lakefs_commit_id=lakefs_commit_id,
        result_json=None,
        error_code=None,
        error_summary=None,
        created_at=now,
        updated_at=now,
        finalized_at=None,
        version=version,
    )


class _OperationStore:
    def __init__(
        self,
        *,
        operation=None,
        step=None,
        existing_by_idempotency=None,
        blocking_operation=None,
        cancel_result=_MISSING,
    ):
        self.operation = operation
        self.step = step
        self.existing_by_idempotency = existing_by_idempotency
        self.blocking_operation = blocking_operation
        self.cancel_result = cancel_result
        self.inserted_operation: dict[str, object] | None = None
        self.inserted_steps: list[dict[str, object]] = []
        self.job_ids: list[tuple[int, int]] = []
        self.active_step_calls = 0
        self.request_cancel_calls = 0

    async def get_operation_by_idempotency(self, *_args, **_kwargs):
        return self.existing_by_idempotency

    async def get_blocking_repository_operation(self, *_args, **_kwargs):
        return self.blocking_operation

    async def insert_operation(self, _connection, values):
        self.inserted_operation = dict(values)
        self.operation = _operation(
            state="accepted",
            kind=values["kind"],
            operation_id=values["id"],
            request_hash=values["request_hash"],
            repository_id=values["repository_id"],
        )

    async def insert_step(self, _connection, values):
        self.inserted_steps.append(dict(values))
        self.step = _step(operation_id=values["operation_id"])
        return self.step.id

    async def set_job_id(self, _connection, step_id, job_id):
        self.job_ids.append((step_id, job_id))

    async def get_operation(self, _connection, _operation_id, **_kwargs):
        return self.operation

    async def get_step(self, _connection, _step_id, **_kwargs):
        return self.step

    async def get_active_step_for_operation(self, _connection, _operation_id):
        self.active_step_calls += 1
        return self.step

    async def request_cancel(self, _connection, _operation_id):
        self.request_cancel_calls += 1
        if self.cancel_result is not _MISSING:
            return self.cancel_result
        return self.operation


class _IntentStore:
    def __init__(
        self,
        *,
        intent=None,
        existing_by_request=None,
        blocking_intent=None,
        update_result=_MISSING,
    ):
        self.intent = intent
        self.existing_by_request = existing_by_request
        self.blocking_intent = blocking_intent
        self.update_result = update_result
        self.inserted_values = None
        self.update_calls: list[dict[str, object]] = []
        self.payload_updates: list[dict[str, object]] = []
        self.deadline_updates: list[object] = []
        self.quota_transitions: list[tuple[object, str, str]] = []
        self.finished_observations: list[dict[str, object]] = []

    async def get_commit_intent(self, _connection, _intent_id, **_kwargs):
        return self.intent

    async def get_commit_intent_by_request(self, _connection, **_kwargs):
        return self.existing_by_request

    async def get_blocking_commit_intent(self, *_args, **_kwargs):
        return self.blocking_intent

    async def insert_commit_intent(self, _connection, values):
        self.inserted_values = dict(values)
        self.intent = _intent(
            intent_id=values["id"],
            payload=values["payload_json"],
            payload_hash=values["payload_hash"],
            request_hash=values["request_hash"],
        )
        self.intent.repository_id = values["repository_id"]
        self.intent.requested_by_user_id = values["requested_by_user_id"]
        self.intent.ref = values["ref"]
        self.intent.base_head = values["base_head"]
        self.intent.marker = values["marker"]
        self.intent.idempotency_key = values["idempotency_key"]
        return self.intent

    async def update_commit_intent(self, _connection, _intent_id, **kwargs):
        self.update_calls.append(dict(kwargs))
        if self.update_result is not _MISSING:
            return self.update_result
        if self.intent is None:
            return None
        self.intent.state = kwargs["state"]
        self.intent.version += 1
        if kwargs.get("lakefs_commit_id") is not None:
            self.intent.lakefs_commit_id = kwargs["lakefs_commit_id"]
        if kwargs.get("result_json") is not None:
            self.intent.result_json = kwargs["result_json"]
        self.intent.error_code = kwargs.get("error_code")
        self.intent.error_summary = kwargs.get("error_summary")
        return self.intent

    async def update_prepared_commit_payload(
        self, _connection, _intent_id, **kwargs
    ):
        self.payload_updates.append(dict(kwargs))
        if self.intent is None:
            return None
        self.intent.payload_json = dict(kwargs["payload"])
        self.intent.payload_hash = kwargs["payload_hash"]
        self.intent.version += 1
        return self.intent

    async def refresh_prepared_deadline(self, _connection, _intent_id, deadline):
        self.deadline_updates.append(deadline)
        self.intent.prepared_deadline_at = deadline
        self.intent.version += 1
        return self.intent

    async def transition_quota_reservations(
        self, _connection, intent_id, *, from_state, to_state
    ):
        self.quota_transitions.append((intent_id, from_state, to_state))

    async def finish_observation_operation(
        self, _connection, operation_id, **kwargs
    ):
        self.finished_observations.append(
            {"operation_id": operation_id, **kwargs}
        )
        return _operation(state=kwargs["state"], operation_id=operation_id)


class _FenceConnection:
    def __init__(self):
        self.queries: list[tuple[str, object]] = []
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        self.exited = True
        return False

    async def execute(self, query, params=None):
        self.queries.append((" ".join(str(query).split()).lower(), params))
        return _Cursor()


class _FenceStore:
    def __init__(self, *, commit_intent=None, repository_intent=None):
        self.commit_intent = commit_intent
        self.repository_intent = repository_intent
        self.commit_refs: list[str] = []

    async def get_blocking_commit_intent(self, _connection, *, ref, **_kwargs):
        self.commit_refs.append(ref)
        return self.commit_intent

    async def get_blocking_repository_intent(self, *_args, **_kwargs):
        return self.repository_intent

    async def get_blocking_repository_operation(self, *_args, **_kwargs):
        return None


def _patch_fence_connection(monkeypatch, connection):
    class _AsyncConnectionFactory:
        @staticmethod
        async def connect(*_args, **_kwargs):
            return connection

    monkeypatch.setattr(
        service_module.psycopg, "AsyncConnection", _AsyncConnectionFactory
    )


def _accept_args(**overrides):
    values = {
        "kind": "maintenance.noop.v1",
        "resource_key": "resource:1",
        "payload": {},
        "requested_by_user_id": 3,
        "idempotency_key": "key-1",
        "repository_id": None,
        "trigger": "api",
        "expected_head": None,
    }
    values.update(overrides)
    return values


def test_canonical_payload_rejects_sensitive_field_in_nested_request():
    with pytest.raises(ValueError, match="sensitive field"):
        _canonical_payload({"metadata": {"headers": [["X-Api-Key", "secret"]]}})


def test_canonical_payload_rejects_credential_bearing_url():
    with pytest.raises(ValueError, match="credential-bearing URL"):
        _canonical_payload(
            {"callback": "https://example.test/hook?access_token=secret"}
        )


def test_canonical_payload_canonicalizes_safe_urls_and_mapping_order():
    first, first_hash = _canonical_payload(
        {"callback": "https://example.test/hook", "z": 2, "a": [1, 2]}
    )
    second, second_hash = _canonical_payload(
        {"a": [1, 2], "z": 2, "callback": "https://example.test/hook"}
    )

    assert first == second
    assert first_hash == second_hash


def test_accept_request_rejects_empty_or_oversized_resource_key():
    service = OperationService(None, None)

    for resource_key in ("", "x" * 513):
        with pytest.raises(ValueError, match="resource_key"):
            service._accept_request(**_accept_args(resource_key=resource_key))


def test_accept_request_rejects_empty_or_oversized_idempotency_key():
    service = OperationService(None, None)

    for idempotency_key in ("", "x" * 257):
        with pytest.raises(ValueError, match="idempotency_key"):
            service._accept_request(
                **_accept_args(idempotency_key=idempotency_key)
            )


def test_accept_request_rejects_unknown_trigger():
    service = OperationService(None, None)

    with pytest.raises(ValueError, match="unsupported operation trigger"):
        service._accept_request(**_accept_args(trigger="client"))


def test_accept_request_rejects_unknown_operation_kind():
    service = OperationService(None, None)

    with pytest.raises(ValueError, match="unknown operation kind"):
        service._accept_request(**_accept_args(kind="client.callable.v1"))


@pytest.mark.asyncio
async def test_accept_persists_canonical_payload_and_enqueues_one_delivery():
    task = _Task()
    app = _app(task=task)
    pool = _Pool()
    store = _OperationStore()
    service = OperationService(pool, app)
    service.store = store

    operation = await service.accept(
        kind="maintenance.noop.v1",
        resource_key="resource:canonical",
        payload={"z": 2, "a": [1, 2]},
        requested_by_user_id=3,
        idempotency_key="canonical-key",
    )

    assert operation.state == "accepted"
    assert store.inserted_steps[0]["input_json"] == {"a": [1, 2], "z": 2}
    assert task.configure_calls[0]["queueing_lock"] == (
        f"operation:{operation.id}:step:0"
    )
    assert task.defer_calls == [
        {"operation_id": str(operation.id), "step_id": "11"}
    ]


@pytest.mark.asyncio
async def test_accept_rejects_idempotency_key_bound_to_a_different_request():
    existing = _operation(request_hash="persisted-request")
    store = _OperationStore(existing_by_idempotency=existing)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(IdempotencyConflict, match="already bound"):
        await service.accept(
            kind="maintenance.noop.v1",
            resource_key="resource:conflict",
            payload={"value": 1},
            requested_by_user_id=3,
            idempotency_key="same-key",
        )

    assert store.inserted_operation is None


@pytest.mark.asyncio
async def test_accept_rejects_repository_operation_when_another_one_is_active():
    async def handler(_operation, _step):
        raise AssertionError("handler is not called by acceptance")

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="repository.test.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=1,
                handler=handler,
            ),
        )
    )
    blocker = _operation(kind="repository.other.v1", state="running")
    store = _OperationStore(blocking_operation=blocker)
    service = OperationService(_Pool(), _app(), registry=registry)
    service.store = store

    with pytest.raises(OperationInProgress) as error:
        await service.accept_in_transaction(
            _Connection(),
            kind="repository.test.v1",
            resource_key="repository:7",
            payload={},
            requested_by_user_id=None,
            repository_id=7,
        )

    assert error.value.operation is blocker
    assert store.inserted_operation is None


@pytest.mark.asyncio
async def test_prepare_commit_rejects_idempotency_key_bound_to_a_different_request():
    existing = _intent(request_hash="persisted-request")
    store = _IntentStore(existing_by_request=existing)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(IdempotencyConflict, match="already bound"):
        await service.prepare_commit_intent(
            repository_id=7,
            ref="main",
            base_head="head-1",
            payload={"files": []},
            requested_by_user_id=3,
            idempotency_key="same-key",
            request_hash="incoming-request",
        )

    assert store.inserted_values is None


@pytest.mark.asyncio
async def test_prepare_commit_rejects_missing_ref_before_opening_connection():
    pool = _Pool()
    service = OperationService(pool, _app())

    with pytest.raises(ValueError, match="requires ref and base_head"):
        await service.prepare_commit_intent(
            repository_id=7,
            ref="",
            base_head="head-1",
            payload={},
        )

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_prepare_commit_rejects_sensitive_payload_before_opening_connection():
    pool = _Pool()
    service = OperationService(pool, _app())

    with pytest.raises(ValueError, match="sensitive field"):
        await service.prepare_commit_intent(
            repository_id=7,
            ref="main",
            base_head="head-1",
            payload={"metadata": {"authorization": "secret"}},
        )

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_cancel_returns_terminal_operation_without_touching_delivery():
    operation = _operation(state="succeeded")
    store = _OperationStore(operation=operation)
    manager = _JobManager()
    service = OperationService(_Pool(), _app(job_manager=manager))
    service.store = store

    result = await service.cancel(operation.id)

    assert result is operation
    assert store.active_step_calls == 0
    assert store.request_cancel_calls == 0
    assert manager.calls == []


@pytest.mark.asyncio
async def test_cancel_rejects_operation_after_external_dispatch():
    operation = _operation(state="dispatch_started")
    store = _OperationStore(operation=operation)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(OperationNotCancellable, match="external side-effect"):
        await service.cancel(operation.id)

    assert store.active_step_calls == 0
    assert store.request_cancel_calls == 0


@pytest.mark.asyncio
async def test_cancel_rejects_running_handler_that_disallows_cancellation():
    async def handler(_operation, _step):
        raise AssertionError("handler is not called by cancellation")

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="repository.uncancellable.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=1,
                handler=handler,
                cancel_while_running=False,
            ),
        )
    )
    operation = _operation(kind="repository.uncancellable.v1", state="running")
    store = _OperationStore(
        operation=operation, step=_step(operation_id=operation.id, state="running")
    )
    service = OperationService(_Pool(), _app(), registry=registry)
    service.store = store

    with pytest.raises(OperationNotCancellable, match="cannot be cancelled"):
        await service.cancel(operation.id)

    assert store.request_cancel_calls == 0


@pytest.mark.asyncio
async def test_cancel_keeps_state_result_when_delivery_cancellation_fails():
    operation = _operation(state="running")
    requested = _operation(
        state="cancel_requested", operation_id=operation.id, kind=operation.kind
    )
    step = _step(operation_id=operation.id, state="running", job_id=77)
    store = _OperationStore(operation=operation, step=step, cancel_result=requested)
    manager = _JobManager(error=RuntimeError("delivery manager unavailable"))
    service = OperationService(_Pool(), _app(job_manager=manager))
    service.store = store

    result = await service.cancel(operation.id)

    assert result is requested
    assert store.request_cancel_calls == 1
    assert manager.calls == [(77, True)]


@pytest.mark.asyncio
async def test_mark_dispatch_started_transitions_operation_and_step_together():
    operation = _operation(state="accepted")
    step = _step(operation_id=operation.id, state="pending")
    connection = _Connection(operation=operation, step=step)
    store = _OperationStore(operation=operation, step=step)
    service = OperationService(_Pool(connection), _app())
    service.store = store

    from datetime import datetime, timedelta, timezone

    remote_deadline = datetime.now(timezone.utc) + timedelta(seconds=30)
    observe_not_before = remote_deadline + timedelta(seconds=30)
    await service.mark_dispatch_started(
        operation.id,
        step.id,
        expected_source="head-a",
        expected_target="head-b",
        remote_deadline=remote_deadline,
        observe_not_before=observe_not_before,
        external_marker="marker:operation",
    )

    assert operation.state == "dispatch_started"
    assert step.state == "dispatch_started"
    operation_update = next(
        params
        for query, params in connection.queries
        if "update khub_repository_operations" in query
    )
    assert operation_update == (remote_deadline, observe_not_before, operation.id)


@pytest.mark.asyncio
async def test_mark_dispatch_started_is_idempotent_after_boundary():
    operation = _operation(state="dispatch_started")
    step = _step(operation_id=operation.id, state="dispatch_started")
    connection = _Connection(operation=operation, step=step)
    store = _OperationStore(operation=operation, step=step)
    service = OperationService(_Pool(connection), _app())
    service.store = store

    await service.mark_dispatch_started(
        operation.id,
        step.id,
        expected_source="head-a",
        expected_target="head-b",
        remote_deadline=object(),
        observe_not_before=object(),
        external_marker="marker:operation",
    )

    assert connection.queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["cancel_requested", "cancelled"])
async def test_mark_dispatch_started_rejects_pre_dispatch_cancellation(state):
    operation = _operation(state=state)
    step = _step(operation_id=operation.id, state="pending")
    connection = _Connection(operation=operation, step=step)
    store = _OperationStore(operation=operation, step=step)
    service = OperationService(_Pool(connection), _app())
    service.store = store

    with pytest.raises(OperationNotCancellable, match="cancelled before external"):
        await service.mark_dispatch_started(
            operation.id,
            step.id,
            expected_source="head-a",
            expected_target="head-b",
            remote_deadline=object(),
            observe_not_before=object(),
            external_marker="marker:operation",
        )

    assert connection.queries == []


@pytest.mark.asyncio
async def test_mark_commit_dispatch_started_sets_observation_deadlines():
    intent = _intent(state="prepared", version=5)
    connection = _Connection(intent=intent)
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(connection), _app())
    service.store = store

    result = await service.mark_commit_intent_dispatch_started(
        intent.id, remote_timeout_seconds=4, quiet_period_seconds=6
    )

    assert result is intent
    assert intent.state == "dispatch_started"
    assert intent.dispatch_started_at is not None
    assert intent.remote_deadline_at is not None
    assert intent.observe_not_before is not None
    assert intent.dispatch_started_at < intent.remote_deadline_at
    assert intent.remote_deadline_at < intent.observe_not_before
    assert store.update_calls[0]["expected_version"] == 5


@pytest.mark.asyncio
async def test_mark_commit_dispatch_started_returns_terminal_intent_unchanged():
    intent = _intent(state="committed", version=9)
    connection = _Connection(intent=intent)
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(connection), _app())
    service.store = store

    result = await service.mark_commit_intent_dispatch_started(intent.id)

    assert result is intent
    assert store.update_calls == []
    assert connection.queries == []


@pytest.mark.asyncio
async def test_abandon_commit_rejects_ambiguous_dispatch_without_no_effect_evidence():
    intent = _intent(state="dispatch_started")
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(OperationNotCancellable, match="affirmative no-effect"):
        await service.abandon_commit_intent(intent.id, error_code="timeout")

    assert store.update_calls == []
    assert store.quota_transitions == []


@pytest.mark.asyncio
async def test_abandon_commit_releases_quota_and_finishes_observer():
    observation_id = uuid4()
    intent = _intent(
        state="dispatch_started", observation_operation_id=observation_id
    )
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(), _app())
    service.store = store

    await service.abandon_commit_intent(
        intent.id, error_code="confirmed_no_effect", confirmed_no_effect=True
    )

    assert intent.state == "abandoned"
    assert store.quota_transitions == [
        (intent.id, "reserved", "released")
    ]
    assert store.finished_observations[0] == {
        "operation_id": observation_id,
        "state": "succeeded",
        "result_json": {
            "outcome": "no_effect",
            "error_code": "confirmed_no_effect",
        },
    }


@pytest.mark.asyncio
async def test_stale_commit_abandonment_does_not_release_quota_or_finish_observation():
    observation_id = uuid4()
    intent = _intent(
        state="dispatch_started", observation_operation_id=observation_id
    )
    store = _IntentStore(intent=intent, update_result=None)
    service = OperationService(_Pool(), _app())
    service.store = store

    await service.abandon_commit_intent(
        intent.id,
        error_code="stale-observer",
        confirmed_no_effect=True,
        expected_version=intent.version,
    )

    assert store.quota_transitions == []
    assert store.finished_observations == []


@pytest.mark.asyncio
async def test_record_staging_path_rejects_intent_after_preparation_boundary():
    intent = _intent(state="dispatch_started")
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(OperationNotCancellable, match="no longer mutable"):
        await service.record_prepared_staging_path(intent.id, path="staging/file")

    assert store.payload_updates == []


@pytest.mark.asyncio
async def test_mark_commit_intent_uncertain_keeps_terminal_intent_unchanged():
    intent = _intent(state="finalized")
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(), _app())
    service.store = store

    result = await service.mark_commit_intent_uncertain(
        intent.id, error_code="late", error_summary="already settled"
    )

    assert result is intent
    assert store.update_calls == []


@pytest.mark.asyncio
async def test_mark_commit_intent_committed_rejects_a_different_commit_id():
    intent = _intent(state="committed", lakefs_commit_id="commit-old")
    store = _IntentStore(intent=intent)
    service = OperationService(_Pool(), _app())
    service.store = store

    with pytest.raises(IdempotencyConflict, match="another LakeFS commit"):
        await service.mark_commit_intent_committed(
            intent.id, lakefs_commit_id="commit-new"
        )

    assert store.update_calls == []


@pytest.mark.asyncio
async def test_repository_ref_fence_rejects_unsupported_scope_before_connecting():
    service = OperationService(
        None, _app(), database_url=f"postgresql://unit-{uuid4()}"
    )

    with pytest.raises(ValueError, match="unsupported mutation fence scope"):
        async with service.repository_ref_fence(7, "main", scope="invalid"):
            pass


@pytest.mark.asyncio
async def test_repository_ref_fence_raises_blocker_and_releases_advisory_locks(
    monkeypatch,
):
    connection = _FenceConnection()
    blocker = _intent(state="prepared")
    store = _FenceStore(commit_intent=blocker)
    app = _app()
    service = OperationService(
        None, app, database_url=f"postgresql://unit-{uuid4()}"
    )
    service.store = store
    _patch_fence_connection(monkeypatch, connection)

    with pytest.raises(CommitInProgress) as error:
        async with service.repository_ref_fence(7, "branch:main"):
            raise AssertionError("blocked fence must not yield")

    assert error.value.intent is blocker
    assert connection.entered is True
    assert connection.exited is True
    assert any(
        "pg_advisory_unlock(hashtextextended" in query
        for query, _ in connection.queries
    )
    assert any(
        "pg_advisory_unlock_shared(hashtextextended" in query
        for query, _ in connection.queries
    )
    assert mutation_gateway.has_active_capability() is False


@pytest.mark.asyncio
async def test_repository_ref_fence_restores_capability_when_body_raises(monkeypatch):
    connection = _FenceConnection()
    store = _FenceStore()
    service = OperationService(
        None, _app(), database_url=f"postgresql://unit-{uuid4()}"
    )
    service.store = store
    _patch_fence_connection(monkeypatch, connection)

    with pytest.raises(RuntimeError, match="body failure"):
        async with service.repository_ref_fence(7, "main"):
            assert mutation_gateway.has_active_capability() is True
            capability = mutation_gateway.require_capability(
                "commit", {"branch": "main"}
            )
            assert capability.ref == "branch:main"
            raise RuntimeError("body failure")

    assert mutation_gateway.has_active_capability() is False
    assert any(
        "pg_advisory_unlock_shared(hashtextextended" in query
        for query, _ in connection.queries
    )


@pytest.mark.asyncio
async def test_repository_cutover_fence_uses_repository_capability(monkeypatch):
    connection = _FenceConnection()
    store = _FenceStore()
    service = OperationService(
        None, _app(), database_url=f"postgresql://unit-{uuid4()}"
    )
    service.store = store
    _patch_fence_connection(monkeypatch, connection)

    async with service.repository_ref_fence(7, "__repository__"):
        capability = mutation_gateway.require_capability("delete_repository", {})
        assert capability.scope == "cutover"
        assert capability.ref == "__repository__"

    assert any(
        "select pg_advisory_lock(hashtextextended" in query
        for query, _ in connection.queries
    )
    assert not any(
        "select pg_advisory_lock_shared(hashtextextended" in query
        for query, _ in connection.queries
    )


@pytest.mark.asyncio
async def test_repository_name_fence_restores_name_capability_after_body(monkeypatch):
    connection = _FenceConnection()
    service = OperationService(
        None, _app(), database_url=f"postgresql://unit-{uuid4()}"
    )
    _patch_fence_connection(monkeypatch, connection)

    async with service.repository_name_fence("owner/repository"):
        capability = mutation_gateway.require_capability("create_repository", {})
        assert capability.scope == "repository_name"
        assert capability.ref == "__repository_name__"

    assert mutation_gateway.has_active_capability() is False
    assert any(
        "pg_advisory_unlock(hashtextextended" in query
        for query, _ in connection.queries
    )

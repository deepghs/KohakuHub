"""Additional boundary tests for the durable operation and worker platform."""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from procrastinate.schema import SchemaManager

import kohakuhub.operations.readiness as readiness
import kohakuhub.operations.store as store_module
import kohakuhub.worker.supervisor as supervisor_module
from kohakuhub.migrations.schema import (
    EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS,
    EXPECTED_OPERATION_INDEX_DEFINITIONS,
    KERNEL_COLUMN_CONTRACT,
    PROCRASTINATE_REQUIRED_INDEXES,
    PROCRASTINATE_TABLE_COLUMNS,
    PROCRASTINATE_TYPES,
    REQUIRED_OPERATION_CONSTRAINTS,
    REQUIRED_OPERATION_INDEXES,
    expected_table_columns,
    signature_digest,
)
from kohakuhub.operations.registry import DEFAULT_REGISTRY, HandlerSpec, OperationRegistry
from kohakuhub.operations.sql import (
    OPERATION_SCHEMA_VERSION,
    operation_table_columns,
    operation_table_columns_for_version,
)
from kohakuhub.operations.store import OPERATION_COLUMNS, OperationStore
from kohakuhub.operations.types import OperationRecord, StepResult, operation_max_attempts
from kohakuhub.worker.config import WorkerSettings
from kohakuhub.worker.supervisor import WorkerSupervisor


class _Rows:
    def __init__(self, *, rows=(), row=None, rowcount=0):
        self._rows = list(rows)
        self._row = row
        self.rowcount = rowcount

    async def fetchone(self):
        return self._row

    async def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, responses=(), *, fail_at=None, failure=None):
        self.responses = list(responses)
        self.fail_at = fail_at
        self.failure = failure or RuntimeError("fake connection failure")
        self.calls = []

    async def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        call_index = len(self.calls)
        self.calls.append((normalized, params))
        if call_index == self.fail_at:
            raise self.failure
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return _Rows()


def _column_names(columns):
    return tuple(name.strip() for name in columns.replace("\n", " ").split(","))


def test_store_mapping_row_preserves_mapping_rows():
    row = {"id": "operation-id"}

    assert store_module._mapping_row(row, "id") is row


def _readiness_responses():
    expected_tables = set(operation_table_columns()) | readiness.PROCRASTINATE_TABLES | {
        "khub_schema_migrations"
    }
    table_rows = [(table,) for table in sorted(expected_tables)]
    ledger_rows = [
        (
            "khub-current-adoption",
            1,
            signature_digest(expected_table_columns(include_operations=False)),
        ),
        (
            "procrastinate-3.9.0",
            1,
            hashlib.sha256(SchemaManager.get_schema().encode("utf-8")).hexdigest(),
        ),
        (
            f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION}",
            OPERATION_SCHEMA_VERSION,
            signature_digest(operation_table_columns()),
        ),
    ]
    operation_columns = [
        (table, column)
        for table, columns in operation_table_columns().items()
        for column in columns
    ]
    worker_columns = [
        (table, column)
        for table, columns in PROCRASTINATE_TABLE_COLUMNS.items()
        for column in columns
    ]
    worker_indexes = [(name,) for name in sorted(PROCRASTINATE_REQUIRED_INDEXES)]
    worker_types = []
    for name, (kind, values) in PROCRASTINATE_TYPES.items():
        if kind == "enum":
            worker_types.extend((name, "e", value) for value in values)
        else:
            worker_types.append((name, "c", None))
    index_definitions = dict(EXPECTED_OPERATION_INDEX_DEFINITIONS)
    for name in REQUIRED_OPERATION_INDEXES:
        index_definitions.setdefault(
            name, f"CREATE INDEX {name} ON public.placeholder (id)"
        )
    index_rows = [
        (name, definition, True)
        for name, definition in sorted(index_definitions.items())
    ]
    constraint_rows = [
        (name, kind, True, definition)
        for name, (kind, definition) in EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS.items()
    ]
    semantic_rows = [
        (table, column, *expected)
        for table, columns in KERNEL_COLUMN_CONTRACT.items()
        for column, expected in columns.items()
    ]
    return [
        _Rows(rows=table_rows),
        _Rows(rows=ledger_rows),
        _Rows(rows=operation_columns),
        _Rows(rows=worker_columns),
        _Rows(rows=worker_indexes),
        _Rows(rows=worker_types),
        _Rows(rows=index_rows),
        _Rows(rows=constraint_rows),
        _Rows(rows=semantic_rows),
    ]


def _readiness_responses_with(stage):
    responses = _readiness_responses()
    if stage == "missing_tables":
        responses[0] = _Rows(rows=responses[0]._rows[1:])
    elif stage == "ledger_mismatch":
        responses[1] = _Rows(rows=[])
    elif stage == "missing_operation_columns":
        responses[2] = _Rows(rows=responses[2]._rows[1:])
    elif stage == "missing_worker_columns":
        responses[3] = _Rows(rows=responses[3]._rows[1:])
    elif stage == "missing_worker_indexes":
        responses[4] = _Rows(rows=responses[4]._rows[1:])
    elif stage == "invalid_worker_types":
        rows = list(responses[5]._rows)
        name, _, _ = rows[0]
        rows[0] = (name, "e", "unexpected")
        responses[5] = _Rows(rows=rows)
    elif stage == "missing_operation_indexes":
        responses[6] = _Rows(rows=responses[6]._rows[1:])
    elif stage == "invalid_operation_indexes":
        rows = list(responses[6]._rows)
        name, _, valid = rows[0]
        rows[0] = (name, "CREATE INDEX broken ON public.other (value)", valid)
        responses[6] = _Rows(rows=rows)
    elif stage == "missing_constraints":
        responses[7] = _Rows(rows=responses[7]._rows[1:])
    elif stage == "invalid_constraints":
        rows = list(responses[7]._rows)
        name, kind, _, definition = rows[0]
        rows[0] = (name, kind, False, definition)
        responses[7] = _Rows(rows=rows)
    elif stage == "missing_semantics":
        responses[8] = _Rows(rows=responses[8]._rows[1:])
    elif stage == "invalid_semantics":
        rows = list(responses[8]._rows)
        table, column, _, nullable, default = rows[0]
        rows[0] = (table, column, "wrong type", nullable, default)
        responses[8] = _Rows(rows=rows)
    else:
        raise AssertionError(f"unknown readiness stage: {stage}")
    return responses


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "message"),
    [
        ("missing_tables", "durable operation schema is incomplete"),
        ("ledger_mismatch", "durable schema ledger mismatch"),
        ("missing_operation_columns", "durable operation columns are incomplete"),
        ("missing_worker_columns", "Procrastinate columns are incomplete"),
        ("missing_worker_indexes", "Procrastinate indexes are incomplete"),
        ("invalid_worker_types", "Procrastinate types are incompatible"),
        ("missing_operation_indexes", "durable operation indexes are incomplete"),
        ("invalid_operation_indexes", "durable operation index definitions are invalid"),
        ("missing_constraints", "durable operation constraints are incomplete"),
        ("invalid_constraints", "durable operation constraint definitions are invalid"),
        ("missing_semantics", "durable operation schema semantics mismatch"),
        ("invalid_semantics", "durable operation schema semantics mismatch"),
    ],
)
async def test_verify_operation_schema_rejects_catalog_contract_boundary(stage, message):
    connection = _Connection(_readiness_responses_with(stage))

    with pytest.raises(RuntimeError, match=re.escape(message)):
        await readiness.verify_operation_schema(connection)


@pytest.mark.asyncio
async def test_verify_operation_schema_propagates_catalog_query_failure():
    connection = _Connection(
        _readiness_responses(),
        fail_at=0,
        failure=RuntimeError("catalog unavailable"),
    )

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        await readiness.verify_operation_schema(connection)


@pytest.mark.asyncio
async def test_verify_operation_schema_rejects_non_numeric_ledger_version():
    responses = _readiness_responses()
    name, _, checksum = responses[1]._rows[0]
    responses[1] = _Rows(rows=[(name, "not-a-version", checksum)])
    connection = _Connection(responses)

    with pytest.raises(ValueError, match="invalid literal"):
        await readiness.verify_operation_schema(connection)


def _operation_tuple(**overrides):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    operation_id = uuid4()
    values = {name: None for name in _column_names(OPERATION_COLUMNS)}
    values.update(
        {
            "id": operation_id,
            "kind": "maintenance.noop.v1",
            "handler_version": "1",
            "repository_id": 7,
            "resource_key": "repository:7",
            "requested_by_user_id": 11,
            "trigger": "api",
            "idempotency_key": "idempotency-key",
            "request_hash": "request-hash",
            "state": "running",
            "phase": "execute",
            "progress_current": 0,
            "expected_head": None,
            "created_at": now,
            "updated_at": now,
            "version": 0,
        }
    )
    values.update(overrides)
    return tuple(values[name] for name in _column_names(OPERATION_COLUMNS))


@pytest.mark.asyncio
async def test_store_converts_positional_operation_row_to_record():
    operation_id = uuid4()
    connection = _Connection(
        [_Rows(row=_operation_tuple(id=operation_id))]
    )

    result = await OperationStore(None).get_operation(connection, operation_id)

    assert result is not None
    assert result.id == operation_id
    assert result.kind == "maintenance.noop.v1"


@pytest.mark.asyncio
async def test_store_returns_none_when_marker_lookup_has_no_row():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).get_commit_intent_by_marker(
        connection, "marker:missing", for_update=True
    )

    assert result is None
    assert "FOR UPDATE" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_store_returns_none_when_repository_operation_blocker_is_absent():
    excluded_id = uuid4()
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).get_blocking_repository_operation(
        connection, repository_id=7, exclude_operation_id=excluded_id
    )

    assert result is None
    assert connection.calls[0][1] == (7, excluded_id)
    assert "id <> %s" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_store_returns_none_when_repository_intent_blocker_is_absent():
    excluded_id = uuid4()
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).get_blocking_repository_intent(
        connection, repository_id=7, exclude_intent_id=excluded_id
    )

    assert result is None
    assert connection.calls[0][1] == (7, excluded_id)
    assert "id <> %s" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_store_lists_empty_quota_reservations_with_row_locking():
    connection = _Connection([_Rows(rows=[])])

    result = await OperationStore(None).list_quota_reservations(
        connection, uuid4(), for_update=True
    )

    assert result == []
    assert connection.calls[0][0].endswith("FOR UPDATE")


@pytest.mark.asyncio
async def test_store_rejects_non_terminal_observation_completion():
    connection = _Connection()

    with pytest.raises(ValueError, match="terminal state"):
        await OperationStore(None).finish_observation_operation(
            connection,
            uuid4(),
            state="observing",
            result_json=None,
        )

    assert connection.calls == []


@pytest.mark.asyncio
async def test_store_returns_false_when_observation_cursor_update_loses_version_race():
    intent_id = uuid4()
    connection = _Connection([_Rows(rowcount=0)])

    result = await OperationStore(None).update_observation_cursor(
        connection, intent_id, "cursor-2", expected_version=4
    )

    assert result is False
    assert connection.calls[0][1] == ("cursor-2", intent_id, 4)


@pytest.mark.asyncio
async def test_store_returns_none_when_observation_head_was_already_set():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).set_observation_head(
        connection, uuid4(), "head-2", expected_version=3
    )

    assert result is None


@pytest.mark.asyncio
async def test_store_returns_false_when_observer_step_update_matches_no_row():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).update_observing_checkpoint(
        connection,
        12,
        uuid4(),
        checkpoint={"cursor": "next"},
        error_code="observing",
        error_summary="still checking",
    )

    assert result is False
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_returns_true_when_observer_checkpoint_updates_both_rows():
    operation_id = uuid4()
    connection = _Connection(
        [_Rows(row=(12,)), _Rows(row=(operation_id,))]
    )

    result = await OperationStore(None).update_observing_checkpoint(
        connection,
        12,
        operation_id,
        checkpoint={"cursor": "next"},
        error_code="observing",
        error_summary="still checking",
    )

    assert result is True
    assert len(connection.calls) == 2


@pytest.mark.asyncio
async def test_store_marks_step_observing_and_operation_uncertain():
    operation_id = uuid4()
    connection = _Connection([_Rows(), _Rows()])

    await OperationStore(None).mark_step_observing(
        connection,
        12,
        operation_id,
        error_code="remote_uncertain",
        error_summary="remote result is unknown",
    )

    assert len(connection.calls) == 2
    assert "SET state = 'observing'" in connection.calls[0][0]
    assert "SET state = 'uncertain'" in connection.calls[1][0]


@pytest.mark.asyncio
async def test_store_returns_false_when_stalled_step_update_matches_no_row():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).requeue_stalled_step(
        connection, 12, uuid4(), max_attempts=3
    )

    assert result is False
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_returns_false_when_stalled_external_step_update_matches_no_row():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).requeue_stalled_external_step(
        connection, 12, uuid4(), max_attempts=3
    )

    assert result is False
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_requeues_retryable_step_and_operation():
    operation_id = uuid4()
    connection = _Connection([_Rows(), _Rows()])

    await OperationStore(None).requeue_retryable_step(
        connection,
        12,
        operation_id,
        error_code="temporary_failure",
        error_summary="retry later",
    )

    assert len(connection.calls) == 2
    assert connection.calls[0][1] == (
        "temporary_failure",
        "retry later",
        12,
        operation_id,
    )
    assert connection.calls[1][1] == (
        "temporary_failure",
        "retry later",
        operation_id,
    )


@pytest.mark.asyncio
async def test_store_returns_false_when_replay_safe_step_update_matches_no_row():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).requeue_replay_safe_step(
        connection,
        12,
        uuid4(),
        error_code="temporary_failure",
        error_summary="retry later",
        max_attempts=3,
    )

    assert result is False
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_rejects_non_positive_terminal_history_limit():
    with pytest.raises(ValueError, match="retention limit must be positive"):
        await OperationStore(None).prune_terminal_history(
            _Connection(), cutoff=object(), limit=0
        )


@pytest.mark.asyncio
async def test_store_returns_zero_counts_when_terminal_history_has_no_candidates():
    connection = _Connection([_Rows(rows=[])])

    result = await OperationStore(None).prune_terminal_history(
        connection, cutoff=object(), limit=10
    )

    assert result == (0, 0)
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_prunes_terminal_operations_and_terminal_jobs():
    operation_id = uuid4()
    connection = _Connection(
        [
            _Rows(rows=[(operation_id,)]),
            _Rows(rows=[(101,), (102,)]),
            _Rows(),
            _Rows(rows=[(101,)]),
            _Rows(),
            _Rows(rows=[(operation_id,)]),
        ]
    )

    result = await OperationStore(None).prune_terminal_history(
        connection, cutoff=object(), limit=10
    )

    assert result == (1, 1)
    assert len(connection.calls) == 6
    assert connection.calls[1][1] == ([operation_id],)
    assert connection.calls[3][1] == ([101, 102],)


@pytest.mark.asyncio
async def test_store_rejects_non_positive_unlinked_job_retention_limit():
    with pytest.raises(ValueError, match="retention limit must be positive"):
        await OperationStore(None).prune_unlinked_terminal_jobs(
            _Connection(), cutoff=object(), limit=0
        )


@pytest.mark.asyncio
async def test_store_returns_zero_when_no_unlinked_terminal_jobs_are_old():
    connection = _Connection([_Rows(rows=[])])

    result = await OperationStore(None).prune_unlinked_terminal_jobs(
        connection, cutoff=object(), limit=10
    )

    assert result == 0
    assert len(connection.calls) == 1


@pytest.mark.asyncio
async def test_store_prunes_unlinked_terminal_jobs_after_removing_periodic_metadata():
    connection = _Connection(
        [_Rows(rows=[(101,)]), _Rows(), _Rows(rows=[(101,)])]
    )

    result = await OperationStore(None).prune_unlinked_terminal_jobs(
        connection, cutoff=object(), limit=10
    )

    assert result == 1
    assert len(connection.calls) == 3
    assert connection.calls[2][1] == ([101],)


@pytest.mark.asyncio
async def test_store_returns_none_when_operation_row_is_missing_before_mark_running():
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).mark_running(connection, uuid4(), 12)

    assert result is None
    assert len(connection.calls) == 2


@pytest.mark.asyncio
async def test_store_refreshes_step_and_operation_heartbeats():
    operation_id = uuid4()
    connection = _Connection([_Rows(), _Rows()])

    await OperationStore(None).touch_heartbeat(connection, operation_id, 12)

    assert len(connection.calls) == 2
    assert connection.calls[0][1] == (12, operation_id)
    assert connection.calls[1][1] == (operation_id,)


@pytest.mark.asyncio
async def test_store_returns_false_when_finish_step_update_matches_no_row():
    operation = SimpleNamespace(id=uuid4())
    step = SimpleNamespace(id=12)
    connection = _Connection([_Rows(row=None)])

    result = await OperationStore(None).finish_step(
        connection,
        operation,
        step,
        state="succeeded",
        progress_current=1,
        progress_total=1,
        progress_message="done",
        result_json={"ok": True},
        error_code=None,
        error_summary=None,
    )

    assert result is False
    assert len(connection.calls) == 1


def test_sql_preserves_unreleased_historical_columns():
    version_six = operation_table_columns_for_version(6)
    version_seven = operation_table_columns_for_version(7)

    assert "observation_cursor" not in version_six["khub_commit_intents"]
    assert "observation_head" not in version_seven["khub_commit_intents"]


def test_sql_rejects_unsupported_historical_version():
    with pytest.raises(ValueError, match="unsupported historical"):
        operation_table_columns_for_version(999)


def _handler_spec(**overrides):
    async def handler(_operation, _step):
        return StepResult.succeeded()

    values = {
        "kind": "test.edge.v1",
        "version": "1",
        "task_name": "khub:operation:execute.v1",
        "queue": "control-v1",
        "priority": 1,
        "handler": handler,
    }
    values.update(overrides)
    return HandlerSpec(**values)


def test_registry_rejects_missing_handler_identity():
    with pytest.raises(ValueError, match="identity is required"):
        OperationRegistry((_handler_spec(kind=""),))


def test_registry_rejects_unknown_lock_scope():
    with pytest.raises(ValueError, match="unsupported operation lock scope"):
        OperationRegistry((_handler_spec(lock_scope="tenant"),))


def test_registry_returns_sorted_operation_kinds():
    registry = OperationRegistry(
        (_handler_spec(kind="test.z.v1"), _handler_spec(kind="test.a.v1"))
    )

    assert registry.kinds() == ("test.a.v1", "test.z.v1")


def test_registry_rejects_step_version_mismatch():
    registry = OperationRegistry((_handler_spec(),))

    with pytest.raises(ValueError, match="unsupported step"):
        registry.resolve_step("test.edge.v1", "test.edge.v1", "2")


@pytest.mark.asyncio
async def test_default_observation_handler_returns_uncertain_result():
    spec = DEFAULT_REGISTRY.get("commit.observe.v1")

    result = await spec.handler(SimpleNamespace(), SimpleNamespace())

    assert result.state == "uncertain"
    assert result.error_code == "commit_observing"


def test_operation_max_attempts_rejects_zero_budget(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_MAX_RETRIES", "0")

    with pytest.raises(ValueError, match="must be positive"):
        operation_max_attempts()


def test_operation_record_public_dict_serializes_error_and_timestamps():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    record = OperationRecord(
        id=uuid4(),
        kind="maintenance.noop.v1",
        handler_version="1",
        repository_id=None,
        resource_key="resource",
        requested_by_user_id=None,
        trigger="api",
        idempotency_key=None,
        request_hash="hash",
        state="failed",
        phase="failed",
        progress_current=1,
        progress_total=1,
        progress_message="done",
        expected_head=None,
        cancel_requested_at=now,
        started_at=now,
        heartbeat_at=now,
        finished_at=now,
        dispatch_started_at=None,
        remote_deadline_at=None,
        observe_not_before=None,
        observation_cursor=None,
        result_json={"ok": False},
        error_code="failed",
        error_summary="handler failed",
        created_at=now,
        updated_at=now,
        version=2,
    )

    public = record.public_dict()

    assert public["id"] == str(record.id)
    assert public["error"] == {"code": "failed", "message": "handler failed"}
    assert public["cancel_requested"] is True
    assert public["started_at"] == now.isoformat()
    assert public["finished_at"] == now.isoformat()


def test_step_result_retry_with_creates_retryable_successor():
    result = StepResult.retry_with(
        input_json={"cursor": "next"},
        progress_current=2,
        progress_total=4,
        progress_message="retrying",
        error_code="temporary_failure",
        error_summary="try again",
    )

    assert result.state == "succeeded"
    assert result.retryable is True
    assert result.next_step_input == {"cursor": "next"}
    assert result.error_code == "temporary_failure"


def _postgres_env(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv(
        "KOHAKU_HUB_DATABASE_URL", "postgresql://user:pass@localhost/db"
    )


def test_worker_rejects_non_postgres_database_url(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_DB_BACKEND", "postgres")
    monkeypatch.setenv("KOHAKU_HUB_DATABASE_URL", "sqlite:///hub.db")

    with pytest.raises(ValueError, match="PostgreSQL.*DATABASE_URL"):
        WorkerSettings.from_env()


def test_worker_control_pool_must_cover_minimum_pool_size(monkeypatch):
    _postgres_env(monkeypatch)
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MIN", "4")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONTROL_POOL_MAX", "3")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_WORK_POOL_MAX", "4")

    with pytest.raises(ValueError, match="control worker pool max size"):
        WorkerSettings.from_env()


def test_worker_work_pool_must_cover_minimum_pool_size(monkeypatch):
    _postgres_env(monkeypatch)
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MIN", "4")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONTROL_POOL_MAX", "4")
    monkeypatch.setenv("KOHAKU_HUB_WORKER_WORK_POOL_MAX", "3")

    with pytest.raises(ValueError, match="work worker pool max size"):
        WorkerSettings.from_env()


def test_worker_rejects_non_positive_integer_setting(monkeypatch):
    _postgres_env(monkeypatch)
    monkeypatch.setenv("KOHAKU_HUB_WORKER_POOL_MIN", "0")

    with pytest.raises(ValueError, match="must be positive"):
        WorkerSettings.from_env()


def test_worker_rejects_non_positive_float_setting(monkeypatch):
    _postgres_env(monkeypatch)
    monkeypatch.setenv("KOHAKU_HUB_WORKER_SHUTDOWN_SECONDS", "0")

    with pytest.raises(ValueError, match="must be positive"):
        WorkerSettings.from_env()


@pytest.mark.parametrize("port", ["-1", "65536"])
def test_worker_rejects_metrics_port_outside_tcp_range(monkeypatch, port):
    _postgres_env(monkeypatch)
    monkeypatch.setenv("KOHAKU_HUB_WORKER_METRICS_PORT", port)

    with pytest.raises(ValueError, match="between 0 and 65535"):
        WorkerSettings.from_env()


class _AppContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _WorkerApp:
    def open_async(self):
        return _AppContext()


class _PoolContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _Pool:
    def __init__(self, connection):
        self.connection_value = connection

    def connection(self):
        return _PoolContext(self.connection_value)


def _settings():
    return WorkerSettings(database_url="postgresql://user:pass@localhost/db")


@pytest.mark.asyncio
async def test_supervisor_rejects_app_count_mismatch_and_closes_http(monkeypatch):
    runner = object()
    closed = []

    async def serve(*_args):
        return runner

    async def close(value):
        closed.append(value)

    monkeypatch.setattr(supervisor_module, "serve_worker_http", serve)
    monkeypatch.setattr(supervisor_module, "close_worker_http", close)
    supervisor = WorkerSupervisor(
        _settings(),
        app_factory=lambda _settings: (_WorkerApp(),),
    )

    with pytest.raises(RuntimeError, match="app count must match"):
        await supervisor.run()

    assert closed == [runner]


@pytest.mark.asyncio
async def test_supervisor_propagates_ready_lane_failure_after_startup(monkeypatch):
    runner = object()
    closed = []

    async def serve(*_args):
        return runner

    async def close(value):
        closed.append(value)

    class FailingReadyWorker:
        instances = []

        def __init__(self, *_args, **_kwargs):
            self.worker_id = "registered"
            self.stopped = False
            self.instances.append(self)

        async def run(self):
            raise RuntimeError("lane failed after readiness")

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(supervisor_module, "serve_worker_http", serve)
    monkeypatch.setattr(supervisor_module, "close_worker_http", close)
    supervisor = WorkerSupervisor(
        _settings(),
        app_factory=lambda _settings: (_WorkerApp(), _WorkerApp()),
        worker_factory=FailingReadyWorker,
    )
    supervisor._install_signal_handlers = lambda: None
    supervisor._remove_signal_handlers = lambda: None

    with pytest.raises(RuntimeError, match="lane failed after readiness"):
        await supervisor.run()

    assert closed == [runner]
    assert all(worker.stopped for worker in FailingReadyWorker.instances)


@pytest.mark.asyncio
async def test_supervisor_rejects_cancelled_lane_before_readiness():
    supervisor = WorkerSupervisor(_settings())
    supervisor._workers = [SimpleNamespace(worker_id=None)]
    task = asyncio.create_task(asyncio.sleep(0))
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    with pytest.raises(RuntimeError, match="cancelled before readiness"):
        await supervisor._wait_for_workers_started([task])


@pytest.mark.asyncio
async def test_supervisor_rejects_failed_database_readiness_probe():
    connection = _Connection([_Rows(row=(0,))])
    app = SimpleNamespace(
        connector=SimpleNamespace(pool=_Pool(connection)),
    )
    supervisor = WorkerSupervisor(_settings())

    with pytest.raises(RuntimeError, match="readiness check failed"):
        await supervisor._verify_readiness(app)


@pytest.mark.asyncio
async def test_supervisor_rejects_non_positive_connection_budget(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONNECTION_BUDGET", "0")
    connection = _Connection(
        [_Rows(row=(1,)), _Rows(row=(100,)), _Rows(row=(1,))]
    )
    app = SimpleNamespace(connector=SimpleNamespace(pool=_Pool(connection)))

    with pytest.raises(RuntimeError, match="connection budget must be positive"):
        await WorkerSupervisor(_settings())._verify_readiness(app)


@pytest.mark.asyncio
async def test_supervisor_rejects_current_connections_over_headroom(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_WORKER_CONNECTION_BUDGET", "10")
    connection = _Connection(
        [_Rows(row=(1,)), _Rows(row=(100,)), _Rows(row=(81,))]
    )
    app = SimpleNamespace(connector=SimpleNamespace(pool=_Pool(connection)))

    with pytest.raises(RuntimeError, match="current PostgreSQL connections"):
        await WorkerSupervisor(_settings())._verify_readiness(app)


@pytest.mark.asyncio
async def test_supervisor_skips_pool_health_when_pool_has_no_stats(monkeypatch):
    stop = asyncio.Event()

    async def wait_for(awaitable, timeout):
        awaitable.close()
        stop.set()
        raise asyncio.TimeoutError

    monkeypatch.setattr(supervisor_module.asyncio, "wait_for", wait_for)
    supervisor = WorkerSupervisor(_settings())
    supervisor._apps = (
        SimpleNamespace(connector=SimpleNamespace(pool=object())),
    )

    await supervisor._observe_process_health(stop)


def test_run_worker_loads_settings_and_runs_supervisor(monkeypatch):
    settings = _settings()
    constructed = []
    ran = []

    class FakeSupervisor:
        def __init__(self, value):
            constructed.append(value)

        async def run(self):
            ran.append(True)

    real_asyncio_run = asyncio.run

    def fake_asyncio_run(coro):
        real_asyncio_run(coro)

    monkeypatch.setattr(
        supervisor_module.WorkerSettings,
        "from_env",
        classmethod(lambda cls: settings),
    )
    monkeypatch.setattr(supervisor_module, "WorkerSupervisor", FakeSupervisor)
    monkeypatch.setattr(supervisor_module.asyncio, "run", fake_asyncio_run)

    supervisor_module.run_worker()

    assert constructed == [settings]
    assert len(ran) == 1

"""High-value reconciliation tests that do not require PostgreSQL."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from kohakuhub.operations import reconciliation


class _Result:
    def __init__(self, *, many=(), one=None):
        self._many = list(many)
        self._one = one

    async def fetchall(self):
        return self._many

    async def fetchone(self):
        return self._one


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _TransactionalConnection:
    def transaction(self):
        return _Transaction()


class _ConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _Pool:
    def __init__(self, connection):
        self._connection = connection

    def connection(self):
        return _ConnectionContext(self._connection)


class _MetricHandle:
    def __init__(self, metric, labels):
        self.metric = metric
        self.labels = labels

    def set(self, value):
        self.metric.set_calls.append((self.labels, value))

    def inc(self, value=1):
        self.metric.inc_calls.append((self.labels, value))


class _Metric:
    def __init__(self):
        self.label_calls = []
        self.set_calls = []
        self.inc_calls = []
        self.observe_calls = []

    def labels(self, **labels):
        self.label_calls.append(labels)
        return _MetricHandle(self, labels)

    def set(self, value):
        self.set_calls.append((None, value))

    def inc(self, value=1):
        self.inc_calls.append((None, value))

    def observe(self, value):
        self.observe_calls.append(value)


def _patch_reconciliation_metrics(monkeypatch):
    names = (
        "OPERATION_JOBS_REPAIRED",
        "OPERATION_RETENTION_PRUNED",
        "OPERATION_STALLED",
        "OPERATION_UNCERTAIN",
        "RECONCILIATION_DURATION",
        "RECONCILIATION_LAG_SECONDS",
        "RECONCILIATION_RUNS",
    )
    metrics = {name: _Metric() for name in names}
    for name, metric in metrics.items():
        monkeypatch.setattr(reconciliation, name, metric)
    return metrics


class _ReconcileConnection:
    def __init__(self, *, advisory_lock=True, stale_rows=()):
        self.advisory_lock = advisory_lock
        self.stale_rows = list(stale_rows)
        self.execute_calls = []

    def transaction(self):
        return _Transaction()

    async def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.execute_calls.append((normalized, params))
        if normalized.startswith("SELECT pg_try_advisory_xact_lock"):
            return _Result(one=(self.advisory_lock,))
        if normalized.startswith("SELECT o.id, o.kind, s.id, s.external_marker"):
            return _Result(many=self.stale_rows)
        if "MIN(updated_at)" in normalized:
            return _Result(one=(0,))
        if normalized.startswith("SELECT count(*) FROM khub_repository_operations"):
            return _Result(one=(0,))
        raise AssertionError(f"unexpected database query: {normalized}")


class _ReconcileTask:
    def __init__(self):
        self.configure_calls = []
        self.defer_calls = []

    def configure(self, **kwargs):
        self.configure_calls.append(kwargs)
        return self

    async def defer_async(self, **kwargs):
        self.defer_calls.append(kwargs)
        return 9001


def _install_reconcile_fakes(
    monkeypatch,
    *,
    advisory_lock=True,
    missing_jobs=(),
    recoverable_intents=(),
    stale_prepared=(),
    stale_rows=(),
    retention=(0, 0),
    unlinked_jobs=0,
    repair_error=None,
):
    connection = _ReconcileConnection(
        advisory_lock=advisory_lock,
        stale_rows=stale_rows,
    )
    pool = _Pool(connection)
    task = _ReconcileTask()
    metrics = _patch_reconciliation_metrics(monkeypatch)
    monkeypatch.setattr(
        reconciliation,
        "_recover_stalled_deliveries",
        _return_empty_recovered_deliveries,
    )
    monkeypatch.setattr(
        reconciliation,
        "_observe_runtime_metrics",
        _noop_observe_runtime_metrics,
    )

    class Store:
        instance = None

        def __init__(self, _pool):
            self.missing_jobs = list(missing_jobs)
            self.recoverable_intents = list(recoverable_intents)
            self.stale_prepared = list(stale_prepared)
            self.retention = retention
            self.unlinked_jobs = unlinked_jobs
            self.repair_error = repair_error
            self.set_job_calls = []
            self.requeue_calls = []
            self.observing_calls = []
            self.cursor_calls = []
            Store.instance = self

        async def finalize_cancelled_pending_steps(self, *_args, **_kwargs):
            return 0

        async def list_steps_missing_jobs(self, *_args, **_kwargs):
            return self.missing_jobs

        async def set_job_id(self, _connection, step_id, job_id):
            self.set_job_calls.append((step_id, job_id))

        async def list_recoverable_commit_intents(self, *_args, **_kwargs):
            return self.recoverable_intents

        async def list_stale_prepared_commit_intents(self, *_args, **_kwargs):
            return self.stale_prepared

        async def update_observation_cursor(self, *_args, **kwargs):
            self.cursor_calls.append(kwargs)
            return True

        async def requeue_stalled_external_step(self, _connection, step_id, operation_id, **kwargs):
            self.requeue_calls.append(
                ("external", step_id, operation_id, kwargs)
            )
            return True

        async def requeue_stalled_step(self, _connection, step_id, operation_id, **kwargs):
            self.requeue_calls.append(("ordinary", step_id, operation_id, kwargs))
            return True

        async def mark_step_observing(
            self, _connection, step_id, operation_id, **kwargs
        ):
            self.observing_calls.append((step_id, operation_id, kwargs))

        async def prune_terminal_history(self, *_args, **_kwargs):
            if self.repair_error is not None:
                raise self.repair_error
            return self.retention

        async def prune_unlinked_terminal_jobs(self, *_args, **_kwargs):
            return self.unlinked_jobs

    class Service:
        instance = None

        def __init__(self, *_args, **_kwargs):
            self.sync_calls = []
            Service.instance = self

        async def sync_commit_observation_operation(self, intent_id):
            self.sync_calls.append(intent_id)

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "OperationService", Service)
    app = SimpleNamespace(
        job_manager=None,
        tasks={"khub:operation:execute.v1": task},
        khub_stalled_worker_timeout_seconds=10,
    )
    return pool, app, connection, task, Store, Service, metrics


async def _return_empty_recovered_deliveries(*_args, **_kwargs):
    return set()


async def _noop_observe_runtime_metrics(*_args, **_kwargs):
    return None


@pytest.mark.asyncio
async def test_revert_observer_fails_closed_when_lakefs_branch_lookup_errors(monkeypatch):
    operation = SimpleNamespace(
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    step = SimpleNamespace(
        input_json={"lakefs_repo": "repo", "branch": "main"},
        checkpoint_json={},
        external_marker="marker",
    )
    finished = []

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

        async def finish_step(self, *_args, **kwargs):
            finished.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            raise RuntimeError("LakeFS unavailable")

        async def log_commits(self, **_kwargs):
            raise AssertionError("history must not be queried after branch failure")

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())

    assert not await reconciliation._observe_revert_operation_unfenced(
        _Pool(SimpleNamespace()), "operation", 1
    )
    assert finished == []


@pytest.mark.asyncio
async def test_revert_observer_rejects_duplicate_marker_commits(monkeypatch):
    operation = SimpleNamespace(
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    step = SimpleNamespace(
        input_json={
            "lakefs_repo": "repo",
            "branch": "main",
            "base_head": "base-head",
        },
        checkpoint_json={"observation_head": "snapshot-head"},
        external_marker="marker",
    )
    finished = []

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

        async def finish_step(self, *_args, **kwargs):
            finished.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "new-head"}

        async def log_commits(self, **_kwargs):
            return {
                "results": [
                    {"id": "marker-a", "metadata": {"khub_marker": "marker"}},
                    {"id": "marker-b", "metadata": {"khub_marker": "marker"}},
                    {"id": "base-head", "metadata": {}},
                ],
                "pagination": {"has_more": False},
            }

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())

    assert not await reconciliation._observe_revert_operation_unfenced(
        _Pool(SimpleNamespace()), "operation", 1
    )
    assert finished == []


@pytest.mark.asyncio
async def test_revert_observer_finishes_as_no_effect_at_unchanged_base(monkeypatch):
    operation = SimpleNamespace(
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    step = SimpleNamespace(
        input_json={
            "lakefs_repo": "repo",
            "branch": "main",
            "base_head": "base-head",
        },
        checkpoint_json={"observation_head": "base-head"},
        external_marker="marker",
    )
    finished = []

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

        async def finish_step(self, *_args, **kwargs):
            finished.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "base-head"}

        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "base-head", "metadata": {}}],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            return {"id": "base-head", "metadata": {}}

    async def unexpected_finalize(*_args, **_kwargs):
        raise AssertionError("no-effect observation must not finalize a commit")

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    monkeypatch.setattr(reconciliation, "finalize_revert_commit", unexpected_finalize)

    pool = _Pool(_TransactionalConnection())
    assert await reconciliation._observe_revert_operation_unfenced(
        pool, "operation", 1
    )
    assert len(finished) == 1
    assert finished[0]["result_json"] == {"success": True, "outcome": "no_effect"}


@pytest.mark.asyncio
async def test_revert_observer_reports_false_when_final_transition_loses_race(monkeypatch):
    operation = SimpleNamespace(
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    step = SimpleNamespace(
        input_json={
            "lakefs_repo": "repo",
            "branch": "main",
            "base_head": "base-head",
        },
        checkpoint_json={"observation_head": "snapshot-head"},
        external_marker="marker",
    )

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

        async def finish_step(self, *_args, **_kwargs):
            return False

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "snapshot-head"}

        async def log_commits(self, **_kwargs):
            return {
                "results": [
                    {"id": "marker-commit", "metadata": {"khub_marker": "marker"}},
                    {"id": "base-head", "metadata": {}},
                ],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            return {"metadata": {}}

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    async def finalize(*_args, **_kwargs):
        return {"success": True}

    monkeypatch.setattr(reconciliation, "finalize_revert_commit", finalize)

    pool = _Pool(_TransactionalConnection())
    assert not await reconciliation._observe_revert_operation_unfenced(
        pool, "operation", 1
    )


@pytest.mark.asyncio
async def test_revert_observer_holds_repository_ref_fence_while_observing(monkeypatch):
    operation = SimpleNamespace(repository_id=7, resource_key="repo")
    step = SimpleNamespace(input_json={"branch": "feature"})
    fence_calls = []
    observe_calls = []

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

    class Service:
        def repository_ref_fence(self, *args, **kwargs):
            fence_calls.append((args, kwargs))

            class Fence:
                async def __aenter__(self):
                    return _TransactionalConnection()

                async def __aexit__(self, *_args):
                    return False

            return Fence()

    async def observe(*args):
        observe_calls.append(args)
        return True

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "_observe_revert_operation_unfenced", observe)

    pool = _Pool(SimpleNamespace())
    assert await reconciliation._observe_revert_operation(
        pool, "operation", 1, service=Service()
    )
    assert fence_calls == [
        ((7, "feature"), {"exclude_operation_id": "operation"})
    ]
    assert observe_calls == [(pool, "operation", 1)]


@pytest.mark.asyncio
async def test_revert_observer_fails_closed_when_page_cursor_does_not_progress(
    monkeypatch,
):
    operation = SimpleNamespace(
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    step = SimpleNamespace(
        input_json={
            "lakefs_repo": "repo",
            "branch": "main",
            "base_head": "base-head",
        },
        checkpoint_json={
            "observation_head": "snapshot-head",
            "cursor": reconciliation._encode_observation_cursor("page-1", set()),
        },
        external_marker="marker",
    )
    checkpoint_calls = []

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, *_args, **_kwargs):
            return operation

        async def get_step(self, *_args, **_kwargs):
            return step

        async def update_observing_checkpoint(self, *_args, **kwargs):
            checkpoint_calls.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "new-head"}

        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "newer", "metadata": {}}],
                "pagination": {"has_more": True, "next_offset": "page-1"},
            }

        async def get_commit(self, **_kwargs):
            return {"metadata": {}}

    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())

    assert not await reconciliation._observe_revert_operation_unfenced(
        _Pool(SimpleNamespace()), "operation", 1
    )
    assert checkpoint_calls == []


@pytest.mark.asyncio
async def test_stale_prepared_cleanup_leaves_moved_branch_untouched(monkeypatch):
    current = SimpleNamespace(
        id="intent",
        state="prepared",
        ref="main",
        base_head="base-head",
        prepared_deadline_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        version=4,
    )
    reset_calls = []
    abandon_calls = []

    class Store:
        async def get_commit_intent(self, *_args, **_kwargs):
            return current

    class Service:
        store = Store()

        def repository_ref_fence(self, *_args, **_kwargs):
            class Fence:
                async def __aenter__(self):
                    return _TransactionalConnection()

                async def __aexit__(self, *_args):
                    return False

            return Fence()

        async def abandon_prepared_commit_intent_on_connection(self, *_args, **kwargs):
            abandon_calls.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "moved-head"}

    async def hard_reset(*args, **kwargs):
        reset_calls.append((args, kwargs))

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    monkeypatch.setattr(reconciliation.mutation_gateway, "hard_reset_branch", hard_reset)

    intent = SimpleNamespace(
        id="intent",
        repository_id=1,
        ref="main",
        payload_json={"lakefs_repo": "repo"},
    )

    assert not await reconciliation._reset_stale_prepared_intent(Service(), intent)
    assert reset_calls == []
    assert abandon_calls == []


@pytest.mark.asyncio
async def test_stale_prepared_cleanup_skips_intent_before_deadline(monkeypatch):
    current = SimpleNamespace(
        id="intent",
        state="prepared",
        ref="main",
        base_head="base-head",
        prepared_deadline_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        version=4,
    )

    class Store:
        async def get_commit_intent(self, *_args, **_kwargs):
            return current

    class Service:
        store = Store()

        def repository_ref_fence(self, *_args, **_kwargs):
            class Fence:
                async def __aenter__(self):
                    return _TransactionalConnection()

                async def __aexit__(self, *_args):
                    return False

            return Fence()

    class Client:
        async def get_branch(self, **_kwargs):
            raise AssertionError("a live prepared intent must not inspect LakeFS")

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())

    intent = SimpleNamespace(
        id="intent",
        repository_id=1,
        ref="main",
        payload_json={"lakefs_repo": "repo"},
    )

    assert not await reconciliation._reset_stale_prepared_intent(Service(), intent)


@pytest.mark.asyncio
async def test_reconcile_repairs_missing_job_with_delivery_lock_and_queueing_lock(
    monkeypatch,
):
    item = {
        "operation_id": "operation",
        "step_id": 12,
        "kind": "commit.postprocess.v1",
        "resource_key": "repository:main",
        "delivery_key": "operation:operation:step:12",
    }
    pool, app, _connection, task, Store, _Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        missing_jobs=[item],
    )

    assert await reconciliation.reconcile_once(pool, app) == 1
    assert task.configure_calls == [
        {
            "connection": _connection,
            "queue": "sync-v1",
            "priority": 10,
            "lock": "commit-postprocess:repository:main",
            "queueing_lock": "operation:operation:step:12",
        }
    ]
    assert task.defer_calls == [
        {"operation_id": "operation", "step_id": "12"}
    ]
    assert Store.instance.set_job_calls == [(12, 9001)]
    assert metrics["OPERATION_JOBS_REPAIRED"].inc_calls == [
        ({"kind": "commit.postprocess.v1"}, 1)
    ]


@pytest.mark.asyncio
async def test_reconcile_marks_non_replay_safe_stale_delivery_observing_after_observer_error(
    monkeypatch,
):
    stale_row = ("operation", "repository.revert.v1", 21, "marker")
    pool, app, _connection, _task, Store, _Service, _metrics = _install_reconcile_fakes(
        monkeypatch,
        stale_rows=[stale_row],
    )

    class Registry:
        def get(self, kind):
            assert kind == "repository.revert.v1"
            return SimpleNamespace(replay_safe_after_dispatch=False)

    async def observer_failure(*_args, **_kwargs):
        raise RuntimeError("remote observation failed")

    monkeypatch.setattr(reconciliation, "_observe_revert_operation", observer_failure)

    assert await reconciliation.reconcile_once(pool, app, registry=Registry()) == 0
    assert Store.instance.observing_calls == [
        (
            21,
            "operation",
            {
                "error_code": "stalled_delivery",
                "error_summary": "worker delivery heartbeat expired; awaiting observation",
            },
        )
    ]
    assert Store.instance.requeue_calls == []


@pytest.mark.asyncio
async def test_reconcile_requeues_replay_safe_stale_external_step(monkeypatch):
    stale_row = ("operation", "commit.postprocess.v1", 22, "marker")
    pool, app, _connection, _task, Store, _Service, _metrics = _install_reconcile_fakes(
        monkeypatch,
        stale_rows=[stale_row],
    )

    class Registry:
        def get(self, kind):
            assert kind == "commit.postprocess.v1"
            return SimpleNamespace(replay_safe_after_dispatch=True)

    monkeypatch.setattr(reconciliation, "operation_max_attempts", lambda: 6)

    assert await reconciliation.reconcile_once(pool, app, registry=Registry()) == 0
    assert Store.instance.requeue_calls == [
        (
            "external",
            22,
            "operation",
            {"max_attempts": 6},
        )
    ]
    assert Store.instance.observing_calls == []


@pytest.mark.asyncio
async def test_reconcile_requeues_stale_pre_dispatch_step_without_marker(monkeypatch):
    stale_row = ("operation", "maintenance.noop.v1", 23, None)
    pool, app, _connection, _task, Store, _Service, _metrics = _install_reconcile_fakes(
        monkeypatch,
        stale_rows=[stale_row],
    )

    class Registry:
        def get(self, kind):
            assert kind == "maintenance.noop.v1"
            return SimpleNamespace(replay_safe_after_dispatch=False)

    monkeypatch.setattr(reconciliation, "operation_max_attempts", lambda: 5)

    assert await reconciliation.reconcile_once(pool, app, registry=Registry()) == 0
    assert Store.instance.requeue_calls == [
        (
            "ordinary",
            23,
            "operation",
            {"max_attempts": 5},
        )
    ]
    assert Store.instance.observing_calls == []


@pytest.mark.asyncio
async def test_runtime_metrics_reset_gauges_and_publish_clamped_values(monkeypatch):
    metrics = {
        name: _Metric()
        for name in (
            "OPERATION_BACKLOG",
            "QUEUE_DEPTH",
            "QUEUE_OLDEST_AGE_SECONDS",
            "RECONCILIATION_BACKLOG",
            "OPERATION_RETENTION_BACKLOG",
        )
    }
    for name, metric in metrics.items():
        monkeypatch.setattr(reconciliation, name, metric)

    class Connection:
        def __init__(self):
            self.calls = 0

        async def execute(self, _query, _params=None):
            self.calls += 1
            return (
                _Result(many=[("control-v1", "todo", 3)])
                if self.calls == 1
                else _Result(many=[("control-v1", -4)])
                if self.calls == 2
                else _Result(many=[("running", 5)])
                if self.calls == 3
                else _Result(one=(2,))
                if self.calls == 4
                else _Result(one=(7,))
            )

    await reconciliation._observe_runtime_metrics(Connection(), retention_hours=24)

    assert ({"state": "accepted"}, 0) in metrics["OPERATION_BACKLOG"].set_calls
    assert ({"state": "running"}, 5.0) in metrics["OPERATION_BACKLOG"].set_calls
    assert ({"queue": "control-v1", "status": "todo"}, 3.0) in metrics[
        "QUEUE_DEPTH"
    ].set_calls
    assert ({"queue": "control-v1"}, 0.0) in metrics[
        "QUEUE_OLDEST_AGE_SECONDS"
    ].set_calls
    assert metrics["RECONCILIATION_BACKLOG"].set_calls == [(None, 2.0)]
    assert metrics["OPERATION_RETENTION_BACKLOG"].set_calls == [(None, 7.0)]


@pytest.mark.asyncio
async def test_reconcile_increments_retention_metrics_and_success_run(monkeypatch):
    pool, app, _connection, _task, _Store, _Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        retention=(2, 1),
        unlinked_jobs=3,
    )

    assert await reconciliation.reconcile_once(pool, app) == 0
    assert metrics["OPERATION_RETENTION_PRUNED"].inc_calls == [
        (None, 2),
        (None, 3),
    ]
    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "ok"}, 1)
    ]
    assert len(metrics["RECONCILIATION_DURATION"].observe_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_propagates_retention_error_and_records_error_run(monkeypatch):
    pool, app, _connection, _task, _Store, _Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        repair_error=RuntimeError("retention unavailable"),
    )

    with pytest.raises(RuntimeError, match="retention unavailable"):
        await reconciliation.reconcile_once(pool, app)

    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "error"}, 1)
    ]
    assert len(metrics["RECONCILIATION_DURATION"].observe_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_swallows_stale_cleanup_error_and_preserves_pass(monkeypatch):
    stale_intent = SimpleNamespace(id="stale-intent")
    pool, app, _connection, _task, _Store, Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        stale_prepared=[stale_intent],
    )

    async def cleanup_failure(*_args, **_kwargs):
        raise RuntimeError("LakeFS reset failed")

    monkeypatch.setattr(reconciliation, "_reset_stale_prepared_intent", cleanup_failure)

    assert await reconciliation.reconcile_once(pool, app) == 0
    assert Service.instance.sync_calls == []
    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "ok"}, 1)
    ]


@pytest.mark.asyncio
async def test_reconcile_propagates_missing_job_repair_error_and_records_error_run(
    monkeypatch,
):
    item = {
        "operation_id": "operation",
        "step_id": 24,
        "kind": "maintenance.noop.v1",
        "resource_key": "resource",
        "delivery_key": "operation:operation:step:24",
    }
    pool, app, _connection, task, Store, _Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        missing_jobs=[item],
    )

    async def fail_defer(**_kwargs):
        raise RuntimeError("queue unavailable")

    task.defer_async = fail_defer

    with pytest.raises(RuntimeError, match="queue unavailable"):
        await reconciliation.reconcile_once(pool, app)

    assert Store.instance.set_job_calls == []
    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "error"}, 1)
    ]
    assert len(metrics["RECONCILIATION_DURATION"].observe_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_returns_zero_when_advisory_lock_unavailable(monkeypatch):
    pool, app, connection, _task, _Store, _Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        advisory_lock=False,
        missing_jobs=[{"kind": "maintenance.noop.v1"}],
    )

    assert await reconciliation.reconcile_once(pool, app) == 0
    assert connection.execute_calls == [
        (
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
            ("khub-operation-reconciliation:v1",),
        )
    ]
    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "locked"}, 1)
    ]


@pytest.mark.asyncio
async def test_reconcile_rejects_non_positive_retention_batch(monkeypatch):
    pool, app, _connection, _task, _Store, _Service, _metrics = _install_reconcile_fakes(
        monkeypatch,
    )

    with pytest.raises(ValueError, match="retention_batch_size must be positive"):
        await reconciliation.reconcile_once(pool, app, retention_batch_size=0)


@pytest.mark.asyncio
async def test_reconcile_repairs_jobs_intents_stale_deliveries_and_retention(
    monkeypatch,
):
    missing_job = {
        "operation_id": "missing-operation",
        "step_id": 41,
        "kind": "maintenance.noop.v1",
        "resource_key": "repo:missing",
        "delivery_key": "operation:missing-operation:step:41",
    }
    intents = [
        SimpleNamespace(
            id="intent-no-effect",
            version=1,
            payload_json={"repository_id": 7},
            result_json=None,
        ),
        SimpleNamespace(
            id="intent-ambiguous",
            version=2,
            payload_json={"repository_id": 7},
            result_json=None,
        ),
        SimpleNamespace(
            id="intent-cursor",
            version=3,
            payload_json={"repository_id": 7},
            result_json=None,
        ),
        SimpleNamespace(
            id="intent-commit",
            version=4,
            payload_json={"repository_id": 7, "author_id": 3},
            result_json={"message": "accepted"},
        ),
    ]
    stale_prepared = [SimpleNamespace(id="stale-prepared")]
    stale_rows = [
        ("operation-observe", "commit.observe.v1", 51, "marker"),
        ("operation-replay", "commit.postprocess.v1", 52, "marker"),
        ("operation-ordinary", "maintenance.noop.v1", 53, None),
    ]
    pool, app, _connection, task, Store, Service, metrics = _install_reconcile_fakes(
        monkeypatch,
        missing_jobs=[missing_job],
        recoverable_intents=intents,
        stale_prepared=stale_prepared,
        stale_rows=stale_rows,
        retention=(2, 3),
        unlinked_jobs=4,
    )

    service_calls = []

    async def abandon(*args, **kwargs):
        service_calls.append(("abandon", args, kwargs))

    async def uncertain(*args, **kwargs):
        service_calls.append(("uncertain", args, kwargs))

    async def committed(*args, **kwargs):
        service_calls.append(("committed", args, kwargs))

    async def finalize(*args, **kwargs):
        service_calls.append(("finalize", args, kwargs))

    Service.abandon_commit_intent = abandon
    Service.mark_commit_intent_uncertain = uncertain
    Service.mark_commit_intent_committed = committed
    Service.finalize_commit_intent = finalize

    async def cleanup(*_args, **_kwargs):
        return True

    monkeypatch.setattr(reconciliation, "_reset_stale_prepared_intent", cleanup)
    async def ensure_head(_pool, intent, **_kwargs):
        return intent

    monkeypatch.setattr(reconciliation, "_ensure_observation_head", ensure_head)

    observations = iter(
        [
            (None, None, reconciliation.NO_EFFECT_CURSOR),
            (None, None, reconciliation.AMBIGUOUS_MARKER_CURSOR),
            (None, None, "cursor-page-2"),
            ("confirmed-commit", {"id": "confirmed-commit"}, None),
        ]
    )
    async def observe_commit(_intent):
        return next(observations)

    monkeypatch.setattr(reconciliation, "_observe_commit", observe_commit)

    repaired = await reconciliation.reconcile_once(pool, app)

    assert repaired == 5
    assert task.defer_calls == [
        {"operation_id": "missing-operation", "step_id": "41"}
    ]
    assert Store.instance.set_job_calls == [(41, 9001)]
    assert Store.instance.requeue_calls == [
        ("external", 52, "operation-replay", {"max_attempts": 3}),
        ("ordinary", 53, "operation-ordinary", {"max_attempts": 3}),
    ]
    assert [item[0] for item in service_calls] == [
        "abandon",
        "uncertain",
        "committed",
        "finalize",
    ]
    assert Service.instance.sync_calls == [
        "stale-prepared",
        "intent-no-effect",
        "intent-ambiguous",
        "intent-commit",
    ]
    assert Store.instance.cursor_calls == [
        {"expected_version": 3}
    ]
    assert len(Store.instance.observing_calls) == 1
    assert metrics["OPERATION_RETENTION_PRUNED"].inc_calls == [
        (None, 2),
        (None, 4),
    ]
    assert metrics["RECONCILIATION_RUNS"].inc_calls == [
        ({"result": "ok"}, 1)
    ]

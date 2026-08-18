"""Adversarial tests for bounded external observation decisions."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from kohakuhub.operations import reconciliation


@pytest.mark.asyncio
async def test_quiet_period_complete_history_proves_commit_no_effect(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "base-head", "metadata": {}}],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            return {"id": "base-head", "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        ref="main",
        base_head="base-head",
        marker="khub:v1:missing",
    )

    commit_id, commit, cursor = await reconciliation._observe_commit(intent)

    assert commit_id is None
    assert commit is None
    assert cursor == reconciliation.NO_EFFECT_CURSOR


@pytest.mark.asyncio
async def test_negative_observation_before_quiet_period_is_not_no_effect(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            raise AssertionError("remote history must not be queried early")

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) + timedelta(minutes=1),
        observation_cursor=None,
        ref="main",
        base_head="base-head",
        marker="khub:v1:missing",
    )

    commit_id, commit, cursor = await reconciliation._observe_commit(intent)

    assert (commit_id, commit, cursor) == (None, None, None)


@pytest.mark.asyncio
async def test_observation_head_is_not_frozen_before_quiet_period(monkeypatch):
    class Client:
        async def get_branch(self, **_kwargs):
            raise AssertionError("the branch head must not be frozen early")

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        observation_head=None,
        observe_not_before=datetime.now(timezone.utc) + timedelta(minutes=1),
        ref="main",
    )

    assert await reconciliation._ensure_observation_head(None, intent) is intent
    assert intent.observation_head is None


@pytest.mark.asyncio
async def test_history_without_base_head_is_not_affirmative_no_effect(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "newer", "metadata": {}}],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            return {"id": "newer", "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        ref="main",
        base_head="base-head",
        marker="khub:v1:missing",
    )

    assert await reconciliation._observe_commit(intent) == (None, None, None)


@pytest.mark.asyncio
async def test_last_page_detail_failure_is_not_affirmative_no_effect(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "base-head", "metadata": {}}],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            raise RuntimeError("detail endpoint unavailable")

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        ref="main",
        base_head="base-head",
        marker="khub:v1:missing",
    )

    assert await reconciliation._observe_commit(intent) == (None, None, None)


@pytest.mark.asyncio
async def test_non_progressing_cursor_is_not_affirmative_no_effect(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [{"id": "newer", "metadata": {}}],
                "pagination": {"has_more": True, "next_offset": "page-1"},
            }

        async def get_commit(self, **_kwargs):
            return {"id": "newer", "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor="page-1",
        ref="main",
        base_head="base-head",
        marker="khub:v1:missing",
    )

    assert await reconciliation._observe_commit(intent) == (None, None, None)


@pytest.mark.asyncio
async def test_multiple_marker_commits_are_not_attributed_to_one_intent(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [
                    {"id": "marker-a", "metadata": {"khub_marker": "same"}},
                    {"id": "marker-b", "metadata": {"khub_marker": "same"}},
                    {"id": "base-head", "metadata": {}},
                ],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **kwargs):
            return {"id": kwargs["commit_id"], "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        observation_head="snapshot-head",
        ref="main",
        base_head="base-head",
        marker="same",
    )

    assert await reconciliation._observe_commit(intent) == (
        None,
        None,
        reconciliation.AMBIGUOUS_MARKER_CURSOR,
    )


@pytest.mark.asyncio
async def test_multiple_markers_across_pages_remain_ambiguous(monkeypatch):
    calls = []

    class Client:
        async def log_commits(self, **kwargs):
            calls.append(kwargs["after"])
            if len(calls) == 1:
                return {
                    "results": [
                        {"id": "marker-a", "metadata": {"khub_marker": "same"}}
                    ],
                    "pagination": {"has_more": True, "next_offset": "page-2"},
                }
            return {
                "results": [
                    {"id": "marker-b", "metadata": {"khub_marker": "same"}},
                    {"id": "base-head", "metadata": {}},
                ],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **kwargs):
            return {"id": kwargs["commit_id"], "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        observation_head="snapshot-head",
        ref="main",
        base_head="base-head",
        marker="same",
    )

    _, _, cursor = await reconciliation._observe_commit(intent)
    assert cursor.startswith(reconciliation.OBSERVATION_CURSOR_PREFIX)
    intent.observation_cursor = cursor
    assert await reconciliation._observe_commit(intent) == (
        None,
        None,
        reconciliation.AMBIGUOUS_MARKER_CURSOR,
    )
    assert calls == [None, "page-2"]


@pytest.mark.asyncio
async def test_observer_reads_only_from_immutable_observation_head(monkeypatch):
    seen_refs = []

    class Client:
        async def log_commits(self, **kwargs):
            seen_refs.append(kwargs["ref"])
            return {
                "results": [{"id": "snapshot-head", "metadata": {}}],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **_kwargs):
            return {"id": "snapshot-head", "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_head="snapshot-head",
        observation_cursor=None,
        ref="main",
        base_head="snapshot-head",
        marker="khub:v1:missing",
    )

    assert await reconciliation._observe_commit(intent) == (
        None,
        None,
        reconciliation.NO_EFFECT_CURSOR,
    )
    assert seen_refs == ["snapshot-head"]


@pytest.mark.asyncio
async def test_revert_observer_persists_cursor_and_immutable_head(monkeypatch):
    """A long revert history resumes from a durable bounded page."""

    calls = []
    saved_checkpoints = []

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "snapshot-head" if not calls else "new-head"}

        async def log_commits(self, **kwargs):
            calls.append((kwargs["ref"], kwargs.get("after")))
            if len(calls) == 1:
                return {
                    "results": [{"id": "newer", "metadata": {}}],
                    "pagination": {"has_more": True, "next_offset": "page-2"},
                }
            return {
                "results": [
                    {"id": "marker", "metadata": {"khub_marker": "marker"}},
                    {"id": "base-head", "metadata": {}},
                ],
                # The base head is inside this page; older history is outside
                # the operation's immutable observation range.
                "pagination": {"has_more": True, "next_offset": "page-3"},
            }

        async def get_commit(self, **kwargs):
            return {"id": kwargs["commit_id"], "metadata": {}}

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

    class Pool:
        def connection(self):
            class Context:
                async def __aenter__(self):
                    return Connection()

                async def __aexit__(self, *_args):
                    return False

            return Context()

    class Store:
        def __init__(self, _pool):
            pass

        async def get_operation(self, _connection, _operation_id, **_kwargs):
            return SimpleNamespace(
                observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
                repository_id=1,
                state="dispatch_started",
            )

        async def get_step(self, _connection, _step_id, **_kwargs):
            return SimpleNamespace(
                input_json={
                    "lakefs_repo": "repo",
                    "branch": "main",
                    "base_head": "base-head",
                },
                checkpoint_json=saved_checkpoints[-1] if saved_checkpoints else {},
                external_marker="marker",
            )

        async def update_observing_checkpoint(
            self, _connection, _step_id, _operation_id, **kwargs
        ):
            saved_checkpoints.append(kwargs["checkpoint"])
            return True

        async def finish_step(self, *_args, **_kwargs):
            return True

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    async def finalize(*_args, **_kwargs):
        return {"success": True}

    monkeypatch.setattr(reconciliation, "finalize_revert_commit", finalize)

    pool = Pool()
    assert await reconciliation._observe_revert_operation_unfenced(pool, "op", 1)
    assert saved_checkpoints[0]["observation_head"] == "snapshot-head"
    assert saved_checkpoints[0]["cursor"].startswith(
        reconciliation.OBSERVATION_CURSOR_PREFIX
    )

    assert await reconciliation._observe_revert_operation_unfenced(pool, "op", 1)
    assert calls == [("snapshot-head", None), ("snapshot-head", "page-2")]


def test_observation_cursor_round_trip_and_malformed_values_are_fail_closed():
    encoded = reconciliation._encode_observation_cursor(
        "page-2", {"commit-b", "commit-a"}
    )

    assert reconciliation._decode_observation_cursor(encoded) == (
        "page-2",
        {"commit-a", "commit-b"},
    )
    assert reconciliation._decode_observation_cursor("not-a-cursor") == (
        "not-a-cursor",
        set(),
    )
    assert reconciliation._decode_observation_cursor(
        reconciliation.OBSERVATION_CURSOR_PREFIX + "{}"
    ) == (None, set())
    assert reconciliation._decode_observation_cursor(
        reconciliation.OBSERVATION_CURSOR_PREFIX + "not-json"
    ) == (None, set())


@pytest.mark.asyncio
async def test_stalled_delivery_recovery_is_bounded_and_tolerates_races():
    retried = []

    class Manager:
        async def get_stalled_jobs(self, **kwargs):
            assert kwargs["seconds_since_heartbeat"] == 10
            return [SimpleNamespace(id=1), SimpleNamespace(id=2), SimpleNamespace(id=3)]

        async def retry_job_by_id_async(self, job_id, **_kwargs):
            if job_id == 2:
                raise RuntimeError("another reconciler won")
            retried.append(job_id)

    app = SimpleNamespace(job_manager=Manager())

    assert await reconciliation._recover_stalled_deliveries(
        app, seconds_since_heartbeat=10, limit=2
    ) == {1}
    assert retried == [1]


@pytest.mark.asyncio
async def test_stalled_delivery_recovery_is_noop_without_procrastinate_manager():
    assert await reconciliation._recover_stalled_deliveries(
        SimpleNamespace(), seconds_since_heartbeat=10, limit=10
    ) == set()


@pytest.mark.asyncio
async def test_runtime_metrics_reset_known_gauges_and_publish_rows(monkeypatch):
    class Result:
        def __init__(self, *, many=(), one=None):
            self.many = list(many)
            self.one = one

        async def fetchall(self):
            return self.many

        async def fetchone(self):
            return self.one

    class Connection:
        async def execute(self, query, _params=None):
            query = " ".join(str(query).split())
            if query.startswith("SELECT queue_name, status, count"):
                return Result(many=[("control-v1", "todo", 2)])
            if query.startswith("SELECT j.queue_name"):
                return Result(many=[("control-v1", 3.5)])
            if query.startswith("SELECT state, count"):
                return Result(many=[("running", 4)])
            if "state IN ('uncertain', 'dispatch_started')" in query:
                return Result(one=(1,))
            return Result(one=(2,))

    await reconciliation._observe_runtime_metrics(Connection(), retention_hours=168)


@pytest.mark.asyncio
async def test_observation_head_freezes_one_remote_head_after_quiet_period(monkeypatch):
    calls = []

    class Client:
        async def get_branch(self, **kwargs):
            calls.append(kwargs)
            return {"commit_id": "snapshot-head"}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        observation_head=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        ref="main",
        version=None,
    )

    assert await reconciliation._ensure_observation_head(None, intent) is intent
    assert calls == [{"repository": "repo", "branch": "main"}]


@pytest.mark.asyncio
async def test_observe_commit_accepts_marker_found_in_commit_detail(monkeypatch):
    calls = []

    class Client:
        async def log_commits(self, **kwargs):
            calls.append(("log", kwargs))
            return {
                "results": [{"id": "marker-commit", "metadata": {}}],
                "pagination": {"has_more": True, "next_offset": "next"},
            }

        async def get_commit(self, **kwargs):
            calls.append(("detail", kwargs))
            return {"id": kwargs["commit_id"], "metadata": {"khub_marker": "marker"}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        observation_head="snapshot-head",
        ref="main",
        base_head="base-head",
        marker="marker",
    )

    commit_id, commit, cursor = await reconciliation._observe_commit(intent)

    assert commit_id is None
    assert commit is None
    assert cursor.startswith(reconciliation.OBSERVATION_CURSOR_PREFIX)
    assert "marker-commit" in cursor
    assert calls[0][0] == "log"
    assert calls[1][0] == "detail"


@pytest.mark.asyncio
async def test_observe_commit_resolves_marker_when_base_is_reached(monkeypatch):
    class Client:
        async def log_commits(self, **_kwargs):
            return {
                "results": [
                    {"id": "marker-commit", "metadata": {"khub_marker": "marker"}},
                    {"id": "base-head", "metadata": {}},
                ],
                "pagination": {"has_more": False},
            }

        async def get_commit(self, **kwargs):
            return {"id": kwargs["commit_id"], "metadata": {}}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id=None,
        observe_not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
        observation_cursor=None,
        observation_head="snapshot-head",
        ref="main",
        base_head="base-head",
        marker="marker",
    )

    commit_id, commit, cursor = await reconciliation._observe_commit(intent)

    assert commit_id == "marker-commit"
    assert commit["id"] == "marker-commit"
    assert cursor is None


@pytest.mark.asyncio
async def test_observe_commit_uses_confirmed_commit_id_without_scanning_history(monkeypatch):
    class Client:
        async def get_commit(self, **kwargs):
            return {"id": kwargs["commit_id"], "metadata": {"khub_marker": "marker"}}

        async def log_commits(self, **_kwargs):
            raise AssertionError("confirmed commit must not scan history")

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    intent = SimpleNamespace(
        payload_json={"lakefs_repo": "repo"},
        lakefs_commit_id="confirmed",
        observe_not_before=None,
        marker="marker",
        ref="main",
        base_head="base",
    )

    assert await reconciliation._observe_commit(intent) == (
        "confirmed",
        {"id": "confirmed", "metadata": {"khub_marker": "marker"}},
        None,
    )


@pytest.mark.asyncio
async def test_stale_prepared_cleanup_resets_only_unchanged_branch(monkeypatch):
    reset_calls = []
    abandon_calls = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

    current = SimpleNamespace(
        id="intent",
        state="prepared",
        ref="main",
        base_head="base",
        prepared_deadline_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        version=3,
    )

    class Store:
        async def get_commit_intent(self, *_args, **_kwargs):
            return current

    class Service:
        store = Store()

        @staticmethod
        def repository_ref_fence(*_args, **_kwargs):
            class Fence:
                async def __aenter__(self):
                    return Connection()

                async def __aexit__(self, *_args):
                    return False

            return Fence()

        async def abandon_prepared_commit_intent_on_connection(self, *_args, **kwargs):
            abandon_calls.append(kwargs)
            return True

    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "base"}

    monkeypatch.setattr(reconciliation, "get_lakefs_client", lambda: Client())
    async def hard_reset(*args, **kwargs):
        reset_calls.append((args, kwargs))

    monkeypatch.setattr(reconciliation.mutation_gateway, "hard_reset_branch", hard_reset)
    intent = SimpleNamespace(
        id="intent",
        repository_id=1,
        ref="main",
        payload_json={"lakefs_repo": "repo"},
    )

    assert await reconciliation._reset_stale_prepared_intent(Service(), intent)
    assert reset_calls[0][1]["ref"] == "base"
    assert abandon_calls == [
        {"expected_version": 3, "error_code": "stale_prepared_cleanup"}
    ]

    current.state = "dispatch_started"
    assert not await reconciliation._reset_stale_prepared_intent(Service(), intent)


@pytest.mark.asyncio
async def test_reconcile_once_returns_when_another_reconciler_holds_lock(monkeypatch):
    class Result:
        async def fetchone(self):
            return (False,)

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

        async def execute(self, *_args, **_kwargs):
            return Result()

    class Pool:
        def connection(self):
            class Context:
                async def __aenter__(self):
                    return Connection()

                async def __aexit__(self, *_args):
                    return False

            return Context()

    class Service:
        def __init__(self, *_args, **_kwargs):
            pass

    class Store:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(reconciliation, "OperationService", Service)
    monkeypatch.setattr(reconciliation, "OperationStore", Store)
    async def recover(*_args, **_kwargs):
        return set()

    monkeypatch.setattr(reconciliation, "_recover_stalled_deliveries", recover)

    assert await reconciliation.reconcile_once(
        Pool(), SimpleNamespace(khub_stalled_worker_timeout_seconds=10)
    ) == 0

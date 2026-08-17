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

"""Unit coverage for registered operation handlers and recovery boundaries."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from kohakuhub.operations import handlers
from kohakuhub.operations.types import RetryableOperationError


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


@pytest.mark.asyncio
async def test_postprocess_tracks_changed_lfs_and_defers_destructive_gc(monkeypatch):
    usage_calls = []
    tracked = []
    gc_calls = []

    def read_usage(*args):
        usage_calls.append(args)
        return {
            "repository_used_bytes": 10,
            "namespace_used_bytes": 20,
            "namespace_total_used_bytes": 30,
        }

    def track(**kwargs):
        tracked.append(kwargs)

    def run_gc(**kwargs):
        gc_calls.append(kwargs)

    monkeypatch.setattr(handlers, "_read_incremental_usage", read_usage)
    monkeypatch.setattr(handlers, "track_lfs_object", track)
    monkeypatch.setattr(handlers, "run_gc_for_file", run_gc)
    monkeypatch.setattr(handlers.cfg.app, "lfs_auto_gc", True)

    result = await handlers.perform_commit_postprocess(
        _postprocess_payload(
            lfs_tracking=[
                {
                    "path": "new.bin",
                    "sha256": "new-sha",
                    "size": 11,
                },
                {
                    "path": "replaced.bin",
                    "sha256": "new-sha-2",
                    "size": 12,
                    "old_sha256": "old-sha",
                },
            ]
        )
    )

    assert result == {
        "tracked_lfs": 2,
        "gc_deferred": 1,
        "repository_used_bytes": 10,
        "namespace_used_bytes": 20,
        "namespace_total_used_bytes": 30,
    }
    assert len(usage_calls) == 2
    assert len(tracked) == 2
    assert gc_calls == []


@pytest.mark.asyncio
async def test_postprocess_legacy_gc_is_explicitly_opt_in(monkeypatch):
    gc_calls = []
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
    monkeypatch.setattr(
        handlers,
        "run_gc_for_file",
        lambda **kwargs: gc_calls.append(kwargs),
    )
    monkeypatch.setattr(handlers.cfg.app, "lfs_auto_gc", True)

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

    assert len(gc_calls) == 1
    assert gc_calls[0]["path_in_repo"] == "replaced.bin"


@pytest.mark.asyncio
async def test_postprocess_dependency_failure_is_retryable(monkeypatch):
    monkeypatch.setattr(
        handlers,
        "_read_incremental_usage",
        lambda *_args: {
            "repository_used_bytes": 0,
            "namespace_used_bytes": 0,
            "namespace_total_used_bytes": 0,
        },
    )

    def fail(**_kwargs):
        raise ConnectionError("storage is unavailable")

    monkeypatch.setattr(handlers, "track_lfs_object", fail)

    with pytest.raises(RetryableOperationError) as exc_info:
        await handlers.perform_commit_postprocess(
            _postprocess_payload(
                lfs_tracking=[{"path": "file", "sha256": "sha", "size": 1}]
            )
        )

    assert exc_info.value.error_code == "postprocess_dependency_unavailable"
    assert "temporarily unavailable" in exc_info.value.error_summary


@pytest.mark.asyncio
async def test_postprocess_requires_current_repository_and_namespace(monkeypatch):
    class FakeRepository:
        private = False
        used_bytes = 0

        @classmethod
        def get_by_id(cls, _repository_id):
            return None

    monkeypatch.setattr(handlers, "Repository", FakeRepository)

    with pytest.raises(ValueError, match="repository no longer exists"):
        await handlers.perform_commit_postprocess(_postprocess_payload())


def test_read_incremental_usage_selects_the_namespace_counter(monkeypatch):
    class RepositoryRecord:
        private = True
        used_bytes = 11

        @classmethod
        def get_by_id(cls, _repository_id):
            return cls()

    monkeypatch.setattr(handlers, "Repository", RepositoryRecord)
    monkeypatch.setattr(
        handlers,
        "get_organization",
        lambda _namespace: SimpleNamespace(private_used_bytes=2, public_used_bytes=3),
    )

    assert handlers._read_incremental_usage(7, "org", True) == {
        "repository_used_bytes": 11,
        "namespace_used_bytes": 2,
        "namespace_total_used_bytes": 5,
    }


@pytest.mark.asyncio
async def test_commit_postprocess_handler_returns_confirmed_step_result(monkeypatch):
    payload = {"tracked_lfs": 1, "gc_deferred": 0}

    async def perform(_payload):
        return payload

    monkeypatch.setattr(
        handlers,
        "perform_commit_postprocess",
        perform,
    )

    result = await handlers.commit_postprocess_handler(
        SimpleNamespace(), SimpleNamespace(input_json={})
    )

    assert result.state == "succeeded"
    assert result.result_json == payload
    assert result.external_effect_confirmed is True


@pytest.mark.asyncio
async def test_revert_operation_handler_supplies_marker_and_confirms_result(monkeypatch):
    calls = []

    async def perform(payload, *, marker):
        calls.append((payload, marker))
        return {"success": True, "new_commit_id": "commit"}

    monkeypatch.setattr(handlers, "perform_revert_operation", perform)
    operation = SimpleNamespace(id="operation-id")
    step = SimpleNamespace(id=9, external_marker=None, input_json={"ref": "target"})

    result = await handlers.revert_operation_handler(operation, step)

    assert calls == [
        ({"ref": "target"}, "khub:operation:v1:operation-id:step:9")
    ]
    assert result.state == "succeeded"
    assert result.result_json["new_commit_id"] == "commit"
    assert result.external_effect_confirmed is True


@pytest.mark.asyncio
async def test_finalize_revert_is_idempotent_when_local_commit_exists(monkeypatch):
    repo = object()
    author = object()
    create_calls = []
    track_calls = []

    monkeypatch.setattr(handlers, "get_repository", lambda *_args: repo)
    monkeypatch.setattr(handlers.User, "get_by_id", lambda _user_id: author)
    monkeypatch.setattr(handlers, "get_commit", lambda *_args: object())
    monkeypatch.setattr(
        handlers,
        "create_commit",
        lambda **kwargs: create_calls.append(kwargs),
    )

    async def track(**kwargs):
        track_calls.append(kwargs)

    monkeypatch.setattr(handlers, "track_commit_lfs_objects", track)

    result = await handlers.finalize_revert_commit(
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
        commit_id="already-recorded",
    )

    assert result["new_commit_id"] == "already-recorded"
    assert create_calls == []
    assert track_calls == []


@pytest.mark.asyncio
async def test_finalize_revert_records_confirmed_remote_commit_once(monkeypatch):
    repo = object()
    author = object()
    create_calls = []
    track_calls = []

    monkeypatch.setattr(handlers, "get_repository", lambda *_args: repo)
    monkeypatch.setattr(handlers.User, "get_by_id", lambda _user_id: author)
    monkeypatch.setattr(handlers, "get_commit", lambda *_args: None)
    monkeypatch.setattr(
        handlers,
        "create_commit",
        lambda **kwargs: create_calls.append(kwargs),
    )

    async def track(**kwargs):
        track_calls.append(kwargs)

    monkeypatch.setattr(handlers, "track_commit_lfs_objects", track)

    result = await handlers.finalize_revert_commit(
        {
            "lakefs_repo": "lakefs-repo",
            "branch": "main",
            "ref": "old-commit",
            "repo_type": "model",
            "namespace": "owner",
            "name": "repo",
            "author_id": 1,
            "username": "owner",
            "message": "revert message",
        },
        commit_id="confirmed-commit",
    )

    assert result["success"] is True
    assert track_calls[0]["commit_id"] == "confirmed-commit"
    assert create_calls[0]["commit_id"] == "confirmed-commit"
    assert create_calls[0]["message"] == "revert message"


@pytest.mark.asyncio
async def test_revert_handler_rejects_moved_head_before_mutation(monkeypatch):
    class Client:
        async def get_branch(self, **_kwargs):
            return {"commit_id": "new-head"}

    monkeypatch.setattr(handlers, "get_lakefs_client", lambda: Client())

    with pytest.raises(ValueError, match="head changed"):
        await handlers.perform_revert_operation(
            {
                "lakefs_repo": "lakefs-repo",
                "branch": "main",
                "base_head": "old-head",
                "ref": "target",
            },
            marker="marker-1",
        )


@pytest.mark.asyncio
async def test_revert_handler_marks_remote_call_and_finalizes_observed_head(monkeypatch):
    calls = []

    class Client:
        def __init__(self):
            self.heads = iter(({"commit_id": "base"}, {"commit_id": "result"}))

        async def get_branch(self, **_kwargs):
            return next(self.heads)

    client = Client()
    monkeypatch.setattr(handlers, "get_lakefs_client", lambda: client)

    async def revert(_client, **kwargs):
        calls.append(kwargs)

    async def finalize(payload, *, commit_id):
        return {"success": True, "new_commit_id": commit_id, "payload": payload}

    monkeypatch.setattr(handlers.mutation_gateway, "revert_branch", revert)
    monkeypatch.setattr(handlers, "finalize_revert_commit", finalize)

    result = await handlers.perform_revert_operation(
        {
            "lakefs_repo": "lakefs-repo",
            "branch": "main",
            "base_head": "base",
            "ref": "target",
            "metadata": {"source": "test"},
        },
        marker="marker-1",
    )

    assert result["new_commit_id"] == "result"
    assert calls[0]["metadata"]["khub_marker"] == "marker-1"

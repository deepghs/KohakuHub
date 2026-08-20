"""Focused regression tests for high-value commit-operation branches."""

from __future__ import annotations

import base64
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException

import kohakuhub.api.commit.routers.operations as commit_ops


class _Expr:
    def __init__(self, value):
        self.value = value

    def __and__(self, other):
        return _Expr(("and", self.value, getattr(other, "value", other)))


class _Field:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other):
        return _Expr((self.name, "==", other))

    def startswith(self, other):
        return _Expr((self.name, "startswith", other))

    def __hash__(self):
        return hash(self.name)


class _Query:
    def __init__(self, rows=(), execute_result=1):
        self.rows = list(rows)
        self.execute_result = execute_result
        self.where_calls = []

    def where(self, *args):
        self.where_calls.append(args)
        return self

    def on_conflict(self, **kwargs):
        return self

    def tuples(self):
        return iter(self.rows)

    def execute(self):
        return self.execute_result


class _FakeFileModel:
    repository = _Field("repository")
    path_in_repo = _Field("path_in_repo")
    sha256 = _Field("sha256")
    size = _Field("size")
    lfs = _Field("lfs")
    is_deleted = _Field("is_deleted")
    updated_at = _Field("updated_at")
    owner = _Field("owner")
    id = _Field("id")

    get_or_none_result = None
    select_query = _Query()
    insert_calls = []
    update_query = _Query()

    @classmethod
    def reset(cls):
        cls.get_or_none_result = None
        cls.select_query = _Query()
        cls.insert_calls = []
        cls.update_query = _Query()

    @classmethod
    def get_or_none(cls, *args):
        return cls.get_or_none_result

    @classmethod
    def select(cls, *args):
        return cls.select_query

    @classmethod
    def insert(cls, **kwargs):
        cls.insert_calls.append(kwargs)
        return _Query()

    @classmethod
    def update(cls, **kwargs):
        cls.update_kwargs = kwargs
        return cls.update_query


class _FakeHistory:
    repository = _Field("repository")
    sha256 = _Field("sha256")
    get_or_none_result = None

    @classmethod
    def get_or_none(cls, *args):
        return cls.get_or_none_result


class _FakeLakeFSClient:
    def __init__(self):
        self.calls = []
        self.branch_data = {"commit_id": "base-head"}
        self.commit_data = {"id": "commit-created"}
        self.stat_data = {
            "physical_address": "s3://bucket/source",
            "checksum": "sha256:source",
            "size_bytes": 12,
        }
        self.list_responses = []
        self.branch_responses = []
        self.raise_on = {}

    def _maybe_raise(self, name):
        error = self.raise_on.get(name)
        if error:
            raise error

    async def stat_object(self, **kwargs):
        self.calls.append(("stat_object", kwargs))
        self._maybe_raise("stat_object")
        return self.stat_data

    async def upload_object(self, **kwargs):
        self.calls.append(("upload_object", kwargs))
        self._maybe_raise("upload_object")
        return {"ok": True}

    async def link_physical_address(self, **kwargs):
        self.calls.append(("link_physical_address", kwargs))
        self._maybe_raise("link_physical_address")
        return {"ok": True}

    async def delete_object(self, **kwargs):
        self.calls.append(("delete_object", kwargs))
        self._maybe_raise("delete_object")
        return {"ok": True}

    async def list_objects(self, **kwargs):
        self.calls.append(("list_objects", kwargs))
        self._maybe_raise("list_objects")
        if self.list_responses:
            return self.list_responses.pop(0)
        return {"results": []}

    async def get_branch(self, **kwargs):
        self.calls.append(("get_branch", kwargs))
        self._maybe_raise("get_branch")
        if self.branch_responses:
            response = self.branch_responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return self.branch_data

    async def commit(self, **kwargs):
        self.calls.append(("commit", kwargs))
        self._maybe_raise("commit")
        return self.commit_data


class _FakeRequest:
    def __init__(self, body: bytes, *, headers=None, app=None, query_params=None):
        self._body = body
        self.headers = headers or {}
        self.query_params = query_params or {}
        if app is not None:
            self.app = app

    async def body(self):
        return self._body


class _FakeOperationService:
    def __init__(self, *, intent=None, prepare_error=None):
        self.intent = intent
        self.existing_intent = None
        self.prepare_error = prepare_error
        self.calls = []
        self.observation = SimpleNamespace(id="observation-1")
        self.finalize_error = None

    async def get_commit_intent(self, intent_id):
        self.calls.append(("get_commit_intent", intent_id))
        return self.existing_intent

    async def ensure_commit_observation_operation(self, intent_id):
        self.calls.append(("ensure_commit_observation_operation", intent_id))
        return self.observation

    async def prepare_commit_intent(self, **kwargs):
        self.calls.append(("prepare_commit_intent", kwargs))
        if self.prepare_error:
            raise self.prepare_error
        return self.intent

    async def record_prepared_staging_path(self, *args, **kwargs):
        self.calls.append(("record_prepared_staging_path", args, kwargs))

    async def update_prepared_commit_payload(self, *args, **kwargs):
        self.calls.append(("update_prepared_commit_payload", args, kwargs))
        return self.intent

    async def mark_commit_intent_dispatch_started(self, *args, **kwargs):
        self.calls.append(("mark_commit_intent_dispatch_started", args, kwargs))

    async def mark_commit_intent_committed(self, *args, **kwargs):
        self.calls.append(("mark_commit_intent_committed", args, kwargs))

    async def finalize_commit_intent(self, *args, **kwargs):
        self.calls.append(("finalize_commit_intent", args, kwargs))
        if self.finalize_error:
            raise self.finalize_error

    async def abandon_commit_intent(self, *args, **kwargs):
        self.calls.append(("abandon_commit_intent", args, kwargs))


class _BatchFakeOperationService(_FakeOperationService):
    async def record_prepared_staging_paths(self, *args, **kwargs):
        self.calls.append(("record_prepared_staging_paths", args, kwargs))


def _intent(**overrides):
    values = {
        "id": "intent-1",
        "marker": "khub:v1:intent-1",
        "payload_hash": "payload-hash",
        "request_hash": "request-hash",
        "payload_json": {"quota_delta": 0, "file_mutations": []},
        "state": "prepared",
        "result_json": None,
        "lakefs_commit_id": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _commit_body(*operations, summary="commit") -> bytes:
    lines = [{"key": "header", "value": {"summary": summary}}]
    lines.extend(operations)
    return "\n".join(json.dumps(line) for line in lines).encode()


def _request_hash(repo_type, repository_id, revision, operations, summary="commit"):
    return hashlib.sha256(
        json.dumps(
            {
                "repo_type": repo_type,
                "repository_id": repository_id,
                "ref": revision,
                "header": {"summary": summary},
                "operations": list(operations),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def _file_operation(path="README.md"):
    return {
        "key": "file",
        "value": {
            "path": path,
            "content": base64.b64encode(b"hello").decode(),
            "encoding": "base64",
        },
    }


def _commit_dependencies(monkeypatch, client, repo):
    monkeypatch.setattr(commit_ops.Repository, "get_or_none", lambda *args: repo)
    monkeypatch.setattr(commit_ops, "check_repo_write_permission", lambda *args: None)
    monkeypatch.setattr(
        commit_ops, "resolve_lakefs_repo", lambda _repo: "model:owner/repo"
    )
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)
    monkeypatch.setattr(commit_ops, "get_organization", lambda _namespace: None)


@pytest.fixture(autouse=True)
def _reset_fakes(monkeypatch):
    _FakeFileModel.reset()
    _FakeHistory.get_or_none_result = None
    monkeypatch.setenv("KOHAKU_HUB_TEST_COMPATIBILITY", "true")


@pytest.mark.asyncio
async def test_estimate_quota_delta_counts_missing_copy_as_regular_storage(monkeypatch):
    repo = SimpleNamespace()
    client = _FakeLakeFSClient()
    monkeypatch.setattr(commit_ops, "get_file", lambda *_args: None)
    monkeypatch.setattr(commit_ops, "should_use_lfs", lambda *_args: False)

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {"key": "copyFile", "value": {"path": "copy.bin", "srcPath": "src.bin"}},
        ],
        client,
        "lakefs-repo",
    )

    assert delta == 12
    assert client.calls == [
        (
            "stat_object",
            {
                "repository": "lakefs-repo",
                "ref": "main",
                "path": "src.bin",
            },
        )
    ]


@pytest.mark.asyncio
async def test_estimate_quota_delta_counts_missing_copy_as_new_lfs_history(monkeypatch):
    repo = SimpleNamespace()
    client = _FakeLakeFSClient()
    monkeypatch.setattr(commit_ops, "get_file", lambda *_args: None)
    monkeypatch.setattr(commit_ops, "should_use_lfs", lambda *_args: True)
    monkeypatch.setattr(commit_ops, "LFSObjectHistory", _FakeHistory)
    monkeypatch.setattr(
        commit_ops, "get_object_metadata", lambda *_args: _async_return({"size": 21})
    )
    monkeypatch.setattr(commit_ops, "LFSObjectHistory", _FakeHistory)

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "copyFile",
                "value": {
                    "path": "copy.bin",
                    "srcPath": "src.bin",
                    "srcRevision": "feature",
                },
            },
        ],
        client,
        "lakefs-repo",
    )

    assert delta == 12
    assert client.calls[0][1]["ref"] == "feature"


@pytest.mark.asyncio
async def test_estimate_quota_delta_does_not_charge_copy_for_existing_lfs_history(
    monkeypatch,
):
    repo = SimpleNamespace()
    client = _FakeLakeFSClient()
    monkeypatch.setattr(
        commit_ops,
        "get_file",
        lambda _repo, path: SimpleNamespace(
            path_in_repo=path, size=21, sha256="sha256:source", lfs=True, is_deleted=False
        ),
    )
    monkeypatch.setattr(commit_ops, "LFSObjectHistory", _FakeHistory)
    _FakeHistory.get_or_none_result = SimpleNamespace(id=1)

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "copyFile",
                "value": {"path": "copy.bin", "srcPath": "src.bin"},
            },
        ],
        client,
        "lakefs-repo",
    )

    assert delta == 0
    assert not client.calls


@pytest.mark.asyncio
async def test_estimate_quota_delta_counts_regular_copy_from_metadata_mirror(monkeypatch):
    repo = SimpleNamespace()
    client = _FakeLakeFSClient()
    monkeypatch.setattr(
        commit_ops,
        "get_file",
        lambda _repo, path: (
            SimpleNamespace(
                path_in_repo=path,
                size=9,
                sha256="sha1",
                lfs=False,
                is_deleted=False,
            )
            if path == "src.bin"
            else None
        ),
    )

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "copyFile",
                "value": {"path": "copy.bin", "srcPath": "src.bin"},
            },
        ],
        client,
        "lakefs-repo",
    )

    assert delta == 9
    assert not client.calls


@pytest.mark.asyncio
async def test_estimate_quota_delta_subtracts_only_live_non_lfs_folder_files(monkeypatch):
    repo = SimpleNamespace()
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    _FakeFileModel.select_query = _Query(rows=[(10, False), (7, True), (5, False)])

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [{"key": "deletedFolder", "value": {"path": "folder/"}}],
        _FakeLakeFSClient(),
        "lakefs-repo",
    )

    assert delta == -15
    assert _FakeFileModel.select_query.where_calls


@pytest.mark.asyncio
async def test_estimate_quota_delta_uses_s3_size_and_history_for_new_lfs_file(
    monkeypatch,
):
    repo = SimpleNamespace()
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    monkeypatch.setattr(commit_ops, "LFSObjectHistory", _FakeHistory)
    monkeypatch.setattr(
        commit_ops, "get_object_metadata", lambda *_args: _async_return({"size": 31})
    )
    monkeypatch.setattr(commit_ops, "get_file", lambda *_args: None)

    delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "lfsFile",
                "value": {"path": "weights.bin", "oid": "abcdef", "size": 1},
            },
        ],
        _FakeLakeFSClient(),
        "lakefs-repo",
    )

    assert delta == 31


@pytest.mark.asyncio
async def test_estimate_quota_delta_skips_unchanged_lfs_file_and_historical_object(
    monkeypatch,
):
    repo = SimpleNamespace()
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    monkeypatch.setattr(commit_ops, "LFSObjectHistory", _FakeHistory)
    monkeypatch.setattr(
        commit_ops,
        "get_file",
        lambda *_args: SimpleNamespace(
            size=31, sha256="abcdef", lfs=True, is_deleted=False
        ),
    )

    unchanged_delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "lfsFile",
                "value": {"path": "weights.bin", "oid": "abcdef", "size": 31},
            },
        ],
        _FakeLakeFSClient(),
        "lakefs-repo",
    )
    assert unchanged_delta == 0

    monkeypatch.setattr(commit_ops, "get_file", lambda *_args: None)
    monkeypatch.setattr(
        commit_ops, "get_object_metadata", lambda *_args: _async_return({"size": 31})
    )
    _FakeHistory.get_or_none_result = SimpleNamespace(id=2)
    historical_delta = await commit_ops._estimate_commit_quota_delta(
        repo,
        [
            {
                "key": "lfsFile",
                "value": {"path": "other.bin", "oid": "abcdef", "size": 31},
            },
        ],
        _FakeLakeFSClient(),
        "lakefs-repo",
    )
    assert historical_delta == 0


@pytest.mark.asyncio
async def test_process_regular_file_restores_deleted_file_with_same_content(monkeypatch):
    repo = SimpleNamespace(owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    content = b"hello"
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)
    monkeypatch.setattr(commit_ops, "get_effective_lfs_threshold", lambda *_args: 10)
    monkeypatch.setattr(commit_ops, "should_use_lfs", lambda *_args: False)
    monkeypatch.setattr(
        commit_ops,
        "get_file",
        lambda *_args: SimpleNamespace(
            sha256=commit_ops.calculate_git_blob_sha1(content),
            size=len(content),
            is_deleted=True,
        ),
    )
    mutations = []

    changed = await commit_ops.process_regular_file(
        "README.md",
        base64.b64encode(content).decode(),
        "base64",
        repo,
        "lakefs-repo",
        "main",
        file_mutations=mutations,
    )

    assert changed is True
    assert client.calls[-1][0] == "upload_object"
    assert mutations == [
        {
            "action": "upsert",
            "path": "README.md",
            "size": 5,
            "sha256": commit_ops.calculate_git_blob_sha1(content),
            "lfs": False,
        }
    ]


@pytest.mark.asyncio
async def test_process_regular_file_reports_upload_exception_as_http_500(monkeypatch):
    repo = SimpleNamespace(owner=SimpleNamespace(username="owner"))
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: _FakeLakeFSClient())
    monkeypatch.setattr(commit_ops, "get_effective_lfs_threshold", lambda *_args: 10)
    monkeypatch.setattr(commit_ops, "should_use_lfs", lambda *_args: False)
    monkeypatch.setattr(commit_ops, "get_file", lambda *_args: None)

    async def fail_upload(*args, **kwargs):
        raise RuntimeError("lakefs unavailable")

    monkeypatch.setattr(commit_ops.mutation_gateway, "upload_object", fail_upload)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.process_regular_file(
            "README.md",
            base64.b64encode(b"hello").decode(),
            "base64",
            repo,
            "lakefs-repo",
            "main",
        )

    assert exc_info.value.status_code == 500
    assert "lakefs unavailable" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_process_lfs_file_restores_deleted_file_into_mutation_queue(monkeypatch):
    repo = SimpleNamespace(owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)
    monkeypatch.setattr(commit_ops.cfg.s3, "bucket", "hub-storage")
    _FakeFileModel.get_or_none_result = SimpleNamespace(
        id=4,
        sha256="a" * 6,
        size=10,
        lfs=True,
        is_deleted=True,
    )
    mutations = []

    changed, tracking = await commit_ops.process_lfs_file(
        "weights.bin",
        "a" * 6,
        10,
        "sha256",
        repo,
        "lakefs-repo",
        "main",
        file_mutations=mutations,
    )

    assert changed is True
    assert tracking["old_sha256"] is None
    assert mutations == [
        {
            "action": "upsert",
            "path": "weights.bin",
            "size": 10,
            "sha256": "a" * 6,
            "lfs": True,
        }
    ]


@pytest.mark.asyncio
async def test_process_lfs_file_reports_restore_link_failure_as_http_500(monkeypatch):
    repo = SimpleNamespace(owner=SimpleNamespace(username="owner"))
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: _FakeLakeFSClient())
    monkeypatch.setattr(commit_ops.cfg.s3, "bucket", "hub-storage")
    _FakeFileModel.get_or_none_result = SimpleNamespace(
        id=4,
        sha256="a" * 6,
        size=10,
        lfs=True,
        is_deleted=True,
    )

    async def fail_link(*args, **kwargs):
        raise RuntimeError("restore link unavailable")

    monkeypatch.setattr(commit_ops.mutation_gateway, "link_physical_address", fail_link)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.process_lfs_file(
            "weights.bin", "a" * 6, 10, "sha256", repo, "lakefs-repo", "main"
        )

    assert exc_info.value.status_code == 500
    assert "restore link unavailable" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_process_lfs_file_reports_storage_probe_failure_as_http_500(monkeypatch):
    repo = SimpleNamespace(owner=SimpleNamespace(username="owner"))
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: _FakeLakeFSClient())
    monkeypatch.setattr(commit_ops.cfg.s3, "bucket", "hub-storage")
    monkeypatch.setattr(
        commit_ops,
        "object_exists",
        lambda *_args: _async_raise(RuntimeError("s3 unavailable")),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.process_lfs_file(
            "weights.bin", "a" * 6, 10, "sha256", repo, "lakefs-repo", "main"
        )

    assert exc_info.value.status_code == 500
    assert "s3 unavailable" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_process_deleted_file_treats_lakefs_404_as_success(monkeypatch):
    repo = SimpleNamespace()
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: _FakeLakeFSClient())
    mutations = []
    request = httpx.Request("DELETE", "http://lakefs/object")
    response = httpx.Response(404, request=request)

    async def report_missing(*args, **kwargs):
        raise httpx.HTTPStatusError("missing", request=request, response=response)

    monkeypatch.setattr(commit_ops.mutation_gateway, "delete_object", report_missing)

    deleted = await commit_ops.process_deleted_file(
        "README.md", repo, "lakefs-repo", "main", file_mutations=mutations
    )

    assert deleted is True
    assert mutations == [{"action": "delete", "path": "README.md"}]


@pytest.mark.asyncio
async def test_process_deleted_folder_rejects_non_advancing_cursor(monkeypatch):
    client = _FakeLakeFSClient()
    client.list_responses = [
        {
            "results": [],
            "pagination": {"has_more": True, "next_offset": ""},
        }
    ]
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.process_deleted_folder(
            "folder", SimpleNamespace(), "lakefs-repo", "main"
        )

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["error"] == "LakeFS returned a non-advancing folder cursor"


@pytest.mark.asyncio
async def test_process_deleted_folder_treats_missing_object_as_deleted(monkeypatch):
    client = _FakeLakeFSClient()
    client.list_responses = [
        {
            "results": [{"path_type": "object", "path": "folder/missing.txt"}],
        }
    ]
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)
    request = httpx.Request("DELETE", "http://lakefs/object")
    response = httpx.Response(404, request=request)

    async def report_missing(*args, **kwargs):
        raise httpx.HTTPStatusError("missing", request=request, response=response)

    monkeypatch.setattr(commit_ops.mutation_gateway, "delete_object", report_missing)
    mutations = []

    deleted = await commit_ops.process_deleted_folder(
        "folder", SimpleNamespace(), "lakefs-repo", "main", file_mutations=mutations
    )

    assert deleted is True
    assert mutations == [{"action": "delete", "path": "folder/missing.txt"}]


@pytest.mark.asyncio
async def test_process_deleted_folder_reports_object_delete_failure(monkeypatch):
    client = _FakeLakeFSClient()
    client.list_responses = [
        {
            "results": [{"path_type": "object", "path": "folder/broken.txt"}],
        }
    ]
    monkeypatch.setattr(commit_ops, "get_lakefs_client", lambda: client)

    async def fail_delete(*args, **kwargs):
        raise RuntimeError("delete transport failed")

    monkeypatch.setattr(commit_ops.mutation_gateway, "delete_object", fail_delete)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.process_deleted_folder(
            "folder", SimpleNamespace(), "lakefs-repo", "main"
        )

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["error"] == "LakeFS failed to delete 1 folder object(s)"


@pytest.mark.asyncio
async def test_raise_commit_observing_exposes_operation_location(monkeypatch):
    service = _FakeOperationService()
    monkeypatch.setattr(commit_ops.cfg.app, "api_base", "/api")

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._raise_commit_observing(
            service, SimpleNamespace(id="intent-1"), message="still pending"
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {
        "error": "still pending",
        "operation_id": "observation-1",
    }
    assert exc_info.value.headers == {
        "Retry-After": "30",
        "Location": "/api/operations/observation-1",
    }


@pytest.mark.asyncio
async def test_raise_commit_observing_keeps_retry_response_when_handle_creation_fails():
    class FailingService:
        async def ensure_commit_observation_operation(self, intent_id):
            raise RuntimeError("database unavailable")

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._raise_commit_observing(
            FailingService(), SimpleNamespace(id="intent-1")
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {"error": "Commit result is being observed"}
    assert exc_info.value.headers == {"Retry-After": "30"}


@pytest.mark.asyncio
async def test_commit_unlocked_returns_503_when_base_head_cannot_be_resolved(monkeypatch):
    repo = SimpleNamespace(id=1, owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    client.raise_on["get_branch"] = RuntimeError("lakefs unavailable")
    _commit_dependencies(monkeypatch, client, repo)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            _FakeRequest(_commit_body()),
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {"error": "Unable to resolve commit base head"}


@pytest.mark.asyncio
async def test_commit_unlocked_returns_503_when_file_metadata_cannot_be_loaded(monkeypatch):
    repo = SimpleNamespace(id=1, owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    service = _BatchFakeOperationService()
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")

    def fail_file_map(*_args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        commit_ops,
        "get_repo_file_map",
        fail_file_map,
    )
    request = _FakeRequest(
        _commit_body(_file_operation()),
        app=SimpleNamespace(
            state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {"error": "Unable to load commit file metadata"}
    assert exc_info.value.headers == {"Retry-After": "30"}


@pytest.mark.asyncio
async def test_commit_unlocked_rejects_invalid_ndjson_line(monkeypatch):
    repo = SimpleNamespace(id=1, owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    _commit_dependencies(monkeypatch, client, repo)

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            _FakeRequest(b'{"key":"header","value":{}}\n{broken'),
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 400
    assert "Invalid JSON line" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_commit_unlocked_returns_existing_finalized_idempotent_result(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    service = _FakeOperationService()
    existing_result = {
        "commitUrl": "models/owner/repo/commit/already-done",
        "commitOid": "already-done",
        "pullRequestUrl": None,
    }
    operations = [_file_operation()]
    service.existing_intent = _intent(
        state="finalized",
        result_json=existing_result,
        request_hash=_request_hash("model", 1, "main", operations),
    )
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    request = _FakeRequest(
        _commit_body(*operations),
        headers={"idempotency-key": "same-request"},
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    result = await commit_ops._commit_unlocked(
        commit_ops.RepoType.model,
        "owner",
        "repo",
        "main",
        request,
        SimpleNamespace(id=7, username="owner"),
    )

    assert result == existing_result
    assert [call[0] for call in service.calls] == ["get_commit_intent"]
    assert not [call for call in client.calls if call[0] == "commit"]


@pytest.mark.asyncio
async def test_commit_unlocked_rejects_idempotency_key_bound_to_other_request(monkeypatch):
    repo = SimpleNamespace(id=1, owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    service = _FakeOperationService()
    service.existing_intent = _intent(request_hash="different-request")
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    request = _FakeRequest(
        _commit_body(_file_operation()),
        headers={"idempotency-key": "same-request"},
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 409
    assert "another commit request" in exc_info.value.detail["error"]


@pytest.mark.asyncio
async def test_commit_unlocked_observes_existing_committed_idempotent_intent(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    service = _FakeOperationService()
    operations = [_file_operation()]
    service.existing_intent = _intent(
        state="committed",
        lakefs_commit_id="committed-id",
        result_json={"commitOid": "committed-id"},
        payload_json={"file_mutations": [{"action": "delete", "path": "old.txt"}]},
        request_hash=_request_hash("model", 1, "main", operations),
    )
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    request = _FakeRequest(
        _commit_body(*operations),
        headers={"idempotency-key": "same-request"},
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    result = await commit_ops._commit_unlocked(
        commit_ops.RepoType.model,
        "owner",
        "repo",
        "main",
        request,
        SimpleNamespace(id=7, username="owner"),
    )

    assert result["commitOid"] == "committed-id"
    assert [call[0] for call in service.calls] == [
        "get_commit_intent",
        "finalize_commit_intent",
    ]


@pytest.mark.asyncio
async def test_commit_unlocked_maps_commit_in_progress_to_observation_response(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    blocking_intent = _intent(id="blocking-intent")
    service = _FakeOperationService(
        intent=_intent(),
        prepare_error=commit_ops.CommitInProgress(blocking_intent),
    )
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    request = _FakeRequest(
        _commit_body(_file_operation()),
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["operation_id"] == "observation-1"
    assert not [call for call in client.calls if call[0] == "commit"]


@pytest.mark.asyncio
async def test_commit_route_maps_outer_fence_conflict_to_observation_response(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    blocking_intent = _intent(id="blocking-intent")
    service = _FakeOperationService(intent=_intent())

    class Fence:
        async def __aenter__(self):
            raise commit_ops.CommitInProgress(blocking_intent)

        async def __aexit__(self, *_args):
            return False

    service.repository_ref_fence = lambda *_args, **_kwargs: Fence()
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops, "Request", _FakeRequest)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")

    request = _FakeRequest(
        b"",
        app=SimpleNamespace(
            state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops.commit(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["operation_id"] == "observation-1"


@pytest.mark.asyncio
async def test_commit_unlocked_maps_quota_exceeded_to_http_413(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    service = _FakeOperationService(
        intent=_intent(), prepare_error=commit_ops.QuotaExceeded("quota")
    )
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(10),
    )
    request = _FakeRequest(
        _commit_body(_file_operation()),
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 413
    assert exc_info.value.detail == {"error": "Storage quota exceeded"}


@pytest.mark.asyncio
async def test_commit_unlocked_rejects_changed_base_head_before_dispatch(monkeypatch):
    repo = SimpleNamespace(id=1, owner=SimpleNamespace(username="owner"))
    client = _FakeLakeFSClient()
    client.branch_responses = [
        {"commit_id": "base-head"},
        {"commit_id": "new-head"},
    ]
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(
        commit_ops,
        "process_regular_file",
        lambda **kwargs: _async_true_and_record(kwargs["file_mutations"], "README.md"),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            _FakeRequest(_commit_body(_file_operation())),
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {
        "error": "Repository head changed during commit preparation"
    }
    assert not [call for call in client.calls if call[0] == "commit"]


@pytest.mark.asyncio
async def test_commit_unlocked_maps_lakefs_400_to_confirmed_http_rejection(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    service = _FakeOperationService(intent=_intent())
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    monkeypatch.setattr(
        commit_ops,
        "process_regular_file",
        lambda **kwargs: _async_true_and_record(kwargs["file_mutations"], "README.md"),
    )
    request = httpx.Request("POST", "http://lakefs/commit")
    response = httpx.Response(400, request=request)

    async def reject_commit(*args, **kwargs):
        raise httpx.HTTPStatusError("bad commit", request=request, response=response)

    monkeypatch.setattr(commit_ops.mutation_gateway, "commit", reject_commit)
    request_obj = _FakeRequest(
        _commit_body(_file_operation()),
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request_obj,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == {"error": "LakeFS rejected the commit"}
    assert any(call[0] == "abandon_commit_intent" for call in service.calls)
    abandon_call = next(call for call in service.calls if call[0] == "abandon_commit_intent")
    assert abandon_call[2]["confirmed_no_effect"] is True


@pytest.mark.asyncio
async def test_commit_unlocked_maps_commit_transport_failure_to_observation(monkeypatch):
    repo = SimpleNamespace(
        id=1, owner=SimpleNamespace(username="owner"), private=False
    )
    client = _FakeLakeFSClient()
    service = _FakeOperationService(intent=_intent())
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(
        commit_ops,
        "_estimate_commit_quota_delta",
        lambda *_args: _async_return(0),
    )
    monkeypatch.setattr(
        commit_ops,
        "process_regular_file",
        lambda **kwargs: _async_true_and_record(kwargs["file_mutations"], "README.md"),
    )

    async def fail_commit(*args, **kwargs):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(commit_ops.mutation_gateway, "commit", fail_commit)
    request = _FakeRequest(
        _commit_body(_file_operation()),
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service))),
    )

    with pytest.raises(HTTPException) as exc_info:
        await commit_ops._commit_unlocked(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            request,
            SimpleNamespace(id=7, username="owner"),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == {"error": "Commit result is being observed"}
    assert exc_info.value.headers == {"Retry-After": "30"}


@pytest.mark.asyncio
async def test_commit_unlocked_finalizes_legacy_fallback_after_lakefs_success(monkeypatch):
    repo = SimpleNamespace(
        id=1,
        owner=SimpleNamespace(username="owner"),
        owner_id=2,
        private=False,
    )
    client = _FakeLakeFSClient()
    create_calls = []
    postprocess_calls = []
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(
        commit_ops,
        "process_regular_file",
        lambda **kwargs: _async_true_and_record(kwargs["file_mutations"], "README.md"),
    )
    monkeypatch.setattr(
        commit_ops,
        "create_commit",
        lambda **kwargs: create_calls.append(kwargs),
    )
    monkeypatch.setattr(
        commit_ops,
        "perform_commit_postprocess",
        lambda payload: postprocess_calls.append(payload) or _async_return({}),
    )
    monkeypatch.setattr(commit_ops, "File", _FakeFileModel)

    result = await commit_ops._commit_unlocked(
        commit_ops.RepoType.model,
        "owner",
        "repo",
        "main",
        _FakeRequest(_commit_body(_file_operation())),
        SimpleNamespace(id=7, username="owner"),
    )

    assert result["commitOid"] == "commit-created"
    assert create_calls[0]["commit_id"] == "commit-created"
    assert postprocess_calls[0]["commit_id"] == "commit-created"
    assert _FakeFileModel.insert_calls[0]["path_in_repo"] == "README.md"


@pytest.mark.asyncio
async def test_commit_unlocked_keeps_success_when_legacy_finalization_fails(monkeypatch):
    repo = SimpleNamespace(
        id=1,
        owner=SimpleNamespace(username="owner"),
        owner_id=2,
        private=False,
    )
    client = _FakeLakeFSClient()
    warnings = []
    _commit_dependencies(monkeypatch, client, repo)
    monkeypatch.setattr(
        commit_ops,
        "process_regular_file",
        lambda **kwargs: _async_true_and_record(kwargs["file_mutations"], "README.md"),
    )
    monkeypatch.setattr(
        commit_ops,
        "create_commit",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("db finalization failed")),
    )
    monkeypatch.setattr(
        commit_ops.logger,
        "warning",
        lambda message: warnings.append(message),
    )

    result = await commit_ops._commit_unlocked(
        commit_ops.RepoType.model,
        "owner",
        "repo",
        "main",
        _FakeRequest(_commit_body(_file_operation())),
        SimpleNamespace(id=7, username="owner"),
    )

    assert result["commitOid"] == "commit-created"
    assert any("post-process fallback failed" in message for message in warnings)


async def _async_return(value):
    return value


async def _async_raise(error):
    raise error


async def _async_true_and_record(mutations, path):
    mutations.append(
        {
            "action": "upsert",
            "path": path,
            "size": 5,
            "sha256": "sha1",
            "lfs": False,
        }
    )
    return True

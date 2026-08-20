"""Fail-closed tests for dangerous repository operation gates."""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Request

from kohakuhub import lakefs_mutation_gateway as mutation_gateway
import kohakuhub.api.branches as branches_api
import kohakuhub.api.commit.routers.operations as commit_ops
import kohakuhub.api.repo.routers.crud as repo_crud


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "call"),
    [
        (
            "revert",
            lambda: branches_api.revert_branch(
                "model",
                "owner",
                "repo",
                "main",
                branches_api.RevertPayload(ref="commit"),
                user=SimpleNamespace(username="owner"),
            ),
        ),
        (
            "reset",
            lambda: branches_api.reset_branch(
                "model",
                "owner",
                "repo",
                "main",
                branches_api.ResetPayload(ref="commit", force=True),
                user=SimpleNamespace(username="owner"),
            ),
        ),
    ],
)
async def test_dangerous_branch_operations_fail_before_repository_lookup(
    monkeypatch, operation, call
):
    monkeypatch.setattr(
        branches_api.cfg.app,
        f"enable_{operation}_operations",
        False,
    )
    monkeypatch.setattr(
        branches_api,
        "get_repository",
        lambda *_args: pytest.fail("disabled operation looked up a repository"),
    )
    monkeypatch.setattr(
        branches_api,
        "get_lakefs_client",
        lambda: pytest.fail("disabled operation created a LakeFS client"),
    )

    with pytest.raises(HTTPException) as error:
        await call()

    assert error.value.status_code == 503
    assert error.value.detail == {
        "error": "operation_disabled",
        "operation": operation,
    }


@pytest.mark.asyncio
async def test_squash_fails_before_auth_and_repository_lookup(monkeypatch):
    monkeypatch.setattr(repo_crud.cfg.app, "enable_squash_operations", False)
    monkeypatch.setattr(
        repo_crud,
        "get_repository",
        lambda *_args: pytest.fail("disabled operation looked up a repository"),
    )
    with pytest.raises(HTTPException) as error:
        await repo_crud.squash_repo(
            repo_crud.SquashRepoPayload(repo="owner/repo", type="model"),
            auth=(SimpleNamespace(username="owner"), False),
        )
    assert error.value.status_code == 503
    assert error.value.detail == {
        "error": "operation_disabled",
        "operation": "squash",
    }


@pytest.mark.asyncio
async def test_postgres_http_repo_delete_fails_before_destructive_cleanup(monkeypatch):
    repo = SimpleNamespace(
        id=7,
        repo_type="model",
        full_id="owner/repo",
        lakefs_repo="model:owner/repo",
        delete_instance=lambda: pytest.fail("repository row was deleted"),
    )
    request = Request(
        {
            "type": "http",
            "method": "DELETE",
            "path": "/api/repos/delete",
            "headers": [],
            "app": SimpleNamespace(state=SimpleNamespace()),
        }
    )

    monkeypatch.setattr(repo_crud.cfg.app, "db_backend", "postgres")
    monkeypatch.setattr(repo_crud, "get_repository", lambda *_args: repo)
    monkeypatch.setattr(
        repo_crud,
        "check_repo_delete_permission",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        repo_crud,
        "get_lakefs_client",
        lambda: pytest.fail("LakeFS destructive cleanup was started"),
    )
    monkeypatch.setattr(
        repo_crud,
        "cleanup_repository_storage",
        lambda **_kwargs: pytest.fail("S3 destructive cleanup was started"),
    )

    with pytest.raises(HTTPException) as error:
        await repo_crud.delete_repo(
            repo_crud.DeleteRepoPayload(type="model", name="repo"),
            auth=(SimpleNamespace(username="owner"), False),
            request=request,
        )

    assert error.value.status_code == 503
    assert error.value.detail == {"error": "durable_cleanup_unavailable"}
    assert error.value.headers == {"Retry-After": "30"}


@pytest.fixture
def without_test_compatibility():
    """Override the suite-wide compatibility fixture for security assertions."""

    token = mutation_gateway._TEST_COMPATIBILITY.set(False)
    try:
        yield
    finally:
        mutation_gateway._TEST_COMPATIBILITY.reset(token)


def _http_request(*, compatibility_marker: bool = False) -> Request:
    state = SimpleNamespace()
    if compatibility_marker:
        state._khub_test_compatibility = True
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/mutation",
            "headers": [],
            "query_string": b"",
            "app": SimpleNamespace(state=state),
        }
    )


@pytest.mark.asyncio
async def test_non_postgres_http_branch_mutation_requires_durable_runtime(
    monkeypatch, without_test_compatibility
):
    monkeypatch.setattr(branches_api.cfg.app, "db_backend", "sqlite")
    called = []

    async def callback():
        called.append(True)

    with pytest.raises(HTTPException) as error:
        await branches_api._run_fenced_mutation(
            _http_request(), 7, "branch:main", callback
        )

    assert error.value.status_code == 503
    assert error.value.detail == {"error": "mutation_fence_unavailable"}
    assert called == []


@pytest.mark.asyncio
async def test_repository_wrapper_does_not_trust_app_marker_without_context(
    monkeypatch, without_test_compatibility
):
    monkeypatch.setattr(repo_crud.cfg.app, "db_backend", "sqlite")
    called = []

    async def callback():
        called.append(True)

    with pytest.raises(HTTPException) as error:
        await repo_crud._run_fenced_repository_mutation(
            _http_request(compatibility_marker=True), 7, callback
        )

    assert error.value.status_code == 503
    assert error.value.detail == {"error": "mutation_fence_unavailable"}
    assert called == []


@pytest.mark.asyncio
async def test_non_postgres_http_create_repo_requires_durable_runtime(
    monkeypatch, without_test_compatibility
):
    monkeypatch.setattr(repo_crud.cfg.app, "db_backend", "sqlite")

    with pytest.raises(HTTPException) as error:
        await repo_crud.create_repo(
            repo_crud.CreateRepoPayload(type="model", name="repo"),
            user=SimpleNamespace(username="owner"),
            request=_http_request(),
        )

    assert error.value.status_code == 503
    assert error.value.detail == {"error": "mutation_fence_unavailable"}


@pytest.mark.asyncio
async def test_non_postgres_http_commit_requires_durable_runtime(
    monkeypatch, without_test_compatibility
):
    repo = SimpleNamespace(id=7)
    monkeypatch.setattr(commit_ops.cfg.app, "db_backend", "sqlite")
    monkeypatch.setattr(commit_ops.Repository, "get_or_none", lambda *_args: repo)
    monkeypatch.setattr(
        commit_ops, "check_repo_write_permission", lambda *_args: None
    )

    with pytest.raises(HTTPException) as error:
        await commit_ops.commit(
            commit_ops.RepoType.model,
            "owner",
            "repo",
            "main",
            _http_request(),
            user=SimpleNamespace(username="owner"),
        )

    assert error.value.status_code == 503
    assert error.value.detail == {"error": "mutation_fence_unavailable"}

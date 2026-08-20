"""Unit tests for the non-bypassable LakeFS mutation gateway."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from kohakuhub import lakefs_mutation_gateway as gateway
from kohakuhub.config import cfg
from kohakuhub.operations.service import _fence_connection_limiter


class _Client:
    async def commit(self, **kwargs):
        return kwargs

    async def create_tag(self, **kwargs):
        return kwargs


async def _get_fence_limiter():
    limiter = _fence_connection_limiter("postgresql://test", 2)
    return asyncio.get_running_loop(), limiter, _fence_connection_limiter(
        "postgresql://test", 2
    )


async def _get_owned_fence_limiters():
    first_owner = SimpleNamespace()
    second_owner = SimpleNamespace()
    first = _fence_connection_limiter(
        "postgresql://test", 2, limiter_owner=first_owner
    )
    first_again = _fence_connection_limiter(
        "postgresql://test", 2, limiter_owner=first_owner
    )
    second = _fence_connection_limiter(
        "postgresql://test", 2, limiter_owner=second_owner
    )
    return first, first_again, second


def test_fence_limiter_is_not_reused_across_event_loops():
    first_loop, first, first_again = asyncio.run(_get_fence_limiter())
    second_loop, second, second_again = asyncio.run(_get_fence_limiter())

    assert first is first_again
    assert second is second_again
    assert first is not second
    assert first_loop is not second_loop


def test_fence_limiter_is_shared_only_by_the_same_owner():
    first, first_again, second = asyncio.run(_get_owned_fence_limiters())

    assert first is first_again
    assert first is not second


def test_canonical_ref_preserves_explicit_refs():
    assert gateway._canonical_ref("branch:main") == "branch:main"


@pytest.mark.asyncio
async def test_gateway_rejects_mutation_without_a_fence(monkeypatch):
    monkeypatch.delenv("KOHAKU_HUB_TEST_COMPATIBILITY", raising=False)

    with pytest.raises(gateway.MutationFenceRequired):
        await gateway.commit(_Client(), repository="repo", branch="main")


@pytest.mark.asyncio
async def test_gateway_does_not_infer_compatibility_from_sqlite(monkeypatch):
    monkeypatch.setattr(cfg.app, "db_backend", "sqlite")

    with pytest.raises(gateway.MutationFenceRequired):
        await gateway.commit(_Client(), repository="repo", branch="main")

    with gateway.test_compatibility():
        assert await gateway.commit(
            _Client(), repository="repo", branch="main"
        ) == {"repository": "repo", "branch": "main"}


def test_gateway_bind_requires_explicit_test_compatibility_for_sqlite(monkeypatch):
    monkeypatch.setattr(cfg.app, "db_backend", "sqlite")

    with pytest.raises(gateway.MutationFenceRequired):
        gateway.bind_repository("repo")

    with gateway.test_compatibility():
        gateway.bind_repository("repo")


@pytest.mark.asyncio
async def test_gateway_validates_ref_against_active_capability(monkeypatch):
    monkeypatch.delenv("KOHAKU_HUB_TEST_COMPATIBILITY", raising=False)
    token = gateway.activate_capability(
        gateway.MutationCapability(
            repository_id=7,
            ref="branch:main",
            scope="mutation",
            lakefs_repositories=frozenset({"repo"}),
        )
    )
    try:
        with pytest.raises(gateway.MutationFenceRequired):
            await gateway.commit(_Client(), repository="repo", branch="dev")
        assert await gateway.commit(_Client(), repository="repo", branch="main") == {
            "repository": "repo",
            "branch": "main",
        }
    finally:
        gateway.reset_capability(token)


@pytest.mark.asyncio
async def test_gateway_validates_create_tag_name_as_the_locked_ref():
    token = gateway.activate_capability(
        gateway.MutationCapability(
            repository_id=7,
            ref="tag:v1",
            scope="mutation",
            lakefs_repositories=frozenset({"repo"}),
        )
    )
    try:
        assert await gateway.create_tag(
            _Client(), repository="repo", id="v1", ref="source-commit"
        ) == {"repository": "repo", "id": "v1", "ref": "source-commit"}
        with pytest.raises(gateway.MutationFenceRequired):
            await gateway.create_tag(
                _Client(), repository="repo", id="v2", ref="source-commit"
            )
    finally:
        gateway.reset_capability(token)


@pytest.mark.asyncio
async def test_name_fence_capability_only_allows_repository_creation(monkeypatch):
    monkeypatch.delenv("KOHAKU_HUB_TEST_COMPATIBILITY", raising=False)
    token = gateway.activate_capability(
        gateway.MutationCapability(
            repository_id=None,
            ref="__repository_name__",
            scope="repository_name",
        )
    )
    try:
        with pytest.raises(gateway.MutationFenceRequired):
            await gateway.commit(_Client(), repository="repo", branch="main")
    finally:
        gateway.reset_capability(token)


@pytest.mark.asyncio
async def test_gateway_enforces_create_delete_scopes_and_allows_hard_reset():
    class Client:
        async def hard_reset_branch(self, **kwargs):
            return kwargs

    token = gateway.activate_capability(
        gateway.MutationCapability(
            repository_id=7, ref="branch:main", scope="repository"
        )
    )
    try:
        with pytest.raises(gateway.MutationFenceRequired):
            await gateway.create_repository(Client(), repository="repo")
    finally:
        gateway.reset_capability(token)

    token = gateway.activate_capability(
        gateway.MutationCapability(
            repository_id=7,
            ref="branch:main",
            scope="mutation",
            lakefs_repositories=frozenset({"repo"}),
        )
    )
    try:
        with pytest.raises(gateway.MutationFenceRequired):
            await gateway.delete_repository(Client(), repository="repo")
        assert await gateway.hard_reset_branch(
            Client(), repository="repo", branch="main", ref="main", force=True
        ) == {
            "repository": "repo",
            "branch": "main",
            "ref": "main",
            "force": True,
        }
    finally:
        gateway.reset_capability(token)

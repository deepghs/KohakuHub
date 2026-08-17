"""Unit tests for the non-bypassable LakeFS mutation gateway."""

from __future__ import annotations

import asyncio

import pytest

from kohakuhub import lakefs_mutation_gateway as gateway
from kohakuhub.operations.service import _fence_connection_limiter


class _Client:
    async def commit(self, **kwargs):
        return kwargs


async def _get_fence_limiter():
    limiter = _fence_connection_limiter("postgresql://test", 2)
    return asyncio.get_running_loop(), limiter, _fence_connection_limiter(
        "postgresql://test", 2
    )


def test_fence_limiter_is_not_reused_across_event_loops():
    first_loop, first, first_again = asyncio.run(_get_fence_limiter())
    second_loop, second, second_again = asyncio.run(_get_fence_limiter())

    assert first is first_again
    assert second is second_again
    assert first is not second
    assert first_loop is not second_loop


@pytest.mark.asyncio
async def test_gateway_rejects_mutation_without_a_fence(monkeypatch):
    monkeypatch.delenv("KOHAKU_HUB_TEST_COMPATIBILITY", raising=False)

    with pytest.raises(gateway.MutationFenceRequired):
        await gateway.commit(_Client(), repository="repo", branch="main")


@pytest.mark.asyncio
async def test_gateway_validates_ref_against_active_capability(monkeypatch):
    monkeypatch.delenv("KOHAKU_HUB_TEST_COMPATIBILITY", raising=False)
    token = gateway.activate_capability(
        gateway.MutationCapability(repository_id=7, ref="branch:main", scope="mutation")
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

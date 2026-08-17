"""Adversarial PostgreSQL tests for the cross-process mutation fence."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import psycopg
import pytest

from kohakuhub.operations.runtime import OperationRuntime
from kohakuhub.operations.service import CommitInProgress


pytestmark = pytest.mark.integration


def _database_url() -> str:
    value = os.environ.get("KOHAKU_HUB_DATABASE_URL", "")
    if not value.startswith(("postgresql://", "postgres://")):
        pytest.skip("requires PostgreSQL")
    return value


@pytest.fixture
async def runtimes():
    url = _database_url()
    first = await OperationRuntime.open(url)
    second = await OperationRuntime.open(url)
    try:
        yield first, second
    finally:
        await second.close()
        await first.close()


async def _hold_fence(
    service,
    repository_id: int,
    ref: str,
    entered: asyncio.Event,
    release: asyncio.Event,
    *,
    scope: str = "mutation",
) -> None:
    async with service.repository_ref_fence(
        repository_id, ref, scope=scope
    ):
        entered.set()
        await release.wait()


@pytest.mark.asyncio
async def test_same_ref_is_serialized_across_independent_services(runtimes):
    first, second = runtimes
    repository_id = 7_000_000 + (uuid4().int % 100_000)
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    first_release = asyncio.Event()
    second_release = asyncio.Event()
    first_task = asyncio.create_task(
        _hold_fence(
            first.service,
            repository_id,
            "main",
            first_entered,
            first_release,
        )
    )
    second_task = asyncio.create_task(
        _hold_fence(
            second.service,
            repository_id,
            "branch:main",
            second_entered,
            second_release,
        )
    )
    try:
        await asyncio.wait_for(first_entered.wait(), timeout=5)
        await asyncio.sleep(0.15)
        assert not second_entered.is_set()
        first_release.set()
        await asyncio.wait_for(second_entered.wait(), timeout=5)
        second_release.set()
        await asyncio.gather(first_task, second_task)
    finally:
        first_release.set()
        second_release.set()
        await asyncio.gather(first_task, second_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_different_refs_can_run_concurrently(runtimes):
    first, second = runtimes
    repository_id = 7_000_000 + (uuid4().int % 100_000)
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release = asyncio.Event()
    first_task = asyncio.create_task(
        _hold_fence(
            first.service,
            repository_id,
            "main",
            first_entered,
            release,
        )
    )
    second_task = asyncio.create_task(
        _hold_fence(
            second.service,
            repository_id,
            "dev",
            second_entered,
            release,
        )
    )
    try:
        await asyncio.wait_for(
            asyncio.gather(first_entered.wait(), second_entered.wait()),
            timeout=5,
        )
        release.set()
        await asyncio.gather(first_task, second_task)
    finally:
        release.set()
        await asyncio.gather(first_task, second_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_repository_cutover_blocks_ordinary_mutation(runtimes):
    first, second = runtimes
    repository_id = 7_000_000 + (uuid4().int % 100_000)
    cutover_entered = asyncio.Event()
    mutation_entered = asyncio.Event()
    cutover_release = asyncio.Event()
    mutation_release = asyncio.Event()
    cutover_task = asyncio.create_task(
        _hold_fence(
            first.service,
            repository_id,
            "__repository__",
            cutover_entered,
            cutover_release,
            scope="cutover",
        )
    )
    mutation_task = asyncio.create_task(
        _hold_fence(
            second.service,
            repository_id,
            "main",
            mutation_entered,
            mutation_release,
        )
    )
    try:
        await asyncio.wait_for(cutover_entered.wait(), timeout=5)
        await asyncio.sleep(0.15)
        assert not mutation_entered.is_set()
        cutover_release.set()
        await asyncio.wait_for(mutation_entered.wait(), timeout=5)
        mutation_release.set()
        await asyncio.gather(cutover_task, mutation_task)
    finally:
        cutover_release.set()
        mutation_release.set()
        await asyncio.gather(
            cutover_task, mutation_task, return_exceptions=True
        )


@pytest.mark.asyncio
async def test_connection_death_releases_process_advisory_lock(runtimes):
    first, _second = runtimes
    repository_id = 7_000_000 + (uuid4().int % 100_000)
    lock_key = f"khub-repository:v1:{repository_id}"
    connection = await psycopg.AsyncConnection.connect(
        _database_url(), autocommit=True
    )
    await connection.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (lock_key,)
    )
    await connection.close()

    entered = asyncio.Event()
    release = asyncio.Event()
    task = asyncio.create_task(
        _hold_fence(
            first.service,
            repository_id,
            "main",
            entered,
            release,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_persisted_unresolved_intent_blocks_after_lock_loss(runtimes):
    first, _second = runtimes
    repository_id = 7_000_000 + (uuid4().int % 100_000)
    ref = f"branch:main-{uuid4()}"
    lock_key = f"khub-repository-ref:v1:{repository_id}:{ref}"
    connection = await psycopg.AsyncConnection.connect(
        _database_url(), autocommit=True
    )
    await connection.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (lock_key,)
    )
    await connection.close()

    intent = await first.service.prepare_commit_intent(
        repository_id=repository_id,
        ref=ref,
        base_head="base-head",
        payload={"paths": ["README.md"]},
    )
    try:
        with pytest.raises(CommitInProgress) as error:
            async with first.service.repository_ref_fence(
                repository_id, ref
            ):
                raise AssertionError("unreachable")
        assert error.value.intent.id == intent.id
    finally:
        await first.service.abandon_commit_intent(
            intent.id, error_code="test_cleanup"
        )

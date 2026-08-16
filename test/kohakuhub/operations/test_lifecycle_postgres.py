"""Real PostgreSQL tests for the operation acceptance and delivery contract."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from procrastinate.worker import Worker

from kohakuhub.operations.runtime import OperationRuntime
from kohakuhub.operations.service import IdempotencyConflict, OperationNotCancellable


pytestmark = pytest.mark.integration


def _database_url() -> str:
    value = os.environ.get("KOHAKU_HUB_DATABASE_URL", "")
    if not value.startswith(("postgresql://", "postgres://")):
        pytest.skip("requires PostgreSQL")
    return value


@pytest.fixture
async def runtime():
    from scripts.khub_migrate import migrate

    migrate()
    value = await OperationRuntime.open(_database_url())
    try:
        yield value
    finally:
        await value.close()


@pytest.mark.asyncio
async def test_accept_is_idempotent_and_real_worker_delivers(runtime):
    key = f"operation-{uuid4()}"
    resource = f"test:operation:{uuid4()}"
    first = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=resource,
        payload={"paths": ["a", "b"]},
        requested_by_user_id=101,
        idempotency_key=key,
    )
    second = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=resource,
        payload={"paths": ["a", "b"]},
        requested_by_user_id=101,
        idempotency_key=key,
    )
    assert first.id == second.id

    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"test-worker-{uuid4()}",
        concurrency=1,
        wait=True,
        install_signal_handlers=False,
        fetch_job_polling_interval=0.05,
        abort_job_polling_interval=0.05,
        update_heartbeat_interval=0.1,
        stalled_worker_timeout=2,
    )
    worker_task = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            current = await runtime.service.get(first.id)
            if current is not None and current.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        current = await runtime.service.get(first.id)
        assert current is not None
        assert current.state == "succeeded"
        assert current.progress_current == 1
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)


@pytest.mark.asyncio
async def test_concurrent_first_idempotency_requests_converge(runtime):
    key = f"concurrent-{uuid4()}"
    resource = f"test:concurrent:{uuid4()}"

    async def accept_once():
        return await runtime.service.accept(
            kind="maintenance.noop.v1",
            resource_key=resource,
            payload={"value": 1},
            requested_by_user_id=108,
            idempotency_key=key,
        )

    first, second = await asyncio.gather(accept_once(), accept_once())
    assert first.id == second.id


@pytest.mark.asyncio
async def test_successor_step_and_job_are_delivered_transactionally(runtime):
    operation = await runtime.service.accept(
        kind="maintenance.chain.v1",
        resource_key=f"test:chain:{uuid4()}",
        payload={},
        requested_by_user_id=105,
        idempotency_key=f"chain-{uuid4()}",
    )
    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"chain-worker-{uuid4()}",
        concurrency=1,
        wait=True,
        install_signal_handlers=False,
        fetch_job_polling_interval=0.05,
        abort_job_polling_interval=0.05,
        update_heartbeat_interval=0.1,
        stalled_worker_timeout=2,
    )
    worker_task = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            current = await runtime.service.get(operation.id)
            if current is not None and current.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        current = await runtime.service.get(operation.id)
        assert current is not None
        assert current.state == "succeeded"
        async with runtime.pool.connection() as connection:
            row = await connection.execute(
                "SELECT count(*) FROM khub_operation_steps WHERE operation_id = %s",
                (operation.id,),
            )
            assert (await row.fetchone())[0] == 2
            row = await connection.execute(
                """SELECT count(*) FROM procrastinate_jobs
                   WHERE task_name = 'khub:operation:execute.v1'
                     AND args->>'operation_id' = %s""",
                (str(operation.id),),
            )
            assert (await row.fetchone())[0] == 2
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)


@pytest.mark.asyncio
async def test_idempotency_hash_includes_operation_scope(runtime):
    key = f"operation-{uuid4()}"
    await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=f"test:operation:{uuid4()}",
        payload={"value": 1},
        requested_by_user_id=102,
        idempotency_key=key,
    )
    with pytest.raises(IdempotencyConflict):
        await runtime.service.accept(
            kind="maintenance.noop.v1",
            resource_key=f"test:other:{uuid4()}",
            payload={"value": 1},
            requested_by_user_id=102,
            idempotency_key=key,
        )


@pytest.mark.asyncio
async def test_cancel_before_delivery_is_durable(runtime):
    operation = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=f"test:cancel:{uuid4()}",
        payload={},
        requested_by_user_id=103,
        idempotency_key=f"cancel-{uuid4()}",
    )
    cancelled = await runtime.service.cancel(operation.id)
    assert cancelled is not None
    assert cancelled.state == "cancel_requested"

    # A queued job can still be delivered; the operation row remains the
    # authority and the worker converts the step to cancelled.
    async with runtime.pool.connection() as connection:
        step = await runtime.service.store.get_step_for_operation(
            connection, operation.id, 0
        )
    assert step is not None
    await runtime.app.tasks["khub:operation:execute.v1"](
        str(operation.id), str(step.id)
    )
    current = await runtime.service.get(operation.id)
    assert current is not None
    assert current.state == "cancelled"


@pytest.mark.asyncio
async def test_dispatch_started_is_not_replayed_or_cancellable(runtime):
    operation = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=f"test:dispatch:{uuid4()}",
        payload={},
        requested_by_user_id=106,
        idempotency_key=f"dispatch-{uuid4()}",
    )
    async with runtime.pool.connection() as connection:
        step = await runtime.service.store.get_step_for_operation(
            connection, operation.id, 0
        )
    assert step is not None

    now = datetime.now(timezone.utc)
    await runtime.service.mark_dispatch_started(
        operation.id,
        step.id,
        expected_source="head-1",
        expected_target="head-2",
        remote_deadline=now + timedelta(seconds=30),
        observe_not_before=now + timedelta(seconds=31),
        external_marker=f"marker:{operation.id}",
    )

    current = await runtime.service.get(operation.id)
    assert current is not None
    assert current.state == "dispatch_started"

    with pytest.raises(OperationNotCancellable, match="external side-effect boundary"):
        await runtime.service.cancel(operation.id)

    await runtime.app.tasks["khub:operation:execute.v1"](
        str(operation.id), str(step.id)
    )
    current = await runtime.service.get(operation.id)
    assert current is not None
    assert current.state == "dispatch_started"


@pytest.mark.asyncio
async def test_duplicate_delivery_cannot_claim_running_step_twice(runtime):
    operation = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=f"test:claim:{uuid4()}",
        payload={},
        requested_by_user_id=107,
        idempotency_key=f"claim-{uuid4()}",
    )
    async with runtime.pool.connection() as connection:
        step = await runtime.service.store.get_step_for_operation(
            connection, operation.id, 0
        )
    assert step is not None

    async with runtime.pool.connection() as connection:
        async with connection.transaction():
            first = await runtime.service.store.mark_running(
                connection, operation.id, step.id
            )
    async with runtime.pool.connection() as connection:
        async with connection.transaction():
            second = await runtime.service.store.mark_running(
                connection, operation.id, step.id
            )

    assert first is not None and first[2] is True
    assert second is not None and second[2] is False


@pytest.mark.asyncio
async def test_failed_enqueue_rolls_back_domain_rows(runtime):
    class FailingDeferrer:
        async def defer_async(self, **_kwargs):
            raise RuntimeError("injected enqueue failure")

    class FailingTask:
        def configure(self, **_kwargs):
            return FailingDeferrer()

    class FailingApp:
        tasks = {"khub:operation:execute.v1": FailingTask()}

    from kohakuhub.operations.service import OperationService

    service = OperationService(runtime.pool, FailingApp())
    operation_id = None
    with pytest.raises(RuntimeError, match="injected"):
        await service.accept(
            kind="maintenance.noop.v1",
            resource_key=f"test:rollback:{uuid4()}",
            payload={},
            requested_by_user_id=104,
            idempotency_key=f"rollback-{uuid4()}",
        )

    async with runtime.pool.connection() as connection:
        row = await connection.execute(
            "SELECT count(*) FROM khub_repository_operations WHERE requested_by_user_id = 104"
        )
        assert (await row.fetchone())[0] == 0

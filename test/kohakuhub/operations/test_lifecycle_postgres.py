"""Real PostgreSQL tests for the operation acceptance and delivery contract."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from procrastinate.worker import Worker

from kohakuhub.operations.runtime import OperationRuntime
from kohakuhub.operations.reconciliation import reconcile_once
from kohakuhub.operations.registry import HandlerSpec, OperationRegistry
from kohakuhub.operations.service import (
    CommitInProgress,
    IdempotencyConflict,
    OperationNotCancellable,
    QuotaExceeded,
)
from kohakuhub.operations.types import RetryableOperationError, StepResult


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
async def test_external_handler_persists_dispatch_before_side_effect(runtime):
    observed_states: list[tuple[str, str, str | None]] = []

    async def external_handler(_operation, step):
        async with runtime.pool.connection() as connection:
            row = await connection.execute(
                """SELECT o.state, s.state, s.external_marker
                   FROM khub_repository_operations o
                   JOIN khub_operation_steps s ON s.operation_id = o.id
                   WHERE s.id = %s""",
                (step.id,),
            )
            observed_states.append(tuple(await row.fetchone()))
        return StepResult(
            state="succeeded",
            progress_current=1,
            progress_total=1,
            external_effect_confirmed=True,
        )

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="test.external.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=100,
                handler=external_handler,
                external_side_effect=True,
                replay_safe_after_dispatch=True,
            ),
        )
    )
    await runtime.close()
    runtime = await OperationRuntime.open(_database_url(), registry=registry)
    operation = await runtime.service.accept(
        kind="test.external.v1",
        resource_key=f"test:external:{uuid4()}",
        payload={"target": "external-target"},
        requested_by_user_id=None,
        trigger="system",
    )
    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"external-worker-{uuid4()}",
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
        assert current is not None and current.state == "succeeded"
        assert len(observed_states) == 1
        operation_state, step_state, marker = observed_states[0]
        assert (operation_state, step_state) == ("dispatch_started", "dispatch_started")
        assert marker
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        await runtime.close()


@pytest.mark.asyncio
async def test_retryable_handler_redelivers_from_durable_step(runtime):
    calls = 0

    async def retrying_handler(_operation, _step):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RetryableOperationError(
                "transient dependency failure", error_code="dependency_unavailable"
            )
        return StepResult(
            state="succeeded",
            progress_current=1,
            progress_total=1,
            external_effect_confirmed=True,
        )

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="test.retryable.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=100,
                handler=retrying_handler,
                external_side_effect=True,
                replay_safe_after_dispatch=True,
            ),
        )
    )
    await runtime.close()
    runtime = await OperationRuntime.open(_database_url(), registry=registry)
    operation = await runtime.service.accept(
        kind="test.retryable.v1",
        resource_key=f"test:retry:{uuid4()}",
        payload={},
        requested_by_user_id=111,
        idempotency_key=f"retry-{uuid4()}",
    )
    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"retry-worker-{uuid4()}",
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
        for _ in range(150):
            current = await runtime.service.get(operation.id)
            if current is not None and current.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        current = await runtime.service.get(operation.id)
        assert current is not None and current.state == "succeeded"
        assert calls == 3
        async with runtime.pool.connection() as connection:
            row = await runtime.service.store.get_step_for_operation(
                connection, operation.id, 0
            )
        assert row is not None and row.attempt == 3
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        await runtime.close()


@pytest.mark.asyncio
async def test_terminal_handler_failure_is_not_retried(runtime):
    calls = 0

    async def terminal_handler(_operation, _step):
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent failure")

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="test.terminal.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=100,
                handler=terminal_handler,
            ),
        )
    )
    await runtime.close()
    runtime = await OperationRuntime.open(_database_url(), registry=registry)
    operation = await runtime.service.accept(
        kind="test.terminal.v1",
        resource_key=f"test:terminal:{uuid4()}",
        payload={},
        requested_by_user_id=112,
        idempotency_key=f"terminal-{uuid4()}",
    )
    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"terminal-worker-{uuid4()}",
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
            if current is not None and current.state == "failed":
                break
            await asyncio.sleep(0.05)
        current = await runtime.service.get(operation.id)
        assert current is not None and current.state == "failed"
        assert calls == 1
    finally:
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        await runtime.close()


@pytest.mark.asyncio
async def test_cancel_aborts_running_pre_dispatch_handler(runtime):
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_handler(_operation, _step):
        started.set()
        await release.wait()
        return StepResult.succeeded(progress_current=1, progress_total=1)

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="test.cancellable.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=100,
                handler=blocking_handler,
            ),
        )
    )
    await runtime.close()
    runtime = await OperationRuntime.open(_database_url(), registry=registry)
    operation = await runtime.service.accept(
        kind="test.cancellable.v1",
        resource_key=f"test:cancel-running:{uuid4()}",
        payload={},
        requested_by_user_id=113,
        idempotency_key=f"cancel-running-{uuid4()}",
    )
    worker = Worker(
        runtime.app,
        queues=["control-v1"],
        name=f"cancel-worker-{uuid4()}",
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
        await asyncio.wait_for(started.wait(), timeout=5)
        requested = await runtime.service.cancel(operation.id)
        assert requested is not None
        assert requested.state == "cancel_requested"
        for _ in range(100):
            current = await runtime.service.get(operation.id)
            if current is not None and current.state == "cancelled":
                break
            await asyncio.sleep(0.05)
        current = await runtime.service.get(operation.id)
        assert current is not None and current.state == "cancelled"
        async with runtime.pool.connection() as connection:
            step = await runtime.service.store.get_step_for_operation(
                connection, operation.id, 0
            )
        assert step is not None and step.state == "cancelled"
    finally:
        release.set()
        worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_queued_job_converges_without_worker(runtime):
    operation = await runtime.service.accept(
        kind="maintenance.noop.v1",
        resource_key=f"test:cancel-queued:{uuid4()}",
        payload={},
        requested_by_user_id=114,
        idempotency_key=f"cancel-queued-{uuid4()}",
    )
    requested = await runtime.service.cancel(operation.id)
    assert requested is not None and requested.state == "cancel_requested"

    async with runtime.pool.connection() as connection:
        row = await connection.execute(
            """SELECT j.status
               FROM khub_operation_steps s
               JOIN procrastinate_jobs j ON j.id = s.procrastinate_job_id
               WHERE s.operation_id = %s""",
            (operation.id,),
        )
        assert (await row.fetchone())[0] == "cancelled"

    await reconcile_once(runtime.pool, runtime.app)
    current = await runtime.service.get(operation.id)
    assert current is not None and current.state == "cancelled"


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


@pytest.mark.asyncio
async def test_commit_quota_reservation_is_atomic_and_consumed_once(runtime):
    """Concurrent ref commits cannot overbook a shared quota bucket."""

    async with runtime.pool.connection() as connection:
        row = await connection.execute(
            """SELECT r.id, r.owner_id, r.quota_bytes, r.used_bytes,
                      u.public_quota_bytes, u.public_used_bytes
               FROM repository r JOIN "user" u ON u.id = r.owner_id
               ORDER BY r.id LIMIT 1"""
        )
        repository_id, owner_id, repo_quota, repo_used, ns_quota, ns_used = (
            await row.fetchone()
        )

    intent_ids = []
    try:
        async with runtime.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "UPDATE repository SET quota_bytes = 10, used_bytes = 0 WHERE id = %s",
                    (repository_id,),
                )
                await connection.execute(
                    """UPDATE "user"
                       SET public_quota_bytes = 10, public_used_bytes = 0
                       WHERE id = %s""",
                    (owner_id,),
                )

        def payload(ref: str) -> dict:
            return {
                "repository_id": repository_id,
                "repo_type": "model",
                "namespace": "owner",
                "name": "quota-test",
                "lakefs_repo": "quota-test",
                "branch": ref,
                "base_head": "head",
                "is_org": False,
                "is_private": False,
                "quota_delta": 7,
                "file_mutations": [],
                "lfs_tracking": [],
                "author_id": owner_id,
                "owner_id": owner_id,
                "username": "owner",
                "message": "quota test",
                "description": "",
            }

        first_payload = payload(f"quota-a-{uuid4()}")
        first = await runtime.service.prepare_commit_intent(
            repository_id=repository_id,
            ref=first_payload["branch"],
            base_head="head",
            payload=first_payload,
            requested_by_user_id=owner_id,
            idempotency_key=f"quota-{uuid4()}",
            request_hash=uuid4().hex,
            quota_delta=7,
        )
        intent_ids.append(first.id)

        with pytest.raises(QuotaExceeded):
            second_payload = payload(f"quota-b-{uuid4()}")
            await runtime.service.prepare_commit_intent(
                repository_id=repository_id,
                ref=second_payload["branch"],
                base_head="head",
                payload=second_payload,
                requested_by_user_id=owner_id,
                idempotency_key=f"quota-{uuid4()}",
                request_hash=uuid4().hex,
                quota_delta=7,
            )

        updated = await runtime.service.update_prepared_commit_payload(
            first.id, payload=first_payload
        )
        commit_id = f"quota-commit-{uuid4()}"
        await runtime.service.mark_commit_intent_committed(
            first.id,
            lakefs_commit_id=commit_id,
            result_json={"commitOid": commit_id},
        )
        await runtime.service.finalize_commit_intent(
            first.id,
            payload=updated.payload_json,
            result_json={"commitOid": commit_id},
            requested_by_user_id=owner_id,
            idempotency_key=f"quota-finalize-{uuid4()}",
        )

        async with runtime.pool.connection() as connection:
            row = await connection.execute(
                """SELECT state, reserved_bytes FROM khub_quota_reservations
                   WHERE intent_id = %s""",
                (first.id,),
            )
            assert (await row.fetchone()) == ("consumed", 7)
            row = await connection.execute(
                "SELECT used_bytes FROM repository WHERE id = %s", (repository_id,)
            )
            assert (await row.fetchone())[0] == 7
            row = await connection.execute(
                "SELECT public_used_bytes FROM \"user\" WHERE id = %s",
                (owner_id,),
            )
            assert (await row.fetchone())[0] == 7
    finally:
        async with runtime.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "DELETE FROM khub_commit_intents WHERE id = ANY(%s)",
                    (intent_ids,),
                )
                await connection.execute(
                    """UPDATE repository
                       SET quota_bytes = %s, used_bytes = %s WHERE id = %s""",
                    (repo_quota, repo_used, repository_id),
                )
                await connection.execute(
                    """UPDATE "user"
                       SET public_quota_bytes = %s, public_used_bytes = %s
                       WHERE id = %s""",
                    (ns_quota, ns_used, owner_id),
                )


@pytest.mark.asyncio
async def test_unresolved_commit_gets_one_observation_operation(runtime):
    repository_id = 1
    payload = {
        "repository_id": repository_id,
        "repo_type": "model",
        "namespace": "owner",
        "name": "observe-test",
        "lakefs_repo": "observe-test",
        "branch": "main",
        "base_head": "head",
        "staging_paths": [],
        "file_mutations": [],
    }
    intent = await runtime.service.prepare_commit_intent(
        repository_id=repository_id,
        ref="main",
        base_head="head",
        payload=payload,
        requested_by_user_id=1,
        idempotency_key=f"observe-{uuid4()}",
        request_hash=uuid4().hex,
    )
    first = await runtime.service.ensure_commit_observation_operation(intent.id)
    second = await runtime.service.ensure_commit_observation_operation(intent.id)
    assert first is not None
    assert second is not None
    assert first.id == second.id
    assert first.kind == "commit.observe.v1"

    current_intent = await runtime.service.get_commit_intent(intent.id)
    assert current_intent is not None
    assert current_intent.observation_operation_id == first.id

    await runtime.service.mark_commit_intent_dispatch_started(
        intent.id, remote_timeout_seconds=1, quiet_period_seconds=1
    )
    with pytest.raises(OperationNotCancellable, match="affirmative no-effect"):
        await runtime.service.abandon_commit_intent(intent.id)

    await runtime.service.abandon_commit_intent(
        intent.id, error_code="test_no_effect", confirmed_no_effect=True
    )
    observed = await runtime.service.sync_commit_observation_operation(intent.id)
    assert observed is not None
    assert observed.state == "succeeded"

    async with runtime.pool.connection() as connection:
        row = await connection.execute(
            "SELECT state FROM khub_commit_intents WHERE id = %s", (intent.id,)
        )
        assert (await row.fetchone())[0] == "abandoned"
        row = await connection.execute(
            "SELECT state FROM khub_repository_operations WHERE id = %s",
            (first.id,),
        )
        assert (await row.fetchone())[0] == "succeeded"
    async with runtime.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute(
                "DELETE FROM khub_commit_intents WHERE id = %s", (intent.id,)
            )


@pytest.mark.asyncio
async def test_stale_no_effect_settlement_keeps_intent_and_observer_open(runtime):
    intent = await runtime.service.prepare_commit_intent(
        repository_id=1,
        ref="main",
        base_head="stale-base",
        payload={"lakefs_repo": "stale-observer"},
        requested_by_user_id=None,
        idempotency_key=None,
        request_hash=None,
    )
    dispatched = await runtime.service.mark_commit_intent_dispatch_started(
        intent.id, remote_timeout_seconds=1, quiet_period_seconds=1
    )
    observation = await runtime.service.ensure_commit_observation_operation(intent.id)
    assert observation is not None

    # Binding the status operation advances the intent version after the
    # observer captured its stale expected version.
    await runtime.service.abandon_commit_intent(
        intent.id,
        error_code="stale-attempt",
        confirmed_no_effect=True,
        expected_version=dispatched.version,
    )

    current = await runtime.service.get_commit_intent(intent.id)
    assert current is not None
    assert current.state == "dispatch_started"
    status = await runtime.service.get(observation.id)
    assert status is not None
    assert status.state != "succeeded"

    await runtime.service.abandon_commit_intent(
        intent.id, error_code="test_cleanup", confirmed_no_effect=True
    )


@pytest.mark.asyncio
async def test_observation_head_and_cursor_are_version_cas_protected(runtime):
    intent = await runtime.service.prepare_commit_intent(
        repository_id=1,
        ref="main",
        base_head="base-head",
        payload={"lakefs_repo": "observer-cas"},
        requested_by_user_id=None,
        idempotency_key=None,
        request_hash=None,
    )
    try:
        await runtime.service.mark_commit_intent_dispatch_started(
            intent.id, remote_timeout_seconds=1, quiet_period_seconds=1
        )
        current = await runtime.service.get_commit_intent(intent.id)
        assert current is not None

        async with runtime.pool.connection() as connection:
            async with connection.transaction():
                first = await runtime.service.store.set_observation_head(
                    connection,
                    intent.id,
                    "snapshot-head",
                    expected_version=current.version,
                )
                assert first is not None
                stale_head = await runtime.service.store.set_observation_head(
                    connection,
                    intent.id,
                    "different-head",
                    expected_version=current.version,
                )
                assert stale_head is None

        async with runtime.pool.connection() as connection:
            async with connection.transaction():
                assert await runtime.service.store.update_observation_cursor(
                    connection,
                    intent.id,
                    "page-1",
                    expected_version=first.version,
                )
                assert not await runtime.service.store.update_observation_cursor(
                    connection,
                    intent.id,
                    "page-stale",
                    expected_version=first.version,
                )
    finally:
        await runtime.service.abandon_commit_intent(
            intent.id,
            error_code="test_cleanup",
            confirmed_no_effect=True,
        )


@pytest.mark.asyncio
async def test_repository_fence_blocks_canonical_aliases_for_unresolved_commit(runtime):
    repository_id = 700000 + (uuid4().int % 100000)
    intent = await runtime.service.prepare_commit_intent(
        repository_id=repository_id,
        ref="main",
        base_head="head",
        payload={"repository_id": repository_id, "lakefs_repo": "fence-test"},
        requested_by_user_id=None,
        idempotency_key=None,
        request_hash=None,
    )
    try:
        with pytest.raises(CommitInProgress):
            async with runtime.service.repository_ref_fence(
                repository_id, "branch:main"
            ):
                pass
    finally:
        await runtime.service.abandon_commit_intent(
            intent.id,
            error_code="test_cleanup",
            confirmed_no_effect=True,
        )

"""Real dual-lane worker tests against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import AsyncExitStack
from uuid import uuid4

import pytest
from procrastinate.worker import Worker
from psycopg_pool import AsyncConnectionPool

from kohakuhub.operations.service import OperationService
from kohakuhub.operations.registry import HandlerSpec, OperationRegistry
from kohakuhub.operations.types import OperationRecord, StepRecord, StepResult
from kohakuhub.worker.app import build_worker_apps
from kohakuhub.worker.config import WorkerSettings


pytestmark = pytest.mark.integration


def _database_url() -> str:
    value = os.environ.get("KOHAKU_HUB_DATABASE_URL", "")
    if not value.startswith(("postgresql://", "postgres://")):
        pytest.skip("requires PostgreSQL")
    return value


@pytest.mark.asyncio
async def test_control_lane_starts_with_all_work_slots_occupied():
    started = 0
    work_started = asyncio.Event()
    release_work = asyncio.Event()
    control_started = asyncio.Event()

    async def blocking_work(
        _operation: OperationRecord, _step: StepRecord
    ) -> StepResult:
        nonlocal started
        started += 1
        if started == 3:
            work_started.set()
        await release_work.wait()
        return StepResult.succeeded(progress_current=1, progress_total=1)

    async def control_handler(
        _operation: OperationRecord, _step: StepRecord
    ) -> StepResult:
        control_started.set()
        return StepResult.succeeded(progress_current=1, progress_total=1)

    registry = OperationRegistry(
        (
            HandlerSpec(
                kind="test.saturation.work.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="sync-v1",
                priority=10,
                handler=blocking_work,
            ),
            HandlerSpec(
                kind="test.saturation.control.v1",
                version="1",
                task_name="khub:operation:execute.v1",
                queue="control-v1",
                priority=100,
                handler=control_handler,
            ),
        )
    )
    settings = WorkerSettings(
        database_url=_database_url(),
        pool_min_size=1,
        control_pool_max_size=2,
        work_pool_max_size=6,
        polling_interval_seconds=0.05,
        heartbeat_interval_seconds=0.1,
        stalled_worker_timeout_seconds=5,
    )
    control_app, work_app = build_worker_apps(settings, registry=registry)
    pool = AsyncConnectionPool(
        conninfo=settings.database_url, min_size=1, max_size=4, open=False
    )
    await pool.open(wait=True)
    control_worker = Worker(
        control_app,
        queues=["control-v1"],
        name=f"test-control-{uuid4()}",
        concurrency=1,
        wait=True,
        install_signal_handlers=False,
        fetch_job_polling_interval=0.05,
        abort_job_polling_interval=0.05,
        update_heartbeat_interval=0.1,
        stalled_worker_timeout=5,
    )
    work_worker = Worker(
        work_app,
        queues=["sync-v1", "bulk-v1", "cleanup-v1"],
        name=f"test-work-{uuid4()}",
        concurrency=3,
        wait=True,
        install_signal_handlers=False,
        fetch_job_polling_interval=0.05,
        abort_job_polling_interval=0.05,
        update_heartbeat_interval=0.1,
        stalled_worker_timeout=5,
    )
    control_task = work_task = None
    stack = AsyncExitStack()
    await stack.__aenter__()
    try:
        await stack.enter_async_context(control_app.open_async())
        await stack.enter_async_context(work_app.open_async())
        control_task = asyncio.create_task(control_worker.run())
        work_task = asyncio.create_task(work_worker.run())
        work_service = OperationService(
            pool, control_app, registry, database_url=settings.database_url
        )
        for _ in range(3):
            await work_service.accept(
                kind="test.saturation.work.v1",
                resource_key=f"saturation-work:{uuid4()}",
                payload={},
                requested_by_user_id=None,
                trigger="system",
            )
        await asyncio.wait_for(work_started.wait(), timeout=5)
        accepted_at = time.monotonic()
        control_operation = await work_service.accept(
            kind="test.saturation.control.v1",
            resource_key=f"saturation-control:{uuid4()}",
            payload={},
            requested_by_user_id=None,
            trigger="system",
        )
        await asyncio.wait_for(control_started.wait(), timeout=3)
        assert time.monotonic() - accepted_at < 3
        release_work.set()
        for _ in range(100):
            current = await work_service.get(control_operation.id)
            if current is not None and current.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        current = await work_service.get(control_operation.id)
        assert current is not None and current.state == "succeeded"
    finally:
        release_work.set()
        control_worker.stop()
        work_worker.stop()
        if control_task is not None and work_task is not None:
            await asyncio.gather(control_task, work_task, return_exceptions=True)
        await stack.__aexit__(None, None, None)
        await pool.close()

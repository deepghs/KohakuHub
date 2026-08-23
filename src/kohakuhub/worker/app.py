"""Procrastinate application factory for KHub workers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from procrastinate import App
from procrastinate.psycopg_connector import PsycopgConnector
from procrastinate.retry import RetryStrategy

from kohakuhub.operations.executor import execute_operation_step
from kohakuhub.operations.types import RetryableOperationError, operation_max_attempts
from kohakuhub.operations.registry import DEFAULT_REGISTRY, OperationRegistry
from kohakuhub.operations.reconciliation import reconcile_once

from .config import CONTROL_QUEUE, SYNC_QUEUE, WorkerSettings


def build_worker_app(
    settings: WorkerSettings,
    *,
    connector: Any | None = None,
    registry: OperationRegistry = DEFAULT_REGISTRY,
    include_periodic: bool = True,
) -> App:
    """Build one task registry for a supervised worker lane.

    The connector is injectable so registry and supervisor tests can run without
    a database. Production callers use one shared connector/pool for both lanes.
    """

    if connector is None:
        connector = PsycopgConnector(
            conninfo=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
        )

    app = App(connector=connector)
    # The executor uses this only to open a dedicated advisory-fence
    # connection for registered repository mutation handlers. It is never
    # persisted in an operation payload.
    app.khub_database_url = settings.database_url
    app.khub_fence_connection_limit = settings.worker_fence_max_connections
    app.khub_stalled_worker_timeout_seconds = settings.stalled_worker_timeout_seconds

    @app.task(
        name="khub:worker:probe.v1",
        queue=CONTROL_QUEUE,
        priority=100,
    )
    async def worker_probe() -> None:
        """A harmless control task used for readiness and smoke tests."""

    @app.task(
        name="khub:operation:execute.v1",
        queue=SYNC_QUEUE,
        priority=0,
        retry=RetryStrategy(
            max_attempts=operation_max_attempts(),
            exponential_wait=2,
            retry_exceptions=(RetryableOperationError,),
        ),
    )
    async def operation_execute(operation_id: str, step_id: str) -> None:
        """Execute one code-registered, bounded operation step."""

        await execute_operation_step(
            connector.pool,
            operation_id,
            step_id,
            registry=registry,
            app=app,
        )

    @app.task(
        name="khub:worker:reconcile.v1",
        queue=CONTROL_QUEUE,
        priority=110,
        queueing_lock="khub:worker:reconcile",
    )
    async def reconcile_operations(timestamp: int | None = None) -> None:
        """Repair missing deliveries and observe external commit intents."""

        await reconcile_once(
            connector.pool,
            app,
            registry=registry,
            database_url=settings.database_url,
            retention_hours=settings.operation_retention_hours,
            retention_batch_size=settings.operation_retention_batch_size,
        )

    if include_periodic:
        # Only the control App owns periodic deferral.  The work App registers
        # the same task name for delivery but has no periodic registry, so a
        # second worker lane cannot create duplicate scheduler activity.
        app.periodic(
            cron="*/30 * * * * *",
            periodic_id="operation-reconciliation-v1",
            queue=CONTROL_QUEUE,
            priority=110,
            queueing_lock="khub:worker:reconcile",
        )(reconcile_operations)

    return app


def build_worker_apps(
    settings: WorkerSettings,
    *,
    registry: OperationRegistry = DEFAULT_REGISTRY,
) -> tuple[App, App]:
    """Build the control and work Apps inside one khub-worker service."""

    control_connector = PsycopgConnector(
        conninfo=settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.control_pool_max_size,
    )
    work_connector = PsycopgConnector(
        conninfo=settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.work_pool_max_size,
    )
    limiter_owner = SimpleNamespace()
    control_app = build_worker_app(
        settings,
        connector=control_connector,
        registry=registry,
        include_periodic=True,
    )
    work_app = build_worker_app(
        settings,
        connector=work_connector,
        registry=registry,
        include_periodic=False,
    )
    # Both lanes belong to one worker process and therefore share its fence
    # connection budget even though Procrastinate uses separate App objects.
    control_app.khub_fence_limiter_owner = limiter_owner
    work_app.khub_fence_limiter_owner = limiter_owner
    return control_app, work_app

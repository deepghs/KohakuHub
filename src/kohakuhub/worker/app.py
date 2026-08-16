"""Procrastinate application factory for KHub workers."""

from __future__ import annotations

from typing import Any

from procrastinate import App
from procrastinate.psycopg_connector import PsycopgConnector

from kohakuhub.operations.executor import execute_operation_step
from kohakuhub.operations.registry import DEFAULT_REGISTRY, OperationRegistry

from .config import CONTROL_QUEUE, SYNC_QUEUE, WorkerSettings


def build_worker_app(
    settings: WorkerSettings,
    *,
    connector: Any | None = None,
    registry: OperationRegistry = DEFAULT_REGISTRY,
) -> App:
    """Build one task registry used by both supervised worker lanes.

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

    return app

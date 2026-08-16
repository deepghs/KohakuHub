"""API-process lifecycle for the operation enqueue client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg_pool import AsyncConnectionPool
from procrastinate.psycopg_connector import PsycopgConnector

from kohakuhub.worker.app import build_worker_app
from kohakuhub.worker.config import WorkerSettings

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .service import OperationService


@dataclass
class OperationRuntime:
    pool: AsyncConnectionPool
    connector: PsycopgConnector
    app: Any
    service: OperationService

    @classmethod
    async def open(
        cls,
        database_url: str,
        *,
        registry: OperationRegistry = DEFAULT_REGISTRY,
    ) -> "OperationRuntime":
        pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=4,
            open=False,
        )
        await pool.open(wait=True)
        connector = PsycopgConnector()
        settings = WorkerSettings(database_url=database_url, pool_max_size=4)
        worker_app = build_worker_app(
            settings,
            connector=connector,
            registry=registry,
        )
        await worker_app.open_async(pool=pool)
        return cls(
            pool=pool,
            connector=connector,
            app=worker_app,
            service=OperationService(pool, worker_app, registry),
        )

    async def close(self) -> None:
        await self.app.close_async()
        await self.pool.close()

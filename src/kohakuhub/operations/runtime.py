"""API-process lifecycle for the operation enqueue client."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import suppress
from typing import Any

from psycopg_pool import AsyncConnectionPool
from procrastinate.psycopg_connector import PsycopgConnector

from kohakuhub.worker.app import build_worker_app
from kohakuhub.worker.config import (
    WorkerSettings,
    api_fence_connection_limit_from_env,
    api_operation_pool_max_size_from_env,
)

from .registry import DEFAULT_REGISTRY, OperationRegistry
from .service import OperationService
from .readiness import verify_operation_schema


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
        api_pool_max_size = api_operation_pool_max_size_from_env()
        pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=api_pool_max_size,
            open=False,
        )
        worker_app: Any | None = None
        try:
            await pool.open(wait=True)
            async with pool.connection() as connection:
                await verify_operation_schema(connection)
            connector = PsycopgConnector()
            settings = WorkerSettings(
                database_url=database_url,
                pool_max_size=api_pool_max_size,
                api_pool_max_size=api_pool_max_size,
            )
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
                service=OperationService(
                    pool,
                    worker_app,
                    registry,
                    database_url=database_url,
                    fence_connection_limit=api_fence_connection_limit_from_env(),
                ),
            )
        except BaseException:
            if worker_app is not None:
                with suppress(Exception):
                    await worker_app.close_async()
            with suppress(Exception):
                await pool.close()
            raise

    async def close(self) -> None:
        try:
            await self.app.close_async()
        finally:
            await self.pool.close()

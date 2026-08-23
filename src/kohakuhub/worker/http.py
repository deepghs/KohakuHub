"""Small worker health and metrics HTTP surface."""

from __future__ import annotations

import asyncio
from typing import Any

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from kohakuhub.operations.metrics import METRICS_REGISTRY


async def serve_worker_http(
    host: str,
    port: int,
    ready: asyncio.Event,
) -> web.AppRunner | None:
    """Start the worker's metrics/readiness server, if a port is configured."""

    if port <= 0:
        return None

    application = web.Application()

    async def metrics(_request: web.Request) -> web.Response:
        return web.Response(
            body=generate_latest(METRICS_REGISTRY),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    async def readyz(_request: web.Request) -> web.Response:
        if not ready.is_set():
            return web.json_response(
                {"ready": False, "reason": "worker_not_ready"}, status=503
            )
        return web.json_response({"ready": True})

    async def livez(_request: web.Request) -> web.Response:
        return web.json_response({"alive": True})

    application.router.add_get("/metrics", metrics)
    application.router.add_get("/readyz", readyz)
    application.router.add_get("/healthz", livez)
    runner = web.AppRunner(application)
    await runner.setup()
    await web.TCPSite(runner, host=host, port=port).start()
    return runner


async def close_worker_http(runner: Any | None) -> None:
    if runner is not None:
        await runner.cleanup()

"""HTTP contract tests for worker liveness, readiness, and metrics."""

from __future__ import annotations

import asyncio
import socket

import aiohttp
import pytest

from kohakuhub.worker.http import close_worker_http, serve_worker_http


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.asyncio
async def test_worker_http_reports_readiness_liveness_and_metrics():
    ready = asyncio.Event()
    runner = await serve_worker_http("127.0.0.1", _free_port(), ready)
    assert runner is not None
    port = runner.addresses[0][1]

    try:
        async with aiohttp.ClientSession() as client:
            response = await client.get(f"http://127.0.0.1:{port}/readyz")
            assert response.status == 503
            assert await response.json() == {
                "ready": False,
                "reason": "worker_not_ready",
            }

            response = await client.get(f"http://127.0.0.1:{port}/healthz")
            assert response.status == 200
            assert await response.json() == {"alive": True}

            ready.set()
            response = await client.get(f"http://127.0.0.1:{port}/readyz")
            assert response.status == 200
            assert await response.json() == {"ready": True}

            response = await client.get(f"http://127.0.0.1:{port}/metrics")
            assert response.status == 200
            assert "khub_" in await response.text()
    finally:
        await close_worker_http(runner)


@pytest.mark.asyncio
async def test_worker_http_can_be_disabled():
    assert await serve_worker_http("127.0.0.1", 0, asyncio.Event()) is None

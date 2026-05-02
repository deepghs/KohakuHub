"""Tests for the bulk-replace admin endpoint (#78).

The previous chain-tester ``/admin/api/fallback/test/simulate`` and
``/admin/api/fallback/test/real`` endpoints were retired in favour of
the chain tester sending real production requests from the browser and
reading the ``X-Chain-Trace`` response header — see
``test_with_repo_fallback_trace.py`` for the trace-emission coverage.

What remains here is the **bulk-replace** path used by the chain
tester's "Push to system" button: an atomic transactional replace of
every ``FallbackSource`` row, plus its surrounding contract (cache
clear, validation, admin-token gating, 500 catch-all).
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# bulk-replace
# ---------------------------------------------------------------------------


async def test_bulk_replace_swaps_entire_source_list(
    admin_client, backend_test_state
):
    """Add three rows in one shot, then swap to a different two rows.

    Confirms ``before`` / ``after`` counts and that intermediate state
    is not visible (atomic transactional swap).
    """
    cache = backend_test_state.modules.fallback_cache_module.get_cache()
    initial_global_gen = cache.global_gen

    # First swap: 3 rows.
    r1 = await admin_client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={
            "sources": [
                {
                    "namespace": "", "url": "https://a.example/",
                    "token": None, "priority": 10, "name": "A",
                    "source_type": "huggingface", "enabled": True,
                },
                {
                    "namespace": "", "url": "https://b.example/",
                    "token": None, "priority": 20, "name": "B",
                    "source_type": "huggingface", "enabled": True,
                },
                {
                    "namespace": "ns1", "url": "https://c.example/",
                    "token": None, "priority": 30, "name": "C",
                    "source_type": "kohakuhub", "enabled": False,
                },
            ],
        },
    )
    assert r1.status_code == 200, r1.text
    body1 = r1.json()
    assert body1["success"] is True
    assert body1["after"] == 3
    # Cache cleared, global_gen bumped.
    assert cache.global_gen == initial_global_gen + 1

    list_r = await admin_client.get("/admin/api/fallback-sources")
    assert {s["name"] for s in list_r.json()} == {"A", "B", "C"}

    # Second swap: 2 entirely-different rows.
    r2 = await admin_client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={
            "sources": [
                {
                    "namespace": "", "url": "https://d.example",
                    "priority": 5, "name": "D",
                    "source_type": "huggingface", "enabled": True,
                },
                {
                    "namespace": "", "url": "https://e.example",
                    "priority": 6, "name": "E",
                    "source_type": "huggingface", "enabled": True,
                },
            ],
        },
    )
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["before"] == 3
    assert body2["after"] == 2
    assert cache.global_gen == initial_global_gen + 2

    final_list = await admin_client.get("/admin/api/fallback-sources")
    assert {s["name"] for s in final_list.json()} == {"D", "E"}


async def test_bulk_replace_with_empty_list_clears_table(
    admin_client, backend_test_state
):
    # Seed one row first.
    seed = await admin_client.post(
        "/admin/api/fallback-sources",
        json={
            "namespace": "", "url": "https://seed.example",
            "name": "Seed", "source_type": "huggingface",
            "priority": 1, "enabled": True,
        },
    )
    assert seed.status_code == 200

    response = await admin_client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={"sources": []},
    )
    assert response.status_code == 200
    assert response.json()["after"] == 0


async def test_bulk_replace_rejects_invalid_source_type(admin_client):
    response = await admin_client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={
            "sources": [
                {
                    "namespace": "", "url": "https://x.example",
                    "name": "X", "source_type": "totally-bogus",
                    "priority": 1, "enabled": True,
                },
            ],
        },
    )
    assert response.status_code == 400
    assert "Invalid source_type" in response.json()["detail"]["error"]


async def test_bulk_replace_requires_admin_token(client):
    response = await client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={"sources": []},
    )
    assert response.status_code in (401, 403)


async def test_bulk_replace_500_path(
    admin_client, monkeypatch, backend_test_state
):
    """Force the inner FallbackSource.create to raise so we hit the 500
    catch-all that builds a friendly error envelope."""
    from kohakuhub.db import FallbackSource

    def boom(*_a, **_k):
        raise RuntimeError("synthetic db error")

    monkeypatch.setattr(FallbackSource, "create", boom)

    response = await admin_client.put(
        "/admin/api/fallback/sources-bulk-replace",
        json={
            "sources": [
                {
                    "namespace": "", "url": "https://x.example",
                    "name": "X", "source_type": "huggingface",
                    "priority": 1, "enabled": True,
                },
            ],
        },
    )
    assert response.status_code == 500
    assert "Bulk-replace failed" in response.json()["detail"]["error"]

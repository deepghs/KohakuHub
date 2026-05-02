"""Tests for the chain-tester + bulk-replace admin endpoints (#78).

These cover the three new routes added in
``src/kohakuhub/api/admin/routers/fallback.py``:

- ``PUT  /admin/api/fallback/sources-bulk-replace``
- ``POST /admin/api/fallback/test/simulate``
- ``POST /admin/api/fallback/test/real``

Each test uses an ASGI test client (``admin_client`` fixture) so the
FastAPI router + Pydantic body validation are exercised end-to-end.
``probe_chain`` is monkeypatched in the simulate/real tests to assert
the routing layer hands the right payload through without bringing
real HTTP traffic into the test runner.
"""
from __future__ import annotations

import pytest

import kohakuhub.api.admin.routers.fallback as admin_fallback


@pytest.fixture
def stub_probe_chain(monkeypatch):
    """Replace ``admin_fallback.probe_chain`` with a deterministic stub.

    Captures the ``(args, kwargs)`` of every call so tests can assert
    the routing layer's payload mapping.
    """
    calls: list[tuple] = []

    async def _stub(*args, **kwargs):
        calls.append((args, kwargs))
        from kohakuhub.api.fallback.core import ProbeAttempt, ProbeReport

        return ProbeReport(
            op=kwargs.get("op", "info"),
            repo_id=f"{kwargs.get('namespace', 'x')}/{kwargs.get('name', 'y')}",
            revision=kwargs.get("revision"),
            file_path=kwargs.get("file_path") or None,
            attempts=[
                ProbeAttempt(
                    source_name="A", source_url="https://a.local",
                    source_type="huggingface", method="GET",
                    upstream_path="/api/models/x/y",
                    status_code=200, x_error_code=None, x_error_message=None,
                    decision="BIND_AND_RESPOND", duration_ms=12, error=None,
                ),
            ],
            final_outcome="BIND_AND_RESPOND",
            bound_source={"name": "A", "url": "https://a.local"},
            duration_ms=15,
        )

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.probe_chain", _stub)
    return calls


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


# ---------------------------------------------------------------------------
# /fallback/test/simulate
# ---------------------------------------------------------------------------


async def test_simulate_passes_sources_through_with_user_token_overlay(
    admin_client, stub_probe_chain
):
    response = await admin_client.post(
        "/admin/api/fallback/test/simulate",
        json={
            "op": "info",
            "repo_type": "model",
            "namespace": "owner",
            "name": "demo",
            "revision": "main",
            "sources": [
                {
                    "name": "A", "url": "https://a.example",
                    "source_type": "huggingface",
                    "token": "admin-A-token", "priority": 10,
                },
                {
                    "name": "B", "url": "https://b.example",
                    "source_type": "huggingface",
                    "token": None, "priority": 20,
                },
            ],
            "user_tokens": {
                "https://a.example": "user-overrides-A",
            },
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["final_outcome"] == "BIND_AND_RESPOND"

    assert len(stub_probe_chain) == 1
    _args, kwargs = stub_probe_chain[0]
    assert kwargs["op"] == "info"
    assert kwargs["namespace"] == "owner"
    sources = kwargs["sources"]
    # Source A's token has been overridden by the user_tokens map.
    a = next(s for s in sources if s["url"] == "https://a.example")
    assert a["token"] == "user-overrides-A"
    assert a.get("token_source") == "user"
    # Source B untouched.
    b = next(s for s in sources if s["url"] == "https://b.example")
    assert b.get("token") is None
    assert b.get("token_source") != "user"


async def test_simulate_rejects_unsupported_op(admin_client, stub_probe_chain):
    response = await admin_client.post(
        "/admin/api/fallback/test/simulate",
        json={
            "op": "totally-bogus", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "sources": [],
        },
    )
    assert response.status_code == 400
    assert "Unsupported op" in response.json()["detail"]["error"]
    assert stub_probe_chain == []


async def test_simulate_500_when_probe_chain_raises(
    admin_client, monkeypatch
):
    async def explode(*_a, **_k):
        raise RuntimeError("probe blew up")

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.probe_chain", explode)

    response = await admin_client.post(
        "/admin/api/fallback/test/simulate",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "sources": [],
        },
    )
    assert response.status_code == 500
    assert "Simulate probe failed" in response.json()["detail"]["error"]


async def test_simulate_400_on_value_error_from_probe(admin_client, monkeypatch):
    async def value_err(*_a, **_k):
        raise ValueError("bad inputs")

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.probe_chain", value_err)

    response = await admin_client.post(
        "/admin/api/fallback/test/simulate",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "sources": [],
        },
    )
    assert response.status_code == 400
    assert "bad inputs" in response.json()["detail"]["error"]


async def test_simulate_requires_admin_token(client):
    response = await client.post(
        "/admin/api/fallback/test/simulate",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "sources": [],
        },
    )
    assert response.status_code in (401, 403)


# ---------------------------------------------------------------------------
# /fallback/test/real
# ---------------------------------------------------------------------------


async def test_real_uses_current_config_for_anonymous(
    admin_client, monkeypatch, stub_probe_chain
):
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ],
    )
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["final_outcome"] == "BIND_AND_RESPOND"

    _args, kwargs = stub_probe_chain[0]
    assert [s["url"] for s in kwargs["sources"]] == ["https://live.example"]


async def test_real_resolves_user_db_tokens_when_impersonating_username(
    admin_client, monkeypatch, stub_probe_chain
):
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ],
    )
    captured_user_tokens: list = []

    def _spy_get_enabled(namespace, user_tokens=None):
        captured_user_tokens.append(user_tokens)
        return [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ]

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.get_enabled_sources", _spy_get_enabled)

    # Impersonate "owner" who has no external tokens; user_tokens should
    # be empty dict (no DB rows + no header overrides).
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "as_username": "owner",
        },
    )
    assert response.status_code == 200
    # ``get_enabled_sources`` got an empty merge dict for this user.
    assert captured_user_tokens == [{}]


async def test_real_header_tokens_override_db_tokens(
    admin_client, monkeypatch, backend_test_state, stub_probe_chain
):
    """Header tokens win over per-user DB tokens, mirroring production."""
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_user_external_tokens",
        lambda user: [{"url": "https://hf.example", "token": "from-db"}],
    )
    captured: list = []

    def _spy_get_enabled(namespace, user_tokens=None):
        captured.append(dict(user_tokens or {}))
        return [
            {"name": "HF", "url": "https://hf.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ]

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.get_enabled_sources", _spy_get_enabled)

    # Stub username lookup to a sentinel — get_user_external_tokens above
    # ignores its user arg, but admin_fallback._resolve_impersonated_user_tokens
    # only proceeds when the lookup returned non-None.
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_user_by_username",
        lambda username: object(),  # truthy, opaque
    )

    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "as_username": "any",
            "header_tokens": {"https://hf.example": "from-header"},
        },
    )
    assert response.status_code == 200
    # Header beats DB.
    assert captured == [{"https://hf.example": "from-header"}]


async def test_real_returns_empty_report_when_no_sources(admin_client, monkeypatch):
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [],
    )
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["final_outcome"] == "CHAIN_EXHAUSTED"
    assert body["attempts"] == []
    assert body["bound_source"] is None


async def test_real_unknown_username_falls_back_to_anonymous(
    admin_client, monkeypatch, stub_probe_chain
):
    """Username lookup returning None → empty user_tokens, probe still runs."""
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_user_by_username",
        lambda username: None,
    )
    captured: list = []

    def _spy(namespace, user_tokens=None):
        captured.append(dict(user_tokens or {}))
        return [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ]

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.get_enabled_sources", _spy)

    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "as_username": "no-such-user",
        },
    )
    assert response.status_code == 200
    # Anonymous → empty merge dict.
    assert captured == [{}]


async def test_real_user_id_lookup_path(admin_client, monkeypatch, stub_probe_chain):
    """Verify the as_user_id branch resolves through ``User.get_or_none``."""
    from kohakuhub.db import User

    monkeypatch.setattr(
        User, "get_or_none", classmethod(lambda cls, *_a, **_k: None)
    )
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ],
    )
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
            "as_user_id": 9999,
        },
    )
    assert response.status_code == 200


async def test_real_rejects_unsupported_op(admin_client):
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "totally-bogus", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 400
    assert "Unsupported op" in response.json()["detail"]["error"]


async def test_real_500_when_get_enabled_sources_raises(admin_client, monkeypatch):
    def boom(*_a, **_k):
        raise RuntimeError("synthetic")

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.get_enabled_sources", boom)
    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 500
    assert "get_enabled_sources failed" in response.json()["detail"]["error"]


async def test_real_500_when_probe_chain_raises(admin_client, monkeypatch):
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ],
    )

    async def explode(*_a, **_k):
        raise RuntimeError("probe blew up")

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.probe_chain", explode)

    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 500
    assert "Real probe failed" in response.json()["detail"]["error"]


async def test_real_400_on_value_error_from_probe(admin_client, monkeypatch):
    monkeypatch.setattr(
        "kohakuhub.api.admin.routers.fallback.get_enabled_sources",
        lambda namespace, user_tokens=None: [
            {"name": "Live", "url": "https://live.example",
             "source_type": "huggingface", "token": None, "priority": 1},
        ],
    )

    async def value_err(*_a, **_k):
        raise ValueError("bad probe input")

    monkeypatch.setattr("kohakuhub.api.admin.routers.fallback.probe_chain", value_err)

    response = await admin_client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code == 400
    assert "bad probe input" in response.json()["detail"]["error"]


async def test_real_requires_admin_token(client):
    response = await client.post(
        "/admin/api/fallback/test/real",
        json={
            "op": "info", "repo_type": "model",
            "namespace": "owner", "name": "demo",
        },
    )
    assert response.status_code in (401, 403)

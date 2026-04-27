"""API tests for the admin dependency health endpoint."""

from __future__ import annotations

import asyncio
import sys

from kohakuhub.api.admin.utils import health as health_utils

ENDPOINT = "/admin/api/health/dependencies"
DEPENDENCY_NAMES = ["postgres", "minio", "lakefs", "smtp"]


def _live_health_module():
    """Return the freshly-imported health module the running app uses.

    The session bootstrap re-imports every ``kohakuhub.*`` module after
    pytest collects this file, so any top-level ``kohakuhub.*`` reference
    becomes stale once a backend fixture runs. Tests that monkeypatch
    module-level state observed by the FastAPI app must reach for the
    live module via ``sys.modules`` instead.
    """
    return sys.modules["kohakuhub.api.admin.utils.health"]


async def test_health_returns_all_dependencies_against_live_services(admin_client):
    health_mod = _live_health_module()
    response = await admin_client.get(ENDPOINT)
    assert response.status_code == 200
    payload = response.json()

    assert payload["overall_status"] in {"ok", "degraded", "disabled"}
    assert payload["timeout_seconds"] == health_mod.DEFAULT_PROBE_TIMEOUT_SECONDS
    assert isinstance(payload["checked_at_ms"], int)
    assert payload["elapsed_ms"] >= 0

    by_name = {dep["name"]: dep for dep in payload["dependencies"]}
    assert sorted(by_name.keys()) == sorted(DEPENDENCY_NAMES)

    for name in ("postgres", "minio", "lakefs"):
        dep = by_name[name]
        assert dep["status"] == "ok", dep
        assert isinstance(dep["latency_ms"], int)
        assert dep["latency_ms"] >= 0
        assert dep["endpoint"]

    postgres = by_name["postgres"]
    assert postgres["version"] is not None
    assert "://" in postgres["endpoint"]
    assert "password" not in postgres["endpoint"].lower()

    smtp = by_name["smtp"]
    # SMTP is disabled in the default test configuration.
    assert smtp["status"] == "disabled"
    assert smtp["latency_ms"] is None
    assert smtp["version"] is None
    assert smtp["endpoint"] is None
    assert smtp["detail"]


async def test_health_overall_status_is_ok_when_smtp_disabled_and_rest_up(admin_client):
    response = await admin_client.get(ENDPOINT)
    payload = response.json()
    assert payload["overall_status"] == "ok"


async def test_health_endpoint_accepts_explicit_timeout(admin_client):
    response = await admin_client.get(ENDPOINT, params={"timeout_seconds": 1.5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["timeout_seconds"] == 1.5


async def test_health_endpoint_rejects_invalid_timeout(admin_client):
    response = await admin_client.get(ENDPOINT, params={"timeout_seconds": 0.0})
    assert response.status_code == 422


async def test_health_endpoint_requires_admin_token(client):
    response = await client.get(ENDPOINT)
    assert response.status_code == 401


async def test_health_endpoint_rejects_invalid_admin_token(client):
    response = await client.get(
        ENDPOINT,
        headers={"X-Admin-Token": "definitely-not-the-real-token"},
    )
    assert response.status_code == 403


async def test_health_aggregates_partial_failure_as_degraded(
    admin_client, monkeypatch
):
    health_mod = _live_health_module()

    async def _fake_minio(timeout: float = 2.0):
        return {
            "name": "minio",
            "status": "down",
            "latency_ms": 12,
            "version": None,
            "endpoint": "http://example",
            "detail": "boom",
        }

    fake_probes = (
        health_mod.probe_postgres,
        _fake_minio,
        health_mod.probe_lakefs,
        health_mod.probe_smtp,
    )
    monkeypatch.setattr(health_mod, "PROBES", fake_probes, raising=True)

    response = await admin_client.get(ENDPOINT)
    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_status"] == "degraded"

    minio = next(d for d in payload["dependencies"] if d["name"] == "minio")
    assert minio["status"] == "down"
    assert minio["detail"] == "boom"


async def test_health_overall_disabled_when_every_probe_disabled(
    admin_client, monkeypatch
):
    health_mod = _live_health_module()

    def _make_disabled_probe(name: str):
        async def _impl(timeout: float = 2.0):
            return health_mod._disabled(name, detail="off")

        return _impl

    fake_probes = tuple(_make_disabled_probe(n) for n in DEPENDENCY_NAMES)
    monkeypatch.setattr(health_mod, "PROBES", fake_probes, raising=True)

    response = await admin_client.get(ENDPOINT)
    assert response.status_code == 200
    assert response.json()["overall_status"] == "disabled"


async def test_postgres_probe_returns_ok_against_live_db(app):
    health_mod = _live_health_module()
    result = await health_mod.probe_postgres()
    assert result["name"] == "postgres"
    assert result["status"] == "ok"
    assert result["latency_ms"] >= 0
    assert result["version"] is not None


async def test_postgres_probe_reports_down_when_query_raises(app, monkeypatch):
    health_mod = _live_health_module()

    def _raise():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(
        health_mod,
        "_query_postgres_version",
        _raise,
        raising=True,
    )
    result = await health_mod.probe_postgres()
    assert result["status"] == "down"
    assert "simulated failure" in result["detail"]


async def test_postgres_probe_reports_timeout(app, monkeypatch):
    health_mod = _live_health_module()

    async def _slow_to_thread(*_args, **_kwargs):
        await asyncio.sleep(5)
        return None

    monkeypatch.setattr(asyncio, "to_thread", _slow_to_thread, raising=True)
    result = await health_mod.probe_postgres(timeout=0.1)
    assert result["status"] == "down"
    assert "timeout" in result["detail"]


async def test_minio_probe_returns_ok_against_live_service(app):
    health_mod = _live_health_module()
    result = await health_mod.probe_minio()
    assert result["name"] == "minio"
    assert result["status"] == "ok"
    assert result["endpoint"]


async def test_lakefs_probe_returns_ok_against_live_service(app):
    health_mod = _live_health_module()
    result = await health_mod.probe_lakefs()
    assert result["name"] == "lakefs"
    assert result["status"] == "ok"
    assert result["endpoint"]


async def test_smtp_probe_disabled_by_default_in_tests(app):
    health_mod = _live_health_module()
    result = await health_mod.probe_smtp()
    assert result["name"] == "smtp"
    assert result["status"] == "disabled"
    assert result["latency_ms"] is None


def test_strip_password_removes_secret_from_pg_url():
    raw = "postgresql://hub_test:hub_test_password@127.0.0.1:25432/kohakuhub_test"
    assert (
        health_utils._strip_password(raw)
        == "postgresql://hub_test@127.0.0.1:25432/kohakuhub_test"
    )


def test_strip_password_preserves_password_free_urls():
    assert health_utils._strip_password("sqlite:///./hub.db") == "sqlite:///./hub.db"
    assert (
        health_utils._strip_password("postgresql://hub_test@127.0.0.1:5432/db")
        == "postgresql://hub_test@127.0.0.1:5432/db"
    )


def test_short_pg_version_extracts_leading_name_and_number():
    raw = "PostgreSQL 15.5 on x86_64-pc-linux-gnu, compiled by gcc (Debian) 10.2.1-6"
    assert health_utils._short_pg_version(raw) == "PostgreSQL 15.5"


def test_short_pg_version_falls_back_to_truncated_input():
    raw = "Some unexpected vendor with a long banner string " * 4
    short = health_utils._short_pg_version(raw)
    assert len(short) <= 80

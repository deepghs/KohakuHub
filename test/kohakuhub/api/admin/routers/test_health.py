"""API tests for the admin dependency health endpoint."""

from __future__ import annotations

import asyncio
import sys

import pytest

from kohakuhub.api.admin.utils import health as health_utils

ENDPOINT = "/admin/api/health/dependencies"
DEPENDENCY_NAMES = ["postgres", "minio", "lakefs", "redis", "smtp"]


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

    # Redis is ``ok`` on the main matrix (Valkey service container reachable
    # via KOHAKU_HUB_CACHE_URL) and ``disabled`` on the dedicated
    # cache-disabled CI job (KOHAKU_HUB_CACHE_ENABLED=false). Both states
    # are valid for this assertion — what we're guarding is that the probe
    # exists, mirrors the live cfg.cache.enabled toggle, and never spills
    # an exception into the dependency list.
    redis = by_name["redis"]
    if health_mod.cfg.cache.enabled:
        assert redis["status"] == "ok", redis
        assert isinstance(redis["latency_ms"], int)
        assert redis["latency_ms"] >= 0
        assert redis["endpoint"]
        # The probe distinguishes Valkey from Redis in the version string;
        # CI runs Valkey, so we expect a "Valkey x.y.z" prefix.
        assert redis["version"] and redis["version"].startswith(("Valkey ", "Redis "))
    else:
        assert redis["status"] == "disabled", redis
        assert redis["latency_ms"] is None
        assert redis["version"] is None
        assert redis["endpoint"] is None
        assert redis["detail"]


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
        health_mod.probe_redis,
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
    # The CI MinIO image must report a real release tag in addition to the
    # default "Server: MinIO" header — that is the regression this whole
    # admin-probe path exists to prevent.
    assert result["version"] and result["version"] != "MinIO"
    assert result["version"].startswith("MinIO ")


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


def test_extract_minio_release_handles_modern_payload():
    payload = {
        "mode": "online",
        "deploymentID": "abc",
        "servers": [
            {
                "state": "online",
                "version": "2025-09-07T16:13:09Z",
                "commitID": "deadbeef",
            }
        ],
    }
    assert (
        health_utils._extract_minio_release(payload) == "2025-09-07T16:13:09Z"
    )


def test_extract_minio_release_falls_through_legacy_keys():
    # 2018-era servers used "Build" instead of "version".
    payload = {"servers": [{"Build": "RELEASE.2018-08-23"}]}
    assert (
        health_utils._extract_minio_release(payload) == "RELEASE.2018-08-23"
    )


def test_extract_minio_release_handles_top_level_version():
    # Some early dev builds and S3-shaped fakes report version at the top.
    payload = {"version": "2024-01-01T00:00:00Z"}
    assert (
        health_utils._extract_minio_release(payload) == "2024-01-01T00:00:00Z"
    )


def test_extract_minio_release_returns_none_for_unknown_payloads():
    assert health_utils._extract_minio_release(None) is None
    assert health_utils._extract_minio_release({}) is None
    assert health_utils._extract_minio_release({"servers": []}) is None
    assert (
        health_utils._extract_minio_release(
            {"servers": [{"state": "online"}]}
        )
        is None
    )


async def test_fetch_minio_admin_version_falls_back_when_endpoint_returns_403(
    app, monkeypatch
):
    """Non-MinIO endpoints (AWS, R2, Ceph) typically 403 the admin path."""
    import httpx as httpx_module

    health_mod = _live_health_module()

    def _handler(_request: httpx_module.Request) -> httpx_module.Response:
        return httpx_module.Response(403, text="AccessDenied")

    real_async_client = httpx_module.AsyncClient
    transport = httpx_module.MockTransport(_handler)

    def _factory(*_args, **kwargs):
        return real_async_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr(
        health_mod.httpx, "AsyncClient", _factory, raising=True
    )
    assert await health_mod._fetch_minio_admin_version(timeout=1.0) is None


async def test_fetch_minio_admin_version_returns_release_for_signed_response(
    app, monkeypatch
):
    """A 200 OK with a MinIO-shaped payload yields the release tag."""
    import httpx as httpx_module
    import json as json_module

    health_mod = _live_health_module()

    def _handler(request: httpx_module.Request) -> httpx_module.Response:
        # The probe must include an Authorization header for SigV4 — we are
        # not validating the signature value, only that the wire shape is
        # what MinIO would expect.
        assert "Authorization" in request.headers
        assert request.headers["Authorization"].startswith("AWS4-HMAC-SHA256 ")
        assert request.headers.get("x-amz-content-sha256")
        assert request.headers.get("x-amz-date")
        return httpx_module.Response(
            200,
            content=json_module.dumps(
                {
                    "mode": "online",
                    "servers": [{"version": "2024-12-13T22-19-12Z"}],
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
        )

    real_async_client = httpx_module.AsyncClient
    transport = httpx_module.MockTransport(_handler)

    def _factory(*_args, **kwargs):
        return real_async_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr(
        health_mod.httpx, "AsyncClient", _factory, raising=True
    )
    release = await health_mod._fetch_minio_admin_version(timeout=1.0)
    assert release == "2024-12-13T22-19-12Z"


async def test_minio_probe_reports_down_when_list_buckets_raises(app, monkeypatch):
    health_mod = _live_health_module()

    def _raise():
        raise RuntimeError("simulated s3 failure")

    monkeypatch.setattr(
        health_mod, "_list_buckets_sync", _raise, raising=True
    )
    result = await health_mod.probe_minio()
    assert result["status"] == "down"
    assert "simulated s3 failure" in result["detail"]
    assert result["endpoint"]


async def test_minio_probe_reports_timeout(app, monkeypatch):
    health_mod = _live_health_module()

    async def _slow_to_thread(*_args, **_kwargs):
        await asyncio.sleep(5)
        return None

    monkeypatch.setattr(asyncio, "to_thread", _slow_to_thread, raising=True)
    result = await health_mod.probe_minio(timeout=0.1)
    assert result["status"] == "down"
    assert "timeout" in result["detail"]


async def test_lakefs_probe_reports_down_when_healthcheck_returns_5xx(
    app, monkeypatch
):
    import httpx as httpx_module

    health_mod = _live_health_module()

    def _handler(request: httpx_module.Request) -> httpx_module.Response:
        return httpx_module.Response(503, text="boom")

    real_async_client = httpx_module.AsyncClient
    transport = httpx_module.MockTransport(_handler)

    def _factory(*_args, **kwargs):
        return real_async_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr(
        health_mod.httpx, "AsyncClient", _factory, raising=True
    )
    result = await health_mod.probe_lakefs()
    assert result["status"] == "down"
    assert "503" in result["detail"]


async def test_lakefs_probe_reports_down_when_healthcheck_raises(
    app, monkeypatch
):
    import httpx as httpx_module

    health_mod = _live_health_module()

    def _handler(_request: httpx_module.Request) -> httpx_module.Response:
        raise httpx_module.ConnectError("simulated network error")

    real_async_client = httpx_module.AsyncClient
    transport = httpx_module.MockTransport(_handler)

    def _factory(*_args, **kwargs):
        return real_async_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr(
        health_mod.httpx, "AsyncClient", _factory, raising=True
    )
    result = await health_mod.probe_lakefs()
    assert result["status"] == "down"
    assert "simulated network error" in result["detail"]


async def test_lakefs_probe_keeps_ok_when_only_version_lookup_fails(
    app, monkeypatch
):
    import httpx as httpx_module

    health_mod = _live_health_module()

    def _handler(request: httpx_module.Request) -> httpx_module.Response:
        if request.url.path.endswith("/healthcheck"):
            return httpx_module.Response(204)
        return httpx_module.Response(401, text="auth required")

    real_async_client = httpx_module.AsyncClient
    transport = httpx_module.MockTransport(_handler)

    def _factory(*_args, **kwargs):
        return real_async_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr(
        health_mod.httpx, "AsyncClient", _factory, raising=True
    )
    result = await health_mod.probe_lakefs()
    assert result["status"] == "ok"
    assert result["version"] is None


async def test_smtp_probe_reports_ok_when_enabled_and_banner_is_returned(
    app, monkeypatch
):
    health_mod = _live_health_module()

    monkeypatch.setattr(health_mod.cfg.smtp, "enabled", True, raising=True)

    def _fake_smtp(_timeout):
        return "mail.example ESMTP ready"

    monkeypatch.setattr(
        health_mod, "_smtp_probe_sync", _fake_smtp, raising=True
    )
    result = await health_mod.probe_smtp()
    assert result["status"] == "ok"
    assert result["version"] == "mail.example ESMTP ready"
    assert result["endpoint"] == f"{health_mod.cfg.smtp.host}:{health_mod.cfg.smtp.port}"


async def test_smtp_probe_reports_down_when_ehlo_raises(app, monkeypatch):
    health_mod = _live_health_module()

    monkeypatch.setattr(health_mod.cfg.smtp, "enabled", True, raising=True)

    def _raise(_timeout):
        raise OSError("connection refused")

    monkeypatch.setattr(
        health_mod, "_smtp_probe_sync", _raise, raising=True
    )
    result = await health_mod.probe_smtp()
    assert result["status"] == "down"
    assert "connection refused" in result["detail"]


async def test_smtp_probe_reports_timeout(app, monkeypatch):
    health_mod = _live_health_module()

    monkeypatch.setattr(health_mod.cfg.smtp, "enabled", True, raising=True)

    async def _slow_to_thread(*_args, **_kwargs):
        await asyncio.sleep(10)
        return None

    monkeypatch.setattr(asyncio, "to_thread", _slow_to_thread, raising=True)
    result = await health_mod.probe_smtp(timeout=0.1)
    assert result["status"] == "down"
    assert "timeout" in result["detail"]


async def test_query_postgres_version_handles_sqlite_branch(app, monkeypatch):
    health_mod = _live_health_module()

    class _Cursor:
        def __init__(self, payload):
            self._payload = payload

        def fetchone(self):
            return self._payload

    class _FakeDb:
        def __init__(self):
            self.calls = []

        def execute_sql(self, sql):
            self.calls.append(sql.strip())
            if "sqlite_version" in sql:
                return _Cursor(("3.40.0",))
            return _Cursor((1,))

    fake_db = _FakeDb()
    monkeypatch.setattr(health_mod, "db", fake_db, raising=True)
    monkeypatch.setattr(health_mod.cfg.app, "db_backend", "sqlite", raising=True)
    version = health_mod._query_postgres_version()
    assert version == "SQLite 3.40.0"
    assert fake_db.calls == ["SELECT 1", "SELECT sqlite_version()"]


def test_strip_password_returns_input_when_urlsplit_raises(monkeypatch):
    """urlsplit raises ValueError on invalid IPv6 literals, e.g. unmatched ``[``."""

    def _raise(_url):
        raise ValueError("invalid url")

    monkeypatch.setattr(health_utils, "urlsplit", _raise, raising=True)
    assert health_utils._strip_password("anything") == "anything"


async def test_smtp_probe_sync_uses_smtplib_and_returns_first_banner_line(
    app, monkeypatch
):
    health_mod = _live_health_module()

    class _FakeSMTP:
        instances = []

        def __init__(self, host, port, timeout):
            self.host = host
            self.port = port
            self.timeout = timeout
            self.quit_called = False
            self.closed = False
            _FakeSMTP.instances.append(self)

        def ehlo(self):
            return 250, b"mail.example greets you\nfollow-up line"

        def quit(self):
            self.quit_called = True

        def close(self):
            self.closed = True

    monkeypatch.setattr(health_mod.smtplib, "SMTP", _FakeSMTP, raising=True)
    banner = health_mod._smtp_probe_sync(2.0)
    assert banner == "mail.example greets you"
    assert _FakeSMTP.instances and _FakeSMTP.instances[0].quit_called


async def test_smtp_probe_sync_raises_when_ehlo_returns_error_code(
    app, monkeypatch
):
    health_mod = _live_health_module()

    class _ErrorSMTP:
        def __init__(self, host, port, timeout):
            pass

        def ehlo(self):
            return 421, b"go away"

        def quit(self):
            raise health_mod.smtplib.SMTPException("quit failed")

        def close(self):
            pass

    monkeypatch.setattr(health_mod.smtplib, "SMTP", _ErrorSMTP, raising=True)
    with pytest.raises(health_mod.smtplib.SMTPResponseException):
        health_mod._smtp_probe_sync(2.0)


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


# ---------------------------------------------------------------------------
# Redis (cache) probe — see probe_redis in admin/utils/health.py
# ---------------------------------------------------------------------------


async def test_redis_probe_returns_ok_against_live_service(app):
    """End-to-end probe against the CI Valkey service.

    Skips on the cache-disabled CI matrix (the probe correctly returns
    ``disabled`` there; that path is exercised by the dedicated test
    below).
    """
    health_mod = _live_health_module()
    if not health_mod.cfg.cache.enabled:
        pytest.skip("cache.enabled is false in this environment")

    result = await health_mod.probe_redis()
    assert result["name"] == "redis"
    assert result["status"] == "ok", result
    assert result["latency_ms"] >= 0
    assert result["endpoint"] and result["endpoint"].startswith("redis://")
    assert result["version"] and result["version"].startswith(("Valkey ", "Redis "))


async def test_redis_probe_returns_disabled_when_cache_off(app, monkeypatch):
    """When cache.enabled is False, the probe MUST short-circuit to
    ``disabled`` — never attempt a connection.
    """
    health_mod = _live_health_module()
    monkeypatch.setattr(health_mod.cfg.cache, "enabled", False, raising=True)
    result = await health_mod.probe_redis()
    assert result["name"] == "redis"
    assert result["status"] == "disabled"
    assert result["latency_ms"] is None
    assert result["version"] is None
    assert result["endpoint"] is None
    assert "disabled" in result["detail"].lower()


async def test_redis_probe_reports_down_when_unreachable(app, monkeypatch):
    """Point the probe at a port nothing is listening on and assert it
    surfaces ``down`` with the underlying connect error.
    """
    health_mod = _live_health_module()
    monkeypatch.setattr(health_mod.cfg.cache, "enabled", True, raising=True)
    monkeypatch.setattr(
        health_mod.cfg.cache, "url", "redis://127.0.0.1:1/0", raising=True
    )
    result = await health_mod.probe_redis(timeout=0.5)
    assert result["status"] == "down", result
    assert result["latency_ms"] is not None
    # Don't pin the exact string — different platforms phrase the
    # connect failure differently. Just confirm it carries SOMETHING.
    assert result["detail"]


async def test_redis_probe_reports_down_on_ping_timeout(app, monkeypatch):
    """A live Valkey that PINGs slower than the probe timeout must be
    classified ``down`` with the timeout message — not raised, not
    silently ``ok``.
    """
    import redis.asyncio as aioredis

    health_mod = _live_health_module()

    class _SlowClient:
        async def ping(self):
            await asyncio.sleep(5)
            return True

        async def info(self, _section):
            return {}

        async def aclose(self):
            return None

    def _factory(*_args, **_kwargs):
        return _SlowClient()

    monkeypatch.setattr(aioredis, "from_url", _factory, raising=True)
    monkeypatch.setattr(health_mod.cfg.cache, "enabled", True, raising=True)
    result = await health_mod.probe_redis(timeout=0.1)
    assert result["status"] == "down"
    assert "timeout" in result["detail"].lower()

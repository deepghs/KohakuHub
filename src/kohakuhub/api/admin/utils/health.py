"""Health probes for the admin dependency dashboard.

Each probe verifies a single external dependency with a tight timeout and
returns a small dict describing the result. Probes never raise: any failure
is captured as a ``down`` entry so the aggregator can surface partial
outages without one slow component blocking the rest.
"""

from __future__ import annotations

import asyncio
import re
import smtplib
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from kohakuhub.config import cfg
from kohakuhub.db import db
from kohakuhub.logger import get_logger
from kohakuhub.utils.s3 import get_s3_client

logger = get_logger("ADMIN")

DEFAULT_PROBE_TIMEOUT_SECONDS = 2.0

# PostgreSQL "version()" output is verbose ("PostgreSQL 15.5 on x86_64-pc-linux-gnu, ...").
# We only surface the leading name + numeric version to keep the UI compact.
_PG_VERSION_PATTERN = re.compile(r"^(PostgreSQL\s+\d+(?:\.\d+)*)")


def _strip_password(url: str) -> str:
    """Return ``url`` with any embedded password removed."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if not parts.password:
        return url

    user = parts.username or ""
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    netloc = f"{user}@{host}" if user else host
    return urlunsplit(parts._replace(netloc=netloc))


def _short_pg_version(raw: str) -> str:
    match = _PG_VERSION_PATTERN.match(raw)
    return match.group(1) if match else raw[:80]


def _ms_since(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def _ok(
    name: str,
    *,
    start: float,
    version: str | None,
    endpoint: str | None,
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "ok",
        "latency_ms": _ms_since(start),
        "version": version,
        "endpoint": endpoint,
        "detail": detail,
    }


def _down(
    name: str,
    *,
    endpoint: str | None,
    detail: str,
    latency_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "down",
        "latency_ms": latency_ms,
        "version": None,
        "endpoint": endpoint,
        "detail": detail,
    }


def _disabled(name: str, *, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "status": "disabled",
        "latency_ms": None,
        "version": None,
        "endpoint": None,
        "detail": detail,
    }


def _query_postgres_version() -> str | None:
    """Run a liveness query plus a backend-aware version lookup."""
    db.execute_sql("SELECT 1").fetchone()
    if cfg.app.db_backend == "sqlite":
        row = db.execute_sql("SELECT sqlite_version()").fetchone()
        return f"SQLite {row[0]}" if row and row[0] else None
    row = db.execute_sql("SELECT version()").fetchone()
    if row and row[0]:
        return _short_pg_version(row[0])
    return None


async def probe_postgres(
    timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Probe the configured database backend."""
    start = time.perf_counter()
    endpoint = _strip_password(cfg.app.database_url)
    try:
        version = await asyncio.wait_for(
            asyncio.to_thread(_query_postgres_version),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return _down(
            "postgres",
            endpoint=endpoint,
            detail=f"timeout after {int(timeout * 1000)}ms",
            latency_ms=_ms_since(start),
        )
    except Exception as exc:
        logger.warning(f"postgres probe failed: {exc}")
        return _down(
            "postgres",
            endpoint=endpoint,
            detail=str(exc) or exc.__class__.__name__,
            latency_ms=_ms_since(start),
        )

    return _ok(
        "postgres",
        start=start,
        version=version,
        endpoint=endpoint,
    )


def _list_buckets_sync() -> str | None:
    s3 = get_s3_client()
    response = s3.list_buckets()
    server = (
        response.get("ResponseMetadata", {})
        .get("HTTPHeaders", {})
        .get("server")
    )
    return server


async def probe_minio(
    timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Probe S3 / MinIO via list_buckets, capturing the Server header."""
    start = time.perf_counter()
    endpoint = cfg.s3.endpoint
    try:
        server = await asyncio.wait_for(
            asyncio.to_thread(_list_buckets_sync),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return _down(
            "minio",
            endpoint=endpoint,
            detail=f"timeout after {int(timeout * 1000)}ms",
            latency_ms=_ms_since(start),
        )
    except Exception as exc:
        logger.warning(f"minio probe failed: {exc}")
        return _down(
            "minio",
            endpoint=endpoint,
            detail=str(exc) or exc.__class__.__name__,
            latency_ms=_ms_since(start),
        )

    return _ok(
        "minio",
        start=start,
        version=server,
        endpoint=endpoint,
    )


async def probe_lakefs(
    timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Probe LakeFS via /healthcheck plus an optional version lookup.

    The healthcheck endpoint is unauthenticated and authoritative for liveness;
    the version endpoint requires Basic Auth. If the version call fails but
    healthcheck succeeded, the probe still reports ``ok`` with an unknown
    version so a misconfigured admin token does not mask a healthy LakeFS.
    """
    start = time.perf_counter()
    endpoint = cfg.lakefs.endpoint.rstrip("/")
    base = f"{endpoint}/api/v1"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            health = await client.get(f"{base}/healthcheck")
            if not health.is_success:
                return _down(
                    "lakefs",
                    endpoint=endpoint,
                    detail=(
                        f"healthcheck returned "
                        f"{health.status_code} {health.reason_phrase}"
                    ),
                    latency_ms=_ms_since(start),
                )

            version: str | None = None
            try:
                version_resp = await client.get(
                    f"{base}/config/version",
                    auth=(cfg.lakefs.access_key, cfg.lakefs.secret_key),
                )
                if version_resp.is_success:
                    payload = version_resp.json()
                    version = (
                        payload.get("version")
                        or payload.get("Version")
                        or None
                    )
            except (httpx.HTTPError, ValueError) as exc:
                logger.debug(f"lakefs version lookup skipped: {exc}")
    except (httpx.HTTPError, asyncio.TimeoutError) as exc:
        return _down(
            "lakefs",
            endpoint=endpoint,
            detail=str(exc) or exc.__class__.__name__,
            latency_ms=_ms_since(start),
        )

    return _ok(
        "lakefs",
        start=start,
        version=version,
        endpoint=endpoint,
    )


def _smtp_probe_sync(timeout: float) -> str | None:
    """Open a TCP+EHLO session against the configured SMTP host."""
    smtp = smtplib.SMTP(
        host=cfg.smtp.host,
        port=cfg.smtp.port,
        timeout=timeout,
    )
    try:
        code, response = smtp.ehlo()
        text: str | None
        if isinstance(response, bytes):
            text = response.decode("utf-8", errors="replace")
        else:
            text = str(response) if response is not None else None
        if text:
            text = text.splitlines()[0].strip()[:120] or None
        if code and code >= 400:
            raise smtplib.SMTPResponseException(code, text or "")
        return text
    finally:
        try:
            smtp.quit()
        except smtplib.SMTPException:
            smtp.close()


async def probe_smtp(
    timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Probe SMTP only when ``smtp.enabled`` is true.

    The probe is intentionally TCP+EHLO only — it never authenticates or sends
    mail, so it cannot trigger lockouts on misconfigured providers.
    """
    if not cfg.smtp.enabled:
        return _disabled(
            "smtp",
            detail="SMTP is disabled in configuration (smtp.enabled = false)",
        )

    start = time.perf_counter()
    endpoint = f"{cfg.smtp.host}:{cfg.smtp.port}"
    try:
        banner = await asyncio.wait_for(
            asyncio.to_thread(_smtp_probe_sync, timeout),
            timeout=timeout + 0.5,
        )
    except asyncio.TimeoutError:
        return _down(
            "smtp",
            endpoint=endpoint,
            detail=f"timeout after {int(timeout * 1000)}ms",
            latency_ms=_ms_since(start),
        )
    except Exception as exc:
        logger.warning(f"smtp probe failed: {exc}")
        return _down(
            "smtp",
            endpoint=endpoint,
            detail=str(exc) or exc.__class__.__name__,
            latency_ms=_ms_since(start),
        )

    return _ok(
        "smtp",
        start=start,
        version=banner,
        endpoint=endpoint,
    )


PROBES = (
    probe_postgres,
    probe_minio,
    probe_lakefs,
    probe_smtp,
)


async def run_all_probes(
    timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Run every probe concurrently and return their results in stable order."""
    results = await asyncio.gather(
        *(probe(timeout) for probe in PROBES),
        return_exceptions=False,
    )
    return list(results)

"""Pure chain-probe primitive used by the admin debug surface (#78).

``probe_chain`` walks an explicitly-supplied source list, performs the
appropriate HTTP probe per source for the requested operation, classifies
each response with ``utils.classify_upstream``, and returns a structured
``ProbeReport`` capturing every attempt plus the final binding outcome.

Differences from the production path in ``operations.py``:

- **No cache.** Doesn't read or write the per-process ``RepoSourceCache``;
  doesn't take the per-repo binding lock. Safe for repeated tester
  invocations without polluting the live binding state.
- **No FastAPI request context.** Sources, tokens, and probe params come
  in as plain values — no dependency on FastAPI's ``Request`` /
  ``Depends`` machinery. Callable from unit tests, scripts, and the
  admin debug endpoint alike.
- **Single-call per source.** Where ``operations._resolve_one_source``
  does HEAD-then-GET and commits to a bound source for the GET phase,
  the tester probe issues exactly one HTTP call per source matched to
  the operation type (HEAD for ``resolve``, GET for ``info`` / ``tree``,
  POST for ``paths_info``). The single-call surface is enough to show
  what each source returns and which one would bind.

The classifier behaviour matches ``utils.classify_upstream`` exactly,
so a tester verdict (``BIND_AND_RESPOND`` / ``BIND_AND_PROPAGATE`` /
``TRY_NEXT_SOURCE``) on a source carries the same meaning as the
production path's verdict on that source given identical input.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

import httpx

from kohakuhub.logger import get_logger
from kohakuhub.api.fallback.client import FallbackClient
from kohakuhub.api.fallback.utils import (
    FallbackDecision,
    classify_upstream,
)

logger = get_logger("FALLBACK_CORE")

ProbeOp = str  # "resolve" | "info" | "tree" | "paths_info"
SUPPORTED_OPS = ("resolve", "info", "tree", "paths_info")


# Response-body preview cap. Long bodies (especially HF metadata JSON
# pages) can run to ~50–100 kB; capping the preview keeps the
# ProbeReport JSON small enough to render in a browser timeline without
# DOM-thrash. Caller can re-issue the probe directly if they need
# more.
_BODY_PREVIEW_LIMIT = 4096

# Curated headers worth surfacing in the UI per attempt — the rest are
# internal CDN bookkeeping not useful for a chain-debug timeline.
_RELEVANT_HEADERS = {
    "content-type",
    "content-length",
    "etag",
    "location",
    "x-error-code",
    "x-error-message",
    "x-linked-size",
    "x-repo-commit",
    "x-source",
    "x-source-url",
    "x-source-status",
    "www-authenticate",
}


def _preview_body(response: httpx.Response) -> Optional[str]:
    """Return a UTF-8 preview of the response body, truncated.

    Falls back to a ``[binary, N bytes]`` placeholder when the body
    isn't decodable (e.g. resolve target is a model weight file).
    """
    if response is None:
        return None
    raw = response.content or b""
    if not raw:
        return ""
    truncated = raw[:_BODY_PREVIEW_LIMIT]
    try:
        text = truncated.decode("utf-8")
    except UnicodeDecodeError:
        return f"[binary, {len(raw)} bytes]"
    if len(raw) > _BODY_PREVIEW_LIMIT:
        return text + f"\n…[truncated, total {len(raw)} bytes]"
    return text


def _curated_headers(response: httpx.Response) -> dict[str, str]:
    if response is None:
        return {}
    return {
        k.lower(): v
        for k, v in response.headers.items()
        if k.lower() in _RELEVANT_HEADERS
    }


@dataclass
class ProbeAttempt:
    """A single source's probe result.

    ``decision`` is a string-name version of ``FallbackDecision`` plus
    transport-failure outcomes (``NETWORK_ERROR`` / ``TIMEOUT``).

    ``response_body_preview`` is the first ``_BODY_PREVIEW_LIMIT`` bytes
    of the upstream's response body, decoded as UTF-8 when possible.
    ``response_headers`` is a curated subset (see ``_RELEVANT_HEADERS``).
    Both are ``None`` when the per-source request raised before a
    response was produced (timeout / network failure).
    """

    source_name: str
    source_url: str
    source_type: str
    method: str
    upstream_path: str
    status_code: Optional[int]
    x_error_code: Optional[str]
    x_error_message: Optional[str]
    decision: str
    duration_ms: int
    error: Optional[str]
    response_body_preview: Optional[str] = None
    response_headers: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProbeReport:
    """Aggregate result of a tester probe.

    ``final_response`` mirrors what a production caller would receive
    given the same probe inputs:

    - ``BIND_AND_RESPOND`` / ``BIND_AND_PROPAGATE``: the bound source's
      response (status, headers, body preview).
    - ``CHAIN_EXHAUSTED``: ``None`` here (the production aggregate
      response is built by ``utils.build_aggregate_failure_response``
      and depends on op-specific ``aggregate_scope`` semantics that
      the tester deliberately does not bake in — the per-attempt list
      is enough to diagnose what each source said).
    """

    op: ProbeOp
    repo_id: str
    revision: Optional[str]
    file_path: Optional[str]
    attempts: list[ProbeAttempt] = field(default_factory=list)
    final_outcome: str = "CHAIN_EXHAUSTED"
    bound_source: Optional[dict] = None
    duration_ms: int = 0
    final_response: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "op": self.op,
            "repo_id": self.repo_id,
            "revision": self.revision,
            "file_path": self.file_path,
            "attempts": [a.to_dict() for a in self.attempts],
            "final_outcome": self.final_outcome,
            "bound_source": self.bound_source,
            "duration_ms": self.duration_ms,
            "final_response": self.final_response,
        }


def _build_kohaku_path(
    op: ProbeOp,
    repo_type: str,
    namespace: str,
    name: str,
    revision: str,
    file_path: str,
) -> str:
    """Construct the upstream path the chain will hit for one op.

    Mirrors the path shapes used by ``operations.try_fallback_*`` so
    a tester verdict aligns 1:1 with the production probe's verdict.
    """
    repo_id = f"{namespace}/{name}"
    if op == "resolve":
        return f"/{repo_type}s/{repo_id}/resolve/{revision}/{file_path}"
    if op == "tree":
        clean = (file_path or "").lstrip("/")
        return f"/api/{repo_type}s/{repo_id}/tree/{revision}/{clean}".rstrip("/")
    if op == "paths_info":
        return f"/api/{repo_type}s/{repo_id}/paths-info/{revision}"
    if op == "info":
        return f"/api/{repo_type}s/{repo_id}"
    raise ValueError(f"Unsupported probe op: {op!r}")  # pragma: no cover


async def _probe_one_source(
    source: dict,
    op: ProbeOp,
    repo_type: str,
    kohaku_path: str,
    paths: Optional[list[str]],
    client_factory,
) -> ProbeAttempt:
    """Run a single per-source HTTP probe + classification.

    Builds a ``ProbeAttempt`` regardless of outcome — including
    timeouts, network errors, and unexpected exceptions — so callers
    can present a complete row per source even when something blew up.
    """
    method = "HEAD" if op == "resolve" else (
        "POST" if op == "paths_info" else "GET"
    )
    started = time.perf_counter()
    response: Optional[httpx.Response] = None
    err: Optional[str] = None
    decision_name: str

    client = client_factory(
        source_url=source["url"],
        source_type=source.get("source_type", "huggingface"),
        token=source.get("token"),
    )

    try:
        if op == "resolve":
            response = await client.head(kohaku_path, repo_type)
        elif op == "paths_info":
            response = await client.post(
                kohaku_path,
                repo_type,
                data={"paths": paths or [], "expand": False},
            )
        elif op == "tree":
            response = await client.get(
                kohaku_path, repo_type, params={"recursive": False, "expand": False}
            )
        else:  # info
            response = await client.get(kohaku_path, repo_type)
    except httpx.TimeoutException as e:
        err = f"timeout: {e}"
        decision_name = "TIMEOUT"
    except Exception as e:  # network / unknown
        err = f"{type(e).__name__}: {e}"
        decision_name = "NETWORK_ERROR"

    duration_ms = int((time.perf_counter() - started) * 1000)

    if response is None:
        return ProbeAttempt(
            source_name=source.get("name", source["url"]),
            source_url=source["url"],
            source_type=source.get("source_type", "huggingface"),
            method=method,
            upstream_path=kohaku_path,
            status_code=None,
            x_error_code=None,
            x_error_message=None,
            decision=decision_name,
            duration_ms=duration_ms,
            error=err,
            response_body_preview=None,
            response_headers={},
        )

    decision = classify_upstream(response)
    decision_name = decision.name
    return ProbeAttempt(
        source_name=source.get("name", source["url"]),
        source_url=source["url"],
        source_type=source.get("source_type", "huggingface"),
        method=method,
        upstream_path=kohaku_path,
        status_code=response.status_code,
        x_error_code=response.headers.get("x-error-code"),
        x_error_message=response.headers.get("x-error-message"),
        decision=decision_name,
        duration_ms=duration_ms,
        error=None,
        response_body_preview=_preview_body(response),
        response_headers=_curated_headers(response),
    )


async def probe_chain(
    op: ProbeOp,
    repo_type: str,
    namespace: str,
    name: str,
    sources: list[dict],
    *,
    revision: str = "main",
    file_path: str = "",
    paths: Optional[list[str]] = None,
    client_factory=None,
) -> ProbeReport:
    """Walk the chain. Stop at first BIND_*. Return a ProbeReport.

    Args:
        op: One of ``"resolve" / "info" / "tree" / "paths_info"``.
        repo_type: ``"model" / "dataset" / "space"``.
        namespace: Repository namespace.
        name: Repository name.
        sources: Priority-ordered list of source dicts. Each must have
            ``url``; may have ``name`` (default = url), ``source_type``
            (default ``huggingface``), ``token`` (default ``None``).
        revision: Branch / commit, only used for resolve / tree / paths_info.
        file_path: File path (resolve, tree).
        paths: List of paths (paths_info).
        client_factory: For tests — defaults to ``FallbackClient``.

    Returns:
        ``ProbeReport`` with one ``ProbeAttempt`` per source consulted
        (chain stops at the first BIND_* outcome). ``final_outcome`` is
        ``BIND_AND_RESPOND`` / ``BIND_AND_PROPAGATE`` / ``CHAIN_EXHAUSTED``.
    """
    if op not in SUPPORTED_OPS:
        raise ValueError(
            f"Unsupported probe op: {op!r}. Expected one of {SUPPORTED_OPS}."
        )

    factory = client_factory or FallbackClient
    kohaku_path = _build_kohaku_path(
        op, repo_type, namespace, name, revision, file_path
    )

    started = time.perf_counter()
    attempts: list[ProbeAttempt] = []
    bound_source: Optional[dict] = None
    final_outcome = "CHAIN_EXHAUSTED"

    for source in sources:
        attempt = await _probe_one_source(
            source=source,
            op=op,
            repo_type=repo_type,
            kohaku_path=kohaku_path,
            paths=paths,
            client_factory=factory,
        )
        attempts.append(attempt)

        if attempt.decision == FallbackDecision.BIND_AND_RESPOND.name:
            bound_source = source
            final_outcome = "BIND_AND_RESPOND"
            break
        if attempt.decision == FallbackDecision.BIND_AND_PROPAGATE.name:
            bound_source = source
            final_outcome = "BIND_AND_PROPAGATE"
            break
        # TRY_NEXT_SOURCE / TIMEOUT / NETWORK_ERROR — keep walking.

    duration_ms = int((time.perf_counter() - started) * 1000)

    # Final response: for BIND_* outcomes, replay the bound attempt's
    # response shape — that's literally what a production caller would
    # see (modulo the production path's HEAD-then-GET commit for
    # ``resolve``, which the tester intentionally simplifies). For
    # CHAIN_EXHAUSTED we leave ``final_response = None`` since the
    # aggregate envelope production builds depends on
    # ``aggregate_scope`` semantics the tester doesn't impose.
    final_response: Optional[dict] = None
    if final_outcome != "CHAIN_EXHAUSTED" and attempts:
        last = attempts[-1]
        final_response = {
            "status_code": last.status_code,
            "headers": dict(last.response_headers),
            "body_preview": last.response_body_preview,
        }

    report = ProbeReport(
        op=op,
        repo_id=f"{namespace}/{name}",
        revision=revision if op != "info" else None,
        file_path=file_path or None,
        attempts=attempts,
        final_outcome=final_outcome,
        bound_source=bound_source,
        duration_ms=duration_ms,
        final_response=final_response,
    )
    logger.info(
        f"probe_chain {op} {repo_type}/{namespace}/{name}: "
        f"{len(attempts)} attempt(s), final={final_outcome}, "
        f"bound={bound_source.get('name') if bound_source else None}, "
        f"{duration_ms}ms"
    )
    return report

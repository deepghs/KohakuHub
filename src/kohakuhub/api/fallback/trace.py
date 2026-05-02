"""Per-request chain trace, emitted as ``X-Chain-Trace`` response header.

Lets the admin chain-tester (and any external client) read the actual
chain a production request travelled — local backend first, then each
fallback source consulted, with status / X-Error-Code / decision /
duration per hop.

Storage is a ``contextvars.ContextVar`` so concurrent requests don't
overlap. The decorator (``with_repo_fallback``) bootstraps a fresh
trace for its request, records the local hop after calling the wrapped
handler, lets the fallback layer (``operations.try_fallback_*``)
append source hops via ``record_source_hop``, then encodes the final
trace into a single ``X-Chain-Trace: <base64-JSON>`` header on the
outgoing response.

Header format:

    X-Chain-Trace: <base64(json({"version": 1, "hops": [HOP, ...]}))>

Each HOP:

    {
        "kind": "local" | "fallback",
        "source_name": "local" | "<configured source name>",
        "source_url": null | "<source url>",
        "source_type": null | "huggingface" | "kohakuhub",
        "method": "GET" | "HEAD" | "POST",
        "upstream_path": null | "<probe path>",
        "status_code": 200 | null,
        "x_error_code": null | "EntryNotFound" | ...,
        "x_error_message": null | "...",
        "decision": "LOCAL_HIT" | "LOCAL_FILTERED" | "LOCAL_MISS"
                  | "LOCAL_OTHER_ERROR"
                  | "BIND_AND_RESPOND" | "BIND_AND_PROPAGATE"
                  | "TRY_NEXT_SOURCE" | "TIMEOUT" | "NETWORK_ERROR",
        "duration_ms": 12,
        "error": null | "<message>",
    }

The chain stops at the first ``LOCAL_HIT`` / ``LOCAL_FILTERED`` /
``BIND_AND_RESPOND`` / ``BIND_AND_PROPAGATE`` / non-404 ``LOCAL_OTHER_ERROR``.
``LOCAL_MISS`` is the gate condition that causes the fallback chain to
be consulted; subsequent hops are ``fallback`` ``kind``.
"""
from __future__ import annotations

import base64
import json
from contextvars import ContextVar
from typing import Any, Optional

import httpx

from kohakuhub.logger import get_logger
from kohakuhub.api.fallback.utils import FallbackDecision, classify_upstream

logger = get_logger("FALLBACK_TRACE")

X_CHAIN_TRACE = "X-Chain-Trace"

# ContextVar default = None means "no active trace; recording is a no-op".
# The decorator at request entry sets a fresh list. ``record_*`` helpers
# append to whatever list is active. The decorator at request exit reads
# the list and encodes the final header.
_chain_trace: ContextVar[Optional[list[dict]]] = ContextVar(
    "fallback_chain_trace", default=None
)


def start_trace() -> list[dict]:
    """Begin recording a chain trace for the current request.

    Returns the underlying list so the caller can read it later — the
    decorator pattern is:

        hops = start_trace()
        # ... handler runs, helpers append to hops ...
        inject_trace_header(response, hops)

    Calling ``start_trace`` while a trace is already active replaces it
    (no nesting — admin endpoints don't fan out).
    """
    hops: list[dict] = []
    _chain_trace.set(hops)
    return hops


def current_trace() -> Optional[list[dict]]:
    """Return the active hop list, or ``None`` if no trace is recording."""
    return _chain_trace.get()


def record_local_hop(
    *,
    decision: str,
    status_code: Optional[int],
    x_error_code: Optional[str] = None,
    x_error_message: Optional[str] = None,
    duration_ms: int,
    error: Optional[str] = None,
) -> None:
    """Record the local-backend hop. Always the first hop in the trace."""
    hops = current_trace()
    if hops is None:
        return
    hops.append(
        {
            "kind": "local",
            "source_name": "local",
            "source_url": None,
            "source_type": None,
            "method": None,
            "upstream_path": None,
            "status_code": status_code,
            "x_error_code": x_error_code,
            "x_error_message": x_error_message,
            "decision": decision,
            "duration_ms": duration_ms,
            "error": error,
        }
    )


def record_source_hop(
    source: dict,
    *,
    method: str,
    upstream_path: str,
    response: Optional[httpx.Response] = None,
    decision: Optional[FallbackDecision] = None,
    duration_ms: int,
    error: Optional[str] = None,
    transport_decision: Optional[str] = None,
) -> None:
    """Record one fallback-source attempt.

    Two call shapes:

    - Normal HTTP response: pass ``response`` (and optionally a
      pre-computed ``decision`` to avoid re-classifying); status,
      X-Error-Code, X-Error-Message are read from the response headers.

    - Transport failure: pass ``transport_decision`` (``"TIMEOUT"`` or
      ``"NETWORK_ERROR"``) plus ``error`` describing the exception;
      ``response`` is ``None``, status fields default to ``None``.
    """
    hops = current_trace()
    if hops is None:
        return

    if response is not None:
        decision_name = (decision or classify_upstream(response)).name
        status = response.status_code
        x_code = response.headers.get("x-error-code")
        x_msg = response.headers.get("x-error-message")
    else:
        decision_name = transport_decision or "NETWORK_ERROR"
        status = None
        x_code = None
        x_msg = None

    hops.append(
        {
            "kind": "fallback",
            "source_name": source.get("name") or source.get("url", "?"),
            "source_url": source.get("url"),
            "source_type": source.get("source_type"),
            "method": method,
            "upstream_path": upstream_path,
            "status_code": status,
            "x_error_code": x_code,
            "x_error_message": x_msg,
            "decision": decision_name,
            "duration_ms": duration_ms,
            "error": error,
        }
    )


def encode_trace_header(hops: list[dict]) -> str:
    """Encode a hop list to the ``X-Chain-Trace`` header value.

    Wraps the hop list in a small envelope so future versions can add
    fields without breaking parsers.
    """
    payload = json.dumps({"version": 1, "hops": hops}, separators=(",", ":"))
    return base64.b64encode(payload.encode("utf-8")).decode("ascii")


def decode_trace_header(header_value: str) -> list[dict]:
    """Decode an ``X-Chain-Trace`` header value back into a hop list.

    Tolerates malformed input (returns ``[]``) so a caller never has to
    catch — particularly relevant for the admin tester UI that reads
    response headers without owning the encoder.
    """
    try:
        decoded = base64.b64decode(header_value.encode("ascii")).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    hops = data.get("hops")
    return hops if isinstance(hops, list) else []


def inject_trace_header(response: Any, hops: list[dict]) -> None:
    """Set ``X-Chain-Trace`` on a Response (best-effort).

    Works on Starlette / FastAPI ``Response`` and on objects with a
    mutable ``headers`` mapping. No-ops gracefully if the target
    doesn't have a ``headers`` attribute we can write to (e.g. some
    third-party response stand-ins).
    """
    if not hops:
        return
    header_value = encode_trace_header(hops)
    try:
        response.headers[X_CHAIN_TRACE] = header_value
    except Exception:  # pragma: no cover — defensive only
        logger.debug("inject_trace_header: response has no writable headers")


def inject_trace_into_exception_headers(
    exc_headers: Optional[dict], hops: list[dict]
) -> dict:
    """Build a headers dict for an ``HTTPException`` re-raise.

    ``HTTPException`` carries a flat ``headers`` mapping. To attach the
    chain trace to a non-200 path we need to copy + extend it before
    re-raising.
    """
    out = dict(exc_headers or {})
    if hops:
        out[X_CHAIN_TRACE] = encode_trace_header(hops)
    return out

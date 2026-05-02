"""Tests for ``src/kohakuhub/api/fallback/core.py`` (#78).

Drives the pure ``probe_chain`` function with a mocked client factory
so behaviour can be verified deterministically per source. Each test
pins one classifier branch of the underlying ``utils.classify_upstream``
priority order, so a future change in classifier semantics will surface
both here and in the production-path tests in ``test_operations.py``.
"""
from __future__ import annotations

import httpx
import pytest

from kohakuhub.api.fallback import core


def _resp(
    status: int,
    *,
    headers: dict[str, str] | None = None,
    url: str = "http://upstream/x",
) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        content=b"",
        headers=headers or {},
        request=httpx.Request("GET", url),
    )


class _FakeClient:
    """Per-source response queue keyed by (url, method)."""

    registry: dict[tuple[str, str], list[object]] = {}
    calls: list[tuple[str, str, str]] = []  # (url, method, path)

    def __init__(self, source_url: str, source_type: str, token=None):
        self.source_url = source_url
        self.source_type = source_type
        self.token = token

    @classmethod
    def reset(cls):
        cls.registry = {}
        cls.calls = []

    @classmethod
    def queue(cls, url: str, method: str, *results: object) -> None:
        cls.registry.setdefault((url, method), []).extend(results)

    async def _dispatch(self, method: str, path: str, **_kwargs) -> httpx.Response:
        type(self).calls.append((self.source_url, method, path))
        queue = type(self).registry.get((self.source_url, method))
        if not queue:
            raise AssertionError(
                f"_FakeClient: no scripted response for {self.source_url} {method} {path}"
            )
        result = queue.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def head(self, path, repo_type, **kw):
        return await self._dispatch("HEAD", path, **kw)

    async def get(self, path, repo_type, **kw):
        return await self._dispatch("GET", path, **kw)

    async def post(self, path, repo_type, **kw):
        return await self._dispatch("POST", path, **kw)


@pytest.fixture(autouse=True)
def _reset_fake_client():
    _FakeClient.reset()


# ---------------------------------------------------------------------------
# Path construction
# ---------------------------------------------------------------------------


def test_build_kohaku_path_resolve():
    p = core._build_kohaku_path("resolve", "model", "owner", "demo", "main", "config.json")
    assert p == "/models/owner/demo/resolve/main/config.json"


def test_build_kohaku_path_tree_with_subpath():
    p = core._build_kohaku_path("tree", "dataset", "owner", "demo", "main", "/docs")
    assert p == "/api/datasets/owner/demo/tree/main/docs"


def test_build_kohaku_path_tree_root():
    # No subpath — must not produce a trailing slash.
    p = core._build_kohaku_path("tree", "model", "owner", "demo", "main", "")
    assert p == "/api/models/owner/demo/tree/main"


def test_build_kohaku_path_paths_info():
    p = core._build_kohaku_path("paths_info", "model", "owner", "demo", "main", "")
    assert p == "/api/models/owner/demo/paths-info/main"


def test_build_kohaku_path_info():
    p = core._build_kohaku_path("info", "space", "owner", "demo", "main", "")
    assert p == "/api/spaces/owner/demo"


# ---------------------------------------------------------------------------
# probe_chain — happy paths
# ---------------------------------------------------------------------------


SRC_A = {"name": "A", "url": "https://a.local", "source_type": "huggingface"}
SRC_B = {"name": "B", "url": "https://b.local", "source_type": "huggingface"}


@pytest.mark.asyncio
async def test_probe_chain_first_source_binds_and_responds():
    """200 at the first source → BIND_AND_RESPOND, chain stops."""
    _FakeClient.queue(SRC_A["url"], "GET", _resp(200))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.final_outcome == "BIND_AND_RESPOND"
    assert report.bound_source == SRC_A
    assert len(report.attempts) == 1
    assert report.attempts[0].decision == "BIND_AND_RESPOND"
    assert report.attempts[0].source_name == "A"
    # Other sources untouched.
    assert all(c[0] != SRC_B["url"] for c in _FakeClient.calls)


@pytest.mark.asyncio
async def test_probe_chain_propagates_entry_not_found():
    """X-Error-Code: EntryNotFound → BIND_AND_PROPAGATE, stops chain."""
    _FakeClient.queue(
        SRC_A["url"], "GET",
        _resp(404, headers={"x-error-code": "EntryNotFound"}),
    )
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.final_outcome == "BIND_AND_PROPAGATE"
    assert report.attempts[0].decision == "BIND_AND_PROPAGATE"
    assert report.attempts[0].x_error_code == "EntryNotFound"
    # B never asked.
    assert not any(c[0] == SRC_B["url"] for c in _FakeClient.calls)


@pytest.mark.asyncio
async def test_probe_chain_advances_on_try_next_source():
    """A returns 503, B returns 200 — chain walks A → B and binds B."""
    _FakeClient.queue(SRC_A["url"], "GET", _resp(503))
    _FakeClient.queue(SRC_B["url"], "GET", _resp(200))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.final_outcome == "BIND_AND_RESPOND"
    assert report.bound_source == SRC_B
    assert [a.decision for a in report.attempts] == [
        "TRY_NEXT_SOURCE", "BIND_AND_RESPOND",
    ]


@pytest.mark.asyncio
async def test_probe_chain_exhaustion():
    """All sources TRY_NEXT_SOURCE → final_outcome=CHAIN_EXHAUSTED, no bind."""
    _FakeClient.queue(SRC_A["url"], "GET", _resp(503))
    _FakeClient.queue(SRC_B["url"], "GET", _resp(503))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.final_outcome == "CHAIN_EXHAUSTED"
    assert report.bound_source is None
    assert len(report.attempts) == 2
    assert all(a.decision == "TRY_NEXT_SOURCE" for a in report.attempts)


# ---------------------------------------------------------------------------
# probe_chain — transport failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_chain_records_timeout_then_continues():
    _FakeClient.queue(SRC_A["url"], "GET", httpx.TimeoutException("read timed out"))
    _FakeClient.queue(SRC_B["url"], "GET", _resp(200))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.attempts[0].decision == "TIMEOUT"
    assert "read timed out" in report.attempts[0].error
    assert report.attempts[0].status_code is None
    assert report.final_outcome == "BIND_AND_RESPOND"
    assert report.bound_source == SRC_B


@pytest.mark.asyncio
async def test_probe_chain_records_network_error_then_continues():
    _FakeClient.queue(SRC_A["url"], "GET", RuntimeError("dns failure"))
    _FakeClient.queue(SRC_B["url"], "GET", _resp(200))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A, SRC_B], client_factory=_FakeClient,
    )
    assert report.attempts[0].decision == "NETWORK_ERROR"
    assert "dns failure" in report.attempts[0].error
    assert report.attempts[1].decision == "BIND_AND_RESPOND"


# ---------------------------------------------------------------------------
# Per-op HTTP method dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_chain_resolve_uses_HEAD():
    _FakeClient.queue(SRC_A["url"], "HEAD", _resp(200))
    await core.probe_chain(
        op="resolve", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], file_path="config.json",
        client_factory=_FakeClient,
    )
    assert _FakeClient.calls == [(SRC_A["url"], "HEAD", "/models/owner/demo/resolve/main/config.json")]


@pytest.mark.asyncio
async def test_probe_chain_paths_info_uses_POST():
    _FakeClient.queue(SRC_A["url"], "POST", _resp(200))
    await core.probe_chain(
        op="paths_info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], paths=["a", "b"],
        client_factory=_FakeClient,
    )
    assert _FakeClient.calls == [(SRC_A["url"], "POST", "/api/models/owner/demo/paths-info/main")]


@pytest.mark.asyncio
async def test_probe_chain_tree_uses_GET():
    _FakeClient.queue(SRC_A["url"], "GET", _resp(200))
    await core.probe_chain(
        op="tree", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], file_path="docs",
        client_factory=_FakeClient,
    )
    assert _FakeClient.calls == [(SRC_A["url"], "GET", "/api/models/owner/demo/tree/main/docs")]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_chain_empty_source_list():
    """No sources → CHAIN_EXHAUSTED with zero attempts."""
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[], client_factory=_FakeClient,
    )
    assert report.final_outcome == "CHAIN_EXHAUSTED"
    assert report.attempts == []
    assert report.bound_source is None


@pytest.mark.asyncio
async def test_probe_chain_unsupported_op_raises():
    with pytest.raises(ValueError, match="Unsupported probe op"):
        await core.probe_chain(
            op="totally-bogus", repo_type="model", namespace="owner", name="demo",
            sources=[SRC_A], client_factory=_FakeClient,
        )


@pytest.mark.asyncio
async def test_probe_chain_uses_source_url_as_default_name():
    """Source dict without ``name`` falls back to URL."""
    src = {"url": "https://no-name.local", "source_type": "huggingface"}
    _FakeClient.queue(src["url"], "GET", _resp(200))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[src], client_factory=_FakeClient,
    )
    assert report.attempts[0].source_name == "https://no-name.local"


@pytest.mark.asyncio
async def test_probe_chain_default_client_factory(monkeypatch):
    """When ``client_factory`` is omitted, ``FallbackClient`` is used."""
    captured = []
    real_fc_module = core

    class _CapturingClient:
        def __init__(self, source_url, source_type, token):
            captured.append((source_url, source_type, token))
            self.source_url = source_url
            self.source_type = source_type
            self.token = token

        async def get(self, path, repo_type, **_):
            return _resp(200)

    monkeypatch.setattr(real_fc_module, "FallbackClient", _CapturingClient)
    await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[{"url": "https://a.local", "source_type": "huggingface", "token": "t"}],
    )
    assert captured == [("https://a.local", "huggingface", "t")]


def test_probe_attempt_to_dict_is_jsonable():
    a = core.ProbeAttempt(
        source_name="A", source_url="u", source_type="huggingface",
        method="GET", upstream_path="/x", status_code=200,
        x_error_code=None, x_error_message=None,
        decision="BIND_AND_RESPOND", duration_ms=10, error=None,
        response_body_preview="hello", response_headers={"content-type": "text/plain"},
    )
    d = a.to_dict()
    assert d["source_name"] == "A"
    assert d["decision"] == "BIND_AND_RESPOND"
    assert d["status_code"] == 200
    assert d["response_body_preview"] == "hello"
    assert d["response_headers"] == {"content-type": "text/plain"}


def test_probe_report_to_dict_is_jsonable():
    a = core.ProbeAttempt(
        source_name="A", source_url="u", source_type="huggingface",
        method="GET", upstream_path="/x", status_code=200,
        x_error_code=None, x_error_message=None,
        decision="BIND_AND_RESPOND", duration_ms=10, error=None,
    )
    r = core.ProbeReport(
        op="info", repo_id="owner/demo", revision=None, file_path=None,
        attempts=[a], final_outcome="BIND_AND_RESPOND",
        bound_source={"url": "u"}, duration_ms=15,
        final_response={"status_code": 200, "headers": {}, "body_preview": ""},
    )
    d = r.to_dict()
    assert d["op"] == "info"
    assert d["attempts"][0]["source_name"] == "A"
    assert d["bound_source"] == {"url": "u"}
    assert d["final_response"]["status_code"] == 200


# ---------------------------------------------------------------------------
# Body preview + curated headers (#78 enrichment)
# ---------------------------------------------------------------------------


def test_preview_body_handles_none_response():
    assert core._preview_body(None) is None


def test_preview_body_returns_empty_for_empty_body():
    r = httpx.Response(200, content=b"", request=httpx.Request("GET", "http://x"))
    assert core._preview_body(r) == ""


def test_preview_body_decodes_utf8_under_limit():
    r = httpx.Response(200, content=b'{"id": "demo"}', request=httpx.Request("GET", "http://x"))
    assert core._preview_body(r) == '{"id": "demo"}'


def test_preview_body_truncates_with_marker_when_over_limit():
    big = b"x" * (core._BODY_PREVIEW_LIMIT + 100)
    r = httpx.Response(200, content=big, request=httpx.Request("GET", "http://x"))
    preview = core._preview_body(r)
    assert preview.startswith("x" * core._BODY_PREVIEW_LIMIT)
    assert "[truncated, total" in preview


def test_preview_body_handles_binary_with_placeholder():
    # 0xff is invalid UTF-8 start byte → fallback to placeholder.
    r = httpx.Response(200, content=b"\xff\xfe\xff", request=httpx.Request("GET", "http://x"))
    preview = core._preview_body(r)
    assert preview == "[binary, 3 bytes]"


def test_curated_headers_returns_only_relevant_keys():
    r = httpx.Response(
        200,
        content=b"",
        headers={
            "Content-Type": "application/json",
            "X-Cache-Hit": "1",  # not in curated set
            "X-Error-Code": "EntryNotFound",
            "Server": "Apache",
        },
        request=httpx.Request("GET", "http://x"),
    )
    h = core._curated_headers(r)
    assert h == {
        "content-type": "application/json",
        "x-error-code": "EntryNotFound",
    }


def test_curated_headers_handles_none():
    assert core._curated_headers(None) == {}


@pytest.mark.asyncio
async def test_probe_chain_captures_body_and_headers_per_attempt():
    """Body and curated headers should ride through to the report."""
    body = b'{"id":"openai-community/gpt2"}'
    _FakeClient.queue(
        SRC_A["url"], "GET",
        httpx.Response(
            200, content=body,
            headers={"Content-Type": "application/json", "ETag": "abc"},
            request=httpx.Request("GET", "http://upstream"),
        ),
    )
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], client_factory=_FakeClient,
    )
    a = report.attempts[0]
    assert a.response_body_preview == '{"id":"openai-community/gpt2"}'
    assert a.response_headers["content-type"] == "application/json"
    assert a.response_headers["etag"] == "abc"


@pytest.mark.asyncio
async def test_probe_chain_final_response_mirrors_bound_attempt():
    body = b'{"id":"openai/gpt"}'
    _FakeClient.queue(
        SRC_A["url"], "GET",
        httpx.Response(
            200, content=body,
            headers={"Content-Type": "application/json", "ETag": "v1"},
            request=httpx.Request("GET", "http://upstream"),
        ),
    )
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], client_factory=_FakeClient,
    )
    assert report.final_response is not None
    assert report.final_response["status_code"] == 200
    assert report.final_response["body_preview"] == '{"id":"openai/gpt"}'
    assert report.final_response["headers"]["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_probe_chain_chain_exhausted_has_no_final_response():
    """final_response must be None when the chain exhausts so the UI
    knows to fall back to the per-attempt list rather than rendering a
    misleading `last attempt as final` view."""
    _FakeClient.queue(SRC_A["url"], "GET", _resp(503))
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], client_factory=_FakeClient,
    )
    assert report.final_outcome == "CHAIN_EXHAUSTED"
    assert report.final_response is None


@pytest.mark.asyncio
async def test_probe_chain_propagate_response_lands_in_final_response():
    """BIND_AND_PROPAGATE (e.g. 404 + EntryNotFound) is still a binding —
    the upstream's 4xx body should appear in final_response so the UI
    can show what the bound source said."""
    _FakeClient.queue(
        SRC_A["url"], "GET",
        httpx.Response(
            404,
            content=b'{"error": "no such file"}',
            headers={"X-Error-Code": "EntryNotFound", "Content-Type": "application/json"},
            request=httpx.Request("GET", "http://upstream"),
        ),
    )
    report = await core.probe_chain(
        op="info", repo_type="model", namespace="owner", name="demo",
        sources=[SRC_A], client_factory=_FakeClient,
    )
    assert report.final_outcome == "BIND_AND_PROPAGATE"
    assert report.final_response["status_code"] == 404
    assert report.final_response["headers"]["x-error-code"] == "EntryNotFound"
    assert report.final_response["body_preview"] == '{"error": "no such file"}'

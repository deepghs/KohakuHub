"""Fallback operations for different endpoint types."""

import asyncio
from typing import Optional
from urllib.parse import urljoin

import httpx
from fastapi.responses import JSONResponse, RedirectResponse, Response

from kohakuhub.config import cfg
from kohakuhub.logger import get_logger
from kohakuhub.api.fallback.cache import get_cache
from kohakuhub.api.fallback.client import FallbackClient
from kohakuhub.api.fallback.config import get_enabled_sources
from kohakuhub.api.fallback.utils import (
    add_source_headers,
    build_fallback_attempt,
    build_aggregate_failure_response,
    classify_upstream,
    extract_error_message,
    FallbackDecision,
    is_not_found_error,
    should_retry_source,
    strip_xet_response_headers,
)


def _propagate_upstream_response(
    response: httpx.Response, source: dict
) -> Response:
    """Forward an upstream's BIND_AND_PROPAGATE response verbatim.

    Used when ``classify_upstream`` returns ``BIND_AND_PROPAGATE`` —
    i.e. EntryNotFound, RevisionNotFound, or "Access to this resource
    is disabled.". The repo lives at this source; the upstream's 4xx is
    the right answer for the request, and we forward the body and
    ``X-Error-Code`` / ``X-Error-Message`` headers so a hf_hub client
    raises the right exception (``EntryNotFoundError`` /
    ``RevisionNotFoundError`` / ``DisabledRepoError``).

    Header sanitization mirrors the success path (strip
    encoding/length/transfer headers httpx already decompressed, drop
    Xet hints that would push a hf_hub client onto an endpoint we don't
    serve, attach ``X-Source*`` for telemetry).
    """
    headers = dict(response.headers)
    # httpx already decoded the body, so the original
    # Content-Length/Encoding/Transfer-Encoding values would mislead
    # the next hop. Same hygiene as the 200 success path.
    headers.pop("content-encoding", None)
    headers.pop("content-length", None)
    headers.pop("transfer-encoding", None)
    strip_xet_response_headers(headers)
    headers.update(add_source_headers(response, source["name"], source["url"]))
    return Response(
        status_code=response.status_code,
        content=response.content,
        headers=headers,
    )

logger = get_logger("FALLBACK_OPS")


async def _run_cached_then_chain(
    repo_type: str,
    namespace: str,
    name: str,
    sources: list[dict],
    cache,
    attempts: list[dict],
    attempt_fn,
    aggregate_scope: str,
):
    """Orchestrate the cache-authoritative probe + full-chain fallback.

    Used by ``try_fallback_info`` / ``try_fallback_tree`` /
    ``try_fallback_paths_info`` (and indirectly by
    ``try_fallback_resolve``, which inlines the same shape because it
    has the additional HEAD/GET split). The shared logic is:

    1. If the cache binds this ``repo_id`` to a known source, probe
       that source *and only that source* on the first pass (within
       TTL the cache is authoritative — see #75 on why a cache hit
       previously degenerated to "reorder + still scan the chain").
    2. On stale cache, invalidate and fall through to the full chain
       skipping the already-tried cached source.
    3. If every source falls through (TRY_NEXT_SOURCE), aggregate
       attempts with the right ``scope`` so the final
       ``X-Error-Code`` is RepoNotFound (repo-level ops) or
       EntryNotFound (file-level ops).

    The ``attempt_fn`` is op-specific: it does the request, calls
    ``classify_upstream``, writes the cache on bind, and returns the
    final response (Response / dict / list) on bind, ``None`` on
    TRY_NEXT_SOURCE (the function is responsible for appending to
    ``attempts`` in that case).
    """
    cached_entry = cache.get(repo_type, namespace, name)
    cached_url: str | None = None
    if cached_entry and cached_entry.get("exists"):
        cached_url = cached_entry["source_url"]
        cached_source = next((s for s in sources if s["url"] == cached_url), None)
        if cached_source:
            logger.debug(
                f"Cache hit: probing {cached_source['name']} only for "
                f"{repo_type}/{namespace}/{name}"
            )
            result = await attempt_fn(cached_source)
            if result is not None:
                return result
            logger.debug(
                f"Cache stale: {cached_source['name']} no longer binds "
                f"{repo_type}/{namespace}/{name}; invalidating + falling through"
            )
            cache.invalidate(repo_type, namespace, name)
        else:
            cache.invalidate(repo_type, namespace, name)
            cached_url = None

    for source in sources:
        if cached_url is not None and source["url"] == cached_url:
            continue
        result = await attempt_fn(source)
        if result is not None:
            return result

    if not attempts:  # pragma: no cover
        # Defensive: practically unreachable. Every attempt_fn call
        # appends to ``attempts`` on TRY_NEXT_SOURCE, and the only
        # way to skip every source in the loop is for ``cached_url``
        # to equal every source.url — which can only happen when the
        # cached source was already tried in the cached pass and that
        # call also appended. Caller-side ``if not sources: return
        # None`` covers the literal zero-source case. Kept as belt-
        # and-braces against future refactors that move the order of
        # the cache check vs. the attempt-append; cheaper than a bug
        # that aggregates an empty list and produces a misleading
        # 502.
        return None
    logger.debug(
        f"Fallback MISS: aggregating {len(attempts)} source failure(s) "
        f"for {repo_type}/{namespace}/{name}"
    )
    return build_aggregate_failure_response(attempts, scope=aggregate_scope)


async def try_fallback_resolve(
    repo_type: str,
    namespace: str,
    name: str,
    revision: str,
    path: str,
    user_tokens: dict[str, str] | None = None,
    method: str = "GET",
) -> Optional[Response]:
    """Try to resolve file from fallback sources.

    Args:
        repo_type: "model", "dataset", or "space"
        namespace: Repository namespace
        name: Repository name
        revision: Branch or commit
        path: File path in repository
        user_tokens: User-provided external tokens (overrides admin tokens)
        method: HTTP method ("GET" or "HEAD")

    Returns:
        Response (redirect for GET, response with headers for HEAD) or None if not found
    """
    cache = get_cache()
    sources = get_enabled_sources(namespace, user_tokens=user_tokens)

    if not sources:
        logger.debug(f"No fallback sources configured for {namespace}")
        return None

    # Construct KohakuHub path
    kohaku_path = f"/{repo_type}s/{namespace}/{name}/resolve/{revision}/{path}"

    # Per-source attempts accumulated across the loop. If every source
    # falls through (TRY_NEXT_SOURCE), the aggregated JSON body exposes
    # this list under `body.sources` so the client can tell which
    # sources were asked, what each one answered, and pick the right
    # remediation (token, retry, move on).
    attempts: list[dict] = []

    async def _attempt(source):
        return await _resolve_one_source(
            source,
            repo_type,
            namespace,
            name,
            kohaku_path,
            method,
            attempts,
            cache,
        )

    return await _run_cached_then_chain(
        repo_type, namespace, name, sources, cache, attempts, _attempt,
        # `resolve` is per-file. All-404 → EntryNotFound (default scope
        # for the aggregate). The aggregate's own status-priority logic
        # promotes that to RepoNotFound if any attempt was a bare-401
        # anti-enum, matching hf_hub's behavior.
        aggregate_scope="file",
    )


async def _resolve_one_source(
    source: dict,
    repo_type: str,
    namespace: str,
    name: str,
    kohaku_path: str,
    method: str,
    attempts: list[dict],
    cache,
) -> Optional[Response]:
    """Run a resolve probe (HEAD, then GET if method=GET) against one source.

    Returns:
        ``Response`` if the source binds (BIND_AND_RESPOND or
        BIND_AND_PROPAGATE) — the loop should stop and serve this.
        ``None`` if the source falls through (TRY_NEXT_SOURCE) — the
        attempt has already been appended to ``attempts``.
    """
    try:
        client = FallbackClient(
            source_url=source["url"],
            source_type=source["source_type"],
            token=source.get("token"),
        )
        response = await client.head(kohaku_path, repo_type)
    except httpx.TimeoutException as e:
        logger.warning(f"Fallback source {source['name']} HEAD timed out")
        attempts.append(build_fallback_attempt(source, timeout=e))
        return None
    except Exception as e:
        logger.warning(f"Fallback source {source['name']} HEAD failed: {e}")
        attempts.append(build_fallback_attempt(source, network=e))
        return None

    decision = classify_upstream(response)

    if decision is FallbackDecision.TRY_NEXT_SOURCE:
        logger.warning(
            f"Fallback {source['name']}: HEAD {response.status_code} "
            f"{response.headers.get('x-error-code') or '(no X-Error-Code)'} "
            f"→ TRY_NEXT_SOURCE"
        )
        attempts.append(build_fallback_attempt(source, response=response))
        return None

    # BIND_AND_RESPOND or BIND_AND_PROPAGATE: this source has the repo.
    # Update cache so subsequent requests skip the chain probe.
    cache.set(
        repo_type,
        namespace,
        name,
        source["url"],
        source["name"],
        source["source_type"],
        exists=True,
    )

    if decision is FallbackDecision.BIND_AND_PROPAGATE:
        # 4xx + EntryNotFound / RevisionNotFound / Disabled — the repo
        # is here, but the requested entry/revision is not (or the repo
        # is taken down here). Forward upstream verbatim so a hf_hub
        # client raises the right specific exception. Crucially, do NOT
        # try the next source — a sibling source's same-named repo
        # would be a different repo (#75).
        logger.info(
            f"Fallback BIND_AND_PROPAGATE: {repo_type}/{namespace}/{name} "
            f"at {source['name']} → upstream {response.status_code} "
            f"{response.headers.get('x-error-code') or response.headers.get('x-error-message') or ''}"
        )
        return _propagate_upstream_response(response, source)

    # BIND_AND_RESPOND: HEAD says repo+file present at this source.
    logger.info(
        f"Fallback SUCCESS: {repo_type}/{namespace}/{name} found at {source['name']}"
    )

    if method == "HEAD":
        return await _build_resolve_head_response(response, source, client)

    # GET phase. Once HEAD has bound this source we are committed:
    # the GET response — whether 200, 5xx, or anything else — is what
    # the user gets. Falling through to another source here is the
    # cross-source mixing bug #75 fixes (HEAD-200 at A, GET-502 at A,
    # then sneak over to B's same-named-but-different repo).
    try:
        get_response = await client.get(
            kohaku_path, repo_type, follow_redirects=True
        )
    except httpx.TimeoutException as e:
        logger.warning(
            f"GET timed out at bound source {source['name']} after HEAD bind: {e}"
        )
        # Bound, no upstream response to forward. Synthesize a 502 from
        # this single attempt; the aggregate-failure helper already
        # produces the right shape.
        return build_aggregate_failure_response(
            [build_fallback_attempt(source, timeout=e)]
        )
    except Exception as e:
        logger.warning(
            f"GET failed at bound source {source['name']} after HEAD bind: {e}"
        )
        return build_aggregate_failure_response(
            [build_fallback_attempt(source, network=e)]
        )

    if get_response.status_code == 200:
        # Proxy the content with original headers, stripping the
        # compression-related ones since httpx has already decoded the
        # body and the next hop would otherwise try to decompress an
        # already-decompressed payload.
        resp_headers = dict(get_response.headers)
        resp_headers.pop("content-encoding", None)
        resp_headers.pop("content-length", None)
        resp_headers.pop("transfer-encoding", None)
        strip_xet_response_headers(resp_headers)
        resp_headers.update(
            add_source_headers(get_response, source["name"], source["url"])
        )
        return Response(
            status_code=get_response.status_code,
            content=get_response.content,
            headers=resp_headers,
        )

    # GET non-200 at a bound source: forward upstream's status verbatim.
    # No cross-source retry — see comment above.
    logger.warning(
        f"GET {get_response.status_code} at bound source {source['name']} "
        f"→ propagating (bound)"
    )
    return _propagate_upstream_response(get_response, source)


async def _build_resolve_head_response(
    response: httpx.Response, source: dict, client: "FallbackClient"
) -> Response:
    """Build the HEAD-method response with HF-quirks handling.

    Two HF-specific quirks survive from the original implementation:

    1. **Relative Location → absolute.** HF returns 3xx redirects with
       paths like ``/api/resolve-cache/...`` that only resolve on the HF
       origin. Rewriting against ``response.request.url`` keeps clients
       following the redirect on the upstream rather than bouncing it
       back to KohakuHub.
    2. **Extra HEAD on non-LFS 3xx for Content-Length/ETag.** HF's 307
       on a small file carries the redirect body's Content-Length
       (~278B), not the file's. Without ``X-Linked-Size`` the hf_hub
       client trusts that bogus Content-Length and fails its
       post-download consistency check (observed in
       ``imgutils.get_wd14_tags`` on ``selected_tags.csv``). A second
       HEAD against the rewritten Location picks up the real values.
       LFS files already carry ``X-Linked-Size``; hf_hub prefers it
       over Content-Length so we skip the follow there.
    """
    resp_headers = dict(response.headers)
    location = resp_headers.get("location") or resp_headers.get("Location")
    if location:
        upstream_url = str(response.request.url)
        absolute_location = urljoin(upstream_url, location)
        for k in list(resp_headers.keys()):
            if k.lower() == "location":
                resp_headers.pop(k, None)
        resp_headers["location"] = absolute_location

    if (
        300 <= response.status_code < 400
        and location
        and not any(k.lower() == "x-linked-size" for k in resp_headers)
    ):
        try:
            async with httpx.AsyncClient(timeout=client.timeout) as hc:
                # `identity` asks HF not to gzip the (empty) HEAD body;
                # otherwise httpx's auto-decoding strips Content-Length
                # from the response and we lose the value we came here
                # to fetch.
                extra_headers = {"Accept-Encoding": "identity"}
                if client.token:
                    extra_headers["Authorization"] = f"Bearer {client.token}"
                follow_resp = await hc.head(
                    resp_headers["location"],
                    headers=extra_headers,
                    follow_redirects=False,
                )
            for k in [
                k
                for k in list(resp_headers)
                if k.lower() in ("content-length", "etag")
            ]:
                resp_headers.pop(k)
            for k, v in follow_resp.headers.items():
                if k.lower() in ("content-length", "etag"):
                    resp_headers[k] = v
        except httpx.HTTPError:
            # Extra HEAD failed — return what we have; no worse than
            # the original PR#21 behavior.
            pass

    strip_xet_response_headers(resp_headers)
    resp_headers.update(
        add_source_headers(response, source["name"], source["url"])
    )
    return Response(
        status_code=response.status_code,
        content=response.content,
        headers=resp_headers,
    )


async def try_fallback_info(
    repo_type: str,
    namespace: str,
    name: str,
    user_tokens: dict[str, str] | None = None,
) -> Optional[dict]:
    """Try to get repository info from fallback sources.

    Args:
        repo_type: "model", "dataset", or "space"
        namespace: Repository namespace
        name: Repository name
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        Repository info dict or None if not found
    """
    cache = get_cache()
    sources = get_enabled_sources(namespace, user_tokens=user_tokens)

    if not sources:
        return None

    # Construct API path
    kohaku_path = f"/api/{repo_type}s/{namespace}/{name}"
    attempts: list[dict] = []

    async def _attempt(source):
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )
            response = await client.get(kohaku_path, repo_type)
        except httpx.TimeoutException as e:
            logger.warning(f"Fallback info timed out at {source['name']}")
            attempts.append(build_fallback_attempt(source, timeout=e))
            return None
        except Exception as e:
            logger.warning(f"Fallback info failed for {source['name']}: {e}")
            attempts.append(build_fallback_attempt(source, network=e))
            return None

        decision = classify_upstream(response)
        if decision is FallbackDecision.TRY_NEXT_SOURCE:
            logger.warning(
                f"Fallback info {source['name']}: HTTP {response.status_code} "
                f"{response.headers.get('x-error-code') or ''} → TRY_NEXT_SOURCE"
            )
            attempts.append(build_fallback_attempt(source, response=response))
            return None

        # BIND — write cache for repo-grain reuse.
        cache.set(
            repo_type, namespace, name,
            source["url"], source["name"], source["source_type"],
            exists=True,
        )

        if decision is FallbackDecision.BIND_AND_PROPAGATE:
            # 4xx + EntryNotFound/RevisionNotFound on info is unusual
            # (info is a repo-level endpoint, EntryNotFound semantics
            # don't really apply). If it ever happens, propagate so
            # hf_hub raises the right exception rather than masking it
            # by trying another source.
            logger.info(
                f"Fallback info BIND_AND_PROPAGATE at {source['name']}: "
                f"upstream {response.status_code} "
                f"{response.headers.get('x-error-code') or ''}"
            )
            return _propagate_upstream_response(response, source)

        # BIND_AND_RESPOND — parse and tag.
        data = response.json()
        data["_source"] = source["name"]
        data["_source_url"] = source["url"]
        logger.info(
            f"Fallback info SUCCESS: {repo_type}/{namespace}/{name} from {source['name']}"
        )
        return data

    return await _run_cached_then_chain(
        repo_type, namespace, name, sources, cache, attempts, _attempt,
        aggregate_scope="repo",
    )


async def try_fallback_tree(
    repo_type: str,
    namespace: str,
    name: str,
    revision: str,
    path: str = "",
    recursive: bool = False,
    expand: bool = False,
    limit: int | None = None,
    cursor: str | None = None,
    user_tokens: dict[str, str] | None = None,
) -> Optional[Response]:
    """Try to get repository tree from fallback sources.

    Args:
        repo_type: "model", "dataset", or "space"
        namespace: Repository namespace
        name: Repository name
        revision: Branch or commit
        path: Path within repository
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        JSON response or None if not found
    """
    cache = get_cache()
    sources = get_enabled_sources(namespace, user_tokens=user_tokens)

    if not sources:
        return None

    # Construct API path (strip leading slash from path to avoid double slash)
    clean_path = path.lstrip("/") if path else ""
    kohaku_path = f"/api/{repo_type}s/{namespace}/{name}/tree/{revision}/{clean_path}"
    attempts: list[dict] = []

    params: dict = {"recursive": recursive, "expand": expand}
    if limit is not None:
        params["limit"] = limit
    if cursor:
        params["cursor"] = cursor

    async def _attempt(source):
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )
            response = await client.get(kohaku_path, repo_type, params=params)
        except httpx.TimeoutException as e:
            logger.warning(f"Fallback tree timed out at {source['name']}")
            attempts.append(build_fallback_attempt(source, timeout=e))
            return None
        except Exception as e:
            logger.warning(f"Fallback tree failed for {source['name']}: {e}")
            attempts.append(build_fallback_attempt(source, network=e))
            return None

        decision = classify_upstream(response)
        if decision is FallbackDecision.TRY_NEXT_SOURCE:
            logger.warning(
                f"Fallback tree {source['name']}: HTTP {response.status_code} "
                f"{response.headers.get('x-error-code') or ''} → TRY_NEXT_SOURCE"
            )
            attempts.append(build_fallback_attempt(source, response=response))
            return None

        cache.set(
            repo_type, namespace, name,
            source["url"], source["name"], source["source_type"],
            exists=True,
        )

        if decision is FallbackDecision.BIND_AND_PROPAGATE:
            # tree on a path the repo doesn't have → 404 + EntryNotFound
            # at this source. Repo is bound here; sibling sources'
            # same-named repos are different repos, don't try them.
            logger.info(
                f"Fallback tree BIND_AND_PROPAGATE at {source['name']}: "
                f"upstream {response.status_code} "
                f"{response.headers.get('x-error-code') or ''}"
            )
            return _propagate_upstream_response(response, source)

        # BIND_AND_RESPOND — forward upstream's body with content-type +
        # Link (pagination cursor) intact.
        logger.info(
            f"Fallback tree SUCCESS: {repo_type}/{namespace}/{name}/tree "
            f"from {source['name']}"
        )
        headers = {}
        if response.headers.get("link"):
            headers["Link"] = response.headers["link"]
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type"),
            headers=headers,
        )

    return await _run_cached_then_chain(
        repo_type, namespace, name, sources, cache, attempts, _attempt,
        # Tree is a repo-level operation: scope="repo" so all-404 maps
        # to RepoNotFound (matches HF for a missing repo).
        aggregate_scope="repo",
    )


async def try_fallback_paths_info(
    repo_type: str,
    namespace: str,
    name: str,
    revision: str,
    paths: list[str],
    expand: bool = False,
    user_tokens: dict[str, str] | None = None,
) -> Optional[list]:
    """Try to get paths info from fallback sources.

    Args:
        repo_type: "model", "dataset", or "space"
        namespace: Repository namespace
        name: Repository name
        revision: Branch or commit
        paths: List of paths to query
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        List of path info objects or None if not found
    """
    cache = get_cache()
    sources = get_enabled_sources(namespace, user_tokens=user_tokens)

    if not sources:
        return None

    # Construct API path
    kohaku_path = f"/api/{repo_type}s/{namespace}/{name}/paths-info/{revision}"
    attempts: list[dict] = []

    async def _attempt(source):
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )
            response = await client.post(
                kohaku_path, repo_type, data={"paths": paths, "expand": expand}
            )
        except httpx.TimeoutException as e:
            logger.warning(f"Fallback paths-info timed out at {source['name']}")
            attempts.append(build_fallback_attempt(source, timeout=e))
            return None
        except Exception as e:
            logger.warning(f"Fallback paths-info failed for {source['name']}: {e}")
            attempts.append(build_fallback_attempt(source, network=e))
            return None

        decision = classify_upstream(response)
        if decision is FallbackDecision.TRY_NEXT_SOURCE:
            logger.warning(
                f"Fallback paths-info {source['name']}: HTTP "
                f"{response.status_code} "
                f"{response.headers.get('x-error-code') or ''} → TRY_NEXT_SOURCE"
            )
            attempts.append(build_fallback_attempt(source, response=response))
            return None

        cache.set(
            repo_type, namespace, name,
            source["url"], source["name"], source["source_type"],
            exists=True,
        )

        if decision is FallbackDecision.BIND_AND_PROPAGATE:
            logger.info(
                f"Fallback paths-info BIND_AND_PROPAGATE at {source['name']}: "
                f"upstream {response.status_code} "
                f"{response.headers.get('x-error-code') or ''}"
            )
            return _propagate_upstream_response(response, source)

        # BIND_AND_RESPOND
        logger.info(
            f"Fallback paths-info SUCCESS: {repo_type}/{namespace}/{name} "
            f"from {source['name']}"
        )
        return response.json()

    return await _run_cached_then_chain(
        repo_type, namespace, name, sources, cache, attempts, _attempt,
        # paths-info is per-file (answers "does file X exist at
        # revision R"), so all-404 stays scope="file" → EntryNotFound.
        aggregate_scope="file",
    )


async def fetch_external_list(
    source: dict, repo_type: str, query_params: dict
) -> list[dict]:
    """Fetch repository list from external source.

    Args:
        source: Source config dict
        repo_type: "model", "dataset", or "space"
        query_params: Query parameters (author, limit, sort, etc.)

    Returns:
        List of repository dicts with _source and _source_url added
    """
    try:
        # Construct API path
        kohaku_path = f"/api/{repo_type}s"

        # Build query string
        params = {}
        if query_params.get("author"):
            params["author"] = query_params["author"]
            logger.debug(f"Fetching {repo_type}s with author={params['author']}")
        if query_params.get("limit"):
            params["limit"] = query_params["limit"]
        # Don't send sort to HuggingFace - they don't support it
        # HF returns models sorted by downloads by default

        client = FallbackClient(
            source_url=source["url"],
            source_type=source["source_type"],
            token=source.get("token"),
        )

        # Make request with query params
        external_url = client.map_url(kohaku_path, repo_type)

        async with httpx.AsyncClient(timeout=client.timeout) as http_client:
            response = await http_client.get(external_url, params=params)

        if response.status_code == 200:
            results = response.json()

            # Add source tags to each item
            if isinstance(results, list):
                for item in results:
                    item["_source"] = source["name"]
                    item["_source_url"] = source["url"]

                logger.info(
                    f"Fetched {len(results)} {repo_type}s from {source['name']}"
                )
                return results

        logger.warning(
            f"Failed to fetch list from {source['name']}: {response.status_code}"
        )
        logger.debug(f"Request URL: {response.url}")
        logger.debug(f"Response: {response.text[:200]}")
        return []

    except Exception as e:
        logger.warning(f"Failed to fetch list from {source['name']}: {e}")
        return []


async def try_fallback_user_profile(
    username: str, user_tokens: dict[str, str] | None = None
) -> Optional[dict]:
    """Try to get user profile from fallback sources.

    HuggingFace workflow:
    1. Try /api/users/{name}/overview (works for users, returns 404 for orgs)
    2. If 404, try /api/organizations/{name}/members (works for orgs)
    3. If members succeeds → It's an org, return minimal profile
    4. If both fail → Not found

    Args:
        username: Username or org name to lookup
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        User/org profile dict or None if not found
    """
    sources = get_enabled_sources(
        namespace="", user_tokens=user_tokens
    )  # Global sources only

    if not sources:
        return None

    for source in sources:
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )

            match source["source_type"]:
                case "huggingface":
                    # Step 1: Try user overview
                    user_path = f"/api/users/{username}/overview"
                    user_response = await client.get(user_path, "model")

                    if 200 <= user_response.status_code < 400:
                        # User overview succeeded
                        hf_data = user_response.json()
                        profile_data = {
                            "username": username,
                            "full_name": hf_data.get("fullname")
                            or hf_data.get("name")
                            or username,
                            "bio": None,
                            "website": None,
                            "social_media": None,
                            "created_at": hf_data.get("createdAt"),
                            "_source": source["name"],
                            "_source_url": source["url"],
                            "_partial": True,
                            "_hf_pro": hf_data.get("isPro", False),
                            "_avatar_url": hf_data.get("avatarUrl"),
                            "_hf_type": hf_data.get("type", "user"),
                        }
                        logger.info(
                            f"Fallback user profile SUCCESS: {username} from {source['name']} (type: {profile_data['_hf_type']})"
                        )
                        return profile_data

                    # Step 2: User overview failed, try org members
                    org_members_path = f"/api/organizations/{username}/members"
                    org_response = await client.get(org_members_path, "model")

                    if 200 <= org_response.status_code < 400:
                        # Org members endpoint succeeded → It's an org!
                        members_data = org_response.json()

                        # Try to get org info from first member's avatarUrl or other data
                        first_member = members_data[0] if members_data else {}

                        profile_data = {
                            "username": username,
                            "full_name": username,  # HF doesn't provide org fullname
                            "bio": None,
                            "website": None,
                            "social_media": None,
                            "created_at": None,
                            "_source": source["name"],
                            "_source_url": source["url"],
                            "_partial": True,
                            "_hf_type": "org",  # We know it's an org
                            "_avatar_url": None,  # HF doesn't provide org avatar in members
                            "_member_count": len(members_data),
                        }
                        logger.info(
                            f"Fallback org profile SUCCESS: {username} from {source['name']} ({len(members_data)} members)"
                        )
                        return profile_data

                    # Both failed
                    logger.debug(f"HF user/org not found: {username}")
                    continue

                case "kohakuhub":
                    # Other KohakuHub instances use /profile
                    kohaku_path = f"/api/users/{username}/profile"
                    response = await client.get(kohaku_path, "model")

                    if response.status_code == 200:
                        profile_data = response.json()
                        profile_data["_source"] = source["name"]
                        profile_data["_source_url"] = source["url"]
                        logger.info(
                            f"Fallback user profile SUCCESS: {username} from {source['name']}"
                        )
                        return profile_data

                    elif not should_retry_source(response):
                        return None

                case _:
                    continue

        except Exception as e:
            logger.warning(f"Fallback user profile failed for {source['name']}: {e}")
            continue

    return None


async def try_fallback_user_avatar(
    username: str, user_tokens: dict[str, str] | None = None
) -> Optional[bytes]:
    """Try to get user avatar from fallback sources.

    For HuggingFace: Get avatar URL from overview, then download it
    For KohakuHub: Call /api/users/{username}/avatar directly

    Args:
        username: Username to lookup
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        Avatar image bytes (JPEG) or None if not found
    """
    sources = get_enabled_sources(
        namespace="", user_tokens=user_tokens
    )  # Global sources only

    if not sources:
        return None

    for source in sources:
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )

            match source["source_type"]:
                case "huggingface":
                    # Get avatar URL from user overview
                    user_path = f"/api/users/{username}/overview"
                    user_response = await client.get(user_path, "model")

                    if 200 <= user_response.status_code < 400:
                        hf_data = user_response.json()
                        avatar_url = hf_data.get("avatarUrl")

                        if avatar_url:
                            # Download avatar image
                            import httpx

                            async with httpx.AsyncClient(timeout=30.0) as http_client:
                                avatar_response = await http_client.get(avatar_url)
                                if avatar_response.status_code == 200:
                                    logger.info(
                                        f"Fallback user avatar SUCCESS: {username} from {source['name']}"
                                    )
                                    return avatar_response.content

                    logger.debug(f"HF user avatar not found: {username}")
                    continue

                case "kohakuhub":
                    # Other KohakuHub instances - call avatar endpoint directly
                    avatar_path = f"/api/users/{username}/avatar"
                    response = await client.get(avatar_path, "model")

                    if response.status_code == 200:
                        logger.info(
                            f"Fallback user avatar SUCCESS: {username} from {source['name']}"
                        )
                        return response.content

                    elif not should_retry_source(response):
                        return None

                case _:
                    continue

        except Exception as e:
            logger.warning(f"Fallback user avatar failed for {source['name']}: {e}")
            continue

    return None


async def try_fallback_org_avatar(
    org_name: str, user_tokens: dict[str, str] | None = None
) -> Optional[bytes]:
    """Try to get organization avatar from fallback sources.

    For KohakuHub: Call /api/organizations/{org_name}/avatar directly
    For HuggingFace: Organizations don't have avatars in the API

    Args:
        org_name: Organization name to lookup
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        Avatar image bytes (JPEG) or None if not found
    """
    sources = get_enabled_sources(
        namespace="", user_tokens=user_tokens
    )  # Global sources only

    if not sources:
        return None

    for source in sources:
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )

            match source["source_type"]:
                case "kohakuhub":
                    # Other KohakuHub instances - call avatar endpoint directly
                    avatar_path = f"/api/organizations/{org_name}/avatar"
                    response = await client.get(avatar_path, "model")

                    if response.status_code == 200:
                        logger.info(
                            f"Fallback org avatar SUCCESS: {org_name} from {source['name']}"
                        )
                        return response.content

                    elif not should_retry_source(response):
                        return None

                case "huggingface":
                    # HuggingFace doesn't provide org avatars via API
                    continue

                case _:
                    continue

        except Exception as e:
            logger.warning(f"Fallback org avatar failed for {source['name']}: {e}")
            continue

    return None


async def try_fallback_user_repos(
    username: str, user_tokens: dict[str, str] | None = None
) -> Optional[dict]:
    """Try to get user repositories from fallback sources.

    Args:
        username: Username to lookup
        user_tokens: User-provided external tokens (overrides admin tokens)

    Returns:
        Repos dict with models/datasets/spaces or None if not found
    """
    sources = get_enabled_sources(namespace="", user_tokens=user_tokens)

    if not sources:
        return None

    for source in sources:
        try:
            client = FallbackClient(
                source_url=source["url"],
                source_type=source["source_type"],
                token=source.get("token"),
            )

            match source["source_type"]:
                case "huggingface":
                    # HF doesn't have single repos endpoint, query each type
                    models_path = f"/api/models?author={username}&limit=100"
                    datasets_path = f"/api/datasets?author={username}&limit=100"
                    spaces_path = f"/api/spaces?author={username}&limit=100"

                    # Fetch concurrently
                    models_task = client.get(models_path, "model")
                    datasets_task = client.get(datasets_path, "dataset")
                    spaces_task = client.get(spaces_path, "space")

                    models_resp, datasets_resp, spaces_resp = await asyncio.gather(
                        models_task, datasets_task, spaces_task, return_exceptions=True
                    )

                    result = {"models": [], "datasets": [], "spaces": []}

                    # Parse models
                    if (
                        not isinstance(models_resp, Exception)
                        and models_resp.status_code == 200
                    ):
                        result["models"] = models_resp.json()

                    # Parse datasets
                    if (
                        not isinstance(datasets_resp, Exception)
                        and datasets_resp.status_code == 200
                    ):
                        result["datasets"] = datasets_resp.json()

                    # Parse spaces
                    if (
                        not isinstance(spaces_resp, Exception)
                        and spaces_resp.status_code == 200
                    ):
                        result["spaces"] = spaces_resp.json()

                    # Add source tags to all repos
                    for repo_list in [
                        result["models"],
                        result["datasets"],
                        result["spaces"],
                    ]:
                        for repo in repo_list:
                            if isinstance(repo, dict):
                                repo["_source"] = source["name"]
                                repo["_source_url"] = source["url"]

                    logger.info(
                        f"Fallback user repos SUCCESS: {username} from {source['name']}"
                    )
                    return result

                case "kohakuhub":
                    # Use single endpoint
                    repos_path = f"/api/users/{username}/repos"
                    response = await client.get(repos_path, "model")

                    if response.status_code == 200:
                        data = response.json()

                        # Add source tags
                        for repo_list in [
                            data.get("models", []),
                            data.get("datasets", []),
                            data.get("spaces", []),
                        ]:
                            for repo in repo_list:
                                if isinstance(repo, dict):
                                    repo["_source"] = source["name"]
                                    repo["_source_url"] = source["url"]

                        logger.info(
                            f"Fallback user repos SUCCESS: {username} from {source['name']}"
                        )
                        return data

                    elif not should_retry_source(response):
                        return None

                case _:
                    continue

        except Exception as e:
            logger.warning(f"Fallback user repos failed for {source['name']}: {e}")
            continue

    return None

"""Comprehensive tests for the L2 cache layer (src/kohakuhub/cache.py).

These tests run against a real Valkey instance — the L2 design hinges on
Valkey-side semantics (atomic INCR, SCAN cursors, INFO server.run_id, NX
locks) that an in-process fake cannot validate. Skips automatically when
no Valkey is reachable, so contributors without Docker can still run the
rest of the suite.

Test layout:

- ``valkey_url`` session fixture: resolves connection URL from
  ``KOHAKU_HUB_CACHE_TEST_URL`` env var, then a small list of dev defaults.
  Pings the server; skips the module if unreachable.
- ``cache_setup`` autouse fixture: per-test fresh namespace + ``init_cache``
  + raw client handle. Wipes its own keyspace at teardown so tests don't
  bleed into each other or into long-lived Valkey state.
- Tests are grouped by surface: helpers / read-write / negative / invalidate /
  singleflight / generation / silent-degradation / bootstrap-flush / metrics.

A few tests deliberately reach into Valkey directly via the raw client to
verify semantics the helper layer abstracts away (e.g., TTL bounds, SCAN
cursors over many keys, the bootstrap_run_id sentinel).
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
import uuid

import pytest
import redis.asyncio as aioredis

from kohakuhub import cache as cache_mod
from kohakuhub.cache import (
    BOOTSTRAP_LOCK_KEY,
    BOOTSTRAP_RUN_ID_KEY,
    MODE_B_PREFIXES,
    bump_gen,
    cache_get_json,
    cache_get_or_fetch,
    cache_invalidate,
    cache_invalidate_prefix,
    cache_set_json,
    cache_set_negative,
    close_cache,
    get_memory_info,
    get_metrics_snapshot,
    init_cache,
    is_enabled,
    read_gen,
    reset_metrics,
)
from kohakuhub.config import cfg


_DEFAULT_TEST_URLS = (
    "redis://127.0.0.1:26380/0",  # `make test-cache` ad-hoc container
    "redis://127.0.0.1:26379/0",  # local-dev kohakuhub-dev-valkey
    "redis://127.0.0.1:6379/0",   # bare-metal install
)


# ---------------------------------------------------------------------------
# Connection / fixture machinery
# ---------------------------------------------------------------------------


async def _try_url(url: str) -> bool:
    client = aioredis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        await client.ping()
        return True
    except Exception:
        return False
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


@pytest.fixture(scope="session")
async def valkey_url() -> str:
    # Explicit override wins. ``CACHE_TEST_DEFAULT_URL`` is what CI sets;
    # ``KOHAKU_HUB_CACHE_TEST_URL`` is a contributor convenience.
    candidates: list[str] = []
    for env_var in ("KOHAKU_HUB_CACHE_TEST_URL", "CACHE_TEST_DEFAULT_URL"):
        v = os.environ.get(env_var)
        if v:
            candidates.append(v)
    candidates.extend(_DEFAULT_TEST_URLS)

    for url in candidates:
        if await _try_url(url):
            return url
    pytest.skip(
        "No Valkey/Redis reachable for cache tests. "
        "Start one (e.g. `docker run -d --rm -p 26380:6379 valkey/valkey:8-alpine`) "
        "or set KOHAKU_HUB_CACHE_TEST_URL."
    )


@pytest.fixture(autouse=True)
async def cache_setup(valkey_url, monkeypatch):
    """Per-test cache state.

    Each test gets a fresh namespace so leftover keys from a previous test
    can never produce a phantom hit, and so a CI runner reusing a Valkey
    instance across jobs stays clean. Final tear-down deletes anything
    matching the test's namespace via SCAN — never KEYS.
    """
    # Disable jitter inside the helper for tests that assert TTL values.
    # Specific TTL-jitter tests re-enable it explicitly via monkeypatch.
    namespace = f"kh-test-{uuid.uuid4().hex[:12]}"
    monkeypatch.setattr(cfg.cache, "enabled", True)
    monkeypatch.setattr(cfg.cache, "url", valkey_url)
    monkeypatch.setattr(cfg.cache, "namespace", namespace)
    monkeypatch.setattr(cfg.cache, "default_ttl_seconds", 60)
    monkeypatch.setattr(cfg.cache, "jitter_fraction", 0.0)
    monkeypatch.setattr(cfg.cache, "max_connections", 16)
    monkeypatch.setattr(cfg.cache, "socket_timeout_seconds", 1.0)
    monkeypatch.setattr(cfg.cache, "socket_connect_timeout_seconds", 1.0)

    # Ensure a clean slate — close any module-level client from a previous
    # test, then re-init.
    await close_cache()
    reset_metrics()
    await init_cache()
    yield namespace

    # Tear-down: scan + delete this namespace's keys, leave others alone.
    raw = aioredis.from_url(valkey_url, decode_responses=True)
    try:
        cursor = 0
        while True:
            cursor, keys = await raw.scan(cursor=cursor, match=f"{namespace}:*", count=500)
            if keys:
                await raw.delete(*keys)
            if cursor == 0:
                break
    finally:
        await raw.aclose()
    await close_cache()


@pytest.fixture
async def raw_client(valkey_url):
    """Raw Valkey client for tests that need to verify state out-of-band."""
    client = aioredis.from_url(valkey_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Sanity / contract checks
# ---------------------------------------------------------------------------


async def test_init_cache_is_idempotent():
    """Calling init_cache twice must not produce a second pool."""
    await init_cache()
    pool_a = cache_mod._pool
    await init_cache()
    pool_b = cache_mod._pool
    assert pool_a is pool_b


async def test_is_enabled_reflects_live_state(monkeypatch):
    assert is_enabled() is True
    monkeypatch.setattr(cfg.cache, "enabled", False)
    assert is_enabled() is False


# ---------------------------------------------------------------------------
# Read / write / negative-cache
# ---------------------------------------------------------------------------


async def test_set_get_roundtrip_basic_types(raw_client, cache_setup):
    cases = {
        "lakefs:string": "hello",
        "lakefs:int": 12345,
        "lakefs:float": 3.14,
        "lakefs:list": [1, "two", 3.0, None],
        "lakefs:dict": {"a": 1, "b": [1, 2, 3], "c": {"nested": True}},
        "lakefs:none": None,
        "lakefs:bool": True,
    }
    for key, value in cases.items():
        assert await cache_set_json(key, value, ttl=120)
        hit, got = await cache_get_json(key)
        assert hit is True, f"{key}: expected hit"
        assert got == value, f"{key}: expected {value!r}, got {got!r}"


async def test_get_returns_miss_for_unset_key():
    hit, value = await cache_get_json("lakefs:never-set")
    assert hit is False
    assert value is None


async def test_negative_cache_distinguishes_from_miss():
    """A negative-sentinel hit returns (True, None) — not (False, None).

    This is the crucial distinction that lets cache_get_or_fetch short-circuit
    repeated lookups for nonexistent entities without re-fetching.
    """
    assert await cache_set_negative("repo:info:absent")
    hit, value = await cache_get_json("repo:info:absent")
    assert hit is True
    assert value is None


async def test_set_payload_unencodable_returns_false():
    """Non-JSON-encodable payloads must not poison the cache."""

    class NotJSONEncodable:
        pass

    ok = await cache_set_json("lakefs:bad", NotJSONEncodable())
    assert ok is False
    hit, _ = await cache_get_json("lakefs:bad")
    assert hit is False


async def test_corrupt_value_recovers_as_miss(raw_client, cache_setup):
    """If something writes a non-JSON, non-sentinel value via the raw client,
    cache_get_json must surface a miss rather than raising — this protects
    callers from poisoning a Valkey share.
    """
    namespace = cache_setup
    await raw_client.set(f"{namespace}:lakefs:corrupt", "not-json-{[")
    hit, value = await cache_get_json("lakefs:corrupt")
    assert hit is False
    assert value is None


# ---------------------------------------------------------------------------
# TTL + jitter
# ---------------------------------------------------------------------------


async def test_default_ttl_is_applied(raw_client, cache_setup):
    namespace = cache_setup
    await cache_set_json("lakefs:ttl-default", {"x": 1})
    ttl = await raw_client.ttl(f"{namespace}:lakefs:ttl-default")
    # ``cfg.cache.default_ttl_seconds`` is 60 in cache_setup; jitter is 0.
    assert 55 <= ttl <= 60


async def test_explicit_ttl_overrides_default(raw_client, cache_setup):
    namespace = cache_setup
    await cache_set_json("lakefs:ttl-explicit", "v", ttl=120)
    ttl = await raw_client.ttl(f"{namespace}:lakefs:ttl-explicit")
    assert 110 <= ttl <= 120


async def test_jitter_spreads_ttl(monkeypatch, raw_client, cache_setup):
    """With jitter=0.2, TTLs across 30 sets must show a non-trivial spread.

    Without jitter, TTLs cluster at exactly the requested value, and a
    coordinated burst of writes all expire at the same instant. Jitter is
    the cheapest defense against synchronized stampedes.
    """
    namespace = cache_setup
    monkeypatch.setattr(cfg.cache, "jitter_fraction", 0.2)
    base_ttl = 100
    keys = [f"lakefs:jitter-{i}" for i in range(30)]
    for k in keys:
        await cache_set_json(k, 1, ttl=base_ttl)
    ttls = []
    for k in keys:
        ttls.append(await raw_client.ttl(f"{namespace}:{k}"))
    # Stay strictly inside the [80, 120] window (helper rounds to int).
    assert all(80 <= t <= 120 for t in ttls), ttls
    # But spread must be wider than 5 — without jitter all values are equal.
    assert max(ttls) - min(ttls) >= 5
    # Standard deviation sanity check.
    assert statistics.stdev(ttls) > 2


# ---------------------------------------------------------------------------
# Invalidation
# ---------------------------------------------------------------------------


async def test_cache_invalidate_single_and_multi():
    await cache_set_json("repo:info:a", 1)
    await cache_set_json("repo:info:b", 2)
    await cache_set_json("repo:info:c", 3)
    deleted = await cache_invalidate("repo:info:a", "repo:info:b")
    assert deleted == 2
    assert (await cache_get_json("repo:info:a"))[0] is False
    assert (await cache_get_json("repo:info:b"))[0] is False
    assert (await cache_get_json("repo:info:c"))[0] is True


async def test_cache_invalidate_prefix_uses_scan_for_many_keys(raw_client, cache_setup):
    """Stress the SCAN-based path with > SCAN_BATCH_SIZE keys.

    Verifies two properties at once:
    1. ``cache_invalidate_prefix`` actually deletes everything (not just one batch).
    2. The implementation does not call ``KEYS`` (which on a real Valkey
       would block the whole server).

    We can't easily black-box (2) without process-level tracing, so the
    behavioral check (1) on a > batch-sized keyspace is a strong proxy:
    a KEYS-based implementation would also work, but the reason we wrote
    SCAN-based is correctness *under load*, which (1) at least demonstrates.
    """
    n = 1500  # > SCAN_BATCH_SIZE (500)
    for i in range(n):
        await cache_set_json(f"list:probe-{i:04d}", i)
    deleted = await cache_invalidate_prefix("list:")
    assert deleted == n
    # Confirm none survive
    assert (await cache_get_json("list:probe-0000"))[0] is False
    assert (await cache_get_json("list:probe-1499"))[0] is False


async def test_invalidate_prefix_does_not_touch_neighbors(raw_client, cache_setup):
    await cache_set_json("list:a:1", 1)
    await cache_set_json("list:a:2", 2)
    await cache_set_json("list-other:keep", 3)  # different prefix
    await cache_invalidate_prefix("list:a:")
    assert (await cache_get_json("list:a:1"))[0] is False
    assert (await cache_get_json("list:a:2"))[0] is False
    # The neighbour key uses a different prefix — must be untouched.
    assert (await cache_get_json("list-other:keep"))[0] is True


# ---------------------------------------------------------------------------
# Singleflight + get-or-fetch
# ---------------------------------------------------------------------------


async def test_get_or_fetch_caches_first_call():
    calls = []

    async def fetch():
        calls.append(1)
        return {"shape": "v1"}

    v1 = await cache_get_or_fetch("lakefs:commit:abc", fetch, ttl=120)
    assert v1 == {"shape": "v1"}
    assert calls == [1]

    # Second call reads from cache; fetch is NOT invoked.
    v2 = await cache_get_or_fetch("lakefs:commit:abc", fetch, ttl=120)
    assert v2 == {"shape": "v1"}
    assert calls == [1]


async def test_get_or_fetch_short_circuits_on_existing_negative_sentinel():
    """If a previous call cached the negative sentinel, a later call must
    return None *without* invoking fetch — the sentinel is a real cache hit.
    """
    await cache_set_negative("repo:info:already-marked-absent")

    calls = []

    async def fetch():
        calls.append(1)
        return {"this": "should not appear"}

    v = await cache_get_or_fetch(
        "repo:info:already-marked-absent", fetch, ttl=60
    )
    assert v is None
    assert calls == []


async def test_get_or_fetch_treat_none_as_negative():
    """When configured, fetch returning None caches the negative sentinel
    so subsequent calls don't re-invoke fetch.
    """
    calls = []

    async def fetch_missing():
        calls.append(1)
        return None

    v1 = await cache_get_or_fetch(
        "negative:repo:absent", fetch_missing, ttl=60, treat_none_as_negative=True
    )
    assert v1 is None
    assert calls == [1]

    # Second call: negative sentinel hit, fetch should NOT run.
    v2 = await cache_get_or_fetch(
        "negative:repo:absent", fetch_missing, ttl=60, treat_none_as_negative=True
    )
    assert v2 is None
    assert calls == [1]


async def test_get_or_fetch_singleflight_concurrent_calls():
    """100 concurrent calls miss together; only one fetch runs.

    This is the singleflight contract: prevents N workers stampeding L3
    when a hot cache key expires.
    """
    calls = []
    fetch_started = asyncio.Event()
    fetch_release = asyncio.Event()

    async def fetch():
        calls.append(1)
        fetch_started.set()
        # Hold the fetch open so all coroutines pile up behind the lock.
        await fetch_release.wait()
        return {"v": "shared"}

    async def caller():
        return await cache_get_or_fetch("lakefs:commit:hot", fetch, ttl=60)

    # Kick off 100 concurrent get_or_fetch calls.
    tasks = [asyncio.create_task(caller()) for _ in range(100)]
    # Give the first one time to claim the lock.
    await fetch_started.wait()
    # Now release the fetch.
    fetch_release.set()
    results = await asyncio.gather(*tasks)
    assert all(r == {"v": "shared"} for r in results)
    assert len(calls) == 1, f"expected exactly 1 fetch, got {len(calls)}"

    # Singleflight contention metric reflects the contended waiters.
    metrics = get_metrics_snapshot()
    assert metrics["singleflight_contention"] >= 1


async def test_get_or_fetch_fetch_error_does_not_poison():
    """An exception from fetch must NOT cache anything.

    If we cached the error path, the next request would hit cache and not
    retry the fetch — turning a transient error into a sticky outage.
    """
    attempt = {"n": 0}

    async def fetch_flaky():
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise RuntimeError("transient")
        return {"v": "ok"}

    with pytest.raises(RuntimeError, match="transient"):
        await cache_get_or_fetch("lakefs:commit:flaky", fetch_flaky, ttl=60)

    # Second call retries the fetch and succeeds.
    v = await cache_get_or_fetch("lakefs:commit:flaky", fetch_flaky, ttl=60)
    assert v == {"v": "ok"}
    assert attempt["n"] == 2


async def test_get_or_fetch_after_invalidate_refetches():
    calls = []

    async def fetch():
        calls.append(1)
        return calls[-1]

    await cache_get_or_fetch("repo:info:r", fetch, ttl=120)
    await cache_get_or_fetch("repo:info:r", fetch, ttl=120)
    assert len(calls) == 1

    await cache_invalidate("repo:info:r")
    await cache_get_or_fetch("repo:info:r", fetch, ttl=120)
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Generation counters
# ---------------------------------------------------------------------------


async def test_bump_gen_is_monotonic():
    a = await bump_gen("repo:gen:42")
    b = await bump_gen("repo:gen:42")
    c = await bump_gen("repo:gen:42")
    assert a < b < c


async def test_read_gen_default_zero():
    assert await read_gen("repo:gen:never-touched") == 0


async def test_read_gen_after_bump_matches():
    g1 = await bump_gen("user:gen:7")
    g2 = await read_gen("user:gen:7")
    assert g1 == g2


async def test_gen_keyed_caches_are_isolated():
    """The gen-keyed pattern: derived caches embed the current gen in their
    key, so bumping gen makes old keys unreachable.
    """
    calls = []

    async def fetch_v1():
        calls.append("v1")
        return "value-1"

    async def fetch_v2():
        calls.append("v2")
        return "value-2"

    g = await read_gen("repo:gen:99")
    key1 = f"repo:info:99:g{g}"
    v = await cache_get_or_fetch(key1, fetch_v1, ttl=120)
    assert v == "value-1"

    # Bump gen — key1 is no longer the "current" key.
    new_g = await bump_gen("repo:gen:99")
    assert new_g > g
    key2 = f"repo:info:99:g{new_g}"
    v = await cache_get_or_fetch(key2, fetch_v2, ttl=120)
    assert v == "value-2"
    assert calls == ["v1", "v2"]


# ---------------------------------------------------------------------------
# Silent degradation
# ---------------------------------------------------------------------------


async def test_silent_degradation_when_cache_disabled(monkeypatch):
    """With cfg.cache.enabled=False the helper API must not error and
    must behave as if every call is a cache miss.
    """
    monkeypatch.setattr(cfg.cache, "enabled", False)
    await close_cache()
    await init_cache()  # No-op when disabled.

    assert is_enabled() is False
    assert (await cache_set_json("lakefs:disabled", "x")) is False
    hit, value = await cache_get_json("lakefs:disabled")
    assert hit is False and value is None
    assert (await cache_invalidate("lakefs:disabled")) == 0
    assert (await cache_invalidate_prefix("lakefs:")) == 0
    assert (await bump_gen("repo:gen:1")) == 0
    assert (await read_gen("repo:gen:1")) == 0


async def test_silent_degradation_when_valkey_unreachable(monkeypatch):
    """Point the cache at a port nothing is listening on. Every operation
    must return its degraded sentinel and never raise.
    """
    monkeypatch.setattr(cfg.cache, "url", "redis://127.0.0.1:1/0")
    await close_cache()
    # init must not raise even though PING will fail
    await init_cache()
    # All public ops degrade gracefully
    assert (await cache_set_json("lakefs:dead", 1)) is False
    hit, _ = await cache_get_json("lakefs:dead")
    assert hit is False


async def test_get_or_fetch_falls_back_when_cache_unreachable(monkeypatch):
    """When cache is broken, get_or_fetch must still call fetch and return
    its result (silent degradation extends through the read-through path).
    """
    monkeypatch.setattr(cfg.cache, "url", "redis://127.0.0.1:1/0")
    await close_cache()
    await init_cache()

    calls = []

    async def fetch():
        calls.append(1)
        return {"v": "from-source"}

    v1 = await cache_get_or_fetch("lakefs:commit:nocache", fetch, ttl=60)
    v2 = await cache_get_or_fetch("lakefs:commit:nocache", fetch, ttl=60)
    assert v1 == v2 == {"v": "from-source"}
    # No cache means no caching — fetch runs every time.
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Boot-time flush coordinator
# ---------------------------------------------------------------------------


async def test_bootstrap_flush_wipes_mode_b_only(raw_client, cache_setup):
    """Set up keys in every Mode-B namespace plus a Mode-A namespace,
    poison the bootstrap_run_id sentinel so the next ``_bootstrap_flush``
    treats this Valkey as freshly restarted, and verify exactly the
    Mode-B keys disappear.
    """
    namespace = cache_setup

    # Mode-A surrogates: lakefs:commit:* (immutable, must SURVIVE).
    await cache_set_json("lakefs:commit:cafebabe", {"sha": "cafebabe"}, ttl=3600)
    await cache_set_json("lakefs:stat:repo:cafebabe:foo.txt", {"size": 42}, ttl=3600)
    await cache_set_json("lakefs:list:repo:cafebabe:", [{"path": "x"}], ttl=3600)

    # Mode-B surrogates (one key per Mode-B prefix, must DISAPPEAR).
    for prefix in MODE_B_PREFIXES:
        await cache_set_json(f"{prefix}sample", {"sentinel": True}, ttl=3600)

    # Force a "Valkey was restarted" condition: write a stale run_id.
    await raw_client.set(cache_mod._prefixed(BOOTSTRAP_RUN_ID_KEY), "stale-run-id")

    # Trigger the coordinator.
    await cache_mod._bootstrap_flush()

    # Mode-A keys still there
    assert (await cache_get_json("lakefs:commit:cafebabe"))[0] is True
    assert (await cache_get_json("lakefs:stat:repo:cafebabe:foo.txt"))[0] is True
    assert (await cache_get_json("lakefs:list:repo:cafebabe:"))[0] is True

    # Mode-B keys gone
    for prefix in MODE_B_PREFIXES:
        hit, _ = await cache_get_json(f"{prefix}sample")
        assert hit is False, f"prefix {prefix!r} not flushed"

    # bootstrap_run_id has been advanced to the live value, not the stale one.
    new_marker = await raw_client.get(cache_mod._prefixed(BOOTSTRAP_RUN_ID_KEY))
    assert new_marker != "stale-run-id"
    assert new_marker is not None

    # Metrics reflect the flush.
    metrics = get_metrics_snapshot()
    assert metrics["last_flush_run_id"] == new_marker
    assert metrics["last_flush_at_ms"] is not None
    # ``last_flushed_keys`` includes every Mode-B prefix sample (one per prefix).
    assert metrics["last_flushed_keys"] >= len(MODE_B_PREFIXES)


async def test_bootstrap_flush_noop_when_run_id_unchanged(raw_client, cache_setup):
    """Two ``_bootstrap_flush`` calls in a row: the second is a no-op."""
    # Prime: first call writes the live run_id marker.
    await cache_mod._bootstrap_flush()
    metrics_before = get_metrics_snapshot()
    flushes_before = metrics_before["last_flush_at_ms"]

    # Plant a sample key that would be wiped on a real flush.
    await cache_set_json("sess:should-survive", {"x": 1}, ttl=3600)
    await cache_mod._bootstrap_flush()

    # The Mode-B key is still there because no flush happened.
    hit, _ = await cache_get_json("sess:should-survive")
    assert hit is True

    metrics_after = get_metrics_snapshot()
    assert metrics_after["last_flush_at_ms"] == flushes_before


async def test_bootstrap_flush_two_workers_only_one_flushes(raw_client, cache_setup, monkeypatch):
    """When two workers race, only one performs the flush; the other waits
    for the run_id marker to update and returns without doing the work.

    We simulate two parallel calls by holding the lock from worker A and
    invoking ``_bootstrap_flush`` from worker B with monkeypatched sleep
    so its wait loop is fast; then release the lock + advance the marker.
    """
    namespace = cache_setup

    # Force restart-detected state: stale marker.
    await raw_client.set(cache_mod._prefixed(BOOTSTRAP_RUN_ID_KEY), "stale")

    # Worker A: claim the bootstrap lock manually, simulating "in flight".
    locked = await raw_client.set(
        cache_mod._prefixed(BOOTSTRAP_LOCK_KEY), "1", nx=True, ex=30
    )
    assert locked

    # Plant a Mode-B key that worker B must NOT flush (because it's
    # supposed to wait, not run the flush itself).
    await cache_set_json("sess:from-other-worker", "should-stay", ttl=3600)

    # Speed up the wait-loop inside _bootstrap_flush by collapsing its
    # ``asyncio.sleep(0.1)`` polls. Capture the real sleep first so the
    # patched lambda doesn't recurse into itself.
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda *_a, **_kw: real_sleep(0))

    async def worker_b():
        await cache_mod._bootstrap_flush()

    task_b = asyncio.create_task(worker_b())

    # Let worker B enter its wait loop, then update the marker as if A finished.
    await real_sleep(0)
    info = await raw_client.info("server")
    live_run_id = info["run_id"]
    await raw_client.set(cache_mod._prefixed(BOOTSTRAP_RUN_ID_KEY), live_run_id)
    await raw_client.delete(cache_mod._prefixed(BOOTSTRAP_LOCK_KEY))

    await asyncio.wait_for(task_b, timeout=5)

    # Worker B did NOT perform the flush — sample key still there.
    hit, _ = await cache_get_json("sess:from-other-worker")
    assert hit is True


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


async def test_metrics_count_hits_and_misses():
    reset_metrics()
    await cache_set_json("repo:info:m1", "v1")
    # Hit
    hit, _ = await cache_get_json("repo:info:m1")
    assert hit is True
    # Miss
    hit, _ = await cache_get_json("repo:info:absent")
    assert hit is False

    metrics = get_metrics_snapshot()
    assert metrics["hits"].get("repo", 0) == 1
    assert metrics["misses"].get("repo", 0) == 1
    assert metrics["set_count"].get("repo", 0) == 1


async def test_metrics_count_invalidate_namespaced():
    reset_metrics()
    await cache_set_json("user:gen:1", 100)
    await cache_invalidate("user:gen:1")
    metrics = get_metrics_snapshot()
    assert metrics["invalidate_count"].get("user", 0) >= 1


async def test_metrics_singleflight_contention_recorded():
    reset_metrics()

    fetch_release = asyncio.Event()
    started = asyncio.Event()
    calls = []

    async def fetch():
        calls.append(1)
        started.set()
        await fetch_release.wait()
        return "x"

    tasks = [
        asyncio.create_task(
            cache_get_or_fetch("lakefs:commit:contend", fetch, ttl=60)
        )
        for _ in range(20)
    ]
    await started.wait()
    fetch_release.set()
    await asyncio.gather(*tasks)

    metrics = get_metrics_snapshot()
    assert metrics["singleflight_contention"] >= 1


async def test_get_memory_info_returns_valkey_state():
    info = await get_memory_info()
    assert info["available"] is True
    assert info["maxmemory_policy"] is not None
    # ``used_memory`` must be a positive integer when available is True.
    assert int(info["used_memory"]) >= 0


# ---------------------------------------------------------------------------
# Public namespace contract: every Mode-B prefix is in the list
# ---------------------------------------------------------------------------


def test_mode_b_prefixes_sane_shape():
    """Guard against accidental edits that would cause the bootstrap flush
    to silently miss a namespace.

    Each entry must end with ``:`` so SCAN matching ``f"{prefix}*"`` only
    catches keys explicitly inside that namespace, never neighbouring keys
    that happen to share a string prefix.
    """
    assert len(MODE_B_PREFIXES) == len(set(MODE_B_PREFIXES)), "duplicate entries"
    assert all(p.endswith(":") for p in MODE_B_PREFIXES), MODE_B_PREFIXES
    assert all(p == p.lower() for p in MODE_B_PREFIXES), "lowercase only"
    # No Mode-A prefix accidentally listed (would cause spurious flushes).
    assert "lakefs:commit:" not in MODE_B_PREFIXES
    assert "lakefs:stat:" not in MODE_B_PREFIXES
    assert "lakefs:list:" not in MODE_B_PREFIXES

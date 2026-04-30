"""Tests covering the SQL-aggregate replacement for the per-row LakeFS calls
in the list endpoints (issue #62).

The fix swaps two LakeFS REST round-trips per row (``get_branch`` +
``get_commit``) for one SQL aggregate over the ``Commit`` table, with a
LakeFS-side fallback for repos missing from that table (e.g. created via
``git push`` or ``repo rename``, neither of which calls ``create_commit``
on the DB side today).

The tests in this module verify:

1. ``_latest_main_commits`` reads the latest main-branch commit from the DB
   for a batch of repos, ignoring non-main branches and out-of-set repos.
2. ``_resolve_main_head_via_lakefs`` returns ``(sha, last_modified)`` on the
   happy path and ``(None, None)`` on errors.
3. The list endpoints fire **zero** LakeFS ``get_branch`` / ``get_commit``
   calls when every returned row has a row in the ``Commit`` table — that's
   the perf win.
4. The LakeFS fallback engages when the ``Commit`` rows are deleted, so the
   API contract (``sha`` / ``lastModified`` populated) is preserved for
   git-push / rename / fresh-repo cases.

Reference: issue #62 (perf umbrella #69).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import pytest


def _live_repo_info():
    """Return the live ``kohakuhub.api.repo.routers.info`` module.

    The test fixture machinery reloads backend modules between tests
    (``load_backend_modules(force_reload=True)``), so module references
    captured at import time become stale — the FastAPI handlers run
    against the post-reload module while a top-of-file ``import`` would
    point at the pre-reload one. Resolve via ``sys.modules`` at call
    time to always reach the live module the handlers use.

    Same workaround as ``test_lakefs_rest_client_live.py`` (PR #61)."""
    return sys.modules["kohakuhub.api.repo.routers.info"]


def _live_models():
    """Return live ``Commit`` / ``Repository`` peewee classes from the
    post-reload ``kohakuhub.db`` module — same reasoning as
    ``_live_repo_info``."""
    db_mod = sys.modules["kohakuhub.db"]
    return db_mod.Commit, db_mod.Repository


# --- Helper unit tests ------------------------------------------------------


async def test_latest_main_commits_returns_latest_per_repo(prepared_backend_test_state):
    """SQL aggregate picks the most recent main-branch commit per repo."""
    Commit, Repository = _live_models()
    demo = Repository.get(
        (Repository.repo_type == "model")
        & (Repository.namespace == "owner")
        & (Repository.name == "demo-model")
    )
    private_ds = Repository.get(
        (Repository.repo_type == "dataset")
        & (Repository.namespace == "acme-labs")
        & (Repository.name == "private-dataset")
    )

    heads = _live_repo_info()._latest_main_commits([demo.id, private_ds.id])

    assert demo.id in heads, "seed must have planted at least one main commit on demo-model"
    sha, last_at = heads[demo.id]
    assert isinstance(sha, str) and len(sha) >= 40
    assert isinstance(last_at, datetime)

    # Verify "latest" semantics: the chosen row should equal MAX(created_at)
    # over the demo repo on main.
    latest_seed_at = max(
        c.created_at
        for c in Commit.select().where(
            (Commit.repository == demo) & (Commit.branch == "main")
        )
    )
    assert last_at == latest_seed_at


async def test_latest_main_commits_filters_to_main_only(
    prepared_backend_test_state, owner_client
):
    """A non-main commit row must not influence the result for that repo."""
    Commit, Repository = _live_models()
    demo = Repository.get(
        (Repository.repo_type == "model")
        & (Repository.namespace == "owner")
        & (Repository.name == "demo-model")
    )

    # Plant a synthetic non-main commit far in the future. If the helper
    # accidentally widened its filter, we'd see this timestamp surface.
    future = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=365)
    Commit.create(
        commit_id="ff" * 20,
        repository=demo,
        repo_type="model",
        branch="experimental",
        author=demo.owner,
        owner=demo.owner,
        username=demo.owner.username,
        message="off-branch commit, must be ignored by list endpoints",
        created_at=future,
    )

    heads = _live_repo_info()._latest_main_commits([demo.id])

    assert demo.id in heads
    _, last_at = heads[demo.id]
    # The future timestamp must NOT have been picked up.
    assert last_at < future


async def test_latest_main_commits_omits_repos_without_commits(
    prepared_backend_test_state,
):
    """Repos with no main-branch commit row are absent from the dict (so
    callers know to fall back to LakeFS instead of getting a stale ``None``)."""
    _, Repository = _live_models()
    demo = Repository.get(
        (Repository.repo_type == "model")
        & (Repository.namespace == "owner")
        & (Repository.name == "demo-model")
    )
    bogus_id = -1  # never exists in the seed

    heads = _live_repo_info()._latest_main_commits([demo.id, bogus_id])

    assert demo.id in heads
    assert bogus_id not in heads, "missing repos must not appear in the dict"


async def test_latest_main_commits_handles_empty_input():
    """Cheap short-circuit: empty input returns empty dict, no SQL fires.

    The contract here protects callers that may pass an empty page from
    accidentally running ``WHERE repository IN ()`` (which is a SQL anti-
    pattern across dialects)."""
    assert _live_repo_info()._latest_main_commits([]) == {}


async def test_resolve_main_head_via_lakefs_happy_path(monkeypatch):
    """Fallback helper returns ``(sha, last_modified)`` from LakeFS."""

    class _Stub:
        async def get_branch(self, repository, branch):
            assert branch == "main"
            return {"commit_id": "abc123"}

        async def get_commit(self, repository, commit_id):
            assert commit_id == "abc123"
            return {"creation_date": 1_700_000_000}

    sha, last_modified = await _live_repo_info()._resolve_main_head_via_lakefs(
        _Stub(), "model:owner/demo-model"
    )
    assert sha == "abc123"
    assert last_modified is not None
    # ISO-ish, ends with the project's microsecond+Z marker
    assert last_modified.endswith("Z")


async def test_resolve_main_head_via_lakefs_branch_failure_returns_none(monkeypatch):
    """If ``get_branch`` errors (404, transient, etc.), helper returns
    ``(None, None)`` and does *not* raise — keeps the list response
    rendering even when LakeFS misbehaves for one row."""

    class _Stub:
        async def get_branch(self, repository, branch):
            raise RuntimeError("simulated LakeFS hiccup")

        async def get_commit(self, repository, commit_id):  # pragma: no cover
            raise AssertionError("get_commit should not be reached")

    sha, last_modified = await _live_repo_info()._resolve_main_head_via_lakefs(
        _Stub(), "model:owner/demo-model"
    )
    assert sha is None
    assert last_modified is None


async def test_resolve_main_head_via_lakefs_commit_failure_keeps_sha(monkeypatch):
    """If ``get_branch`` succeeds but ``get_commit`` errors, the sha is still
    returned — the API can show "this commit, unknown timestamp" rather than
    nothing at all."""

    class _Stub:
        async def get_branch(self, repository, branch):
            return {"commit_id": "deadbeef"}

        async def get_commit(self, repository, commit_id):
            raise RuntimeError("commit lookup down")

    sha, last_modified = await _live_repo_info()._resolve_main_head_via_lakefs(
        _Stub(), "model:owner/demo-model"
    )
    assert sha == "deadbeef"
    assert last_modified is None


# --- Endpoint-level tests --------------------------------------------------


class _LakeFSCallCounter:
    """Wraps the real LakeFS client to count specific method invocations
    without changing their behavior. Anything not tracked is delegated."""

    def __init__(self, real_client):
        self._real = real_client
        self.get_branch_calls = 0
        self.get_commit_calls = 0

    async def get_branch(self, *args, **kwargs):
        self.get_branch_calls += 1
        return await self._real.get_branch(*args, **kwargs)

    async def get_commit(self, *args, **kwargs):
        self.get_commit_calls += 1
        return await self._real.get_commit(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.mark.backend_per_test
async def test_list_models_does_zero_lakefs_round_trips_when_db_has_commits(
    prepared_backend_test_state, client, monkeypatch
):
    """The perf-win contract: with ``Commit`` rows present (the seed plants
    them), listing models must not call LakeFS ``get_branch`` or
    ``get_commit`` even once.

    If this regresses, the list endpoint has reverted to N+1 round-trips."""
    real_client = _live_repo_info().get_lakefs_client()
    counter = _LakeFSCallCounter(real_client)

    # Patch the resolver in the module under test to return our counting
    # wrapper. The fallback decorator's nested calls go through whatever
    # ``get_lakefs_client`` returns at the time, so this catches any code
    # path that resolves through the same import.
    monkeypatch.setattr(_live_repo_info(), "get_lakefs_client", lambda: counter)

    response = await client.get(
        "/api/models", params={"author": "owner", "fallback": "false"}
    )
    response.raise_for_status()
    payload = response.json()

    # Sanity: at least one repo (the seed plants ``owner/demo-model``) and
    # ``sha`` / ``lastModified`` are populated from the DB aggregate.
    demo_rows = [r for r in payload if r["id"] == "owner/demo-model"]
    assert demo_rows, "seed must include owner/demo-model"
    demo = demo_rows[0]
    assert demo["sha"] is not None and len(demo["sha"]) >= 40
    assert demo["lastModified"] is not None and demo["lastModified"].endswith("Z")

    # The whole point of the fix:
    assert counter.get_branch_calls == 0, (
        f"list endpoint hit get_branch {counter.get_branch_calls} time(s) — "
        f"the SQL aggregate fix has regressed to N+1 LakeFS round-trips."
    )
    assert counter.get_commit_calls == 0, (
        f"list endpoint hit get_commit {counter.get_commit_calls} time(s) — "
        f"the SQL aggregate fix has regressed to N+1 LakeFS round-trips."
    )


@pytest.mark.backend_per_test
async def test_list_user_repos_does_zero_lakefs_round_trips_when_db_has_commits(
    prepared_backend_test_state, owner_client, monkeypatch
):
    """Same zero-RT contract for ``/api/users/{name}/repos`` (org/user
    profile pages call this surface)."""
    real_client = _live_repo_info().get_lakefs_client()
    counter = _LakeFSCallCounter(real_client)
    monkeypatch.setattr(_live_repo_info(), "get_lakefs_client", lambda: counter)

    response = await owner_client.get("/api/users/owner/repos")
    response.raise_for_status()
    payload = response.json()
    # At least one model populated from the seed
    demo_rows = [r for r in payload["models"] if r["id"] == "owner/demo-model"]
    assert demo_rows
    assert demo_rows[0]["sha"] is not None
    assert demo_rows[0]["lastModified"] is not None

    assert counter.get_branch_calls == 0
    assert counter.get_commit_calls == 0


@pytest.mark.backend_per_test
async def test_list_models_falls_back_to_lakefs_when_commit_rows_missing(
    prepared_backend_test_state, client, owner_client, monkeypatch
):
    """When a repo has no DB ``Commit`` rows (e.g. created via ``git push``
    or ``repo rename``, neither of which calls ``create_commit`` today, or
    a freshly-created repo before the first commit), the list endpoint must
    still emit a populated ``sha`` / ``lastModified`` by falling back to
    LakeFS.

    Simulate by creating a fresh repo and *not* committing anything to it
    via the HF API. LakeFS will have an initial dangling commit on main but
    the DB ``Commit`` table will have no row for that repo. Then verify:

    - the list response still has ``sha`` (from LakeFS fallback)
    - the LakeFS fallback fires exactly once per missing row.

    This exercises the safety net that protects the API contract for the
    git-push-only / fresh-repo cases."""
    _, Repository = _live_models()
    # Create a fresh repo via the HF API. The repo creation path does NOT
    # call create_commit (only the explicit /commit endpoint does), so the
    # DB Commit table will be empty for this repo while LakeFS will have
    # an initial empty-tree commit on main.
    create_resp = await owner_client.post(
        "/api/repos/create",
        json={"type": "model", "name": "fresh-no-commit", "private": False},
    )
    create_resp.raise_for_status()

    fresh = Repository.get(
        (Repository.repo_type == "model")
        & (Repository.namespace == "owner")
        & (Repository.name == "fresh-no-commit")
    )

    # Sanity: helper sees no DB commits for the fresh repo.
    assert _live_repo_info()._latest_main_commits([fresh.id]) == {}

    real_client = _live_repo_info().get_lakefs_client()
    counter = _LakeFSCallCounter(real_client)
    monkeypatch.setattr(_live_repo_info(), "get_lakefs_client", lambda: counter)

    response = await client.get(
        "/api/models", params={"author": "owner", "fallback": "false"}
    )
    response.raise_for_status()
    payload = response.json()

    # demo-model has DB commits → SQL path, no LakeFS hit.
    # fresh-no-commit has no DB commits → LakeFS fallback fires.
    fresh_rows = [r for r in payload if r["id"] == "owner/fresh-no-commit"]
    assert fresh_rows, "fresh-no-commit must appear in owner's listing"
    fresh_payload = fresh_rows[0]
    # LakeFS' initial dangling commit on main has a sha, may or may not have
    # a creation_date depending on the LakeFS version — assert sha is filled
    # (the contract that matters for cards / clients).
    assert fresh_payload["sha"] is not None and len(fresh_payload["sha"]) >= 40

    # Exactly one fallback for the single missing row: one get_branch and
    # one get_commit. demo-model contributes zero (its DB commit row hits
    # the SQL path).
    assert counter.get_branch_calls == 1, (
        f"expected exactly one get_branch fallback for the single missing repo, "
        f"got {counter.get_branch_calls}"
    )
    assert counter.get_commit_calls == 1, (
        f"expected exactly one get_commit fallback for the single missing repo, "
        f"got {counter.get_commit_calls}"
    )

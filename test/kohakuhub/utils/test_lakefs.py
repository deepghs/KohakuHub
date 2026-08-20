"""Tests for LakeFS utility helpers."""

from types import SimpleNamespace

import pytest

from kohakuhub.utils.lakefs import (
    _sanitize_repo_id,
    allocate_lakefs_repo_name,
    lakefs_repo_name,
    resolve_lakefs_repo,
    resolve_revision,
)
from test.kohakuhub.support.fakes import FakeLakeFSClient, FakeS3Service


class _FakeLakeFSClientWithTags(FakeLakeFSClient):
    async def list_tags(self, repository, after=None, amount=None):
        repo = self.repositories[repository]
        return {
            "results": [
                {"id": tag_name, "commit_id": commit_id}
                for tag_name, commit_id in repo["tags"].items()
            ]
        }


class _PaginatedFakeLakeFSClient(_FakeLakeFSClientWithTags):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.list_tags_calls = []

    async def list_tags(self, repository, after=None, amount=None):
        self.list_tags_calls.append((after, amount))
        payload = await super().list_tags(repository, after=after, amount=amount)
        tags = payload["results"]
        if after is None:
            return {
                "results": tags[:1],
                "pagination": {"has_more": True, "next_offset": "page-2"},
            }
        return {"results": tags[1:], "pagination": {"has_more": False}}


def test_sanitize_repo_id_replaces_invalid_characters():
    assert _sanitize_repo_id("Org/My_Repo.v2!") == "org-my-repo-v2"


def test_lakefs_repo_name_is_deterministic_and_length_bound():
    repo_name = lakefs_repo_name("model", "owner/demo-model")

    assert repo_name == lakefs_repo_name("model", "owner/demo-model")
    assert len(repo_name) <= 63
    assert repo_name.startswith("m-")


def test_lakefs_repo_name_generation_zero_matches_the_legacy_name():
    """Generation 0 must stay byte-identical to the pre-generation scheme.

    Existing repositories were created under that scheme and migration 016
    backfills Repository.lakefs_repo with it, so any drift would point stored
    ids at LakeFS repositories that do not exist.
    """
    assert lakefs_repo_name("dataset", "owner/demo", generation=0) == lakefs_repo_name(
        "dataset", "owner/demo"
    )


@pytest.mark.parametrize("repo_type", ["model", "dataset", "space"])
def test_lakefs_repo_name_generations_are_distinct_and_length_bound(repo_type):
    names = {
        lakefs_repo_name(repo_type, "owner/demo-repo", generation=generation)
        for generation in range(5)
    }

    assert len(names) == 5, "each generation must map to its own LakeFS repository"
    for name in names:
        assert len(name) <= 63, "LakeFS rejects ids longer than 63 characters"
        assert name.startswith(repo_type[0])


def test_lakefs_repo_name_generation_stays_bound_for_maximally_long_ids():
    """The layout is exactly 63 chars at generation 0, so the generation token
    must live in the hash input rather than being appended to the name."""
    long_id = "a" * 40 + "/" + "b" * 60

    for generation in range(3):
        name = lakefs_repo_name("model", long_id, generation=generation)
        assert len(name) <= 63


def test_resolve_lakefs_repo_prefers_the_stored_id():
    repo = SimpleNamespace(
        repo_type="dataset", full_id="owner/demo", lakefs_repo="d-stored-id"
    )

    assert resolve_lakefs_repo(repo) == "d-stored-id"


@pytest.mark.parametrize("stored", [None, ""])
def test_resolve_lakefs_repo_falls_back_to_derivation_when_unset(stored):
    """Rows created before migration 016 have no stored id; they must keep
    resolving to the derived (generation 0) name."""
    repo = SimpleNamespace(
        repo_type="dataset", full_id="owner/demo", lakefs_repo=stored
    )

    assert resolve_lakefs_repo(repo) == lakefs_repo_name("dataset", "owner/demo")


def test_resolve_lakefs_repo_handles_rows_without_the_column():
    """A Repository object that predates the column (e.g. a stub in tests) must
    not raise - it should simply derive."""
    repo = SimpleNamespace(repo_type="model", full_id="owner/demo")

    assert resolve_lakefs_repo(repo) == lakefs_repo_name("model", "owner/demo")


@pytest.mark.asyncio
async def test_allocate_lakefs_repo_name_returns_generation_zero_when_free():
    s3 = FakeS3Service()
    lakefs = FakeLakeFSClient(s3_service=s3, default_bucket="test-bucket")

    allocated = await allocate_lakefs_repo_name(lakefs, "dataset", "owner/demo")

    assert allocated == lakefs_repo_name("dataset", "owner/demo")


@pytest.mark.asyncio
async def test_allocate_lakefs_repo_name_skips_ids_still_held_by_lakefs():
    """The core of issue #93: a rename deletes the old LakeFS repository, but
    the deletion is asynchronous, so the id stays taken for a while. Allocation
    must step over it instead of colliding."""
    s3 = FakeS3Service()
    lakefs = FakeLakeFSClient(s3_service=s3, default_bucket="test-bucket")
    taken = lakefs_repo_name("dataset", "owner/demo")
    await lakefs.create_repository(
        name=taken,
        storage_namespace=f"s3://test-bucket/{taken}",
        default_branch="main",
    )

    allocated = await allocate_lakefs_repo_name(lakefs, "dataset", "owner/demo")

    assert allocated == lakefs_repo_name("dataset", "owner/demo", generation=1)
    assert allocated != taken


@pytest.mark.asyncio
async def test_allocate_lakefs_repo_name_raises_when_every_generation_is_taken():
    class _AllTaken:
        async def repository_exists(self, repo_name):
            return True

    with pytest.raises(RuntimeError):
        await allocate_lakefs_repo_name(_AllTaken(), "model", "owner/demo")


@pytest.mark.asyncio
async def test_resolve_revision_prefers_branch_then_commit():
    s3 = FakeS3Service()
    lakefs = _FakeLakeFSClientWithTags(s3_service=s3, default_bucket="test-bucket")
    await lakefs.create_repository(
        name="m-owner-demo",
        storage_namespace="s3://test-bucket/m-owner-demo",
        default_branch="main",
    )
    branch = await lakefs.get_branch("m-owner-demo", "main")

    commit_id, commit_info = await resolve_revision(lakefs, "m-owner-demo", "main")
    assert commit_id == branch["commit_id"]
    assert commit_info["id"] == branch["commit_id"]

    commit_id_from_sha, commit_info_from_sha = await resolve_revision(
        lakefs, "m-owner-demo", branch["commit_id"]
    )
    assert commit_id_from_sha == branch["commit_id"]
    assert commit_info_from_sha["id"] == branch["commit_id"]


@pytest.mark.asyncio
async def test_resolve_revision_resolves_tag():
    s3 = FakeS3Service()
    lakefs = _PaginatedFakeLakeFSClient(s3_service=s3, default_bucket="test-bucket")
    await lakefs.create_repository(
        name="m-owner-demo",
        storage_namespace="s3://test-bucket/m-owner-demo",
        default_branch="main",
    )
    branch = await lakefs.get_branch("m-owner-demo", "main")
    await lakefs.create_tag(
        repository="m-owner-demo", id="v1.0", ref=branch["commit_id"]
    )
    await lakefs.create_tag(
        repository="m-owner-demo", id="v2.0", ref=branch["commit_id"]
    )

    commit_id, commit_info = await resolve_revision(lakefs, "m-owner-demo", "v2.0")

    assert commit_id == branch["commit_id"]
    assert commit_info["id"] == branch["commit_id"]
    assert lakefs.list_tags_calls == [(None, None), ("page-2", None)]


@pytest.mark.asyncio
async def test_resolve_revision_raises_for_missing_tag():
    s3 = FakeS3Service()
    lakefs = _FakeLakeFSClientWithTags(s3_service=s3, default_bucket="test-bucket")
    await lakefs.create_repository(
        name="m-owner-demo",
        storage_namespace="s3://test-bucket/m-owner-demo",
        default_branch="main",
    )
    branch = await lakefs.get_branch("m-owner-demo", "main")
    await lakefs.create_tag(
        repository="m-owner-demo", id="v1.0", ref=branch["commit_id"]
    )

    with pytest.raises(ValueError, match="branch, tag, or commit"):
        await resolve_revision(lakefs, "m-owner-demo", "missing-tag")


@pytest.mark.asyncio
async def test_resolve_revision_raises_for_missing_revision():
    s3 = FakeS3Service()
    lakefs = _FakeLakeFSClientWithTags(s3_service=s3, default_bucket="test-bucket")
    await lakefs.create_repository(
        name="m-owner-demo",
        storage_namespace="s3://test-bucket/m-owner-demo",
        default_branch="main",
    )

    with pytest.raises(ValueError):
        await resolve_revision(lakefs, "m-owner-demo", "missing-ref")

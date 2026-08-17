"""Focused tests for main-only commit projection finalization."""

from __future__ import annotations

import pytest

from kohakuhub.operations.finalizer import finalize_commit_domain
from kohakuhub.api.commit.routers.operations import _estimate_commit_quota_delta


class _Connection:
    def __init__(self):
        self.queries: list[str] = []

    async def execute(self, query, _params=None):
        self.queries.append(" ".join(str(query).split()).lower())


@pytest.mark.asyncio
async def test_feature_commit_does_not_mutate_main_file_projection_or_quota():
    connection = _Connection()
    await finalize_commit_domain(
        connection,
        payload={
            "repository_id": 1,
            "owner_id": 1,
            "commit_id": "feature-commit",
            "repo_type": "model",
            "branch": "feature",
            "author_id": 1,
            "username": "owner",
            "file_mutations": [
                {
                    "path": "README.md",
                    "size": 3,
                    "sha256": "abc",
                    "lfs": False,
                    "action": "upsert",
                }
            ],
            "quota_delta": 99,
        },
    )

    assert any("insert into commit" in query for query in connection.queries)
    assert not any("insert into file" in query for query in connection.queries)
    assert not any("update file" in query for query in connection.queries)
    assert not any("update repository" in query for query in connection.queries)


@pytest.mark.asyncio
async def test_quota_estimate_rejects_duplicate_or_overlapping_paths():
    with pytest.raises(ValueError, match="overlapping"):
        await _estimate_commit_quota_delta(
            None,
            [
                {"key": "file", "value": {"path": "a.txt", "content": ""}},
                {"key": "file", "value": {"path": "a.txt", "content": ""}},
            ],
            None,
            "repo",
        )

    with pytest.raises(ValueError, match="overlapping"):
        await _estimate_commit_quota_delta(
            None,
            [
                {"key": "file", "value": {"path": "dir/a.txt", "content": ""}},
                {"key": "deletedFolder", "value": {"path": "dir"}},
            ],
            None,
            "repo",
        )

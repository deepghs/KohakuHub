"""Built-in handlers for post-commit work."""

from __future__ import annotations

import asyncio
from typing import Any

from kohakuhub.api.quota.util import update_namespace_storage, update_repository_storage
from kohakuhub.api.repo.utils.gc import run_gc_for_file, track_lfs_object
from kohakuhub.config import cfg
from kohakuhub.db import Repository

from .types import OperationRecord, StepRecord, StepResult


async def perform_commit_postprocess(payload: dict[str, Any]) -> dict[str, int]:
    """Finalize bounded commit metadata outside the API request path."""

    repository_id = int(payload["repository_id"])
    repo = await asyncio.to_thread(Repository.get_by_id, repository_id)
    if repo is None:
        raise ValueError("commit post-process repository no longer exists")

    tracked = 0
    for info in payload.get("lfs_tracking", []):
        await asyncio.to_thread(
            track_lfs_object,
            repo_type=payload["repo_type"],
            namespace=payload["namespace"],
            name=payload["name"],
            path_in_repo=info["path"],
            sha256=info["sha256"],
            size=int(info["size"]),
            commit_id=payload["commit_id"],
        )
        tracked += 1
        if cfg.app.lfs_auto_gc and info.get("old_sha256"):
            await asyncio.to_thread(
                run_gc_for_file,
                repo_type=payload["repo_type"],
                namespace=payload["namespace"],
                name=payload["name"],
                path_in_repo=info["path"],
                current_commit_id=payload["commit_id"],
            )

    storage = await update_repository_storage(repo)
    organization = bool(payload.get("is_org", False))
    namespace_storage = await update_namespace_storage(
        payload["namespace"], organization
    )
    return {
        "tracked_lfs": tracked,
        "repository_used_bytes": int(storage["used_bytes"]),
        "namespace_used_bytes": int(namespace_storage["total_bytes"]),
    }


async def commit_postprocess_handler(
    _operation: OperationRecord, step: StepRecord
) -> StepResult:
    result = await perform_commit_postprocess(step.input_json)
    return StepResult.succeeded(progress_current=1, progress_total=1, result_json=result)

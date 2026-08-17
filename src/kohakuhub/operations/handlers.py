"""Built-in handlers for post-commit work."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import psycopg2
from botocore.exceptions import BotoCoreError

from kohakuhub.api.repo.utils.gc import (
    run_gc_for_file,
    track_commit_lfs_objects,
    track_lfs_object,
)
from kohakuhub.config import cfg
from kohakuhub.db import Repository, User
from kohakuhub.db_operations import get_organization
from kohakuhub import lakefs_mutation_gateway as mutation_gateway
from kohakuhub.db_operations import create_commit, get_commit, get_repository
from kohakuhub.utils.lakefs import get_lakefs_client

from .types import OperationRecord, RetryableOperationError, StepRecord, StepResult


def _read_incremental_usage(
    repository_id: int, namespace: str, is_org: bool
) -> dict[str, int]:
    """Read counters maintained by commit finalization.

    A normal post-commit task must be O(changed paths).  Full LakeFS and
    namespace scans remain explicit reconciliation operations and must not be
    hidden behind this handler.
    """

    repo = Repository.get_by_id(repository_id)
    if repo is None:
        raise ValueError("commit post-process repository no longer exists")
    entity = (
        get_organization(namespace)
        if is_org
        else User.get_or_none(User.username == namespace)
    )
    if entity is None:
        raise ValueError("commit post-process namespace no longer exists")
    private_used = int(entity.private_used_bytes or 0)
    public_used = int(entity.public_used_bytes or 0)
    selected = private_used if repo.private else public_used
    return {
        "repository_used_bytes": int(repo.used_bytes or 0),
        "namespace_used_bytes": selected,
        "namespace_total_used_bytes": private_used + public_used,
    }


async def perform_commit_postprocess(payload: dict[str, Any]) -> dict[str, int]:
    """Finalize bounded commit metadata outside the API request path."""

    repository_id = int(payload["repository_id"])
    await asyncio.to_thread(
        _read_incremental_usage,
        repository_id,
        str(payload["namespace"]),
        bool(payload.get("is_org", False)),
    )

    tracked = 0
    deferred_gc = 0
    for info in payload.get("lfs_tracking", []):
        try:
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
        except (BotoCoreError, ConnectionError, OSError, TimeoutError, httpx.HTTPError, psycopg2.OperationalError) as exc:
            raise RetryableOperationError(
                error_code="postprocess_dependency_unavailable",
                error_summary="commit post-processing dependency is temporarily unavailable",
            ) from exc
        tracked += 1
        # Destructive GC is not part of the durable worker consumer until it
        # has a deletion tombstone and observer.  The legacy SQLite fallback
        # may opt in explicitly; PostgreSQL worker payloads never do.
        if cfg.app.lfs_auto_gc and info.get("old_sha256") and payload.get(
            "allow_destructive_gc", False
        ):
            try:
                await asyncio.to_thread(
                    run_gc_for_file,
                    repo_type=payload["repo_type"],
                    namespace=payload["namespace"],
                    name=payload["name"],
                    path_in_repo=info["path"],
                    current_commit_id=payload["commit_id"],
                )
            except (BotoCoreError, ConnectionError, OSError, TimeoutError, httpx.HTTPError, psycopg2.OperationalError) as exc:
                raise RetryableOperationError(
                    error_code="postprocess_dependency_unavailable",
                    error_summary="commit cleanup dependency is temporarily unavailable",
                ) from exc
        elif cfg.app.lfs_auto_gc and info.get("old_sha256"):
            deferred_gc += 1

    usage = await asyncio.to_thread(
        _read_incremental_usage,
        repository_id,
        str(payload["namespace"]),
        bool(payload.get("is_org", False)),
    )
    return {"tracked_lfs": tracked, "gc_deferred": deferred_gc, **usage}


async def commit_postprocess_handler(
    _operation: OperationRecord, step: StepRecord
) -> StepResult:
    result = await perform_commit_postprocess(step.input_json)
    return StepResult(
        state="succeeded",
        progress_current=1,
        progress_total=1,
        result_json=result,
        external_effect_confirmed=True,
    )


async def finalize_revert_commit(
    payload: dict[str, Any],
    *,
    commit_id: str,
) -> dict[str, Any]:
    """Finalize a confirmed revert commit without dispatching LakeFS again."""

    lakefs_repo = str(payload["lakefs_repo"])
    branch = str(payload["branch"])
    new_commit_id = str(commit_id)
    repo = await asyncio.to_thread(
        get_repository,
        str(payload["repo_type"]),
        str(payload["namespace"]),
        str(payload["name"]),
    )
    author = await asyncio.to_thread(User.get_by_id, int(payload["author_id"]))
    if repo is None or author is None:
        raise ValueError("revert repository or author no longer exists")
    existing_commit = await asyncio.to_thread(get_commit, new_commit_id, repo)
    if existing_commit is not None:
        return {
            "success": True,
            "new_commit_id": new_commit_id,
            "message": f"Successfully reverted commit {str(payload['ref'])[:8]} on branch '{branch}'",
        }
    # This helper is idempotent at the LFS history key and the Commit model
    # uses the LakeFS commit id as its identity.
    await track_commit_lfs_objects(
        lakefs_repo=lakefs_repo,
        commit_id=new_commit_id,
        repo_type=str(payload["repo_type"]),
        namespace=str(payload["namespace"]),
        name=str(payload["name"]),
    )
    await asyncio.to_thread(
        create_commit,
        commit_id=new_commit_id,
        repository=repo,
        repo_type=str(payload["repo_type"]),
        branch=branch,
        author=author,
        username=str(payload["username"]),
        message=str(payload.get("message") or f"Revert commit {str(payload['ref'])[:8]}"),
        description=f"Reverted {payload['ref']}",
    )
    return {
        "success": True,
        "new_commit_id": new_commit_id,
        "message": f"Successfully reverted commit {str(payload['ref'])[:8]} on branch '{branch}'",
    }


async def perform_revert_operation(
    payload: dict[str, Any],
    *,
    marker: str,
) -> dict[str, Any]:
    """Run one marker-bearing LakeFS revert and bounded local finalization."""

    client = get_lakefs_client()
    lakefs_repo = str(payload["lakefs_repo"])
    branch = str(payload["branch"])
    current_branch = await client.get_branch(repository=lakefs_repo, branch=branch)
    expected_head = payload.get("base_head") or payload.get("expected_head")
    if expected_head and str(current_branch.get("commit_id")) != str(expected_head):
        raise ValueError("revert expected branch head changed before dispatch")
    metadata = dict(payload.get("metadata") or {})
    metadata["khub_marker"] = marker
    await mutation_gateway.revert_branch(
        client,
        repository=lakefs_repo,
        branch=branch,
        ref=str(payload["ref"]),
        parent_number=int(payload.get("parent_number", 1)),
        message=payload.get("message"),
        metadata=metadata,
        force=bool(payload.get("force", False)),
        allow_empty=bool(payload.get("allow_empty", False)),
    )
    branch_info = await client.get_branch(repository=lakefs_repo, branch=branch)
    return await finalize_revert_commit(
        payload,
        commit_id=str(branch_info["commit_id"]),
    )


async def revert_operation_handler(
    _operation: OperationRecord, step: StepRecord
) -> StepResult:
    marker = step.external_marker or f"khub:operation:v1:{_operation.id}:step:{step.id}"
    result = await perform_revert_operation(step.input_json, marker=marker)
    return StepResult(
        state="succeeded",
        progress_current=1,
        progress_total=1,
        progress_message="revert completed",
        result_json=result,
        external_effect_confirmed=True,
    )

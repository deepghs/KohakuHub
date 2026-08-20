"""Commit creation endpoint - Refactored version with smaller functions."""

from datetime import datetime, timezone
from enum import Enum
import asyncio
import base64
import hashlib
import json
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from kohakuhub.config import cfg
from kohakuhub.db import File, LFSObjectHistory, Repository, User
from kohakuhub.db_operations import (
    create_commit,
    get_effective_lfs_threshold,
    get_file,
    get_repo_file_map,
    get_organization,
    should_use_lfs,
)
from kohakuhub.logger import get_logger
from kohakuhub.auth.dependencies import get_current_user
from kohakuhub.auth.permissions import check_repo_write_permission
from kohakuhub.utils.lakefs import get_lakefs_client, resolve_lakefs_repo
from kohakuhub.utils.s3 import get_object_metadata, object_exists
from kohakuhub.api.repo.utils.hf import HFErrorCode
from kohakuhub.operations.handlers import perform_commit_postprocess
from kohakuhub import lakefs_mutation_gateway as mutation_gateway
from kohakuhub.operations.service import (
    CommitInProgress,
    IdempotencyConflict,
    QuotaExceeded,
)

logger = get_logger("FILE")
router = APIRouter()
# LakeFS stages one object per request. Four concurrent uploads keep a large
# commit below the combined LakeFS/MinIO and backend connection pressure while
# leaving headroom for the branch HEAD/commit calls.
COMMIT_STAGE_CONCURRENCY = 4
_UNSET_FILE = object()


def _log_commit_payload_debug(lines: list[str], raw: bytes) -> None:
    """Log commit diagnostics without emitting private NDJSON content."""

    if cfg.app.debug_log_payloads:
        logger.debug(
            "Commit payload received: {} lines, {} bytes",
            len(lines),
            len(raw),
        )


def _lakefs_status_code(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def _record_file_upsert(
    file_mutations: list[dict] | None,
    *,
    repo: Repository,
    path: str,
    size: int,
    sha256: str,
    lfs: bool,
) -> None:
    """Record a finalization delta or preserve the legacy helper behavior."""

    if file_mutations is not None:
        file_mutations.append(
            {
                "action": "upsert",
                "path": path,
                "size": int(size),
                "sha256": sha256,
                "lfs": bool(lfs),
            }
        )
        return
    File.insert(
        repository=repo,
        path_in_repo=path,
        size=size,
        sha256=sha256,
        lfs=lfs,
        is_deleted=False,
        owner=repo.owner,
    ).on_conflict(
        conflict_target=(File.repository, File.path_in_repo),
        update={
            File.sha256: sha256,
            File.size: size,
            File.lfs: lfs,
            File.is_deleted: False,
            File.updated_at: datetime.now(timezone.utc),
        },
    ).execute()


def _record_file_delete(
    file_mutations: list[dict] | None,
    *,
    repo: Repository,
    path: str,
) -> None:
    if file_mutations is not None:
        file_mutations.append({"action": "delete", "path": path})
        return
    File.update(is_deleted=True, updated_at=datetime.now(timezone.utc)).where(
        (File.repository == repo) & (File.path_in_repo == path)
    ).execute()


class RepoType(str, Enum):
    """Repository type enumeration."""

    model = "model"
    dataset = "dataset"
    space = "space"


def build_public_repo_path(repo_type: RepoType, repo_id: str) -> str:
    """Return the HF-compatible repository path used in commit responses."""
    return f"{repo_type.value}s/{repo_id}"


def calculate_git_blob_sha1(content: bytes) -> str:
    """Calculate SHA1 hash in git blob format.

    Git uses: sha1(f'blob {size}\\0' + content)

    Args:
        content: File content bytes

    Returns:
        SHA1 hex digest
    """
    size = len(content)
    sha = hashlib.sha1()
    sha.update(f"blob {size}\0".encode("utf-8"))
    sha.update(content)
    return sha.hexdigest()


async def _estimate_commit_quota_delta(
    repo: Repository,
    operations: list[dict],
    client,
    lakefs_repo: str,
    existing_files: dict[str, File] | None = None,
) -> int:
    """Estimate the positive net storage delta before LakeFS staging.

    Non-LFS usage follows current-HEAD files. LFS usage follows unique history
    objects and is never released by a delete. The estimate is intentionally
    conservative for a copy whose source is absent from the metadata mirror;
    the LakeFS stat call supplies the missing size before any mutation.
    """

    # Applying two mutations to the same path (or a file and one of its
    # parent folders) against one pre-commit snapshot makes subtraction
    # order-dependent and can under/over-count quota.  Reject the ambiguous
    # request before staging; callers can retry with a canonical operation set.
    seen_paths: set[str] = set()
    normalized_paths: list[tuple[str, str]] = []
    for operation in operations:
        path = str(operation.get("value", {}).get("path", "")).strip("/")
        if not path:
            continue
        key = operation.get("key", "")
        normalized_paths.append((path, key))
        if path in seen_paths:
            raise ValueError("commit contains overlapping mutations")
        seen_paths.add(path)
    for path, _key in normalized_paths:
        prefix = path + "/"
        if any(other != path and (other.startswith(prefix) or path.startswith(other + "/")) for other, _ in normalized_paths):
            raise ValueError("commit contains overlapping file and folder mutations")

    delta = 0

    def active_file(path: str) -> File | None:
        if existing_files is None:
            return get_file(repo, path)
        existing = existing_files.get(path)
        return existing if existing is not None and not existing.is_deleted else None

    def subtract_current(path: str) -> None:
        nonlocal delta
        existing = active_file(path)
        if existing and not existing.is_deleted and not existing.lfs:
            delta -= int(existing.size)

    for operation in operations:
        key = operation["key"]
        value = operation["value"]
        path = value.get("path")
        if key == "file":
            data = base64.b64decode(value.get("content", ""))
            subtract_current(path)
            delta += len(data)
        elif key == "lfsFile":
            oid = value.get("oid")
            size = int(value.get("size") or 0)
            existing = active_file(path)
            if existing and not existing.is_deleted and existing.lfs and existing.sha256 == oid:
                continue
            lfs_key = f"lfs/{oid[:2]}/{oid[2:4]}/{oid}"
            actual_size = await get_object_metadata(cfg.s3.bucket, lfs_key)
            size = int(actual_size["size"])
            if not LFSObjectHistory.get_or_none(
                (LFSObjectHistory.repository == repo)
                & (LFSObjectHistory.sha256 == oid)
            ):
                delta += size
        elif key == "deletedFile":
            subtract_current(path)
        elif key == "deletedFolder":
            folder_path = path if path.endswith("/") else f"{path}/"
            for existing in (
                File.select(File.size, File.lfs)
                .where(
                    (File.repository == repo)
                    & (File.path_in_repo.startswith(folder_path))
                    # Peewee builds SQL expressions from this comparison; a
                    # Python ``not`` would evaluate the field immediately.
                    & (File.is_deleted == False)  # noqa: E712
                )
                .tuples()
            ):
                if not existing[1]:
                    delta -= int(existing[0])
        elif key == "copyFile":
            subtract_current(path)
            source = active_file(value.get("srcPath"))
            if source is None:
                source_object = await client.stat_object(
                    repository=lakefs_repo,
                    ref=value.get("srcRevision", "main"),
                    path=value.get("srcPath"),
                )
                if should_use_lfs(repo, path, int(source_object["size_bytes"])):
                    if not LFSObjectHistory.get_or_none(
                        (LFSObjectHistory.repository == repo)
                        & (LFSObjectHistory.sha256 == source_object["checksum"])
                    ):
                        delta += int(source_object["size_bytes"])
                else:
                    delta += int(source_object["size_bytes"])
            elif source.lfs:
                if not LFSObjectHistory.get_or_none(
                    (LFSObjectHistory.repository == repo)
                    & (LFSObjectHistory.sha256 == source.sha256)
                ):
                    delta += int(source.size)
            else:
                delta += int(source.size)
    return delta


async def process_regular_file(
    path: str,
    content_b64: str,
    encoding: str,
    repo: Repository,
    lakefs_repo: str,
    revision: str,
    file_mutations: list[dict] | None = None,
    existing_file: File | None | object = _UNSET_FILE,
) -> bool:
    """Process regular file with inline base64 content.

    IMPORTANT: This should only be used for small files (< LFS threshold).
    Large files MUST use the lfsFile operation to avoid duplication.

    Args:
        path: File path in repository
        content_b64: Base64 encoded content
        encoding: Content encoding
        repo: Repository object
        lakefs_repo: LakeFS repository name
        revision: Branch name

    Returns:
        True if file was changed, False if unchanged

    Raises:
        HTTPException: If processing fails or file is too large for standard commit
    """
    if not encoding.startswith("base64"):
        raise HTTPException(400, detail={"error": f"Invalid file operation for {path}"})

    # Decode content
    try:
        data = base64.b64decode(content_b64)
    except Exception as e:
        raise HTTPException(400, detail={"error": f"Failed to decode base64: {e}"})

    # Check file size against LFS threshold (use repo-specific settings)
    file_size = len(data)
    lfs_threshold = get_effective_lfs_threshold(repo)

    # Also check if file suffix requires LFS
    if should_use_lfs(repo, path, file_size):
        # File should use LFS (either by size or suffix rule)
        raise HTTPException(
            400,
            detail={
                "error": f"File {path} should use LFS (size: {file_size} bytes, threshold: {lfs_threshold} bytes). "
                f"Files >= {lfs_threshold} bytes or matching LFS suffix rules must be uploaded through Git LFS. "
                f"Use 'lfsFile' operation instead of 'file' operation.",
                "file_size": file_size,
                "lfs_threshold": lfs_threshold,
                "suggested_operation": "lfsFile",
            },
        )

    # Calculate git blob SHA1 for non-LFS files (HuggingFace format)
    git_blob_sha1 = calculate_git_blob_sha1(data)

    # Check if file unchanged (deduplication)
    existing = get_file(repo, path) if existing_file is _UNSET_FILE else existing_file
    if existing and existing.sha256 == git_blob_sha1 and existing.size == len(data):
        if existing.is_deleted:
            # File was deleted, now being restored - need to re-upload to LakeFS
            logger.info(
                f"Restoring deleted non-LFS file: {path} (sha256={git_blob_sha1[:8]}, size={file_size:,})"
            )
        else:
            # File unchanged and active, skip
            logger.info(f"Skipping unchanged file: {path}")
            return False

    # File changed or needs restoration
    if existing and existing.is_deleted:
        logger.info(f"Uploading to restore non-LFS file: {path} ({file_size} bytes)")
    else:
        logger.info(f"Uploading regular file: {path} ({file_size} bytes)")

    # Upload to LakeFS
    try:
        client = get_lakefs_client()
        await mutation_gateway.upload_object(
            client,
            repository=lakefs_repo,
            branch=revision,
            path=path,
            content=data,
        )
    except Exception as e:
        raise HTTPException(500, detail={"error": f"Failed to upload {path}: {e}"})

    # Apply this only after LakeFS commit in production.  Direct helper calls
    # retain the legacy write path for compatibility with isolated tests/tools.
    _record_file_upsert(
        file_mutations,
        repo=repo,
        path=path,
        size=len(data),
        sha256=git_blob_sha1,
        lfs=False,
    )

    return True


async def process_lfs_file(
    path: str,
    oid: str,
    size: int,
    algo: str,
    repo: Repository,
    lakefs_repo: str,
    revision: str,
    file_mutations: list[dict] | None = None,
    existing_file: File | None | object = _UNSET_FILE,
) -> tuple[bool, dict | None]:
    """Process LFS file that was uploaded to S3.

    Args:
        path: File path in repository
        oid: Object ID (SHA256 hash)
        size: File size in bytes
        algo: Hash algorithm (default: sha256)
        repo: Repository object
        lakefs_repo: LakeFS repository name
        revision: Branch name

    Returns:
        Tuple of (changed: bool, lfs_tracking_info: dict | None)

    Raises:
        HTTPException: If processing fails
    """
    if not oid:
        raise HTTPException(400, detail={"error": f"Missing OID for LFS file {path}"})

    # Check for existing file (including deleted files to detect re-upload)
    existing = (
        File.get_or_none((File.repository == repo) & (File.path_in_repo == path))
        if existing_file is _UNSET_FILE
        else existing_file
    )

    # Track old LFS object for potential deletion
    old_lfs_oid = None
    if existing and existing.lfs and existing.sha256 != oid:
        old_lfs_oid = existing.sha256
        logger.info(f"File {path} will be replaced: {old_lfs_oid} → {oid}")

    # Check if same content (including deleted files)
    # If same sha256+size, DON'T create new LFSObjectHistory
    same_content = existing and existing.sha256 == oid and existing.size == size

    if same_content:
        if existing.is_deleted:
            logger.info(
                f"[PROCESS_LFS_FILE] Re-uploading deleted file: {path} (sha256={oid[:8]}, size={size:,}) "
                f"- RESTORING in LakeFS (reusing existing LFSObjectHistory)"
            )
            # File was deleted, now being restored
            # Need to link physical address in LakeFS to restore the file
            # But DON'T create new LFSObjectHistory (already exists)

            # Construct S3 physical address
            lfs_key = f"lfs/{oid[:2]}/{oid[2:4]}/{oid}"
            physical_address = f"s3://{cfg.s3.bucket}/{lfs_key}"

            # Link the physical S3 object to LakeFS to restore
            try:
                staging_metadata = {
                    "staging": {
                        "physical_address": physical_address,
                    },
                    "checksum": f"{algo}:{oid}",
                    "size_bytes": size,
                }

                client = get_lakefs_client()
                await mutation_gateway.link_physical_address(
                    client,
                    repository=lakefs_repo,
                    branch=revision,
                    path=path,
                    staging_metadata=staging_metadata,
                )

                logger.success(
                    f"Successfully restored LFS file in LakeFS: {path} "
                    f"(oid: {oid[:8]}, size: {size}, physical: {physical_address})"
                )

            except Exception as e:
                logger.exception(
                    f"Failed to restore LFS file in LakeFS: {path} "
                    f"(oid: {oid[:8]}, repo: {lakefs_repo}, branch: {revision})",
                    e,
                )
                raise HTTPException(
                    500,
                    detail={
                        "error": f"Failed to restore LFS file {path} in LakeFS: {str(e)}"
                    },
                )

            if file_mutations is None:
                File.update(
                    is_deleted=False, updated_at=datetime.now(timezone.utc)
                ).where(File.id == existing.id).execute()
            else:
                _record_file_upsert(
                    file_mutations,
                    repo=repo,
                    path=path,
                    size=size,
                    sha256=oid,
                    lfs=True,
                )
            logger.success(f"Restored deleted file metadata: {path}")

            # Return tracking info for new commit (reusing existing LFS object)
            return True, {
                "path": path,
                "sha256": oid,
                "size": size,
                "old_sha256": None,  # No old version (same content, just restoring)
            }
        else:
            logger.info(
                f"[PROCESS_LFS_FILE] File unchanged: {path} (sha256={oid[:8]}, size={size:,}) "
                f"- WILL TRACK in LFSObjectHistory"
            )
            # File exists and is active - normal case
            # Still return tracking info for new commit
            return False, {
                "path": path,
                "sha256": oid,
                "size": size,
                "old_sha256": None,  # No old version (file unchanged)
            }

    # File changed or new
    logger.info(f"Linking LFS file: {path}")

    # Construct S3 physical address
    lfs_key = f"lfs/{oid[:2]}/{oid[2:4]}/{oid}"
    physical_address = f"s3://{cfg.s3.bucket}/{lfs_key}"

    # Verify object exists in S3
    try:
        exists = await object_exists(cfg.s3.bucket, lfs_key)
        if not exists:
            logger.error(
                f"LFS object not found in S3: {oid[:8]} "
                f"(path: {path}, bucket: {cfg.s3.bucket}, key: {lfs_key})"
            )
            raise HTTPException(
                400,
                detail={
                    "error": f"LFS object {oid} not found in storage. "
                    f"Upload to S3 may have failed. Path: {lfs_key}"
                },
            )
    except HTTPException:
        raise  # Re-raise HTTPException as-is
    except Exception as e:
        logger.exception(
            f"Failed to check S3 existence for LFS object {oid[:8]} "
            f"(path: {path}, bucket: {cfg.s3.bucket}, key: {lfs_key})",
            e,
        )
        raise HTTPException(
            500, detail={"error": f"Failed to verify LFS object in S3: {str(e)}"}
        )

    # Get actual size from S3 to verify
    try:
        s3_metadata = await get_object_metadata(cfg.s3.bucket, lfs_key)
        actual_size = s3_metadata["size"]

        if actual_size != size:
            logger.warning(
                f"Size mismatch for {path}. Expected: {size}, Got: {actual_size} "
                f"(oid: {oid[:8]}, key: {lfs_key})"
            )
            size = actual_size
    except Exception as e:
        logger.exception(
            f"Failed to get S3 metadata for LFS object {oid[:8]} "
            f"(path: {path}, bucket: {cfg.s3.bucket}, key: {lfs_key})",
            e,
        )
        logger.warning("Could not verify S3 object metadata, continuing without size check")

    # Link the physical S3 object to LakeFS
    try:
        staging_metadata = {
            "staging": {
                "physical_address": physical_address,
            },
            "checksum": f"{algo}:{oid}",
            "size_bytes": size,
        }

        client = get_lakefs_client()
        await mutation_gateway.link_physical_address(
            client,
            repository=lakefs_repo,
            branch=revision,
            path=path,
            staging_metadata=staging_metadata,
        )

        logger.success(
            f"Successfully linked LFS file in LakeFS: {path} "
            f"(oid: {oid[:8]}, size: {size}, physical: {physical_address})"
        )

    except Exception as e:
        logger.exception(
            f"Failed to link LFS file in LakeFS: {path} "
            f"(oid: {oid[:8]}, repo: {lakefs_repo}, branch: {revision}, "
            f"physical_address: {physical_address})",
            e,
        )
        raise HTTPException(
            500,
            detail={"error": f"Failed to link LFS file {path} in LakeFS: {str(e)}"},
        )

    _record_file_upsert(
        file_mutations,
        repo=repo,
        path=path,
        size=size,
        sha256=oid,
        lfs=True,
    )

    logger.success(f"Updated database record for LFS file: {path}")

    # Return tracking info for GC
    tracking_info = {
        "path": path,
        "sha256": oid,
        "size": size,
        "old_sha256": old_lfs_oid,
    }

    logger.info(
        f"[PROCESS_LFS_FILE] File changed/new: {path} (sha256={oid[:8]}, size={size:,}) "
        f"- WILL TRACK in LFSObjectHistory"
    )

    return True, tracking_info


async def process_deleted_file(
    path: str,
    repo: Repository,
    lakefs_repo: str,
    revision: str,
    file_mutations: list[dict] | None = None,
) -> bool:
    """Process file deletion.

    Marks file as deleted (soft delete) instead of removing from database.
    This preserves LFSObjectHistory FK references for quota tracking.

    Args:
        path: File path to delete
        repo: Repository object
        lakefs_repo: LakeFS repository name
        revision: Branch name

    Returns:
        True (always changes repository)
    """
    logger.info(f"Deleting file: {path}")

    try:
        client = get_lakefs_client()
        await mutation_gateway.delete_object(
            client, repository=lakefs_repo, branch=revision, path=path
        )
        logger.success(f"Successfully deleted file from LakeFS: {path}")
    except httpx.HTTPStatusError as e:
        if _lakefs_status_code(e) != 404:
            raise HTTPException(
                502,
                detail={"error": f"LakeFS failed to delete {path}"},
            ) from e
        logger.info(f"LakeFS reported {path} already absent; treating delete as no-op")
    except Exception as e:
        raise HTTPException(
            502,
            detail={"error": f"LakeFS failed to delete {path}"},
        ) from e

    _record_file_delete(file_mutations, repo=repo, path=path)
    if file_mutations is not None:
        logger.info(f"Recorded {path} for post-commit soft delete")
    else:
        logger.success(f"Marked {path} as deleted in database (soft delete)")

    return True


async def process_deleted_folder(
    path: str,
    repo: Repository,
    lakefs_repo: str,
    revision: str,
    file_mutations: list[dict] | None = None,
) -> bool:
    """Process folder deletion.

    Args:
        path: Folder path to delete
        repo: Repository object
        lakefs_repo: LakeFS repository name
        revision: Branch name

    Returns:
        True (always changes repository)
    """
    # Normalize folder path
    folder_path = path if path.endswith("/") else f"{path}/"
    logger.info(f"Deleting folder: {folder_path}")

    try:
        client = get_lakefs_client()

        # List all objects in the folder with pagination
        all_folder_objects = []
        after = ""
        has_more = True

        while has_more:
            objects = await client.list_objects(
                repository=lakefs_repo,
                ref=revision,
                prefix=folder_path,
                delimiter="",
                amount=1000,
                after=after,
            )

            all_folder_objects.extend(objects["results"])

            if objects.get("pagination") and objects["pagination"].get("has_more"):
                next_offset = objects["pagination"].get("next_offset")
                if not next_offset or str(next_offset) == str(after):
                    raise HTTPException(
                        502,
                        detail={"error": "LakeFS returned a non-advancing folder cursor"},
                    )
                after = next_offset
                has_more = True
            else:
                has_more = False

        # Delete each file concurrently
        file_objects = [
            obj for obj in all_folder_objects if obj["path_type"] == "object"
        ]

        semaphore = asyncio.Semaphore(32)

        async def delete_file_obj(obj):
            try:
                async with semaphore:
                    await mutation_gateway.delete_object(
                        client,
                        repository=lakefs_repo, branch=revision, path=obj["path"]
                    )
                logger.info(f"  Deleted: {obj['path']}")
                return obj["path"]
            except httpx.HTTPStatusError as e:
                if _lakefs_status_code(e) == 404:
                    return obj["path"]
                return e
            except Exception as e:
                return e

        results = await asyncio.gather(*[delete_file_obj(obj) for obj in file_objects])
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise HTTPException(
                502,
                detail={
                    "error": f"LakeFS failed to delete {len(failures)} folder object(s)"
                },
            ) from failures[0]
        deleted_files = [path for path in results if path is not None]

        logger.success(f"Deleted {len(deleted_files)} files from folder {folder_path}")

        if deleted_files:
            if file_mutations is not None:
                file_mutations.extend(
                    {"action": "delete", "path": deleted_path}
                    for deleted_path in deleted_files
                )
                logger.info(
                    f"Recorded {len(deleted_files)} file(s) for post-commit soft delete"
                )
            else:
                updated_count = (
                    File.update(is_deleted=True, updated_at=datetime.now(timezone.utc))
                    .where(
                        (File.repository == repo)
                        & (File.path_in_repo.startswith(folder_path))
                    )
                    .execute()
                )
                logger.success(
                    f"Marked {updated_count} file(s) as deleted in database (soft delete)"
                )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            502,
            detail={"error": f"LakeFS failed to delete folder {folder_path}"},
        ) from e

    return True


async def process_copy_file(
    dest_path: str,
    src_path: str,
    src_revision: str,
    repo: Repository,
    lakefs_repo: str,
    revision: str,
    file_mutations: list[dict] | None = None,
    source_file: File | None | object = _UNSET_FILE,
) -> bool:
    """Process file copy operation.

    Args:
        dest_path: Destination file path
        src_path: Source file path
        src_revision: Source revision
        repo: Repository object
        lakefs_repo: LakeFS repository name
        revision: Branch name

    Returns:
        True (always changes repository)

    Raises:
        HTTPException: If copy fails
    """
    if not src_path:
        raise HTTPException(
            400, detail={"error": "Missing srcPath for copyFile operation"}
        )

    logger.info(
        f"Copying file: {src_path} -> {dest_path} (from revision: {src_revision})"
    )

    try:
        # Get source file metadata from LakeFS
        client = get_lakefs_client()
        src_obj = await client.stat_object(
            repository=lakefs_repo, ref=src_revision, path=src_path
        )

        # Use LakeFS staging API to link the physical address
        staging_metadata = {
            "staging": {
                "physical_address": src_obj["physical_address"],
            },
            "checksum": src_obj["checksum"],
            "size_bytes": src_obj["size_bytes"],
        }

        await mutation_gateway.link_physical_address(
            client,
            repository=lakefs_repo,
            branch=revision,
            path=dest_path,
            staging_metadata=staging_metadata,
        )

        logger.success(
            f"Successfully linked {dest_path} to same physical address as {src_path}"
        )

        # Capture metadata for post-commit finalization.  Direct helper calls
        # still use the old Peewee path for compatibility.
        src_file = get_file(repo, src_path) if source_file is _UNSET_FILE else source_file

        if src_file:
            _record_file_upsert(
                file_mutations,
                repo=repo,
                path=dest_path,
                size=src_file.size,
                sha256=src_file.sha256,
                lfs=src_file.lfs,
            )
        else:
            # If not in database, create entry based on LakeFS info
            # Use repo-specific LFS settings
            is_lfs = should_use_lfs(repo, dest_path, src_obj["size_bytes"])
            _record_file_upsert(
                file_mutations,
                repo=repo,
                path=dest_path,
                size=src_obj["size_bytes"],
                sha256=src_obj["checksum"],
                lfs=is_lfs,
            )

        logger.success(f"Successfully copied {src_path} to {dest_path}")

    except Exception as e:
        raise HTTPException(
            500,
            detail={
                "error": f"Failed to copy file {src_path} to {dest_path}: {str(e)}"
            },
        )

    return True


@router.post("/{repo_type}s/{namespace}/{name}/commit/{revision}")
async def commit(
    repo_type: RepoType,
    namespace: str,
    name: str,
    revision: str,
    request: Request,
    user: User = Depends(get_current_user),
):
    """Acquire the shared PostgreSQL ref fence around the full mutation."""

    # The implementation below retains the existing HF-compatible parsing
    # and response path.  The wrapper is intentionally thin so every LakeFS
    # staging call, including the final commit, shares one cross-process gate.
    if request.query_params.get("create_pr") not in ("1", "true", "True"):
        repo_row = Repository.get_or_none(
            (Repository.full_id == f"{namespace}/{name}")
            & (Repository.repo_type == repo_type.value)
        )
        if repo_row is not None:
            check_repo_write_permission(repo_row, user)
            request_app = getattr(request, "app", None)
            request_state = getattr(request_app, "state", None)
            runtime_present = request_state is not None and hasattr(
                request_state, "operation_runtime"
            )
            runtime = getattr(request_state, "operation_runtime", None)
            operation_service = getattr(runtime, "service", None)
            compatibility_mode = bool(
                getattr(request_state, "_khub_test_compatibility", False)
            )
            if (
                cfg.app.db_backend == "postgres"
                and isinstance(request, Request)
                and not compatibility_mode
            ):
                if not runtime_present or operation_service is None:
                    raise HTTPException(
                        status_code=503,
                        detail={"error": "mutation_fence_unavailable"},
                    )
            repository_ref_fence = getattr(
                operation_service, "repository_ref_fence", None
            )
            if (
                cfg.app.db_backend == "postgres"
                and isinstance(request, Request)
                and not compatibility_mode
            ):
                if not callable(repository_ref_fence):
                    raise HTTPException(
                        status_code=503,
                        detail={"error": "mutation_fence_unavailable"},
                    )
                try:
                    async with repository_ref_fence(repo_row.id, revision):
                        return await _commit_unlocked(
                            repo_type, namespace, name, revision, request, user
                        )
                except CommitInProgress as exc:
                    await _raise_commit_observing(
                        operation_service,
                        exc.intent,
                        message="Another commit for this ref is still being observed",
                    )
    return await _commit_unlocked(repo_type, namespace, name, revision, request, user)


async def _raise_commit_observing(
    operation_service: Any,
    intent: Any,
    *,
    message: str = "Commit result is being observed",
) -> None:
    """Expose a durable status handle without allowing a redispatch."""

    operation_id = None
    try:
        operation = await operation_service.ensure_commit_observation_operation(
            intent.id
        )
        operation_id = getattr(operation, "id", None)
    except Exception:
        logger.exception("Failed to create commit observation operation")

    detail: dict[str, Any] = {"error": message}
    headers = {"Retry-After": "30"}
    if operation_id is not None:
        operation_id = str(operation_id)
        detail["operation_id"] = operation_id
        headers["Location"] = f"{cfg.app.api_base}/operations/{operation_id}"
    raise HTTPException(503, detail=detail, headers=headers)


async def _commit_unlocked(
    repo_type: RepoType,
    namespace: str,
    name: str,
    revision: str,
    request: Request,
    user: User,
):
    """Create atomic commit with multiple file operations.

    Accepts NDJSON payload with header and file operations.
    Supports inline base64 content for small files and LFS references for large files.

    Args:
        repo_type: Type of repository
        namespace: Repository namespace
        name: Repository name
        revision: Branch name
        request: FastAPI request with NDJSON payload
        user: Current authenticated user

    Returns:
        Commit result with OID and URL

    Raises:
        HTTPException: If commit fails
    """
    repo_id = f"{namespace}/{name}"

    # `create_pr=True` sends ``?create_pr=1`` on the commit endpoint. KohakuHub
    # does not implement the discussions / pull-request workflow, and silently
    # dropping the flag (as the old handler did) means the commit lands on the
    # target branch instead of an isolated ``refs/pr/<N>`` — a compat-breaking
    # surprise. Reject it up front with a HuggingFace-compatible 501 so the
    # client surfaces a clear HfHubHTTPError carrying our X-Error-Message.
    if request.query_params.get("create_pr") in ("1", "true", "True"):
        raise HTTPException(
            status_code=501,
            detail={
                "error": (
                    "create_commit(create_pr=True) is not supported by KohakuHub. "
                    "Commit to a branch directly instead; the discussions / "
                    "pull-request workflow is not implemented."
                )
            },
            headers={
                "X-Error-Code": HFErrorCode.NOT_IMPLEMENTED,
                "X-Error-Message": (
                    "create_commit(create_pr=True) is not supported by KohakuHub. "
                    "Commit to a branch directly instead; the discussions / "
                    "pull-request workflow is not implemented."
                ),
            },
        )

    # Check repository exists and write permission
    repo_row = Repository.get_or_none(
        (Repository.full_id == repo_id) & (Repository.repo_type == repo_type.value)
    )
    if not repo_row:
        raise HTTPException(404, detail={"error": "Repository not found"})

    check_repo_write_permission(repo_row, user)

    lakefs_repo = resolve_lakefs_repo(repo_row)
    client = get_lakefs_client()
    request_app = getattr(request, "app", None)
    runtime = getattr(getattr(request_app, "state", None), "operation_runtime", None)
    operation_service = getattr(runtime, "service", None)

    try:
        base_branch = await client.get_branch(
            repository=lakefs_repo, branch=revision
        )
        base_head = str(base_branch["commit_id"])
    except Exception as exc:
        raise HTTPException(
            503,
            detail={"error": "Unable to resolve commit base head"},
        ) from exc

    # Parse NDJSON payload
    raw = await request.body()
    lines = raw.decode("utf-8").splitlines()

    _log_commit_payload_debug(lines, raw)

    # Parse header and operations
    header = None
    operations = []

    for line in lines:
        if not line.strip():
            continue

        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise HTTPException(400, detail={"error": f"Invalid JSON line: {e}"})

        key = obj.get("key")
        value = obj.get("value", {})

        if key == "header":
            header = value
        elif key in ("file", "lfsFile", "deletedFile", "deletedFolder", "copyFile"):
            operations.append({"key": key, "value": value})

    if header is None:
        raise HTTPException(400, detail={"error": "Missing commit header"})

    # Persist the request identity before touching LakeFS staging.  The body
    # hash is separate from the staged finalization payload because the latter
    # is only known after object metadata has been resolved.
    files_changed = False
    pending_lfs_tracking = []
    file_mutations: list[dict] = []
    existing_files: dict[str, File] | None = None
    request_headers = getattr(request, "headers", {})
    idempotency_key = request_headers.get("x-khub-idempotency-key") or request_headers.get(
        "idempotency-key"
    )
    request_hash = hashlib.sha256(
        json.dumps(
            {
                "repo_type": repo_type.value,
                "repository_id": repo_row.id,
                "ref": revision,
                "header": header,
                "operations": operations,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    intent = None
    batch_staging_supported = callable(
        getattr(operation_service, "record_prepared_staging_paths", None)
    )
    if cfg.app.db_backend == "postgres" and batch_staging_supported:
        lookup_paths = {
            str(candidate).strip("/")
            for operation in operations
            for candidate in (
                operation.get("value", {}).get("path"),
                operation.get("value", {}).get("srcPath")
                if operation.get("key") == "copyFile"
                else None,
            )
            if candidate
        }
        try:
            existing_files = get_repo_file_map(repo_row, lookup_paths)
        except Exception as exc:
            raise HTTPException(
                503,
                detail={"error": "Unable to load commit file metadata"},
                headers={"Retry-After": "30"},
            ) from exc
    if (
        callable(getattr(operation_service, "prepare_commit_intent", None))
        and cfg.app.db_backend == "postgres"
    ):
        try:
            if existing_files is None:
                # Keep the lightweight route/test doubles on the original
                # call shape; production PostgreSQL runtimes use the shared
                # batch map above.
                quota_delta = await _estimate_commit_quota_delta(
                    repo_row, operations, client, lakefs_repo
                )
            else:
                quota_delta = await _estimate_commit_quota_delta(
                    repo_row,
                    operations,
                    client,
                    lakefs_repo,
                    existing_files=existing_files,
                )
        except ValueError as exc:
            raise HTTPException(
                400,
                detail={"error": "invalid_commit_operations", "detail": str(exc)},
            ) from exc
        except Exception as exc:
            raise HTTPException(
                503,
                detail={"error": "Unable to calculate commit quota delta"},
                headers={"Retry-After": "30"},
            ) from exc
        deterministic_intent_id = (
            uuid5(
                NAMESPACE_URL,
                f"khub-commit:{user.id}:{repo_row.id}:{revision}:{idempotency_key}",
            )
            if idempotency_key
            else None
        )
        existing_intent = (
            await operation_service.get_commit_intent(deterministic_intent_id)
            if deterministic_intent_id is not None
            else None
        )
        if existing_intent is not None:
            if existing_intent.request_hash != request_hash:
                raise HTTPException(
                    409,
                    detail={"error": "Idempotency key is bound to another commit request"},
                )
            if existing_intent.state == "finalized" and existing_intent.result_json:
                return existing_intent.result_json
            if existing_intent.state in {"committed", "reconciliation_required"}:
                if existing_intent.payload_json.get("file_mutations") is not None:
                    existing_result = dict(existing_intent.result_json or {})
                    existing_result.setdefault(
                        "commitOid", existing_intent.lakefs_commit_id
                    )
                    existing_result.setdefault("commitUrl", "")
                    await operation_service.finalize_commit_intent(
                        existing_intent.id,
                        payload=existing_intent.payload_json,
                        result_json=existing_result,
                        requested_by_user_id=user.id,
                        idempotency_key=idempotency_key or str(existing_intent.id),
                    )
                    return existing_result
            await _raise_commit_observing(
                operation_service,
                existing_intent,
                message="Commit request is still being observed",
            )

        initial_intent_payload = {
            "repository_id": repo_row.id,
            "repo_type": repo_type.value,
            "namespace": namespace,
            "name": name,
            "lakefs_repo": lakefs_repo,
            "branch": revision,
            "base_head": base_head,
            "is_org": get_organization(namespace) is not None,
            "file_mutations": [],
            "lfs_tracking": [],
            "author_id": getattr(user, "id", None),
            "owner_id": getattr(repo_row, "owner_id", None)
            or getattr(getattr(repo_row, "owner", None), "id", None),
            "username": user.username,
            "message": header.get("summary", "Commit via API"),
            "description": header.get("description", ""),
            "is_private": bool(repo_row.private),
            "quota_delta": quota_delta,
            "staging_paths": [],
        }
        try:
            intent = await operation_service.prepare_commit_intent(
                repository_id=repo_row.id,
                ref=revision,
                base_head=base_head,
                payload=initial_intent_payload,
                intent_id=deterministic_intent_id,
                requested_by_user_id=getattr(user, "id", None),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                quota_delta=quota_delta,
            )
        except CommitInProgress as exc:
            await _raise_commit_observing(
                operation_service,
                exc.intent,
                message="Another commit for this ref is still being observed",
            )
        except QuotaExceeded as exc:
            raise HTTPException(
                413,
                detail={"error": "Storage quota exceeded"},
            ) from exc

    # Record every target before the first LakeFS staging mutation.  The batch
    # write is important for large HF commits: recovery needs the full target
    # set, but it should not turn one request into one JSONB rewrite per file.
    use_batch_staging = intent is not None and batch_staging_supported
    if use_batch_staging:
        intent = await operation_service.record_prepared_staging_paths(
            intent.id,
            paths=[
                {
                    "path": str(operation["value"].get("path")),
                    "recursive": operation["key"] == "deletedFolder",
                }
                for operation in operations
                if operation["value"].get("path")
            ],
        )

    # Process operations using match-case.  The ref fence acquired by the
    # route wrapper is held for this complete staging and commit sequence.

    index = 0
    while index < len(operations):
        op = operations[index]
        key = op["key"]
        value = op["value"]
        path = value.get("path")
        logger.info(f"Processing {key}: {path}")

        # Independent regular-file uploads commute because overlapping paths
        # were rejected by the quota preflight.  Keep a small cap so a large
        # HF commit uses the pooled LakeFS client without overwhelming it.
        if key == "file":
            group_end = index + 1
            while group_end < len(operations) and operations[group_end]["key"] == "file":
                group_end += 1
            group = operations[index:group_end]
            if not use_batch_staging and intent is not None:
                for group_op in group:
                    group_path = group_op["value"].get("path")
                    if group_path:
                        await operation_service.record_prepared_staging_path(
                            intent.id,
                            path=str(group_path),
                            recursive=False,
                        )

            async def stage_regular_file(group_op: dict) -> tuple[bool, list[dict]]:
                local_mutations: list[dict] = []
                group_value = group_op["value"]
                existing = (
                    existing_files.get(group_value.get("path"))
                    if existing_files is not None
                    else _UNSET_FILE
                )
                if existing is not _UNSET_FILE and existing is not None and existing.is_deleted:
                    existing = None
                async with stage_semaphore:
                    changed = await process_regular_file(
                        path=group_value.get("path"),
                        content_b64=group_value.get("content"),
                        encoding=(group_value.get("encoding") or "").lower(),
                        repo=repo_row,
                        lakefs_repo=lakefs_repo,
                        revision=revision,
                        file_mutations=local_mutations,
                        existing_file=existing,
                    )
                return changed, local_mutations

            stage_semaphore = asyncio.Semaphore(COMMIT_STAGE_CONCURRENCY)
            if len(group) > 1 and existing_files is not None:
                staged = await asyncio.gather(
                    *(stage_regular_file(group_op) for group_op in group),
                    return_exceptions=True,
                )
                for result in staged:
                    if isinstance(result, BaseException):
                        raise result
                    changed, mutations = result
                    files_changed = files_changed or changed
                    file_mutations.extend(mutations)
            else:
                for group_op in group:
                    changed, mutations = await stage_regular_file(group_op)
                    files_changed = files_changed or changed
                    file_mutations.extend(mutations)
            index = group_end
            continue

        if intent is not None and path and not use_batch_staging:
            # Record the target before the first LakeFS staging mutation so a
            # stale prepared intent can reset the unchanged branch safely.
            await operation_service.record_prepared_staging_path(
                intent.id,
                path=str(path),
                recursive=key == "deletedFolder",
            )

        match key:
            case "lfsFile":
                # LFS file already in S3
                changed, lfs_info = await process_lfs_file(
                    path=path,
                    oid=value.get("oid"),
                    size=value.get("size"),
                    algo=value.get("algo", "sha256"),
                    repo=repo_row,
                    lakefs_repo=lakefs_repo,
                    revision=revision,
                    file_mutations=file_mutations,
                    existing_file=(
                        existing_files.get(path) if existing_files is not None else _UNSET_FILE
                    ),
                )
                files_changed = files_changed or changed
                if lfs_info:
                    logger.debug(
                        f"[COMMIT_OP] Adding LFS file to tracking queue: {path} "
                        f"(sha256={lfs_info['sha256'][:8]}, size={lfs_info['size']:,})"
                    )
                    pending_lfs_tracking.append(lfs_info)
                else:
                    logger.warning(
                        f"[COMMIT_OP] process_lfs_file returned NO tracking info for: {path} "
                        f"(oid={value.get('oid', 'MISSING')[:8]})"
                    )

            case "deletedFile":
                # Delete single file
                changed = await process_deleted_file(
                    path=path,
                    repo=repo_row,
                    lakefs_repo=lakefs_repo,
                    revision=revision,
                    file_mutations=file_mutations,
                )
                files_changed = files_changed or changed

            case "deletedFolder":
                # Delete folder recursively
                changed = await process_deleted_folder(
                    path=path,
                    repo=repo_row,
                    lakefs_repo=lakefs_repo,
                    revision=revision,
                    file_mutations=file_mutations,
                )
                files_changed = files_changed or changed

            case "copyFile":
                # Copy file
                source_file = (
                    existing_files.get(value.get("srcPath"))
                    if existing_files is not None
                    else _UNSET_FILE
                )
                if source_file is not _UNSET_FILE and (
                    source_file is None or source_file.is_deleted
                ):
                    source_file = None
                changed = await process_copy_file(
                    dest_path=path,
                    src_path=value.get("srcPath"),
                    src_revision=value.get("srcRevision", revision),
                    repo=repo_row,
                    lakefs_repo=lakefs_repo,
                    revision=revision,
                    file_mutations=file_mutations,
                    source_file=source_file,
                )
                files_changed = files_changed or changed

        index += 1

    # If no files changed, return early
    if not files_changed:
        if intent is not None:
            await operation_service.abandon_commit_intent(
                intent.id, error_code="no_changes"
            )
        try:
            branch = await client.get_branch(repository=lakefs_repo, branch=revision)
            commit_id = branch["commit_id"]
        except Exception as exc:
            raise HTTPException(
                503,
                detail={"error": "commit_head_unavailable"},
                headers={"Retry-After": "30"},
            ) from exc

        commit_url = f"{build_public_repo_path(repo_type, repo_id)}/commit/{commit_id}"

        return {
            "commitUrl": commit_url,
            "commitOid": commit_id,
            "pullRequestUrl": None,
        }

    # Do not publish a stale staging branch.  LakeFS currently does not expose
    # an expected-head argument on this client, so this check is the local
    # fail-closed CAS boundary used until a server-side conditional update is
    # available.
    try:
        current_branch = await client.get_branch(
            repository=lakefs_repo, branch=revision
        )
        if str(current_branch["commit_id"]) != base_head:
            raise HTTPException(
                409,
                detail={"error": "Repository head changed during commit preparation"},
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            503,
            detail={"error": "Unable to validate commit base head"},
        ) from exc

    postprocess_payload = {
        "repository_id": repo_row.id,
        "repo_type": repo_type.value,
        "namespace": namespace,
        "name": name,
        "branch": revision,
        "base_head": base_head,
        "is_org": get_organization(namespace) is not None,
        "lfs_tracking": pending_lfs_tracking,
        "file_mutations": file_mutations,
        "author_id": getattr(user, "id", None),
        "owner_id": getattr(repo_row, "owner_id", None)
        or getattr(getattr(repo_row, "owner", None), "id", None),
        "username": user.username,
        "message": header.get("summary", "Commit via API"),
        "description": header.get("description", ""),
        "lakefs_repo": lakefs_repo,
        "is_private": bool(getattr(repo_row, "private", False)),
        "quota_delta": int((intent.payload_json if intent else {}).get("quota_delta", 0)),
        # Destructive legacy GC is explicitly unavailable to the PostgreSQL
        # durable worker.  Only the SQLite compatibility path may opt in.
        "allow_destructive_gc": cfg.app.db_backend != "postgres",
    }
    if intent is not None:
        intent = await operation_service.update_prepared_commit_payload(
            intent.id, payload=postprocess_payload
        )

    # Create commit in LakeFS
    commit_msg = header.get("summary", "Commit via API")
    commit_desc = header.get("description", "")
    logger.info(f"Commit message: {commit_msg}")

    if intent is not None:
        await operation_service.mark_commit_intent_dispatch_started(intent.id)

    try:
        commit_metadata = {
            "description": commit_desc,
        } if commit_desc else {}
        if intent is not None:
            commit_metadata.update(
                {
                    "khub_operation_id": str(intent.id),
                    "khub_marker": intent.marker,
                    "khub_payload_hash": intent.payload_hash,
                }
            )
        commit_result = await mutation_gateway.commit(
            client,
            repository=lakefs_repo,
            branch=revision,
            message=commit_msg,
            metadata=commit_metadata or None,
        )
    except Exception as e:
        if intent is not None:
            # A transport error after dispatch is ambiguous.  Preserve the
            # intent and force callers through observation instead of a second
            # LakeFS commit.
            from httpx import HTTPStatusError

            if isinstance(e, HTTPStatusError) and e.response.status_code in {
                400,
                404,
                422,
            }:
                await operation_service.abandon_commit_intent(
                    intent.id,
                    error_code=f"lakefs_http_{e.response.status_code}",
                    confirmed_no_effect=True,
                )
                raise HTTPException(
                    e.response.status_code,
                    detail={"error": "LakeFS rejected the commit"},
                ) from e
            raise HTTPException(
                503,
                detail={"error": "Commit result is being observed"},
                headers={"Retry-After": "30"},
            ) from e
        raise HTTPException(500, detail={"error": f"Commit failed: {str(e)}"})

    commit_id = commit_result["id"]
    logger.info(f"LakeFS accepted commit {commit_id[:8]}; visibility is reconciled asynchronously")

    # Generate commit URL
    commit_url = (
        f"{build_public_repo_path(repo_type, repo_id)}/commit/{commit_result['id']}"
    )
    logger.success(f"Commit URL: {commit_url}")

    finalization_payload = dict(postprocess_payload)
    finalization_payload["commit_id"] = commit_result["id"]
    # A confirmed LakeFS commit is never converted into an HTTP failure if
    # PostgreSQL finalization is temporarily unavailable.  The committed
    # intent/marker is the recovery source and a later reaper must finalize it
    # without dispatching LakeFS again.
    if operation_service is not None and intent is not None:
        try:
            await operation_service.mark_commit_intent_committed(
                intent.id,
                lakefs_commit_id=commit_result["id"],
                result_json={
                    "commitUrl": commit_url,
                    "commitOid": commit_result["id"],
                    "pullRequestUrl": None,
                },
            )
            await operation_service.finalize_commit_intent(
                intent.id,
                payload=finalization_payload,
                result_json={
                    "commitUrl": commit_url,
                    "commitOid": commit_result["id"],
                    "pullRequestUrl": None,
                },
                requested_by_user_id=getattr(user, "id", None),
                idempotency_key=f"commit-postprocess:{commit_result['id']}",
            )
        except IdempotencyConflict:
            logger.error(
                f"Commit post-process idempotency conflict for {commit_result['id'][:8]}"
            )
        except Exception as e:
            logger.exception(
                f"Commit finalization deferred for {commit_result['id'][:8]}: {e}"
            )
    elif operation_service is not None:
        # Compatibility path for lightweight runtime test doubles.  Real
        # PostgreSQL runtimes always expose commit intent methods.
        try:
            await operation_service.accept(
                kind="commit.postprocess.v1",
                resource_key=f"commit-postprocess:{repo_row.id}:{commit_result['id']}",
                payload=finalization_payload,
                requested_by_user_id=getattr(user, "id", None),
                idempotency_key=f"commit-postprocess:{commit_result['id']}",
                repository_id=repo_row.id,
                expected_head=commit_result["id"],
            )
        except Exception as e:
            logger.exception(
                f"Failed to enqueue commit post-process for {commit_result['id'][:8]}: {e}"
            )
    else:
        # ASGI unit tests and lightweight SQLite development do not enter the
        # production lifespan. Keep that explicit path functional; production
        # PostgreSQL requests always use the durable operation runtime.
        try:
            # Legacy SQLite/unit-test path: apply the same finalization after
            # LakeFS success, preserving the old helper contract.
            for mutation in file_mutations:
                if mutation["action"] == "delete":
                    _record_file_delete(None, repo=repo_row, path=mutation["path"])
                else:
                    _record_file_upsert(
                        None,
                        repo=repo_row,
                        path=mutation["path"],
                        size=mutation["size"],
                        sha256=mutation["sha256"],
                        lfs=mutation["lfs"],
                    )
            create_commit(
                commit_id=commit_result["id"],
                repository=repo_row,
                repo_type=repo_type.value,
                branch=revision,
                author=user,
                username=user.username,
                message=commit_msg,
                description=commit_desc,
            )
            await perform_commit_postprocess(finalization_payload)
        except Exception as e:
            logger.warning(f"Commit post-process fallback failed: {e}")

    return {
        "commitUrl": commit_url,
        "commitOid": commit_result["id"],
        "pullRequestUrl": None,
    }

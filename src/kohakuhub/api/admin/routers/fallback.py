"""Admin API endpoints for fallback source management."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from kohakuhub.db import FallbackSource, User, db
from kohakuhub.logger import get_logger
from kohakuhub.api.admin.utils.auth import verify_admin_token
from kohakuhub.api.fallback.cache import get_cache

logger = get_logger("ADMIN_FALLBACK")
router = APIRouter()


class FallbackSourceCreate(BaseModel):
    """Model for creating a fallback source."""

    namespace: str = ""  # "" for global, or user/org name
    url: str
    token: Optional[str] = None
    priority: int = 100
    name: str
    source_type: str  # "huggingface" or "kohakuhub"
    enabled: bool = True


class FallbackSourceUpdate(BaseModel):
    """Model for updating a fallback source."""

    url: Optional[str] = None
    token: Optional[str] = None
    priority: Optional[int] = None
    name: Optional[str] = None
    source_type: Optional[str] = None
    enabled: Optional[bool] = None


class FallbackSourceResponse(BaseModel):
    """Model for fallback source response."""

    id: int
    namespace: str
    url: str
    token: Optional[str]
    priority: int
    name: str
    source_type: str
    enabled: bool
    created_at: str
    updated_at: str


@router.post("/fallback-sources", response_model=FallbackSourceResponse)
async def create_fallback_source(
    payload: FallbackSourceCreate, _admin=Depends(verify_admin_token)
):
    """Create a new fallback source.

    Args:
        payload: Fallback source creation data
        _admin: Admin authentication dependency

    Returns:
        Created fallback source
    """
    # Validate source_type
    match payload.source_type:
        case "huggingface" | "kohakuhub":
            pass
        case _:
            raise HTTPException(
                400,
                detail={
                    "error": f"Invalid source_type: {payload.source_type}. Must be 'huggingface' or 'kohakuhub'"
                },
            )

    try:
        source = FallbackSource.create(
            namespace=payload.namespace,
            url=payload.url.rstrip("/"),
            token=payload.token,
            priority=payload.priority,
            name=payload.name,
            source_type=payload.source_type,
            enabled=payload.enabled,
            created_at=datetime.now(tz=timezone.utc),
            updated_at=datetime.now(tz=timezone.utc),
        )

        # Strict-freshness invalidation (#79): a new source can change
        # which source wins for any given repo (e.g. higher-priority
        # source supersedes a previously-bound lower-priority one).
        # Match the existing UPDATE/DELETE paths that already
        # ``cache.clear()``.
        cache = get_cache()
        cache.clear()

        logger.info(f"Created fallback source: {source.name} ({source.url})")

        return FallbackSourceResponse(
            id=source.id,
            namespace=source.namespace,
            url=source.url,
            token=source.token,
            priority=source.priority,
            name=source.name,
            source_type=source.source_type,
            enabled=source.enabled,
            created_at=(
                source.created_at.isoformat()
                if isinstance(source.created_at, datetime)
                else source.created_at
            ),
            updated_at=(
                source.updated_at.isoformat()
                if isinstance(source.updated_at, datetime)
                else source.updated_at
            ),
        )

    except Exception as e:
        logger.error(f"Failed to create fallback source: {e}")
        raise HTTPException(500, detail={"error": f"Failed to create source: {str(e)}"})


@router.get("/fallback-sources", response_model=list[FallbackSourceResponse])
async def list_fallback_sources(
    namespace: Optional[str] = None,
    enabled: Optional[bool] = None,
    _admin=Depends(verify_admin_token),
):
    """List all fallback sources.

    Args:
        namespace: Filter by namespace (optional)
        enabled: Filter by enabled status (optional)
        _admin: Admin authentication dependency

    Returns:
        List of fallback sources
    """
    try:
        query = FallbackSource.select().order_by(FallbackSource.priority)

        if namespace is not None:
            query = query.where(FallbackSource.namespace == namespace)

        if enabled is not None:
            query = query.where(FallbackSource.enabled == enabled)

        sources = list(query)

        return [
            FallbackSourceResponse(
                id=s.id,
                namespace=s.namespace,
                url=s.url,
                token=s.token,
                priority=s.priority,
                name=s.name,
                source_type=s.source_type,
                enabled=s.enabled,
                created_at=(
                    s.created_at.isoformat()
                    if isinstance(s.created_at, datetime)
                    else s.created_at
                ),
                updated_at=(
                    s.updated_at.isoformat()
                    if isinstance(s.updated_at, datetime)
                    else s.updated_at
                ),
            )
            for s in sources
        ]

    except Exception as e:
        logger.error(f"Failed to list fallback sources: {e}")
        raise HTTPException(500, detail={"error": f"Failed to list sources: {str(e)}"})


@router.get("/fallback-sources/{source_id}", response_model=FallbackSourceResponse)
async def get_fallback_source(source_id: int, _admin=Depends(verify_admin_token)):
    """Get a specific fallback source.

    Args:
        source_id: Source ID
        _admin: Admin authentication dependency

    Returns:
        Fallback source
    """
    try:
        source = FallbackSource.get_by_id(source_id)

        return FallbackSourceResponse(
            id=source.id,
            namespace=source.namespace,
            url=source.url,
            token=source.token,
            priority=source.priority,
            name=source.name,
            source_type=source.source_type,
            enabled=source.enabled,
            created_at=(
                source.created_at.isoformat()
                if isinstance(source.created_at, datetime)
                else source.created_at
            ),
            updated_at=(
                source.updated_at.isoformat()
                if isinstance(source.updated_at, datetime)
                else source.updated_at
            ),
        )

    except FallbackSource.DoesNotExist:
        raise HTTPException(404, detail={"error": "Fallback source not found"})
    except Exception as e:
        logger.error(f"Failed to get fallback source: {e}")
        raise HTTPException(500, detail={"error": f"Failed to get source: {str(e)}"})


@router.put("/fallback-sources/{source_id}", response_model=FallbackSourceResponse)
async def update_fallback_source(
    source_id: int, payload: FallbackSourceUpdate, _admin=Depends(verify_admin_token)
):
    """Update a fallback source.

    Args:
        source_id: Source ID
        payload: Update data
        _admin: Admin authentication dependency

    Returns:
        Updated fallback source
    """
    try:
        source = FallbackSource.get_by_id(source_id)

        # Update fields if provided
        if payload.url is not None:
            source.url = payload.url.rstrip("/")
        if payload.token is not None:
            source.token = payload.token
        if payload.priority is not None:
            source.priority = payload.priority
        if payload.name is not None:
            source.name = payload.name
        if payload.source_type is not None:
            match payload.source_type:
                case "huggingface" | "kohakuhub":
                    source.source_type = payload.source_type
                case _:
                    raise HTTPException(
                        400,
                        detail={
                            "error": f"Invalid source_type: {payload.source_type}. Must be 'huggingface' or 'kohakuhub'"
                        },
                    )
        if payload.enabled is not None:
            source.enabled = payload.enabled

        source.updated_at = datetime.now(tz=timezone.utc)
        source.save()

        logger.info(f"Updated fallback source: {source.name} (ID: {source.id})")

        # Clear cache when source is updated
        cache = get_cache()
        cache.clear()
        logger.info("Cleared fallback cache after source update")

        return FallbackSourceResponse(
            id=source.id,
            namespace=source.namespace,
            url=source.url,
            token=source.token,
            priority=source.priority,
            name=source.name,
            source_type=source.source_type,
            enabled=source.enabled,
            created_at=(
                source.created_at.isoformat()
                if isinstance(source.created_at, datetime)
                else source.created_at
            ),
            updated_at=(
                source.updated_at.isoformat()
                if isinstance(source.updated_at, datetime)
                else source.updated_at
            ),
        )

    except FallbackSource.DoesNotExist:
        raise HTTPException(404, detail={"error": "Fallback source not found"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update fallback source: {e}")
        raise HTTPException(500, detail={"error": f"Failed to update source: {str(e)}"})


@router.delete("/fallback-sources/{source_id}")
async def delete_fallback_source(source_id: int, _admin=Depends(verify_admin_token)):
    """Delete a fallback source.

    Args:
        source_id: Source ID
        _admin: Admin authentication dependency

    Returns:
        Success message
    """
    try:
        source = FallbackSource.get_by_id(source_id)
        source_name = source.name

        source.delete_instance()

        logger.info(f"Deleted fallback source: {source_name} (ID: {source_id})")

        # Clear cache when source is deleted
        cache = get_cache()
        cache.clear()
        logger.info("Cleared fallback cache after source deletion")

        return {"success": True, "message": f"Fallback source {source_name} deleted"}

    except FallbackSource.DoesNotExist:
        raise HTTPException(404, detail={"error": "Fallback source not found"})
    except Exception as e:
        logger.error(f"Failed to delete fallback source: {e}")
        raise HTTPException(500, detail={"error": f"Failed to delete source: {str(e)}"})


@router.get("/fallback-sources/cache/stats")
async def get_cache_stats(_admin=Depends(verify_admin_token)):
    """Get fallback cache statistics.

    Args:
        _admin: Admin authentication dependency

    Returns:
        Cache statistics
    """
    try:
        cache = get_cache()
        stats = cache.stats()

        return {
            "size": stats["size"],
            "maxsize": stats["maxsize"],
            "ttl_seconds": stats["ttl_seconds"],
            "usage_percent": (
                round((stats["size"] / stats["maxsize"]) * 100, 2)
                if stats["maxsize"] > 0
                else 0
            ),
        }

    except Exception as e:
        logger.error(f"Failed to get cache stats: {e}")
        raise HTTPException(500, detail={"error": f"Failed to get stats: {str(e)}"})


@router.delete("/fallback-sources/cache/clear")
async def clear_cache(_admin=Depends(verify_admin_token)):
    """Clear the fallback cache.

    Args:
        _admin: Admin authentication dependency

    Returns:
        Success message
    """
    try:
        cache = get_cache()
        old_size = cache.stats()["size"]
        cache.clear()

        logger.info(f"Cleared fallback cache (was {old_size} entries)")

        return {
            "success": True,
            "message": f"Cache cleared ({old_size} entries removed)",
            "old_size": old_size,
        }

    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(500, detail={"error": f"Failed to clear cache: {str(e)}"})


@router.delete("/fallback-sources/cache/repo/{repo_type}/{namespace}/{name}")
async def invalidate_repo_cache(
    repo_type: str,
    namespace: str,
    name: str,
    _admin=Depends(verify_admin_token),
):
    """Evict every cached binding for one repo across all user buckets.

    Bumps ``repo_gens[(repo_type, namespace, name)]`` so any fallback
    probe currently in flight for this repo will have its ``safe_set``
    rejected. Use for operational hygiene — e.g. when you know a repo's
    upstream state changed and want every user's next request to
    re-probe immediately.

    Args:
        repo_type: "model", "dataset", or "space"
        namespace: Repository namespace
        name: Repository name
        _admin: Admin authentication dependency

    Returns:
        Eviction count.
    """
    try:
        cache = get_cache()
        evicted = cache.invalidate_repo(repo_type, namespace, name)

        logger.info(
            f"Admin invalidated fallback cache for "
            f"{repo_type}/{namespace}/{name} ({evicted} entries)"
        )

        return {
            "success": True,
            "evicted": evicted,
            "repo_type": repo_type,
            "namespace": namespace,
            "name": name,
        }

    except Exception as e:
        logger.error(f"Failed to invalidate repo cache: {e}")
        raise HTTPException(
            500, detail={"error": f"Failed to invalidate repo cache: {str(e)}"}
        )


@router.delete("/fallback-sources/cache/user/{user_id}")
async def invalidate_user_cache(
    user_id: int,
    _admin=Depends(verify_admin_token),
):
    """Evict every cached binding for one user across all repos.

    Bumps ``user_gens[user_id]`` so any fallback probe currently in
    flight for this user will have its ``safe_set`` rejected. Use for
    operational hygiene — e.g. when a user's external token has been
    rotated externally and the user_id-keyed cache needs to drop.

    Args:
        user_id: User PK (use ``0`` or negative integers for special
            buckets if needed; the anonymous bucket is keyed by ``None``
            internally and is not addressable through this endpoint —
            use ``DELETE /admin/api/fallback-sources/cache/clear``
            instead to wipe the anonymous bucket).
        _admin: Admin authentication dependency

    Returns:
        Eviction count.
    """
    try:
        cache = get_cache()
        evicted = cache.clear_user(user_id)

        logger.info(
            f"Admin invalidated fallback cache for user_id={user_id} "
            f"({evicted} entries)"
        )

        return {
            "success": True,
            "evicted": evicted,
            "user_id": user_id,
        }

    except Exception as e:
        logger.error(f"Failed to invalidate user cache: {e}")
        raise HTTPException(
            500, detail={"error": f"Failed to invalidate user cache: {str(e)}"}
        )


@router.delete("/fallback-sources/cache/username/{username}")
async def invalidate_user_cache_by_username(
    username: str,
    _admin=Depends(verify_admin_token),
):
    """Evict cache for one user identified by username (UX-friendly path).

    Convenience wrapper around ``DELETE .../cache/user/{user_id}``: looks
    up the user by name and forwards to ``cache.clear_user(user.id)``.
    The admin frontend uses this so operators don't have to hand-look up
    a numeric user_id.

    Args:
        username: Username (case-sensitive, matches the User table).
        _admin: Admin authentication dependency.

    Returns:
        ``{success, evicted, user_id, username}``.

    Raises:
        404 if no user with that username exists.
    """
    try:
        user = User.get_or_none(User.username == username)
        if user is None:
            raise HTTPException(
                404, detail={"error": f"User not found: {username}"}
            )
        cache = get_cache()
        evicted = cache.clear_user(user.id)

        logger.info(
            f"Admin invalidated fallback cache for username={username} "
            f"(user_id={user.id}, {evicted} entries)"
        )

        return {
            "success": True,
            "evicted": evicted,
            "user_id": user.id,
            "username": username,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to invalidate user cache by username: {e}")
        raise HTTPException(
            500,
            detail={
                "error": f"Failed to invalidate user cache by username: {str(e)}"
            },
        )


# ===========================================================================
# Bulk-replace endpoint (#78)
# ===========================================================================
#
# Powers the admin chain-tester UI's "Push to system" button on
# src/kohaku-hub-admin/src/pages/fallback-sources.vue. The chain tester
# itself sends real production requests from the browser and reads the
# X-Chain-Trace response header (set by ``with_repo_fallback``) to
# render the per-hop timeline — so no debug probe endpoint is needed.


class _DebugBulkReplaceRequest(BaseModel):
    """POST body for ``/admin/api/fallback-sources/bulk-replace``."""

    sources: list[FallbackSourceCreate]


@router.put("/fallback/sources-bulk-replace")
async def bulk_replace_fallback_sources(
    payload: _DebugBulkReplaceRequest,
    _admin=Depends(verify_admin_token),
):
    """Atomically replace every fallback source.

    The chain-tester's "Push to system" button calls this after the
    operator has finished editing a draft. All current rows are
    deleted and the new list is inserted in one DB transaction; if any
    row fails validation the entire change rolls back.

    Triggers ``cache.clear()`` on success — same convention as the
    single-source create / update / delete endpoints — so
    ``global_gen`` is bumped and any in-flight probe's ``safe_set`` is
    rejected.

    Returns:
        ``{success, replaced: int, before: int, after: int}`` — the
        counts before / after the swap.
    """
    # Validate every source_type up front so a bad row can be reported
    # without the partial-write window.
    for src in payload.sources:
        if src.source_type not in ("huggingface", "kohakuhub"):
            raise HTTPException(
                400,
                detail={
                    "error": (
                        f"Invalid source_type: {src.source_type}. "
                        f"Must be 'huggingface' or 'kohakuhub'."
                    ),
                },
            )

    try:
        before = FallbackSource.select().count()
        with db.atomic():
            FallbackSource.delete().execute()
            now = datetime.now(tz=timezone.utc)
            for src in payload.sources:
                FallbackSource.create(
                    namespace=src.namespace,
                    url=src.url.rstrip("/"),
                    token=src.token,
                    priority=src.priority,
                    name=src.name,
                    source_type=src.source_type,
                    enabled=src.enabled,
                    created_at=now,
                    updated_at=now,
                )
        after = FallbackSource.select().count()

        get_cache().clear()

        logger.info(
            f"Bulk-replaced fallback sources: before={before}, after={after}"
        )
        return {
            "success": True,
            "replaced": after,
            "before": before,
            "after": after,
        }
    except Exception as e:
        logger.error(f"Bulk-replace failed: {e}")
        raise HTTPException(
            500, detail={"error": f"Bulk-replace failed: {str(e)}"}
        )



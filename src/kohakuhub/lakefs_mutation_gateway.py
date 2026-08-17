"""Approved LakeFS mutation gateway."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
import os
from typing import Any


class MutationFenceRequired(RuntimeError):
    """Raised when a production LakeFS mutation lacks a repository fence."""


@dataclass(frozen=True)
class MutationCapability:
    """Short-lived capability issued only while a PostgreSQL fence is held."""

    repository_id: int | None
    ref: str
    scope: str


_ACTIVE_CAPABILITY: ContextVar[MutationCapability | None] = ContextVar(
    "khub_active_lakefs_mutation_capability", default=None
)


def activate_capability(capability: MutationCapability):
    """Activate a capability for the current async context."""

    return _ACTIVE_CAPABILITY.set(capability)


def reset_capability(token: Any) -> None:
    """Restore the capability that was active before a fence was entered."""

    _ACTIVE_CAPABILITY.reset(token)


def has_active_capability() -> bool:
    """Return whether the current async context holds a real fence capability."""

    return _ACTIVE_CAPABILITY.get() is not None


def _canonical_ref(value: Any) -> str:
    value = str(value).strip()
    if value == "__repository__" or value.startswith(("branch:", "tag:", "commit:")):
        return value
    return f"branch:{value}"


def require_capability(operation: str, kwargs: dict[str, Any]) -> MutationCapability:
    """Require the capability appropriate for one gateway mutation.

    The gateway deliberately does not accept a caller-supplied boolean.  The
    capability is installed by ``OperationService`` only after PostgreSQL has
    acquired the corresponding advisory locks, so new callers cannot silently
    bypass the cross-process fence.
    """

    capability = _ACTIVE_CAPABILITY.get()
    if capability is None:
        # This is intentionally an opt-in test-only escape hatch for direct
        # calls to legacy route functions.  Production services never set it;
        # real API and worker paths must enter one of the fence contexts above.
        if os.getenv("KOHAKU_HUB_TEST_COMPATIBILITY", "").casefold() == "true":
            return MutationCapability(None, "__test__", "test_compatibility")
        raise MutationFenceRequired(
            f"LakeFS mutation {operation} requires an active repository fence"
        )

    if capability.scope == "repository_name":
        if operation != "create_repository":
            raise MutationFenceRequired(
                f"repository-name fence cannot authorize {operation}"
            )
        return capability

    if operation == "create_repository" and capability.scope not in {
        "mutation",
        "cutover",
    }:
        raise MutationFenceRequired(
            "repository creation requires a repository-name or cutover fence"
        )
    if operation == "delete_repository" and capability.scope != "cutover":
        raise MutationFenceRequired("repository deletion requires a cutover fence")

    requested_ref = kwargs.get("branch") or kwargs.get("ref")
    if requested_ref is not None and capability.ref != "__repository__":
        if _canonical_ref(requested_ref) != capability.ref:
            raise MutationFenceRequired(
                f"LakeFS mutation {operation} targets a ref outside the active fence"
            )
    return capability


async def _invoke(operation: str, client: Any, kwargs: dict[str, Any]) -> Any:
    require_capability(operation, kwargs)
    return await getattr(client, operation)(**kwargs)


async def commit(client: Any, **kwargs: Any) -> Any:
    return await _invoke("commit", client, kwargs)


async def upload_object(client: Any, **kwargs: Any) -> Any:
    return await _invoke("upload_object", client, kwargs)


async def link_physical_address(client: Any, **kwargs: Any) -> Any:
    return await _invoke("link_physical_address", client, kwargs)


async def delete_object(client: Any, **kwargs: Any) -> Any:
    return await _invoke("delete_object", client, kwargs)


async def create_repository(client: Any, **kwargs: Any) -> Any:
    return await _invoke("create_repository", client, kwargs)


async def delete_repository(client: Any, **kwargs: Any) -> Any:
    return await _invoke("delete_repository", client, kwargs)


async def create_branch(client: Any, **kwargs: Any) -> Any:
    return await _invoke("create_branch", client, kwargs)


async def delete_branch(client: Any, **kwargs: Any) -> Any:
    return await _invoke("delete_branch", client, kwargs)


async def create_tag(client: Any, **kwargs: Any) -> Any:
    return await _invoke("create_tag", client, kwargs)


async def delete_tag(client: Any, **kwargs: Any) -> Any:
    return await _invoke("delete_tag", client, kwargs)


async def revert_branch(client: Any, **kwargs: Any) -> Any:
    return await _invoke("revert_branch", client, kwargs)


async def merge_into_branch(client: Any, **kwargs: Any) -> Any:
    return await _invoke("merge_into_branch", client, kwargs)


async def hard_reset_branch(client: Any, **kwargs: Any) -> Any:
    return await _invoke("hard_reset_branch", client, kwargs)

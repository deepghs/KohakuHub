"""Approved LakeFS mutation gateway."""

from __future__ import annotations

from contextvars import ContextVar
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any

from kohakuhub.config import cfg


class MutationFenceRequired(RuntimeError):
    """Raised when a production LakeFS mutation lacks a repository fence."""


@dataclass(frozen=True)
class MutationCapability:
    """Short-lived capability issued only while a PostgreSQL fence is held."""

    repository_id: int | None
    ref: str
    scope: str
    lakefs_repositories: frozenset[str] = frozenset()


_ACTIVE_CAPABILITY: ContextVar[MutationCapability | None] = ContextVar(
    "khub_active_lakefs_mutation_capability", default=None
)
_TEST_COMPATIBILITY: ContextVar[bool] = ContextVar(
    "khub_lakefs_mutation_test_compatibility", default=False
)


@contextmanager
def test_compatibility():
    """Allow direct route tests to exercise legacy calls without a fence.

    This is deliberately a context-local switch rather than an environment
    variable.  It cannot be enabled for another process by changing deployed
    configuration, and the test fixture must opt into it for each context.
    """

    token = _TEST_COMPATIBILITY.set(True)
    try:
        yield
    finally:
        _TEST_COMPATIBILITY.reset(token)


def activate_capability(capability: MutationCapability):
    """Activate a capability for the current async context."""

    return _ACTIVE_CAPABILITY.set(capability)


def reset_capability(token: Any) -> None:
    """Restore the capability that was active before a fence was entered."""

    _ACTIVE_CAPABILITY.reset(token)


def has_active_capability() -> bool:
    """Return whether the current async context holds a real fence capability."""

    return _ACTIVE_CAPABILITY.get() is not None


def bind_repository(repository: str) -> None:
    """Add one explicitly allocated LakeFS repository to the active fence.

    Repository-name fences bind the target after LakeFS allocation, while a
    repository cutover binds its newly allocated destination alongside the
    source repository.  Ordinary ref fences must never grow their allowlist.
    """

    capability = _ACTIVE_CAPABILITY.get()
    if capability is None:
        if (
            _TEST_COMPATIBILITY.get()
            or cfg.app.db_backend != "postgres"
        ):
            return
        raise MutationFenceRequired("cannot bind a LakeFS repository without a fence")
    if capability.scope not in {"repository_name", "cutover", "test_compatibility"}:
        raise MutationFenceRequired(
            "ordinary repository/ref fences cannot bind another LakeFS repository"
        )
    value = str(repository).strip()
    if not value:
        raise MutationFenceRequired("LakeFS repository binding cannot be empty")
    _ACTIVE_CAPABILITY.set(
        replace(
            capability,
            lakefs_repositories=capability.lakefs_repositories | frozenset({value}),
        )
    )


def _canonical_ref(value: Any, default_kind: str = "branch") -> str:
    value = str(value).strip()
    if value == "__repository__" or value.startswith(("branch:", "tag:", "commit:")):
        return value
    return f"{default_kind}:{value}"


def require_capability(operation: str, kwargs: dict[str, Any]) -> MutationCapability:
    """Require the capability appropriate for one gateway mutation.

    The gateway deliberately does not accept a caller-supplied boolean.  The
    capability is installed by ``OperationService`` only after PostgreSQL has
    acquired the corresponding advisory locks, so new callers cannot silently
    bypass the cross-process fence.
    """

    capability = _ACTIVE_CAPABILITY.get()
    if capability is None and (
        _TEST_COMPATIBILITY.get()
        or cfg.app.db_backend != "postgres"
    ):
        return MutationCapability(
            None,
            "__test__",
            "test_compatibility",
            frozenset(),
        )
    if capability is None:
        raise MutationFenceRequired(
            f"LakeFS mutation {operation} requires an active repository fence"
        )
    if capability.scope == "test_compatibility":
        return capability

    if capability.scope == "repository_name":
        if operation != "create_repository":
            raise MutationFenceRequired(
                f"repository-name fence cannot authorize {operation}"
            )
        requested_repository = kwargs.get("name") or kwargs.get("repository")
        if (
            not requested_repository
            or str(requested_repository) not in capability.lakefs_repositories
        ):
            raise MutationFenceRequired(
                "repository creation targets an unapproved LakeFS repository"
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

    requested_repository = (
        kwargs.get("name")
        if operation == "create_repository"
        else kwargs.get("repository")
    )
    if not requested_repository or str(requested_repository) not in capability.lakefs_repositories:
        raise MutationFenceRequired(
            f"LakeFS mutation {operation} targets an unapproved repository"
        )

    ref_specs = {
        "create_branch": ("name", "branch"),
        "delete_branch": ("branch", "branch"),
        # The branch router fences the tag namespace by its new name.  The
        # LakeFS ``ref`` is the immutable source commit and is not the locked
        # mutation target.
        "create_tag": ("id", "tag"),
        "delete_tag": ("tag", "tag"),
        "commit": ("branch", "branch"),
        "upload_object": ("branch", "branch"),
        "link_physical_address": ("branch", "branch"),
        "delete_object": ("branch", "branch"),
        "revert_branch": ("branch", "branch"),
        "merge_into_branch": ("destination_branch", "branch"),
        "hard_reset_branch": ("branch", "branch"),
    }
    ref_key, ref_kind = ref_specs.get(operation, ("", "branch"))
    requested_ref = kwargs.get(ref_key)
    if requested_ref is not None and capability.ref != "__repository__":
        if _canonical_ref(requested_ref, ref_kind) != capability.ref:
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

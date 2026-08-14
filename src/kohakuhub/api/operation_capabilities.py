"""Runtime capabilities for operations that can mutate repository history."""

from typing import Literal

from fastapi import HTTPException

from kohakuhub.config import cfg

RepositoryOperation = Literal["revert", "reset", "squash"]

_OPERATION_CONFIG_FIELDS: dict[RepositoryOperation, str] = {
    "revert": "repository_revert_enabled",
    "reset": "repository_reset_enabled",
    "squash": "repository_squash_enabled",
}


def get_repository_operation_capabilities() -> dict[str, bool]:
    """Return the effective public capabilities for dangerous operations."""
    return {
        operation: bool(getattr(cfg.app, config_field, False))
        for operation, config_field in _OPERATION_CONFIG_FIELDS.items()
    }


def ensure_repository_operation_enabled(operation: RepositoryOperation) -> None:
    """Reject disabled history operations before they reach repository logic."""
    if get_repository_operation_capabilities()[operation]:
        return

    operation_name = operation.capitalize()
    raise HTTPException(
        status_code=503,
        detail={
            "code": "operation_disabled",
            "operation": operation,
            "error": f"Repository {operation} is temporarily disabled",
            "message": f"Repository {operation_name} is temporarily disabled",
        },
    )

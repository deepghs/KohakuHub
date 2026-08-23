"""Authenticated operation status and cancellation endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request

from kohakuhub.auth.dependencies import get_current_user_or_admin
from kohakuhub.auth.permissions import (
    RepoReadDeniedError,
    check_repo_read_permission,
    check_repo_write_permission,
)
from kohakuhub.db import Repository
from kohakuhub.db import User
from kohakuhub.operations.service import OperationNotCancellable, OperationService

router = APIRouter()


def _service(request: Request) -> OperationService:
    runtime = getattr(request.app.state, "operation_runtime", None)
    if runtime is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "operation_runtime_unavailable"},
        )
    return runtime.service


async def _owned_operation(
    service: OperationService,
    operation_id: UUID,
    auth: tuple[User | None, bool],
    *,
    for_cancel: bool = False,
):
    operation = await service.get(operation_id)
    user, is_admin = auth
    if operation is None:
        raise HTTPException(status_code=404, detail={"error": "operation_not_found"})
    if is_admin:
        return operation
    repository_id = getattr(operation, "repository_id", None)
    if user is not None and repository_id is not None:
        repository = Repository.get_or_none(Repository.id == repository_id)
        if repository is not None:
            try:
                if for_cancel:
                    check_repo_write_permission(repository, user)
                else:
                    check_repo_read_permission(repository, user)
                return operation
            except (RepoReadDeniedError, HTTPException):
                pass
    elif user is not None and operation.requested_by_user_id == user.id:
        # Internal/system operations without a repository are scoped to their
        # requester. Repository-backed operations always use current access
        # checks above; historical ownership is attribution, not authority.
        return operation
    raise HTTPException(status_code=404, detail={"error": "operation_not_found"})


@router.get("/operations/{operation_id}")
async def get_operation(
    operation_id: UUID,
    request: Request,
    auth: tuple[User | None, bool] = Depends(get_current_user_or_admin),
):
    operation = await _owned_operation(_service(request), operation_id, auth)
    return operation.public_dict()


@router.post("/operations/{operation_id}/cancel")
async def cancel_operation(
    operation_id: UUID,
    request: Request,
    auth: tuple[User | None, bool] = Depends(get_current_user_or_admin),
):
    service = _service(request)
    await _owned_operation(service, operation_id, auth, for_cancel=True)
    try:
        operation = await service.cancel(operation_id)
    except OperationNotCancellable as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "operation_not_cancellable", "message": str(exc)},
        ) from exc
    if operation is None:
        raise HTTPException(status_code=404, detail={"error": "operation_not_found"})
    return operation.public_dict()

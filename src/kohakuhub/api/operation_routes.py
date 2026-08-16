"""Authenticated operation status and cancellation endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request

from kohakuhub.auth.dependencies import get_current_user_or_admin
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
):
    operation = await service.get(operation_id)
    user, is_admin = auth
    if operation is None or (
        not is_admin
        and (user is None or operation.requested_by_user_id != user.id)
    ):
        raise HTTPException(status_code=404, detail={"error": "operation_not_found"})
    return operation


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
    await _owned_operation(service, operation_id, auth)
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

"""Authorization tests for durable operation status and cancellation routes."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from kohakuhub.api import operation_routes


class _FakeOperation:
    requested_by_user_id = 7

    def public_dict(self):
        return {"id": "operation", "state": "accepted"}


class _FakeService:
    def __init__(self):
        self.operation = _FakeOperation()
        self.cancelled = False

    async def get(self, _operation_id):
        return self.operation

    async def cancel(self, _operation_id):
        self.cancelled = True
        return self.operation


def _request(service):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(operation_runtime=SimpleNamespace(service=service)))
    )


@pytest.mark.asyncio
async def test_operation_status_allows_owner_and_admin_but_hides_other_users():
    operation_id = uuid4()
    service = _FakeService()

    owner = await operation_routes.get_operation(
        operation_id, _request(service), (SimpleNamespace(id=7), False)
    )
    assert owner["state"] == "accepted"

    admin = await operation_routes.get_operation(
        operation_id, _request(service), (None, True)
    )
    assert admin["id"] == "operation"

    with pytest.raises(HTTPException) as hidden:
        await operation_routes.get_operation(
            operation_id, _request(service), (SimpleNamespace(id=8), False)
        )
    assert hidden.value.status_code == 404


@pytest.mark.asyncio
async def test_operation_cancel_requires_owner_or_admin():
    operation_id = uuid4()
    service = _FakeService()

    with pytest.raises(HTTPException) as hidden:
        await operation_routes.cancel_operation(
            operation_id, _request(service), (SimpleNamespace(id=8), False)
        )
    assert hidden.value.status_code == 404
    assert service.cancelled is False

    result = await operation_routes.cancel_operation(
        operation_id, _request(service), (SimpleNamespace(id=7), False)
    )
    assert result["state"] == "accepted"
    assert service.cancelled is True


@pytest.mark.asyncio
async def test_current_repository_collaborator_can_read_but_not_cancel(
    monkeypatch,
):
    class RepositoryOperation(_FakeOperation):
        repository_id = 42
        requested_by_user_id = 7

    service = _FakeService()
    service.operation = RepositoryOperation()
    repository = object()
    monkeypatch.setattr(
        operation_routes.Repository,
        "get_or_none",
        lambda _condition: repository,
    )
    monkeypatch.setattr(
        operation_routes,
        "check_repo_read_permission",
        lambda _repository, _user: True,
    )

    collaborator = SimpleNamespace(id=8)
    result = await operation_routes.get_operation(
        uuid4(), _request(service), (collaborator, False)
    )
    assert result["state"] == "accepted"

    monkeypatch.setattr(
        operation_routes,
        "check_repo_write_permission",
        lambda _repository, _user: (_ for _ in ()).throw(
            HTTPException(status_code=403, detail="no write")
        ),
    )
    with pytest.raises(HTTPException) as denied:
        await operation_routes.cancel_operation(
            uuid4(), _request(service), (collaborator, False)
        )
    assert denied.value.status_code == 404
    assert service.cancelled is False


@pytest.mark.asyncio
async def test_historical_requester_loses_access_after_repository_permission_revocation(
    monkeypatch,
):
    class RepositoryOperation(_FakeOperation):
        repository_id = 42

    service = _FakeService()
    service.operation = RepositoryOperation()
    monkeypatch.setattr(
        operation_routes.Repository,
        "get_or_none",
        lambda _condition: object(),
    )
    monkeypatch.setattr(
        operation_routes,
        "check_repo_read_permission",
        lambda _repository, _user: (_ for _ in ()).throw(
            HTTPException(status_code=404, detail="revoked")
        ),
    )
    monkeypatch.setattr(
        operation_routes,
        "check_repo_write_permission",
        lambda _repository, _user: (_ for _ in ()).throw(
            HTTPException(status_code=404, detail="revoked")
        ),
    )

    with pytest.raises(HTTPException) as status_error:
        await operation_routes.get_operation(
            uuid4(), _request(service), (SimpleNamespace(id=7), False)
        )
    assert status_error.value.status_code == 404

    with pytest.raises(HTTPException) as cancel_error:
        await operation_routes.cancel_operation(
            uuid4(), _request(service), (SimpleNamespace(id=7), False)
        )
    assert cancel_error.value.status_code == 404
    assert service.cancelled is False

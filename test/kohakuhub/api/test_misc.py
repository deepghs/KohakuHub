"""API tests for utility routes."""

from types import SimpleNamespace

import httpx
import pytest

import kohakuhub.api.branches as branches_api
import kohakuhub.api.operation_capabilities as operation_capabilities
import kohakuhub.api.repo.routers.crud as repo_crud


async def test_version_site_config_and_yaml_validation(client):
    version_response = await client.get("/api/version")
    assert version_response.status_code == 200
    assert version_response.json()["api"] == "kohakuhub"

    site_config_response = await client.get("/api/site-config")
    assert site_config_response.status_code == 200
    assert "site_name" in site_config_response.json()
    assert site_config_response.json()["capabilities"]["repository_operations"] == {
        "revert": False,
        "reset": False,
        "squash": False,
    }

    valid_yaml_response = await client.post(
        "/api/validate-yaml",
        json={"content": "model:\\n  name: demo\\n"},
    )
    assert valid_yaml_response.status_code == 200
    assert valid_yaml_response.json()["valid"] is True

    invalid_yaml_response = await client.post(
        "/api/validate-yaml",
        json={"content": "model: [broken"},
    )
    assert invalid_yaml_response.status_code == 200
    assert invalid_yaml_response.json()["valid"] is False


@pytest.mark.asyncio
async def test_disabled_operations_reject_before_auth_and_repository_lookup(
    app, client, monkeypatch
):
    for field in (
        "repository_revert_enabled",
        "repository_reset_enabled",
        "repository_squash_enabled",
    ):
        monkeypatch.setattr(operation_capabilities.cfg.app, field, False)

    auth_calls = []
    repository_lookups = []

    def unexpected_user_dependency():
        auth_calls.append("user")
        return SimpleNamespace(username="owner")

    def unexpected_admin_dependency():
        auth_calls.append("admin")
        return (SimpleNamespace(username="owner"), False)

    def unexpected_repository_lookup(*_args):
        repository_lookups.append(True)
        return SimpleNamespace()

    app.dependency_overrides[branches_api.get_current_user] = unexpected_user_dependency
    app.dependency_overrides[repo_crud.get_current_user_or_admin] = (
        unexpected_admin_dependency
    )
    monkeypatch.setattr(branches_api, "get_repository", unexpected_repository_lookup)
    monkeypatch.setattr(repo_crud, "get_repository", unexpected_repository_lookup)

    try:
        requests = [
            (
                "/api/models/owner/demo-model/branch/main/revert",
                {"ref": "commit-ref"},
            ),
            (
                "/api/models/owner/demo-model/branch/main/reset",
                {"ref": "commit-ref", "force": True},
            ),
            ("/api/repos/squash", {"repo": "owner/demo-model", "type": "model"}),
        ]
        responses = [await client.post(path, json=payload) for path, payload in requests]
    finally:
        app.dependency_overrides.clear()

    assert [response.status_code for response in responses] == [503, 503, 503]
    assert [response.json()["detail"]["code"] for response in responses] == [
        "operation_disabled",
        "operation_disabled",
        "operation_disabled",
    ]
    assert auth_calls == []
    assert repository_lookups == []


def test_repository_operations_stay_disabled_on_sqlite(monkeypatch):
    for backend in ("sqlite", "POSTGRES"):
        monkeypatch.setattr(operation_capabilities.cfg.app, "db_backend", backend)
        monkeypatch.setattr(
            operation_capabilities.cfg.app, "repository_revert_enabled", True
        )
        monkeypatch.setattr(
            operation_capabilities.cfg.app, "repository_reset_enabled", True
        )
        monkeypatch.setattr(
            operation_capabilities.cfg.app, "repository_squash_enabled", True
        )

        assert operation_capabilities.get_repository_operation_capabilities() == {
            "revert": False,
            "reset": False,
            "squash": False,
        }


async def test_whoami_v2_requires_auth_and_returns_orgs(app, owner_client):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as anonymous_client:
        anonymous_response = await anonymous_client.get("/api/whoami-v2")
        assert anonymous_response.status_code == 401

    authenticated_response = await owner_client.get("/api/whoami-v2")
    assert authenticated_response.status_code == 200
    payload = authenticated_response.json()
    assert payload["name"] == "owner"
    assert any(org["name"] == "acme-labs" for org in payload["orgs"])


async def test_whoami_v2_bearer_and_cookie_agree(app, owner_client, hf_api_token):
    """A bearer-token caller and a cookie-session caller for the same user
    must receive identical ``/api/whoami-v2`` payloads (at minimum:
    ``name`` and the set of ``orgs``). Gradio deploys rely on the bearer
    path; the web UI relies on the cookie path — both must stay in sync."""
    cookie_response = await owner_client.get("/api/whoami-v2")
    cookie_response.raise_for_status()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as bearer_client:
        bearer_response = await bearer_client.get(
            "/api/whoami-v2",
            headers={"Authorization": f"Bearer {hf_api_token}"},
        )
    bearer_response.raise_for_status()

    cookie_payload = cookie_response.json()
    bearer_payload = bearer_response.json()
    assert cookie_payload["name"] == bearer_payload["name"] == "owner"
    assert (
        {org["name"] for org in cookie_payload["orgs"]}
        == {org["name"] for org in bearer_payload["orgs"]}
    )

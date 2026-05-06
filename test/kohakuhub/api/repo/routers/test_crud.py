"""API tests for repository CRUD routes."""


async def test_create_repository_and_reject_normalized_duplicate(owner_client):
    create_response = await owner_client.post(
        "/api/repos/create",
        json={"type": "model", "name": "sandbox-repo", "private": False},
    )
    assert create_response.status_code == 200
    assert create_response.json()["repo_id"] == "owner/sandbox-repo"

    duplicate_response = await owner_client.post(
        "/api/repos/create",
        json={"type": "model", "name": "sandbox_repo", "private": False},
    )
    # `huggingface_hub.HfApi.create_repo(..., exist_ok=True)` only accepts the
    # 409 status code as "repo already exists"; see hf_api.py:4501. The body is
    # JSON so the client can still build a RepoUrl from the response.
    assert duplicate_response.status_code == 409
    assert duplicate_response.headers["x-error-code"] == "RepoExists"
    assert "conflicts" in duplicate_response.headers["x-error-message"]
    body = duplicate_response.json()
    assert body["url"].endswith("/models/owner/sandbox-repo")
    assert body["repo_id"] == "owner/sandbox-repo"


async def test_create_repo_visibility_public_creates_public_repo(owner_client):
    """``visibility="public"`` (huggingface_hub>=1.x shape with
    ``private=False``) must round-trip as a public repo. Pairs with the
    ``visibility="private"`` path covered through the live hf-client e2e
    test in ``test_huggingface_hub_deep.py`` — together they exercise
    both branches of the visibility resolver in ``crud.py`` so the
    patch-coverage gate stays clean."""
    response = await owner_client.post(
        "/api/repos/create",
        json={"type": "model", "name": "visibility-public-repo", "visibility": "public"},
    )
    assert response.status_code == 200
    assert response.json()["repo_id"] == "owner/visibility-public-repo"

    info = await owner_client.get("/api/models/owner/visibility-public-repo")
    assert info.status_code == 200
    assert info.json()["private"] is False


async def test_create_repo_unknown_visibility_returns_400(owner_client):
    """The resolver must reject visibility values it cannot map to a
    private bool. Without this guard a typo like ``"hidden"`` would be
    silently treated as public — masking client-side bugs and breaking
    HF API symmetry with ``update_repo_settings``."""
    response = await owner_client.post(
        "/api/repos/create",
        json={"type": "model", "name": "visibility-bogus-repo", "visibility": "hidden"},
    )
    assert response.status_code == 400
    body = response.json()
    error_text = body.get("detail", {}).get("error", "") if isinstance(body.get("detail"), dict) else str(body)
    assert "visibility" in error_text.lower()
    assert "public" in error_text and "private" in error_text


async def test_admin_can_delete_empty_org_repository(admin_client, owner_client):
    create_response = await owner_client.post(
        "/api/repos/create",
        json={
            "type": "dataset",
            "name": "temp-delete",
            "private": False,
            "organization": "acme-labs",
        },
    )
    assert create_response.status_code == 200

    delete_response = await admin_client.request(
        "DELETE",
        "/api/repos/delete",
        json={"type": "dataset", "name": "temp-delete", "organization": "acme-labs"},
    )
    assert delete_response.status_code == 200
    assert "deleted" in delete_response.json()["message"].lower()

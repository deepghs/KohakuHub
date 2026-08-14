"""API tests for repository info routes."""

import httpx


async def test_get_repo_info_returns_siblings_and_lfs_metadata(client):
    response = await client.get("/api/models/owner/demo-model")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "owner/demo-model"
    sibling_names = {item["rfilename"] for item in payload["siblings"]}
    assert {"README.md", "config.json", "weights/model.safetensors"} <= sibling_names

    lfs_sibling = next(
        sibling for sibling in payload["siblings"] if sibling["rfilename"] == "weights/model.safetensors"
    )
    assert lfs_sibling["lfs"]["size"] > 0


async def test_list_repositories_and_user_repo_views_respect_visibility(
    app, client, owner_client
):
    model_list_response = await client.get("/api/models", params={"author": "owner"})
    assert model_list_response.status_code == 200
    assert any(repo["id"] == "owner/demo-model" for repo in model_list_response.json())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as anonymous_client:
        anonymous_user_repos = await anonymous_client.get("/api/users/acme-labs/repos")
        assert anonymous_user_repos.status_code == 200
        assert anonymous_user_repos.json()["datasets"] == []

    owner_user_repos = await owner_client.get("/api/users/acme-labs/repos")
    assert owner_user_repos.status_code == 200
    assert any(
        repo["id"] == "acme-labs/private-dataset"
        for repo in owner_user_repos.json()["datasets"]
    )


async def test_user_overview_endpoint_groups_by_type(owner_client):
    """``repoAPI.getUserOverview`` in kohaku-hub-ui drives the user profile
    page — one round trip must return models/datasets/spaces in a single
    grouped payload. Uses a large ``limit`` so transient repos from sibling
    tests do not push the baseline seed out of the ``recent`` window."""
    response = await owner_client.get(
        "/api/users/owner/repos", params={"sort": "recent", "limit": 200}
    )
    response.raise_for_status()
    payload = response.json()
    for key in ("models", "datasets", "spaces"):
        assert key in payload, f"user overview must include {key!r}, got {payload!r}"
        assert isinstance(payload[key], list)
    assert any(repo["id"] == "owner/demo-model" for repo in payload["models"])


async def test_get_repo_info_default_still_includes_size_and_lfs(client):
    """The default response must not change shape.

    `blobs` defaults to True so existing consumers keep the metadata they get
    today; only callers that explicitly opt out see the lighter form.
    """
    response = await client.get("/api/models/owner/demo-model")

    assert response.status_code == 200
    siblings = response.json()["siblings"]
    assert siblings, "expected the baseline repo to report siblings"
    assert all("size" in sibling for sibling in siblings)
    lfs_sibling = next(
        s for s in siblings if s["rfilename"] == "weights/model.safetensors"
    )
    assert "lfs" in lfs_sibling and lfs_sibling["lfs"]["size"] > 0
    # Assert the sha256 *value*, not merely its presence: if the bulk File load
    # silently returned nothing, every sibling would still carry `size` and
    # `lfs.size` while the sha256 quietly changed to the LakeFS checksum. That
    # is exactly the byte-identity this parameter is meant to preserve.
    from kohakuhub.db_operations import get_repository, get_repo_file_sha256_map

    repo_row = get_repository("model", "owner", "demo-model")
    stored = get_repo_file_sha256_map(repo_row)
    assert stored.get("weights/model.safetensors"), (
        "baseline fixture should have a stored sha256 for the LFS file"
    )
    assert lfs_sibling["lfs"]["sha256"] == stored["weights/model.safetensors"]


async def test_get_repo_info_blobs_false_returns_names_only(client):
    """`blobs=false` drops the per-file metadata that makes large repos expensive.

    This is the wire parameter `huggingface_hub` uses for `files_metadata` —
    every version in the CI matrix maps `files_metadata=True` to
    `params["blobs"] = True` — so opting out is expressible by any HF client.
    The path list stays, because HF's default response includes `rfilename`.
    """
    response = await client.get("/api/models/owner/demo-model", params={"blobs": "false"})

    assert response.status_code == 200
    siblings = response.json()["siblings"]
    assert siblings, "paths must still be listed"
    names = {s["rfilename"] for s in siblings}
    assert {"README.md", "weights/model.safetensors"} <= names
    for sibling in siblings:
        assert set(sibling) == {"rfilename"}, (
            f"blobs=false must emit rfilename only, got {sorted(sibling)}"
        )


async def test_get_repo_info_blobs_true_matches_the_default(client):
    """Explicit `blobs=true` and the default must agree, so the parameter is
    additive rather than a second code path with its own behaviour."""
    default = await client.get("/api/models/owner/demo-model")
    explicit = await client.get("/api/models/owner/demo-model", params={"blobs": "true"})

    assert default.status_code == explicit.status_code == 200
    assert default.json()["siblings"] == explicit.json()["siblings"]

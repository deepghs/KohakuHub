"""API tests for the admin credentials (sessions / tokens / SSH keys) routes."""

from __future__ import annotations

import sys

import pytest

SESSIONS_URL = "/admin/api/sessions"
TOKENS_URL = "/admin/api/tokens"
SSH_KEYS_URL = "/admin/api/ssh-keys"

TEST_PUBLIC_KEY = (
    "ssh-ed25519 "
    "AAAAC3NzaC1lZDI1NTE5AAAAIETd15NJPPGOG7SIPyY4AkAlUJQnjhI/8x2UMhww8PHs "
    "credentials-tests@example"
)
TEST_PUBLIC_KEY_ALT = (
    "ssh-ed25519 "
    "AAAAC3NzaC1lZDI1NTE5AAAAIDLnDjBg9G2T0Sv8DfZuHBZ6nrYxmxqoBE7v4uZRGc8e "
    "alt@example"
)


def _live_db_module():
    return sys.modules["kohakuhub.db"]


# ---------------------------------------------------------------------------
# Auth gating (read-only)
# ---------------------------------------------------------------------------


async def test_credentials_endpoints_require_admin_token(client):
    for url in (SESSIONS_URL, TOKENS_URL, SSH_KEYS_URL):
        response = await client.get(url)
        assert response.status_code == 401, url


async def test_credentials_endpoints_reject_invalid_admin_token(client):
    for url in (SESSIONS_URL, TOKENS_URL, SSH_KEYS_URL):
        response = await client.get(
            url, headers={"X-Admin-Token": "definitely-not-the-real-token"}
        )
        assert response.status_code == 403, url


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


async def test_list_sessions_returns_paginated_payload_with_user_metadata(
    admin_client, owner_client
):
    response = await admin_client.get(SESSIONS_URL, params={"limit": 10})
    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"sessions", "total", "limit", "offset"}
    assert payload["limit"] == 10
    assert payload["offset"] == 0
    assert payload["total"] >= 1

    sample = payload["sessions"][0]
    assert set(sample.keys()) == {
        "id",
        "user_id",
        "username",
        "created_at",
        "expires_at",
        "expired",
    }
    # Sensitive fields must never appear in admin responses.
    assert "session_id" not in sample
    assert "secret" not in sample


async def test_list_sessions_filter_by_user(admin_client, owner_client):
    response = await admin_client.get(SESSIONS_URL, params={"user": "owner"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["sessions"]
    assert all(s["username"] == "owner" for s in payload["sessions"])


async def test_list_sessions_active_only_excludes_expired_rows(
    admin_client, owner_client
):
    db = _live_db_module()
    target_user = db.User.get_or_none(db.User.username == "owner")
    assert target_user is not None

    # Backdate the most recent session so we have one expired row to filter.
    expired_session = (
        db.Session.select()
        .where(db.Session.user == target_user)
        .order_by(db.Session.created_at.desc())
        .first()
    )
    assert expired_session is not None
    db.Session.update(expires_at=db.datetime.utcnow().replace(year=2000)).where(
        db.Session.id == expired_session.id
    ).execute()

    try:
        all_response = await admin_client.get(
            SESSIONS_URL, params={"user": "owner"}
        )
        active_response = await admin_client.get(
            SESSIONS_URL, params={"user": "owner", "active_only": True}
        )
        all_ids = {s["id"] for s in all_response.json()["sessions"]}
        active_ids = {s["id"] for s in active_response.json()["sessions"]}
        assert expired_session.id in all_ids
        assert expired_session.id not in active_ids
    finally:
        db.Session.delete().where(db.Session.id == expired_session.id).execute()


async def test_list_sessions_rejects_unknown_user(admin_client):
    response = await admin_client.get(SESSIONS_URL, params={"user": "ghost-user"})
    assert response.status_code == 404


async def test_revoke_session_returns_404_for_unknown_id(admin_client):
    response = await admin_client.delete(f"{SESSIONS_URL}/9999999")
    assert response.status_code == 404


@pytest.mark.backend_per_test
async def test_revoke_session_deletes_target_row(admin_client, outsider_client):
    db = _live_db_module()
    target_user = db.User.get_or_none(db.User.username == "outsider")
    assert target_user is not None
    session = (
        db.Session.select()
        .where(db.Session.user == target_user)
        .order_by(db.Session.created_at.desc())
        .first()
    )
    assert session is not None

    response = await admin_client.delete(f"{SESSIONS_URL}/{session.id}")
    assert response.status_code == 200
    assert response.json() == {"revoked": 1}
    assert db.Session.get_or_none(db.Session.id == session.id) is None


@pytest.mark.backend_per_test
async def test_revoke_sessions_bulk_by_user_clears_only_that_user(
    admin_client, owner_client, outsider_client
):
    db = _live_db_module()
    response = await admin_client.post(
        f"{SESSIONS_URL}/revoke-bulk", json={"user": "outsider"}
    )
    assert response.status_code == 200
    revoked = response.json()["revoked"]
    assert revoked >= 1

    outsider = db.User.get_or_none(db.User.username == "outsider")
    owner = db.User.get_or_none(db.User.username == "owner")
    assert (
        db.Session.select().where(db.Session.user == outsider).count() == 0
    )
    # Sibling users keep their sessions untouched.
    assert (
        db.Session.select().where(db.Session.user == owner).count() >= 1
    )


async def test_revoke_sessions_bulk_requires_at_least_one_filter(admin_client):
    response = await admin_client.post(f"{SESSIONS_URL}/revoke-bulk", json={})
    assert response.status_code == 400


async def test_revoke_sessions_bulk_rejects_unknown_user(admin_client):
    response = await admin_client.post(
        f"{SESSIONS_URL}/revoke-bulk", json={"user": "ghost-user"}
    )
    assert response.status_code == 404


@pytest.mark.backend_per_test
async def test_revoke_sessions_bulk_by_before_ts_only(admin_client, owner_client):
    db = _live_db_module()
    db.Session.update(created_at=db.datetime(2010, 1, 1)).where(
        db.Session.user.in_(
            db.User.select(db.User.id).where(db.User.username == "owner")
        )
    ).execute()

    response = await admin_client.post(
        f"{SESSIONS_URL}/revoke-bulk",
        json={"before_ts": "2020-01-01T00:00:00+00:00"},
    )
    assert response.status_code == 200
    assert response.json()["revoked"] >= 1


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


async def _create_token(client, name: str) -> int:
    response = await client.post(
        "/api/auth/tokens/create", json={"name": name}
    )
    assert response.status_code == 200
    return response.json()["token_id"]


async def test_list_tokens_returns_metadata_only(admin_client, owner_client):
    await _create_token(owner_client, "credentials-list-token")

    response = await admin_client.get(TOKENS_URL, params={"user": "owner"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["tokens"]
    sample = payload["tokens"][0]
    assert set(sample.keys()) == {
        "id",
        "user_id",
        "username",
        "name",
        "created_at",
        "last_used",
    }
    # The token plaintext / token_hash must never leave the database.
    assert "token" not in sample
    assert "token_hash" not in sample


async def test_list_tokens_filter_by_user(admin_client, owner_client):
    await _create_token(owner_client, "filter-by-user")
    response = await admin_client.get(TOKENS_URL, params={"user": "owner"})
    assert response.status_code == 200
    assert all(t["username"] == "owner" for t in response.json()["tokens"])


async def test_list_tokens_unused_for_days_includes_never_used(
    admin_client, owner_client
):
    await _create_token(owner_client, "never-used-token")
    response = await admin_client.get(
        TOKENS_URL, params={"user": "owner", "unused_for_days": 30}
    )
    assert response.status_code == 200
    payload = response.json()
    assert any(t["last_used"] is None for t in payload["tokens"])


async def test_list_tokens_rejects_unknown_user(admin_client):
    response = await admin_client.get(TOKENS_URL, params={"user": "ghost"})
    assert response.status_code == 404


async def test_revoke_token_returns_404_for_unknown_id(admin_client):
    response = await admin_client.delete(f"{TOKENS_URL}/9999999")
    assert response.status_code == 404


@pytest.mark.backend_per_test
async def test_revoke_token_deletes_target_row(admin_client, owner_client):
    db = _live_db_module()
    token_id = await _create_token(owner_client, "to-be-revoked")

    response = await admin_client.delete(f"{TOKENS_URL}/{token_id}")
    assert response.status_code == 200
    assert response.json() == {"revoked": 1}
    assert db.Token.get_or_none(db.Token.id == token_id) is None


# ---------------------------------------------------------------------------
# SSH keys
# ---------------------------------------------------------------------------


async def _create_ssh_key(client, title: str, key: str = TEST_PUBLIC_KEY) -> int:
    response = await client.post(
        "/api/user/keys", json={"title": title, "key": key}
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


@pytest.mark.backend_per_test
async def test_list_ssh_keys_returns_public_metadata(admin_client, owner_client):
    await _create_ssh_key(owner_client, "credentials-list-key")

    response = await admin_client.get(SSH_KEYS_URL, params={"user": "owner"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ssh_keys"]
    sample = payload["ssh_keys"][0]
    assert set(sample.keys()) == {
        "id",
        "user_id",
        "username",
        "key_type",
        "fingerprint",
        "title",
        "created_at",
        "last_used",
    }
    assert sample["fingerprint"].startswith("SHA256:")


@pytest.mark.backend_per_test
async def test_list_ssh_keys_filter_unused_for_days(admin_client, owner_client):
    await _create_ssh_key(owner_client, "never-used-key")
    response = await admin_client.get(
        SSH_KEYS_URL, params={"user": "owner", "unused_for_days": 365}
    )
    assert response.status_code == 200
    assert any(k["last_used"] is None for k in response.json()["ssh_keys"])


async def test_list_ssh_keys_rejects_unknown_user(admin_client):
    response = await admin_client.get(SSH_KEYS_URL, params={"user": "ghost"})
    assert response.status_code == 404


async def test_revoke_ssh_key_returns_404_for_unknown_id(admin_client):
    response = await admin_client.delete(f"{SSH_KEYS_URL}/9999999")
    assert response.status_code == 404


@pytest.mark.backend_per_test
async def test_revoke_ssh_key_deletes_target_row(admin_client, owner_client):
    db = _live_db_module()
    key_id = await _create_ssh_key(owner_client, "to-be-revoked-key")

    response = await admin_client.delete(f"{SSH_KEYS_URL}/{key_id}")
    assert response.status_code == 200
    assert response.json() == {"revoked": 1}
    assert db.SSHKey.get_or_none(db.SSHKey.id == key_id) is None


# ---------------------------------------------------------------------------
# Pagination + structural
# ---------------------------------------------------------------------------


async def test_pagination_limit_and_offset_round_trip(admin_client):
    response = await admin_client.get(
        SESSIONS_URL, params={"limit": 1, "offset": 0}
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert len(payload["sessions"]) <= 1


async def test_invalid_limit_is_rejected(admin_client):
    response = await admin_client.get(SESSIONS_URL, params={"limit": 0})
    assert response.status_code == 422


async def test_invalid_unused_for_days_is_rejected(admin_client):
    response = await admin_client.get(
        TOKENS_URL, params={"unused_for_days": -1}
    )
    assert response.status_code == 422

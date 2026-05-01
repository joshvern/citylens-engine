from __future__ import annotations

import secrets
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import api_keys as api_keys_routes


class FakeApiKeyStore:
    """Minimal FakeStore covering the API-key methods. Mirrors the real
    Firestore layout: a `keys[key_id]` dict per user + an `index[hash]`
    pointing back. Lets us assert revocation paths and list filters
    without standing up a Firestore emulator."""

    def __init__(self) -> None:
        # users[app_user_id] -> { key_id -> record }
        self.users: dict[str, dict[str, dict]] = {}
        # plaintext_hash -> {app_user_id, key_id, revoked_at}
        self.index: dict[str, dict] = {}

    def create_api_key(self, *, app_user_id: str, label: str):
        from app.services.firestore_store import (
            USER_API_KEY_PREFIX,
            _hash_api_key,
        )

        # Mimic the real implementation's plaintext shape: prefix + url-safe
        # random. Tests need a deterministic-enough shape to assert on.
        key_id = f"k-{len(self.users.get(app_user_id, {}))+1}"
        random_part = secrets.token_urlsafe(32).rstrip("=")
        plaintext = f"{USER_API_KEY_PREFIX}{random_part}"
        plaintext_hash = _hash_api_key(plaintext)
        record = {
            "key_id": key_id,
            "label": label or "untitled",
            "key_prefix": f"{USER_API_KEY_PREFIX}{random_part[:4]}",
            "key_hash": plaintext_hash,
            "created_at": datetime.now(timezone.utc),
            "last_used_at": None,
            "revoked_at": None,
        }
        self.users.setdefault(app_user_id, {})[key_id] = record
        self.index[plaintext_hash] = {
            "app_user_id": app_user_id,
            "key_id": key_id,
            "revoked_at": None,
        }
        return key_id, plaintext, record

    def list_api_keys(self, *, app_user_id: str):
        keys = list((self.users.get(app_user_id) or {}).values())
        return [
            {k: v for k, v in r.items() if k != "key_hash"}
            for r in keys
            if r.get("revoked_at") is None
        ]

    def revoke_api_key(self, *, app_user_id: str, key_id: str) -> bool:
        bucket = self.users.get(app_user_id) or {}
        record = bucket.get(key_id)
        if record is None or record.get("revoked_at") is not None:
            return False
        now = datetime.now(timezone.utc)
        record["revoked_at"] = now
        for entry in self.index.values():
            if entry.get("app_user_id") == app_user_id and entry.get("key_id") == key_id:
                entry["revoked_at"] = now
        return True

    def get_user_id_for_api_key(self, plaintext: str):
        from app.services.firestore_store import _hash_api_key, is_user_api_key

        if not is_user_api_key(plaintext):
            return None
        entry = self.index.get(_hash_api_key(plaintext))
        if not entry or entry.get("revoked_at") is not None:
            return None
        return entry.get("app_user_id")

    def get_user(self, app_user_id: str):
        return {
            "user_id": app_user_id,
            "email": f"{app_user_id}@example.com",
            "email_verified": True,
            "is_admin": False,
            "plan_type": "free",
        }

    # The auth-resolution branch tests below hit `/v1/me`, which needs
    # quota helpers off the same store. Stub them with safe defaults so
    # the FakeStore can stand in for both routes.
    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return 0

    def count_user_concurrent_runs(self, *, user_id: str) -> int:
        return 0


def _install(store: FakeApiKeyStore) -> None:
    app.dependency_overrides[api_keys_routes.get_store] = lambda: store


@pytest.fixture(autouse=True)
def _enable_user_api_keys(monkeypatch) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_USER_API_KEYS", "true")


@pytest.fixture(autouse=True)
def _reset_overrides_after_test():
    yield
    app.dependency_overrides = {}


# -------------------- CRUD route tests --------------------


def test_create_returns_plaintext_once_and_correct_shape(auth_override) -> None:
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)

    resp = client.post("/v1/api-keys", json={"label": "my-laptop"})
    assert resp.status_code == 201
    body = resp.json()

    assert body["label"] == "my-laptop"
    assert body["key_id"].startswith("k-")
    assert body["plaintext_key"].startswith("clk_live_")
    assert body["key_prefix"].startswith("clk_live_")
    assert "key_hash" not in body  # secret material must not leak
    assert body["created_at"] is not None
    assert body["last_used_at"] is None
    assert body["revoked_at"] is None


def test_list_returns_active_keys_only_no_hashes(auth_override) -> None:
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)

    client.post("/v1/api-keys", json={"label": "k1"})
    second = client.post("/v1/api-keys", json={"label": "k2"}).json()
    client.delete(f"/v1/api-keys/{second['key_id']}")

    resp = client.get("/v1/api-keys")
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert {i["label"] for i in items} == {"k1"}
    assert all("key_hash" not in i for i in items)
    assert all("plaintext_key" not in i for i in items)


def test_revoke_returns_204_then_404_on_repeat(auth_override) -> None:
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)

    created = client.post("/v1/api-keys", json={"label": "k1"}).json()
    key_id = created["key_id"]

    assert client.delete(f"/v1/api-keys/{key_id}").status_code == 204
    # Already revoked → 404 (don't leak the difference between
    # "wrong owner", "already revoked", "never existed").
    assert client.delete(f"/v1/api-keys/{key_id}").status_code == 404


def test_user_cannot_revoke_other_users_key(auth_override) -> None:
    # User A creates a key
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)
    created = client.post("/v1/api-keys", json={"label": "k1"}).json()
    key_id = created["key_id"]

    # Switch to user B: revoke must 404 (not "found and revoked")
    auth_override(app_user_id="user-b")
    assert client.delete(f"/v1/api-keys/{key_id}").status_code == 404

    # User A can still see their key
    auth_override(app_user_id="user-a")
    resp = client.get("/v1/api-keys").json()
    assert {i["key_id"] for i in resp["items"]} == {key_id}


def test_403_when_user_api_keys_disabled(auth_override, monkeypatch) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_USER_API_KEYS", "false")
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)

    create_resp = client.post("/v1/api-keys", json={"label": "k"})
    assert create_resp.status_code == 403
    assert create_resp.json()["detail"]["code"] == "USER_API_KEYS_DISABLED"

    list_resp = client.get("/v1/api-keys")
    assert list_resp.status_code == 403


def test_label_is_required(auth_override) -> None:
    auth_override(app_user_id="user-a")
    store = FakeApiKeyStore()
    _install(store)
    client = TestClient(app)

    resp = client.post("/v1/api-keys", json={})
    assert resp.status_code in (400, 422)


# -------------------- Auth-resolution branch tests --------------------
#
# The real `require_auth` dep prefix-routes Bearer tokens that look like
# user API keys to a store lookup. These tests bypass `auth_override`
# and instead patch the store factory the dep uses, so we exercise the
# real auth code path end-to-end.


def _patch_auth_and_me_store(monkeypatch, store: FakeApiKeyStore) -> None:
    """Make both the auth dep and the /v1/me store dep use the FakeStore.
    The auth path constructs a Firestore client via `_store_factory`; the
    me route resolves its own store via `me_routes.get_store`."""
    from app.routes import me as me_routes
    from app.services import auth as auth_module

    monkeypatch.setattr(auth_module, "_store_factory", lambda _settings: store)
    app.dependency_overrides[me_routes.get_store] = lambda: store


def test_user_api_key_bearer_resolves_to_owner(monkeypatch) -> None:
    store = FakeApiKeyStore()
    # Mint a key for user-a directly via the FakeStore (bypassing the
    # CRUD route, which itself depends on auth).
    _, plaintext, _ = store.create_api_key(app_user_id="user-a", label="cli")
    _patch_auth_and_me_store(monkeypatch, store)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"Authorization": f"Bearer {plaintext}"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["user"]["id"] == "user-a"
    assert body["user"]["plan_type"] == "free"


def test_revoked_user_api_key_returns_401(monkeypatch) -> None:
    store = FakeApiKeyStore()
    key_id, plaintext, _ = store.create_api_key(app_user_id="user-a", label="cli")
    store.revoke_api_key(app_user_id="user-a", key_id=key_id)
    _patch_auth_and_me_store(monkeypatch, store)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"Authorization": f"Bearer {plaintext}"})
    assert resp.status_code == 401


def test_user_api_key_disabled_globally_rejects_at_auth(monkeypatch) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_USER_API_KEYS", "false")
    store = FakeApiKeyStore()
    _, plaintext, _ = store.create_api_key(app_user_id="user-a", label="cli")
    _patch_auth_and_me_store(monkeypatch, store)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"Authorization": f"Bearer {plaintext}"})
    assert resp.status_code == 401


def test_unknown_user_api_key_returns_401(monkeypatch) -> None:
    store = FakeApiKeyStore()
    _patch_auth_and_me_store(monkeypatch, store)

    client = TestClient(app)
    # Properly-prefixed but never-issued key:
    resp = client.get(
        "/v1/me",
        headers={"Authorization": "Bearer clk_live_doesnotexist123456789012345678901234"},
    )
    assert resp.status_code == 401

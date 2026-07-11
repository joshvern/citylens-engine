"""Admin API keys are hash-only.

The X-API-Key surface only authenticates when
`CITYLENS_ALLOW_ADMIN_API_KEYS=true` AND the SHA-256 of the provided key
appears in `CITYLENS_ADMIN_API_KEY_HASHES`. The old plaintext env var
(`CITYLENS_ADMIN_API_KEYS`) must be inert.

These tests exercise the REAL `require_auth` dependency (no
`auth_override`), so the store factory is monkeypatched to an in-memory
fake — never live GCP (see conftest).
"""

from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import me as me_routes
from app.services import auth as auth_module

ADMIN_KEY = "test-admin-key-123"
ADMIN_KEY_HASH = hashlib.sha256(ADMIN_KEY.encode("utf-8")).hexdigest()


class FakeStore:
    """In-memory stand-in for FirestoreStore on the admin-key auth path."""

    def __init__(self) -> None:
        self.admin_lookups: list[str] = []

    def get_admin_user_for_api_key(self, api_key_hash: str) -> dict:
        self.admin_lookups.append(api_key_hash)
        return {
            "user_id": f"admin_{api_key_hash[:24]}",
            "email": None,
            "email_verified": False,
            "plan_type": "admin",
            "is_admin": True,
            "monthly_run_limit": None,
            "max_concurrent_runs": None,
        }

    # /v1/me quota shape
    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return 0


@pytest.fixture
def fake_store(monkeypatch):
    store = FakeStore()
    # require_auth builds its store via auth._store_factory; keep it in-memory.
    monkeypatch.setattr(auth_module, "_store_factory", lambda settings: store)
    app.dependency_overrides[me_routes.get_store] = lambda: store
    yield store
    app.dependency_overrides = {}


def test_admin_key_hash_match_authenticates_as_admin(monkeypatch, fake_store) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "true")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEY_HASHES", ADMIN_KEY_HASH)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"X-API-Key": ADMIN_KEY})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["user"]["plan_type"] == "admin"
    assert body["user"]["is_admin"] is True
    assert body["quota"]["unlimited"] is True
    # The store was consulted with the hash, never the plaintext key.
    assert fake_store.admin_lookups == [ADMIN_KEY_HASH]


def test_admin_key_rejected_when_disabled(monkeypatch, fake_store) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "false")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEY_HASHES", ADMIN_KEY_HASH)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"X-API-Key": ADMIN_KEY})
    assert resp.status_code == 401
    assert fake_store.admin_lookups == []


def test_plaintext_env_var_no_longer_grants_access(monkeypatch, fake_store) -> None:
    """Keys configured via the retired CITYLENS_ADMIN_API_KEYS plaintext env
    var must not authenticate — only hashes count."""
    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "true")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEYS", ADMIN_KEY)  # old, now inert
    monkeypatch.delenv("CITYLENS_ADMIN_API_KEY_HASHES", raising=False)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"X-API-Key": ADMIN_KEY})
    assert resp.status_code == 401
    assert fake_store.admin_lookups == []


def test_wrong_admin_key_rejected(monkeypatch, fake_store) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "true")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEY_HASHES", ADMIN_KEY_HASH)

    client = TestClient(app)
    resp = client.get("/v1/me", headers={"X-API-Key": "not-the-admin-key"})
    assert resp.status_code == 401
    assert fake_store.admin_lookups == []

"""Admin user lookup + plan assignment (/v1/admin/users*).

Mirrors test_pilot_requests.py: the store is an in-memory fake installed
via `app.dependency_overrides[admin_users.get_store]`, and admin access
reuses the existing mechanisms — the `auth_override` fixture for JWT-style
admins, and the REAL `require_auth` hash-only X-API-Key path (constant-time
comparison in services/auth.py) for the operator credential.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import admin_users
from app.routes import me as me_routes
from app.services import auth as auth_module
from app.services.plans import get_policy

_NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)

ADMIN_KEY = "test-admin-plan-key-123"
ADMIN_KEY_HASH = hashlib.sha256(ADMIN_KEY.encode("utf-8")).hexdigest()


def _user_doc(user_id: str, **overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "user_id": user_id,
        "email": f"{user_id}@example.com",
        "email_verified": True,
        "plan_type": "free",
        "is_admin": False,
        "created_at": _NOW,
        "updated_at": _NOW,
        "last_login_at": _NOW,
        "monthly_run_limit": None,
        "max_concurrent_runs": None,
        "auth_provider_last": "neon",
        "auth_subject_last": f"sub-{user_id}",
    }
    doc.update(overrides)
    return doc


class FakeUsersStore:
    """In-memory stand-in matching the FirestoreStore methods the admin
    routes (and the auth/user-API-key path) touch."""

    def __init__(self, users: dict[str, dict[str, Any]] | None = None) -> None:
        self.users = users or {}
        self.api_keys: dict[str, str] = {}  # plaintext key -> user_id

    # ---- admin_users routes ----

    def get_user(self, app_user_id: str) -> dict[str, Any] | None:
        return self.users.get(app_user_id)

    def find_users_by_email(
        self, email: str, *, limit: int = 10
    ) -> list[dict[str, Any]]:
        return [
            doc for doc in self.users.values() if doc.get("email") == email
        ][:limit]

    def set_user_plan(
        self,
        *,
        app_user_id: str,
        plan_type: str,
        changed_by: str,
    ) -> dict[str, Any] | None:
        doc = self.users.get(app_user_id)
        if doc is None:
            return None
        previous = str(doc.get("plan_type") or "free")
        if previous == plan_type:
            return doc
        history = list(doc.get("plan_history") or [])
        history.append(
            {
                "previous_plan_type": previous,
                "plan_type": plan_type,
                "changed_at": _NOW,
                "changed_by": changed_by,
            }
        )
        doc.update(
            {
                "plan_type": plan_type,
                "plan_changed_at": _NOW,
                "plan_history": history,
                "updated_at": _NOW,
            }
        )
        return doc

    # ---- require_auth admin-key path ----

    def get_admin_user_for_api_key(self, api_key_hash: str) -> dict[str, Any]:
        return _user_doc(
            f"admin_{api_key_hash[:24]}",
            email=None,
            email_verified=False,
            plan_type="admin",
            is_admin=True,
        )

    # ---- require_auth clk_live_ user-API-key path ----

    def get_user_id_for_api_key(self, token: str) -> str | None:
        return self.api_keys.get(token)

    # ---- /v1/me quota shape ----

    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return 0


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides = {}


def _install(store: FakeUsersStore, monkeypatch) -> TestClient:
    monkeypatch.setattr(auth_module, "_store_factory", lambda settings: store)
    app.dependency_overrides[admin_users.get_store] = lambda: store
    return TestClient(app)


def test_admin_user_routes_reject_unauthenticated_and_non_admin(
    monkeypatch, auth_override
) -> None:
    store = FakeUsersStore({"user-a": _user_doc("user-a")})
    client = _install(store, monkeypatch)

    for request in (
        lambda: client.get("/v1/admin/users", params={"email": "user-a@example.com"}),
        lambda: client.get("/v1/admin/users/user-a"),
        lambda: client.patch(
            "/v1/admin/users/user-a/plan", json={"plan_type": "acquisitions"}
        ),
    ):
        unauthenticated = request()
        assert unauthenticated.status_code == 401
        # Middleware keeps even early admin errors out of shared caches.
        assert unauthenticated.headers["cache-control"] == "private, no-store"

    auth_override(app_user_id="regular-user", is_admin=False)
    for request in (
        lambda: client.get("/v1/admin/users", params={"email": "user-a@example.com"}),
        lambda: client.get("/v1/admin/users/user-a"),
        lambda: client.patch(
            "/v1/admin/users/user-a/plan", json={"plan_type": "acquisitions"}
        ),
    ):
        forbidden = request()
        assert forbidden.status_code == 403

    # Nothing was written along the way.
    assert store.users["user-a"]["plan_type"] == "free"
    assert "plan_history" not in store.users["user-a"]


def test_admin_lookup_get_and_plan_assignment_lifecycle(
    monkeypatch, auth_override
) -> None:
    store = FakeUsersStore({"user-a": _user_doc("user-a")})
    client = _install(store, monkeypatch)
    auth_override(app_user_id="admin-user", is_admin=True)

    # 1. Owner finds the user id by email.
    found = client.get(
        "/v1/admin/users", params={"email": "user-a@example.com"}
    )
    assert found.status_code == 200, found.text
    assert found.headers["cache-control"] == "private, no-store"
    assert found.headers["vary"] == "Authorization, X-API-Key"
    assert [item["user_id"] for item in found.json()["items"]] == ["user-a"]
    # Internal store fields never leave the API.
    assert "auth_subject_last" not in found.json()["items"][0]

    missing_email = client.get(
        "/v1/admin/users", params={"email": "nobody@example.com"}
    )
    assert missing_email.status_code == 200
    assert missing_email.json() == {"items": []}

    # 2. Direct fetch by id.
    fetched = client.get("/v1/admin/users/user-a")
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["plan_type"] == "free"
    assert fetched.json()["plan_history"] == []

    # 3. Assign a paid plan; the audit trail records previous + actor.
    assigned = client.patch(
        "/v1/admin/users/user-a/plan", json={"plan_type": "acquisitions"}
    )
    assert assigned.status_code == 200, assigned.text
    assert assigned.headers["cache-control"] == "private, no-store"
    body = assigned.json()
    assert body["plan_type"] == "acquisitions"
    assert body["plan_changed_at"] == "2026-08-01T12:00:00Z"
    assert body["plan_history"] == [
        {
            "previous_plan_type": "free",
            "plan_type": "acquisitions",
            "changed_at": "2026-08-01T12:00:00Z",
            "changed_by": "admin-user",
        }
    ]
    assert store.users["user-a"]["plan_type"] == "acquisitions"

    # 4. The newly assigned plan actually changes policy results.
    assert get_policy(store.users["user-a"]["plan_type"]) == {
        "monthly_run_limit": 25,
        "max_concurrent_runs": 2,
    }

    # 5. Re-assigning the same plan is idempotent: no duplicate history.
    repeated = client.patch(
        "/v1/admin/users/user-a/plan", json={"plan_type": "acquisitions"}
    )
    assert repeated.status_code == 200
    assert len(repeated.json()["plan_history"]) == 1


def test_assigning_unknown_plan_is_422_and_writes_nothing(
    monkeypatch, auth_override
) -> None:
    store = FakeUsersStore({"user-a": _user_doc("user-a")})
    client = _install(store, monkeypatch)
    auth_override(app_user_id="admin-user", is_admin=True)

    for bad_plan in ("platinum", "smoke_read_only"):
        rejected = client.patch(
            "/v1/admin/users/user-a/plan", json={"plan_type": bad_plan}
        )
        assert rejected.status_code == 422, rejected.text

    # An empty plan_type fails schema validation before the registry check.
    empty = client.patch(
        "/v1/admin/users/user-a/plan", json={"plan_type": ""}
    )
    assert empty.status_code == 422

    # The registry-backed rejection names the allowed plans.
    detail = rejected.json()["detail"]
    assert detail["code"] == "UNKNOWN_PLAN_TYPE"
    assert detail["allowed_plan_types"] == [
        "free",
        "acquisitions",
        "concierge",
        "admin",
    ]

    # Unknown body fields are rejected too (extra=forbid).
    extra = client.patch(
        "/v1/admin/users/user-a/plan",
        json={"plan_type": "acquisitions", "is_admin": True},
    )
    assert extra.status_code == 422

    assert store.users["user-a"]["plan_type"] == "free"
    assert "plan_history" not in store.users["user-a"]


def test_assigning_plan_to_unknown_user_is_404(
    monkeypatch, auth_override
) -> None:
    store = FakeUsersStore()
    client = _install(store, monkeypatch)
    auth_override(app_user_id="admin-user", is_admin=True)

    missing_get = client.get("/v1/admin/users/no-such-user")
    assert missing_get.status_code == 404

    missing_patch = client.patch(
        "/v1/admin/users/no-such-user/plan", json={"plan_type": "concierge"}
    )
    assert missing_patch.status_code == 404


def test_hash_only_admin_api_key_can_assign_plans(monkeypatch) -> None:
    """The operator path: X-API-Key checked in constant time against
    CITYLENS_ADMIN_API_KEY_HASHES via the REAL require_auth dependency."""

    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "true")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEY_HASHES", ADMIN_KEY_HASH)
    store = FakeUsersStore({"user-a": _user_doc("user-a")})
    client = _install(store, monkeypatch)

    wrong_key = client.patch(
        "/v1/admin/users/user-a/plan",
        headers={"X-API-Key": "not-the-admin-key"},
        json={"plan_type": "concierge"},
    )
    assert wrong_key.status_code == 401

    assigned = client.patch(
        "/v1/admin/users/user-a/plan",
        headers={"X-API-Key": ADMIN_KEY},
        json={"plan_type": "concierge"},
    )
    assert assigned.status_code == 200, assigned.text
    assert assigned.json()["plan_type"] == "concierge"
    assert store.users["user-a"]["plan_history"][0]["changed_by"] == (
        f"admin_{ADMIN_KEY_HASH[:24]}"
    )


def test_assigned_plan_flows_into_auth_and_quota(monkeypatch) -> None:
    """A persisted plan_type is picked up by require_auth (clk_live_ key
    resolution) and produces the plan's run quota on /v1/me."""

    monkeypatch.setenv("CITYLENS_ALLOW_USER_API_KEYS", "true")
    store = FakeUsersStore(
        {"user-a": _user_doc("user-a", plan_type="acquisitions")}
    )
    store.api_keys["clk_live_test-key"] = "user-a"
    client = _install(store, monkeypatch)
    app.dependency_overrides[me_routes.get_store] = lambda: store
    try:
        me = client.get(
            "/v1/me",
            headers={"Authorization": "Bearer clk_live_test-key"},
        )
    finally:
        app.dependency_overrides = {}

    assert me.status_code == 200, me.text
    assert me.json()["user"]["plan_type"] == "acquisitions"
    assert me.json()["quota"]["monthly_run_limit"] == 25
    assert me.json()["quota"]["max_concurrent_runs"] == 2
    assert me.json()["quota"]["unlimited"] is False

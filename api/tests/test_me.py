from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.routes import me as me_routes


class _Store:
    def __init__(self, runs_used: int = 0) -> None:
        self.runs_used = runs_used

    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return self.runs_used


def test_me_requires_auth() -> None:
    client = TestClient(app)
    resp = client.get("/v1/me")
    assert resp.status_code == 401


def test_me_free_plan_shape(auth_override) -> None:
    auth_override(app_user_id="u-free", plan_type="free")
    app.dependency_overrides[me_routes.get_store] = lambda: _Store(runs_used=2)

    client = TestClient(app)
    resp = client.get("/v1/me")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["user"]["id"] == "u-free"
    assert body["user"]["plan_type"] == "free"
    assert body["user"]["is_admin"] is False
    quota = body["quota"]
    assert quota["monthly_run_limit"] == 5
    assert quota["runs_used"] == 2
    assert quota["runs_remaining"] == 3
    assert quota["unlimited"] is False
    assert quota["max_concurrent_runs"] == 1
    assert "month_key" in quota


def test_me_admin_plan_shape(auth_override) -> None:
    auth_override(app_user_id="u-admin", plan_type="admin", is_admin=True)
    app.dependency_overrides[me_routes.get_store] = lambda: _Store(runs_used=99)

    client = TestClient(app)
    resp = client.get("/v1/me")
    assert resp.status_code == 200
    body = resp.json()
    assert body["user"]["is_admin"] is True
    assert body["user"]["plan_type"] == "admin"
    quota = body["quota"]
    assert quota["monthly_run_limit"] is None
    assert quota["runs_remaining"] is None
    assert quota["unlimited"] is True
    assert quota["max_concurrent_runs"] is None

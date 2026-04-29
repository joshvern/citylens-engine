from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes
from app.services.firestore_store import MonthlyQuotaExceeded


class FakeStore:
    def __init__(self, *, concurrent: int = 0) -> None:
        self.concurrent = concurrent
        self.created: list[dict] = []
        self.usage: dict[tuple[str, str], int] = {}

    def get_or_create_user_by_identity(self, **_kwargs):
        return {
            "user_id": "u1",
            "plan_type": "free",
            "is_admin": False,
            "email": "u1@example.com",
        }

    def count_user_concurrent_runs(self, *, user_id: str):
        return self.concurrent

    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return self.usage.get((app_user_id, month_key), 0)

    def try_increment_monthly_usage(self, *, app_user_id, month_key, limit):
        used = self.usage.get((app_user_id, month_key), 0)
        if limit is not None and used >= limit:
            raise MonthlyQuotaExceeded(
                runs_used=used, monthly_run_limit=int(limit), month_key=month_key
            )
        new_used = used + 1
        self.usage[(app_user_id, month_key)] = new_used
        return new_used

    def decrement_monthly_usage(self, *, app_user_id, month_key):
        used = self.usage.get((app_user_id, month_key), 0)
        new_used = max(0, used - 1)
        self.usage[(app_user_id, month_key)] = new_used
        return new_used

    def create_run(self, *, user_id: str, request_dict: dict):
        run_id = f"r-{len(self.created) + 1}"
        now = datetime.now(timezone.utc)
        doc = {
            "run_id": run_id,
            "user_id": user_id,
            "status": "queued",
            "stage": "queued",
            "progress": 0,
            "request": request_dict,
            "error": None,
            "execution_id": None,
            "created_at": now,
            "updated_at": now,
        }
        self.created.append(doc)
        return doc

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        return None

    def mark_failed(self, run_id: str, error) -> None:
        return None

    def list_runs(self, *, user_id: str, limit: int, cursor: str | None = None):
        return [r for r in self.created if r["user_id"] == user_id], None

    def get_run(self, run_id: str):
        for r in self.created:
            if r["run_id"] == run_id:
                return r
        return None

    def list_artifacts(self, run_id: str):
        return []


class FakeTrigger:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def run(self, *, run_id: str) -> str:
        if self.fail:
            raise RuntimeError("simulated trigger failure")
        return "exec-1"


def _install(monkeypatch, store: FakeStore, *, fail_trigger: bool = False) -> None:
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: FakeTrigger(fail=fail_trigger)


def test_post_runs_no_auth_returns_401() -> None:
    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St"})
    assert resp.status_code == 401


def test_get_runs_no_auth_returns_401() -> None:
    client = TestClient(app)
    resp = client.get("/v1/runs")
    assert resp.status_code == 401


def test_free_user_can_create_five_runs(auth_override) -> None:
    store = FakeStore()
    auth_override(app_user_id="u-free-1", plan_type="free")
    _install(None, store)

    client = TestClient(app)
    for i in range(5):
        resp = client.post("/v1/runs", json={"address": f"{i} Main St"})
        assert resp.status_code == 200, resp.text

    assert sum(store.usage.values()) == 5


def test_free_user_sixth_run_returns_429_monthly_quota(auth_override) -> None:
    store = FakeStore()
    auth_override(app_user_id="u-free-2", plan_type="free")
    _install(None, store)

    client = TestClient(app)
    for i in range(5):
        assert client.post("/v1/runs", json={"address": f"{i}"}).status_code == 200
    resp = client.post("/v1/runs", json={"address": "6"})
    assert resp.status_code == 429
    detail = resp.json()["detail"]
    assert detail["code"] == "MONTHLY_QUOTA_EXCEEDED"
    assert detail["plan_type"] == "free"
    assert detail["monthly_run_limit"] == 5
    assert detail["runs_used"] == 5
    assert detail["runs_remaining"] == 0
    assert "month_key" in detail


def test_admin_user_can_create_more_than_five(auth_override) -> None:
    store = FakeStore()
    auth_override(app_user_id="u-admin", plan_type="admin", is_admin=True)
    _install(None, store)

    client = TestClient(app)
    for i in range(7):
        resp = client.post("/v1/runs", json={"address": f"{i}"})
        assert resp.status_code == 200, resp.text


def test_run_user_id_is_app_user_id_not_subject(auth_override) -> None:
    store = FakeStore()
    auth_override(app_user_id="app-uuid-xyz", plan_type="free")
    _install(None, store)

    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St"})
    assert resp.status_code == 200
    assert store.created[0]["user_id"] == "app-uuid-xyz"


def test_concurrent_limit_blocks_second_run(auth_override) -> None:
    store = FakeStore(concurrent=1)
    auth_override(app_user_id="u-c", plan_type="free")
    _install(None, store)

    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St"})
    assert resp.status_code == 429
    detail = resp.json()["detail"]
    assert detail["code"] == "CONCURRENT_LIMIT_EXCEEDED"
    assert detail["plan_type"] == "free"
    assert detail["max_concurrent_runs"] == 1


def test_quota_released_when_trigger_fails(auth_override) -> None:
    store = FakeStore()
    auth_override(app_user_id="u-rel", plan_type="free")
    _install(None, store, fail_trigger=True)

    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St"})
    assert resp.status_code == 500
    # Counter must have been decremented back to 0
    assert sum(store.usage.values()) == 0

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes


class FakeStore:
    def __init__(self, *, runs_today: int = 0, concurrent: int = 0) -> None:
        self._runs_today = runs_today
        self._concurrent = concurrent
        self.created = []

    def get_or_create_user(self, user_id: str):
        return {"user_id": user_id, "quota_per_day": 1, "max_concurrent_runs": 1}

    def count_user_runs_since(self, *, user_id: str, since):
        return self._runs_today

    def count_user_concurrent_runs(self, *, user_id: str):
        return self._concurrent

    def create_run(self, *, user_id: str, request_dict: dict):
        doc = {
            "run_id": "r1",
            "user_id": user_id,
            "status": "queued",
            "stage": "queued",
            "progress": 0,
            "request": request_dict,
            "error": None,
            "execution_id": None,
            "created_at": __import__("datetime").datetime.utcnow(),
            "updated_at": __import__("datetime").datetime.utcnow(),
        }
        self.created.append(doc)
        return doc

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        return None

    def mark_failed(self, run_id: str, error: str) -> None:
        return None


class FakeTrigger:
    def run(self, *, run_id: str) -> str:
        return "exec-1"


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")


def test_auth_missing_key_401(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "x"})
    assert resp.status_code == 401


def test_quota_per_day_429(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake_store = FakeStore(runs_today=1, concurrent=0)

    app.dependency_overrides[runs_routes.get_store] = lambda: fake_store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: FakeTrigger()

    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        headers={"X-API-Key": "dev-key-1"},
        json={"address": "x"},
    )
    assert resp.status_code == 429

    app.dependency_overrides = {}


def test_quota_concurrent_429(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake_store = FakeStore(runs_today=0, concurrent=1)

    app.dependency_overrides[runs_routes.get_store] = lambda: fake_store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: FakeTrigger()

    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        headers={"X-API-Key": "dev-key-1"},
        json={"address": "x"},
    )
    assert resp.status_code == 429

    app.dependency_overrides = {}

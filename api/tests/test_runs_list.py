from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes


class FakeStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def list_runs(self, *, user_id: str, limit: int, cursor: str | None = None):
        self.calls.append({"user_id": user_id, "limit": limit, "cursor": cursor})
        now = datetime.now(timezone.utc)
        return (
            [
                {
                    "run_id": "run-a",
                    "user_id": user_id,
                    "status": "succeeded",
                    "stage": "done",
                    "progress": 100,
                    "request": {"address": "1 Main St"},
                    "error": {
                        "code": "PIPELINE_FAILED",
                        "message": "m",
                        "stage": "done",
                        "traceback_summary": ["x"],
                    },
                    "execution_id": "exec-a",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "run_id": "run-b",
                    "user_id": user_id,
                    "status": "running",
                    "stage": "segment",
                    "progress": 50,
                    "request": {"address": "2 Main St"},
                    "error": None,
                    "execution_id": None,
                    "created_at": now,
                    "updated_at": now,
                },
            ],
            "cursor-2",
        )


def test_runs_list_returns_paged_items(auth_override) -> None:
    fake_store = FakeStore()
    auth_override(app_user_id="u-list", plan_type="free")
    app.dependency_overrides[runs_routes.get_store] = lambda: fake_store

    client = TestClient(app)
    resp = client.get("/v1/runs?limit=2")
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["next_cursor"] == "cursor-2"
    assert [item["run_id"] for item in body["items"]] == ["run-a", "run-b"]
    assert body["items"][0]["error"]["code"] == "PIPELINE_FAILED"
    assert fake_store.calls[0]["cursor"] is None
    assert fake_store.calls[0]["user_id"] == "u-list"

    resp2 = client.get("/v1/runs?limit=1&cursor=cursor-2")
    assert resp2.status_code == 200
    assert fake_store.calls[1]["cursor"] == "cursor-2"

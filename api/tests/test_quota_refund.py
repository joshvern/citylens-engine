"""Reactive quota refund: failed runs return their monthly slot to the user."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes


class _RefundStore:
    """Tracks usage counter + run docs in-memory; mimics the Firestore txn API
    surface that runs.py exercises."""

    def __init__(self) -> None:
        self.usage: dict[tuple[str, str], int] = {}
        self.runs: dict[str, dict] = {}

    # API surface used by routes
    def get_or_create_user_by_identity(self, **_kwargs):
        return {"user_id": "u1", "plan_type": "free", "is_admin": False}

    def list_runs(self, *, user_id: str, limit: int, cursor: str | None = None):
        rows = [r for r in self.runs.values() if r["user_id"] == user_id]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        return rows[:limit], None

    def get_run(self, run_id: str):
        return self.runs.get(run_id)

    def list_artifacts(self, run_id: str):
        return []

    def refund_run_quota_if_failed(self, run_id: str) -> bool:
        run = self.runs.get(run_id)
        if not run:
            return False
        if str(run.get("status") or "") != "failed":
            return False
        if run.get("quota_refunded"):
            return False
        user_id = str(run.get("user_id") or "")
        created_at = run.get("created_at")
        mk = f"{created_at.year:04d}-{created_at.month:02d}"
        used = self.usage.get((user_id, mk), 0)
        self.usage[(user_id, mk)] = max(0, used - 1)
        run["quota_refunded"] = True
        return True


def _seed_failed_run(store: _RefundStore, *, user_id: str, run_id: str, mk: str) -> None:
    year, month = mk.split("-")
    created_at = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
    store.runs[run_id] = {
        "run_id": run_id,
        "user_id": user_id,
        "status": "failed",
        "stage": "failed",
        "progress": 100,
        "request": {"address": "x"},
        "error": {
            "code": "WORKER_FAILED",
            "message": "no LAS tile",
            "stage": "failed",
            "traceback_summary": [],
        },
        "execution_id": None,
        "created_at": created_at,
        "updated_at": created_at,
    }
    store.usage[(user_id, mk)] = 1


def test_get_run_refunds_failed_run_quota(auth_override) -> None:
    auth_override(app_user_id="u-refund", plan_type="free")
    store = _RefundStore()
    _seed_failed_run(store, user_id="u-refund", run_id="r-fail-1", mk="2026-04")
    app.dependency_overrides[runs_routes.get_store] = lambda: store

    client = TestClient(app)
    resp = client.get("/v1/runs/r-fail-1")
    assert resp.status_code == 200, resp.text
    assert store.usage[("u-refund", "2026-04")] == 0
    assert store.runs["r-fail-1"]["quota_refunded"] is True

    # Second view is idempotent
    resp = client.get("/v1/runs/r-fail-1")
    assert resp.status_code == 200
    assert store.usage[("u-refund", "2026-04")] == 0


def test_list_runs_refunds_each_failed_run_once(auth_override) -> None:
    auth_override(app_user_id="u-list-refund", plan_type="free")
    store = _RefundStore()
    _seed_failed_run(store, user_id="u-list-refund", run_id="r1", mk="2026-04")
    _seed_failed_run(store, user_id="u-list-refund", run_id="r2", mk="2026-04")
    # Pre-existing usage = 2 (one per failed run before refunds run)
    store.usage[("u-list-refund", "2026-04")] = 2

    app.dependency_overrides[runs_routes.get_store] = lambda: store

    client = TestClient(app)
    resp = client.get("/v1/runs")
    assert resp.status_code == 200, resp.text
    assert store.usage[("u-list-refund", "2026-04")] == 0


def test_succeeded_run_does_not_refund(auth_override) -> None:
    auth_override(app_user_id="u-ok", plan_type="free")
    store = _RefundStore()
    store.runs["r-ok"] = {
        "run_id": "r-ok",
        "user_id": "u-ok",
        "status": "succeeded",
        "stage": "complete",
        "progress": 100,
        "request": {"address": "x"},
        "error": None,
        "execution_id": "exec-1",
        "created_at": datetime(2026, 4, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 4, 1, tzinfo=timezone.utc),
    }
    store.usage[("u-ok", "2026-04")] = 1
    app.dependency_overrides[runs_routes.get_store] = lambda: store

    client = TestClient(app)
    assert client.get("/v1/runs/r-ok").status_code == 200
    assert store.usage[("u-ok", "2026-04")] == 1
    assert "quota_refunded" not in store.runs["r-ok"]

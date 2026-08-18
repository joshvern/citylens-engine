"""Per-credential rate limits on expensive paths + Retry-After on 429s.

Time is frozen inside the limiter module (its `time.monotonic` only) so
bucket refill cannot make the counting assertions flaky.

The limiter is per-instance by design (in-memory buckets); the tests
assert the single-instance contract.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.routes import api_keys as api_keys_routes
from app.routes import me as me_routes
from app.routes import runs as runs_routes
from app.services import rate_limit
from app.services.firestore_store import MonthlyQuotaExceeded


@pytest.fixture
def frozen_time(monkeypatch):
    monkeypatch.setattr(
        rate_limit, "time", SimpleNamespace(monotonic=lambda: 1_000.0)
    )


class _RunsStore:
    """Unlimited-quota store so only the limiter can say no."""

    def __init__(self) -> None:
        self.created = 0
        self.usage: dict[tuple[str, str], int] = {}

    def count_user_concurrent_runs(self, *, user_id: str):
        return 0

    def try_increment_monthly_usage(self, *, app_user_id, month_key, limit):
        if limit is not None and self.usage.get((app_user_id, month_key), 0) >= limit:
            raise MonthlyQuotaExceeded(
                runs_used=self.usage[(app_user_id, month_key)],
                monthly_run_limit=int(limit),
                month_key=month_key,
            )
        self.usage[(app_user_id, month_key)] = (
            self.usage.get((app_user_id, month_key), 0) + 1
        )
        return self.usage[(app_user_id, month_key)]

    def decrement_monthly_usage(self, *, app_user_id, month_key):
        used = max(0, self.usage.get((app_user_id, month_key), 0) - 1)
        self.usage[(app_user_id, month_key)] = used
        return used

    def create_run(self, *, user_id: str, request_dict: dict):
        self.created += 1
        now = datetime.now(timezone.utc)
        return {
            "run_id": f"r-{self.created}",
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

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        return None

    def mark_failed(self, run_id: str, error) -> None:
        return None


class _Trigger:
    def run(self, *, run_id: str) -> str:
        return "exec-1"


class _MeStore:
    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        return 0


class _ApiKeysStore:
    def list_api_keys(self, *, app_user_id: str):
        return []


def _retry_after_seconds(resp) -> int:
    assert "retry-after" in resp.headers, resp.headers
    return int(resp.headers["retry-after"])


def test_create_run_bucket_allows_burst_then_429_with_retry_after(
    auth_override, frozen_time
) -> None:
    auth_override(app_user_id="u-burst", plan_type="admin", is_admin=True)
    store = _RunsStore()
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: _Trigger()
    client = TestClient(app)

    for i in range(10):
        resp = client.post("/v1/runs", json={"address": f"{i} Main St"})
        assert resp.status_code == 200, resp.text

    blocked = client.post("/v1/runs", json={"address": "11 Main St"})
    assert blocked.status_code == 429
    assert blocked.json()["detail"] == "Rate limit exceeded"
    # 6/min refill -> a full token takes 10s.
    assert _retry_after_seconds(blocked) == 10
    # The 429 came from the limiter dependency: no quota was reserved and
    # no run was created for the blocked request.
    assert store.created == 10
    assert sum(store.usage.values()) == 10


def test_create_run_bucket_is_per_credential(auth_override, frozen_time) -> None:
    store = _RunsStore()
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: _Trigger()
    client = TestClient(app)

    auth_override(app_user_id="u-exhausted", plan_type="admin", is_admin=True)
    for i in range(10):
        assert client.post("/v1/runs", json={"address": f"{i}"}).status_code == 200
    assert client.post("/v1/runs", json={"address": "x"}).status_code == 429

    # A different credential has its own bucket.
    auth_override(app_user_id="u-other", plan_type="admin", is_admin=True)
    assert client.post("/v1/runs", json={"address": "y"}).status_code == 200


def test_me_bucket_60_per_minute(auth_override, frozen_time) -> None:
    auth_override(app_user_id="u-me", plan_type="free")
    app.dependency_overrides[me_routes.get_store] = lambda: _MeStore()
    client = TestClient(app)

    for _ in range(60):
        assert client.get("/v1/me").status_code == 200

    blocked = client.get("/v1/me")
    assert blocked.status_code == 429
    # 1 token/s refill -> next token in 1s.
    assert _retry_after_seconds(blocked) == 1


def test_api_keys_bucket_20_per_minute_shared_across_routes(
    auth_override, frozen_time, monkeypatch
) -> None:
    monkeypatch.setenv("CITYLENS_ALLOW_USER_API_KEYS", "true")
    auth_override(app_user_id="u-keys", plan_type="free")
    app.dependency_overrides[api_keys_routes.get_store] = lambda: _ApiKeysStore()
    client = TestClient(app)

    for _ in range(20):
        assert client.get("/v1/api-keys").status_code == 200

    blocked = client.get("/v1/api-keys")
    assert blocked.status_code == 429
    # 20/min refill -> a full token takes 3s.
    assert _retry_after_seconds(blocked) == 3


def test_monthly_quota_429_carries_retry_after(auth_override) -> None:
    auth_override(app_user_id="u-monthly", plan_type="free")
    store = _RunsStore()
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: _Trigger()
    client = TestClient(app)

    for i in range(5):
        assert client.post("/v1/runs", json={"address": f"{i}"}).status_code == 200
    blocked = client.post("/v1/runs", json={"address": "6"})
    assert blocked.status_code == 429
    assert blocked.json()["detail"]["code"] == "MONTHLY_QUOTA_EXCEEDED"
    # Documented constant: the window resets at the UTC month boundary;
    # 1h steers clients to a sane re-check cadence.
    assert blocked.headers["retry-after"] == "3600"


def test_concurrent_quota_429_carries_retry_after(auth_override) -> None:
    auth_override(app_user_id="u-conc", plan_type="free")
    store = _RunsStore()
    store.count_user_concurrent_runs = lambda *, user_id: 1  # type: ignore[method-assign]
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: _Trigger()
    client = TestClient(app)

    blocked = client.post("/v1/runs", json={"address": "1 Main St"})
    assert blocked.status_code == 429
    assert blocked.json()["detail"]["code"] == "CONCURRENT_LIMIT_EXCEEDED"
    # Documented constant: a slot frees when a run finishes (minutes), so
    # 60s is a sensible poll interval.
    assert blocked.headers["retry-after"] == "60"


def test_enforce_token_bucket_429_retry_after_matches_refill(frozen_time) -> None:
    """Every limiter-emitted 429 (demo, pilot, per-credential) carries a
    ceil()'d Retry-After derived from its own refill rate."""

    rate_limit.enforce_token_bucket(
        key="unit:x", capacity=1, refill_per_second=1 / 7
    )
    with pytest.raises(HTTPException) as excinfo:
        rate_limit.enforce_token_bucket(
            key="unit:x", capacity=1, refill_per_second=1 / 7
        )
    assert excinfo.value.status_code == 429
    assert excinfo.value.headers["Retry-After"] == "7"

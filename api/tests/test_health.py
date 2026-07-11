from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import health as health_routes
from app.routes import parcel_intel as parcel_intel_routes


def test_health_ok() -> None:
    client = TestClient(app)
    resp = client.get("/v1/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "version": "0.1.0"}


# --- /v1/health/ready ---


class FakeStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.pings = 0

    def ping(self) -> None:
        self.pings += 1
        if self.fail:
            raise RuntimeError("simulated Firestore outage")


class FakeGcs:
    def __init__(self, store: dict[str, bytes] | None = None) -> None:
        self._store = store or {}

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        if object_name not in self._store:
            raise FileNotFoundError(object_name)
        return self._store[object_name], "application/json"


def _manifest_bytes(generated_at: str) -> bytes:
    return json.dumps(
        {
            "schema": "citylens-parcel-intel/published_sweep@v1",
            "generated_at": generated_at,
            "boroughs": [
                {"slug": "brooklyn", "display_name": "Brooklyn", "count": 1, "top_score": 0.9}
            ],
            "model_metadata": {},
        }
    ).encode("utf-8")


@pytest.fixture(autouse=True)
def _clean_overrides():
    yield
    app.dependency_overrides = {}


def _install(*, store: FakeStore, gcs: FakeGcs) -> parcel_intel_routes.ParcelIntelRegistry:
    registry = parcel_intel_routes.ParcelIntelRegistry()
    app.dependency_overrides[health_routes.get_store] = lambda: store
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: gcs
    app.dependency_overrides[parcel_intel_routes.get_registry] = lambda: registry
    return registry


def test_ready_ok_with_fresh_parcel_intel() -> None:
    store = FakeStore()
    fresh = datetime.now(timezone.utc).isoformat()
    gcs = FakeGcs({"parcel-intel/v1/manifest.json": _manifest_bytes(fresh)})
    _install(store=store, gcs=gcs)

    client = TestClient(app)
    resp = client.get("/v1/health/ready")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["firestore"] is True
    assert body["parcel_intel"]["present"] is True
    assert body["parcel_intel"]["stale"] is False
    assert body["parcel_intel"]["age_days"] is not None
    assert store.pings == 1


def test_ready_503_when_firestore_unreachable() -> None:
    store = FakeStore(fail=True)
    fresh = datetime.now(timezone.utc).isoformat()
    gcs = FakeGcs({"parcel-intel/v1/manifest.json": _manifest_bytes(fresh)})
    _install(store=store, gcs=gcs)

    client = TestClient(app)
    resp = client.get("/v1/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["ok"] is False
    assert body["firestore"] is False
    # Parcel-intel state is still reported alongside the failure.
    assert body["parcel_intel"]["present"] is True


def test_ready_200_degraded_when_parcel_intel_missing() -> None:
    store = FakeStore()
    gcs = FakeGcs({})  # no manifest published → registry raises 503 internally
    _install(store=store, gcs=gcs)

    client = TestClient(app)
    resp = client.get("/v1/health/ready")
    # Missing parcel-intel data is degraded, not fatal.
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["firestore"] is True
    assert body["parcel_intel"] == {"present": False, "age_days": None, "stale": False}


def test_ready_200_degraded_when_manifest_corrupt() -> None:
    store = FakeStore()
    gcs = FakeGcs({"parcel-intel/v1/manifest.json": b"{corrupt"})
    _install(store=store, gcs=gcs)

    client = TestClient(app)
    resp = client.get("/v1/health/ready")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["parcel_intel"]["present"] is False


def test_ready_reports_stale_parcel_intel() -> None:
    store = FakeStore()
    gcs = FakeGcs({"parcel-intel/v1/manifest.json": _manifest_bytes("2026-01-01T00:00:00+00:00")})
    _install(store=store, gcs=gcs)

    client = TestClient(app)
    resp = client.get("/v1/health/ready")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["parcel_intel"]["present"] is True
    assert body["parcel_intel"]["stale"] is True
    assert body["parcel_intel"]["age_days"] > 45

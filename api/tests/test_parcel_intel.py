"""Tests for /v1/parcel-intel/index and /v1/parcel-intel/sweep.

Mirrors `test_demo.py`'s pattern: a FakeGcs that returns canned JSONL +
manifest bytes via `download_bytes`, the router cache reset between
tests so each one runs in isolation.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_intel as parcel_intel_routes


class FakeGcs:
    """In-memory GCS stub. Maps object_name → (bytes, content_type)."""

    def __init__(self, store: dict[str, bytes] | None = None) -> None:
        self._store = store or {}

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        if object_name not in self._store:
            raise FileNotFoundError(object_name)
        ct = (
            "application/json"
            if object_name.endswith(".json")
            else "application/x-ndjson"
        )
        return self._store[object_name], ct


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")


@pytest.fixture(autouse=True)
def _reset_registry_and_overrides():
    # Drop the singleton + any dependency overrides between tests so
    # FakeGcs from one test doesn't leak into another.
    parcel_intel_routes._REGISTRY = parcel_intel_routes.ParcelIntelRegistry()
    yield
    parcel_intel_routes._REGISTRY = parcel_intel_routes.ParcelIntelRegistry()
    app.dependency_overrides = {}


def _row(bbl: str, **overrides) -> dict:
    base = {
        "bbl": bbl,
        "address": "TEST",
        "borough": "BK",
        "score_calibrated": 0.9,
        "score_calibrated_p10": None,
        "score_calibrated_p90": None,
        "lot_area_sqft": 5000.0,
        "allowed_far": 4.0,
        "max_floor_area_sqft": 20000.0,
        "unused_floor_area_sqft": 15000.0,
        "far_utilization_pct": 25.0,
        "zoning_district_1": "R7A",
        "land_use": "11",
        "year_built": 1900,
        "num_floors": 0,
        "last_sale_price": 1_500_000.0,
        "last_sale_year": 2022,
        "years_held": 4,
        "has_recent_sale_5yr": True,
        "is_landmark": False,
        "is_historic_district": False,
        "block_id": bbl[:6],
        "block_rank": 1,
        # Default to empty so existing tests keep their pre-SHAP behavior.
        "top_features": [],
    }
    base.update(overrides)
    return base


def _manifest(boroughs: list[str], generated_at: str = "2026-05-08T00:00:00+00:00") -> dict:
    return {
        "schema": "citylens-parcel-intel/published_sweep@v1",
        "generated_at": generated_at,
        "boroughs": [
            {"slug": b, "display_name": b.title(), "count": 2, "top_score": 0.9}
            for b in boroughs
        ],
        "model_metadata": {"feature_year": 2018},
    }


def _make_fake_gcs(boroughs: list[str], rows_by_borough: dict[str, list[dict]] | None = None) -> FakeGcs:
    store: dict[str, bytes] = {
        "parcel-intel/v1/manifest.json": json.dumps(_manifest(boroughs)).encode("utf-8"),
    }
    rbb = rows_by_borough or {}
    for slug in boroughs:
        rows = rbb.get(slug) or [_row(f"3{slug[:1].upper()}{i:08d}") for i in range(2)]
        store[f"parcel-intel/v1/{slug}.jsonl"] = (
            "\n".join(json.dumps(r) for r in rows) + "\n"
        ).encode("utf-8")
    return FakeGcs(store)


def test_parcel_intel_index_returns_borough_summary(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn", "manhattan"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/index")
    assert r.status_code == 200, r.text
    body = r.json()
    assert {b["slug"] for b in body["boroughs"]} == {"brooklyn", "manhattan"}
    assert body["model_metadata"] == {"feature_year": 2018}
    # Cache header is the gating metric for whether Vercel/CDN edge-caches.
    assert "cache-control" in r.headers
    assert "s-maxage=600" in r.headers["cache-control"]


def test_parcel_intel_sweep_returns_top_n_rows(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows = [_row(f"30200000{i:02d}", score_calibrated=0.99 - i * 0.01) for i in range(5)]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn", "top": 3})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["borough"] == "brooklyn"
    assert len(body["rows"]) == 3
    # Public schema fields come through Pydantic.
    sample = body["rows"][0]
    for key in (
        "bbl", "address", "score_calibrated",
        "is_landmark", "is_historic_district",
        "last_sale_price", "block_id", "block_rank",
    ):
        assert key in sample
    # Cache header present.
    assert "s-maxage=600" in r.headers["cache-control"]


def test_parcel_intel_rejects_unknown_borough(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyne"})
    assert r.status_code == 404
    assert "borough" in r.json()["detail"].lower()


def test_parcel_intel_clamps_top_to_100(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    # FastAPI Query(le=100) returns 422 for top=101.
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn", "top": 101})
    assert r.status_code == 422


def test_parcel_intel_503_when_no_data_published(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    # No manifest.json in the fake store → manifest fetch raises FileNotFoundError.
    fake = FakeGcs(store={})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/index")
    assert r.status_code == 503
    assert "not been published" in r.json()["detail"].lower()


def test_parcel_intel_sweep_returns_top_features(monkeypatch) -> None:
    """When the publisher injects per-row SHAP attributions, the engine must
    surface them through the response without truncation or reshaping."""
    _set_required_env(monkeypatch)
    feats = [
        {
            "name": "lot_area",
            "value": 5000,
            "contribution_logit": 0.85,
            "contribution_pct": 0.31,
        },
        {
            "name": "zoning_district",
            "value": "R7A",
            "contribution_logit": -0.42,
            "contribution_pct": 0.15,
        },
        {
            "name": "is_landmark",
            "value": False,
            "contribution_logit": 0.18,
            "contribution_pct": 0.07,
        },
    ]
    rows = [_row("3020000001", top_features=feats)]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    body = r.json()
    served = body["rows"][0]
    assert "top_features" in served
    assert len(served["top_features"]) == 3
    # Pydantic preserves field names + values; verify the first one
    # round-trips byte-for-byte.
    first = served["top_features"][0]
    assert first["name"] == "lot_area"
    assert first["value"] == 5000
    assert first["contribution_logit"] == 0.85
    assert first["contribution_pct"] == 0.31


def test_parcel_intel_sweep_defaults_top_features_to_empty(monkeypatch) -> None:
    """Older publishes (no top_features field at all) must still deserialize."""
    _set_required_env(monkeypatch)
    row_without = _row("3020000002")
    row_without.pop("top_features")  # simulate v1 sweep
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": [row_without]})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    served = r.json()["rows"][0]
    assert served["top_features"] == []


def test_parcel_intel_invalidates_cache_on_new_generated_at(monkeypatch) -> None:
    """When the publisher re-uploads with a newer manifest.generated_at,
    the registry must drop its borough cache and re-fetch."""
    _set_required_env(monkeypatch)
    rows_v1 = [_row("3020000001", address="ROW V1")]
    rows_v2 = [_row("3020000002", address="ROW V2")]
    fake = FakeGcs({
        "parcel-intel/v1/manifest.json": json.dumps(
            _manifest(["brooklyn"], generated_at="2026-05-08T00:00:00+00:00")
        ).encode("utf-8"),
        "parcel-intel/v1/brooklyn.jsonl": (
            json.dumps(rows_v1[0]) + "\n"
        ).encode("utf-8"),
    })
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    first = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert first.json()["rows"][0]["address"] == "ROW V1"

    # Simulate a republish: bump generated_at + swap rows.
    fake._store["parcel-intel/v1/manifest.json"] = json.dumps(
        _manifest(["brooklyn"], generated_at="2026-05-09T00:00:00+00:00")
    ).encode("utf-8")
    fake._store["parcel-intel/v1/brooklyn.jsonl"] = (
        json.dumps(rows_v2[0]) + "\n"
    ).encode("utf-8")

    second = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert second.json()["rows"][0]["address"] == "ROW V2"

"""Tests for /v1/parcel-intel/index and /v1/parcel-intel/sweep.

Mirrors `test_demo.py`'s pattern: a FakeGcs that returns canned JSONL +
manifest bytes via `download_bytes`, the router cache reset between
tests so each one runs in isolation.

The sweep endpoint is tiered: anonymous callers get a capped preview
with premium fields stripped; any authenticated caller gets the full
feed. Authenticated cases install a dependency override for
`maybe_auth` (mirroring the `auth_override` fixture for `require_auth`).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import ParcelIntelRow
from app.routes import parcel_intel as parcel_intel_routes
from app.services.auth import maybe_auth
from app.services.auth_context import AuthContext


class FakeGcs:
    """In-memory GCS stub. Maps object_name → (bytes, content_type)."""

    def __init__(self, store: dict[str, bytes] | None = None) -> None:
        self._store = store or {}
        self.requests: list[str] = []

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        self.requests.append(object_name)
        if object_name not in self._store:
            raise FileNotFoundError(object_name)
        ct = "application/json" if object_name.endswith(".json") else "application/x-ndjson"
        return self._store[object_name], ct


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")


@pytest.fixture(autouse=True)
def _reset_registry_and_overrides():
    # Drop the singleton + any dependency overrides between tests so
    # FakeGcs from one test doesn't leak into another.
    parcel_intel_routes._REGISTRY = parcel_intel_routes.ParcelIntelRegistry()
    yield
    parcel_intel_routes._REGISTRY = parcel_intel_routes.ParcelIntelRegistry()
    app.dependency_overrides = {}


def _authed(
    *, app_user_id: str = "user-pi-1", plan_type: str = "free", is_admin: bool = False
) -> AuthContext:
    """Install a maybe_auth override so the sweep sees an authenticated
    caller. Cleared by the autouse fixture above."""
    ctx = AuthContext(
        app_user_id=app_user_id,
        auth_provider="mock",
        auth_subject=f"sub-{app_user_id}",
        email=f"{app_user_id}@example.com",
        email_verified=True,
        is_admin=is_admin,
        plan_type=plan_type,
    )
    app.dependency_overrides[maybe_auth] = lambda: ctx
    return ctx


def _row(bbl: str, **overrides) -> dict:
    base = {
        "bbl": bbl,
        "address": "TEST",
        "borough": "BK",
        "score_calibrated": 0.9,
        "score_calibrated_p10": None,
        "score_calibrated_p90": None,
        "priority_rank": 1,
        "priority_tier": "highest",
        "model_rank": 42,
        "acquisition_rank": 1,
        "citywide_rank": 3,
        "acquisition_eligible": True,
        "acquisition_status": "eligible",
        "acquisition_exclusion_reasons": [],
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
        "tax_lien_sale_date": "2025-06-01",
        "tax_lien_sale_year": 2025,
        "tax_lien_water_debt_only": False,
        "tax_lien_data_as_of": "2026-07-23",
        "dob_safety_active_count": 4,
        "dob_safety_latest_issue_date": "2026-07-20",
        "ecb_active_count": 3,
        "ecb_class_1_count": 2,
        "ecb_balance_due": -3125.0,
        "ecb_latest_issue_date": "2026-07-18",
        "hpd_open_count": 7,
        "hpd_class_c_count": 1,
        "hpd_latest_inspection_date": "2026-07-19",
        "critical_violation_count": 3,
        "violation_data_as_of": "2026-07-23",
        "firm07_floodplain": False,
        "pfirm15_floodplain": True,
        "floodplain_1pct": True,
        "floodplain_data_as_of": "2026-07-23",
        "is_landmark": False,
        "is_historic_district": False,
        "block_id": bbl[:6],
        "block_rank": 1,
        "owner_type": None,
        # Default to empty so existing tests keep their pre-SHAP behavior.
        "top_features": [],
    }
    base.update(overrides)
    return base


def test_v4_row_keeps_acquisition_fields_unknown_for_rollout_fallback():
    row = ParcelIntelRow.model_validate({"bbl": "1000010001"})

    assert row.acquisition_eligible is None
    assert row.acquisition_status is None


def test_row_preserves_zap_entitlement_provenance():
    row = ParcelIntelRow.model_validate(
        _row(
            "3058920038",
            acquisition_eligible=False,
            acquisition_status="active_project",
            acquisition_exclusion_reasons=["approved_land_use_project"],
            latest_project_type="land_use_entitlement",
            latest_project_job_number="2023K0205",
            latest_project_status="Completed — approved",
            latest_project_url=(
                "https://zap.planning.nyc.gov/projects/2023K0205"
            ),
            land_use_activity_as_of="2026-07-22",
        )
    )

    assert row.latest_project_type == "land_use_entitlement"
    assert row.latest_project_job_number == "2023K0205"
    assert row.latest_project_url.endswith("/projects/2023K0205")
    assert row.land_use_activity_as_of == "2026-07-22"


def _manifest(boroughs: list[str], generated_at: str = "2026-05-08T00:00:00+00:00") -> dict:
    return {
        "schema": "citylens-parcel-intel/published_sweep@v5",
        "generated_at": generated_at,
        "boroughs": [
            {"slug": b, "display_name": b.title(), "count": 2, "top_score": 0.9} for b in boroughs
        ],
        "model_metadata": {"feature_year": 2018},
        "data_sources": {
            "property_facts": {"source": "NYC PLUTO", "as_of": "2026-07-01"}
        },
        "quality_gate": {"passed": True, "failures": []},
        "generation_diff": {
            "schema": "citylens-parcel-intel/generation-diff@v1",
            "status": "compared",
            "gate": {
                "passed": True,
                "thresholds_passed": True,
                "override_applied": False,
                "override_reason": None,
                "failures": [],
            },
        },
    }


def _make_fake_gcs(
    boroughs: list[str],
    rows_by_borough: dict[str, list[dict]] | None = None,
    generated_at: str = "2026-05-08T00:00:00+00:00",
) -> FakeGcs:
    store: dict[str, bytes] = {
        "parcel-intel/v1/manifest.json": json.dumps(
            _manifest(boroughs, generated_at=generated_at)
        ).encode("utf-8"),
    }
    rbb = rows_by_borough or {}
    map_rows: list[dict] = []
    for slug in boroughs:
        rows = rbb.get(slug) or [_row(f"3{slug[:1].upper()}{i:08d}") for i in range(2)]
        store[f"parcel-intel/v1/{slug}.jsonl"] = (
            "\n".join(json.dumps(r) for r in rows) + "\n"
        ).encode("utf-8")
        map_rows.extend({**row, "borough": slug} for row in rows)
    store["parcel-intel/v1/map.jsonl"] = (
        "\n".join(json.dumps(row) for row in map_rows) + "\n"
    ).encode("utf-8")
    return FakeGcs(store)


def _make_atomic_fake_gcs(
    boroughs: list[str],
    rows_by_borough: dict[str, list[dict]] | None = None,
    *,
    generation: str = "20260723T230308737433Z-aaaaaaaaaaaa",
    generated_at: str = "2026-07-23T23:03:08.737433+00:00",
) -> FakeGcs:
    manifest = _manifest(boroughs, generated_at=generated_at)
    prefix = f"parcel-intel/v1/generations/{generation}"
    store: dict[str, bytes] = {}
    artifacts: dict[str, dict] = {}
    map_rows: list[dict] = []
    rbb = rows_by_borough or {}
    for slug in boroughs:
        rows = rbb.get(slug) or [
            _row(f"3{slug[:1].upper()}{i:08d}") for i in range(2)
        ]
        leaf = f"{slug}.jsonl"
        body = (
            "\n".join(json.dumps(row) for row in rows) + "\n"
        ).encode("utf-8")
        object_name = f"{prefix}/{leaf}"
        store[object_name] = body
        artifacts[leaf] = {
            "object_name": object_name,
            "sha256": hashlib.sha256(body).hexdigest(),
            "size_bytes": len(body),
            "row_count": len(rows),
        }
        map_rows.extend({**row, "borough": slug} for row in rows)
    map_body = (
        "\n".join(json.dumps(row) for row in map_rows) + "\n"
    ).encode("utf-8")
    map_object = f"{prefix}/map.jsonl"
    store[map_object] = map_body
    artifacts["map.jsonl"] = {
        "object_name": map_object,
        "sha256": hashlib.sha256(map_body).hexdigest(),
        "size_bytes": len(map_body),
        "row_count": len(map_rows),
    }
    manifest.update(
        {
            "publication_schema": (
                "citylens-parcel-intel/atomic-publication@v1"
            ),
            "artifact_generation": generation,
            "artifact_prefix": prefix,
            "artifacts": artifacts,
        }
    )
    store["parcel-intel/v1/manifest.json"] = json.dumps(manifest).encode(
        "utf-8"
    )
    return FakeGcs(store)


def _fresh_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    assert body["data_sources"]["property_facts"]["source"] == "NYC PLUTO"
    assert body["quality_gate"]["passed"] is True
    assert body["generation_diff"]["gate"]["passed"] is True
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
        "bbl",
        "address",
        "score_calibrated",
        "is_landmark",
        "is_historic_district",
        "last_sale_price",
        "block_id",
        "block_rank",
        "priority_tier",
        "model_rank",
        "acquisition_rank",
        "citywide_rank",
        "acquisition_eligible",
        "acquisition_status",
        "opportunity_category",
        "property_facts_current",
    ):
        assert key in sample
    # Cache header present.
    assert "s-maxage=600" in r.headers["cache-control"]


def test_parcel_intel_map_combines_boroughs_and_caps_anonymous(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows_by_borough = {
        "brooklyn": [
            _row(
                f"30200001{i:02d}",
                acquisition_rank=i + 1,
                citywide_rank=i * 2 + 1,
                owner_name="BROOKLYN OWNER LLC",
                recent_change=True,
                owner_entity_type="llc",
                owner_portfolio_id="brooklyn-owner",
                owner_portfolio_lot_count=8,
                owner_portfolio_borough_count=1,
                owner_portfolio_candidate_count=4,
            )
            for i in range(30)
        ],
        "queens": [
            _row(
                f"40200001{i:02d}",
                acquisition_rank=i + 1,
                citywide_rank=i * 2 + 2,
                owner_name="QUEENS OWNER LLC",
                recent_change=True,
                owner_entity_type="llc",
                owner_portfolio_id="queens-owner",
                owner_portfolio_lot_count=5,
                owner_portfolio_borough_count=2,
                owner_portfolio_candidate_count=3,
            )
            for i in range(30)
        ],
    }
    fake = _make_fake_gcs(
        ["brooklyn", "queens"], rows_by_borough
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/map", params={"top_per_borough": 1000}
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body["rows"]) == 50
    assert {row["borough"] for row in body["rows"]} == {
        "brooklyn",
        "queens",
    }
    assert all(row["owner_name"] is None for row in body["rows"])
    assert all(row["recent_change"] is False for row in body["rows"])
    assert all(row["owner_entity_type"] is None for row in body["rows"])
    assert all(row["owner_portfolio_id"] is None for row in body["rows"])
    assert all(
        row["owner_portfolio_lot_count"] is None for row in body["rows"]
    )
    assert all(row["tax_lien_sale_year"] is None for row in body["rows"])
    assert all(
        row["critical_violation_count"] is None for row in body["rows"]
    )
    assert all(row["floodplain_1pct"] is None for row in body["rows"])
    assert "s-maxage=600" in response.headers["cache-control"]
    assert response.headers["content-encoding"] == "gzip"
    # Compact rows do not serialize expensive detail-only fields.
    assert "parcel_geometry" not in body["rows"][0]
    assert "top_features" not in body["rows"][0]


def test_parcel_intel_map_returns_full_authenticated_inventory(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows = [
        _row(
            f"30200002{i:02d}",
            acquisition_rank=i + 1,
            citywide_rank=i + 1,
            owner_name="ACME REALTY LLC",
            recent_change=True,
            owner_entity_type="llc",
            owner_portfolio_id="acme-portfolio",
            owner_portfolio_lot_count=9,
            owner_portfolio_borough_count=3,
            owner_portfolio_candidate_count=4,
        )
        for i in range(30)
    ]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    _authed()

    response = TestClient(app).get(
        "/v1/parcel-intel/map", params={"top_per_borough": 1000}
    )

    assert response.status_code == 200, response.text
    assert len(response.json()["rows"]) == 30
    assert response.json()["rows"][0]["owner_name"] == "ACME REALTY LLC"
    assert response.json()["rows"][0]["recent_change"] is True
    assert response.json()["rows"][0]["owner_entity_type"] == "llc"
    assert response.json()["rows"][0]["owner_portfolio_id"] == "acme-portfolio"
    assert response.json()["rows"][0]["owner_portfolio_lot_count"] == 9
    assert response.json()["rows"][0]["owner_portfolio_borough_count"] == 3
    assert response.json()["rows"][0]["owner_portfolio_candidate_count"] == 4
    assert response.json()["rows"][0]["tax_lien_sale_year"] == 2025
    assert response.json()["rows"][0]["critical_violation_count"] == 3
    assert response.json()["rows"][0]["floodplain_1pct"] is True
    assert response.headers["cache-control"] == "private, no-store"


def test_parcel_detail_is_tiered_and_keeps_geometry(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-73.9, 40.7], [-73.8, 40.7], [-73.9, 40.7]]],
    }
    rows = [
        _row(
            "3020000001",
            acquisition_rank=1,
            owner_name="ACME REALTY LLC",
            owner_name_source="acris",
            owner_type="P",
            owner_entity_type="llc",
            owner_portfolio_id="acme-portfolio",
            owner_portfolio_match_method="exact_normalized_pluto_owner_name",
            owner_portfolio_lot_count=9,
            owner_portfolio_borough_count=3,
            owner_portfolio_total_lot_area_sqft=72000.0,
            owner_portfolio_candidate_count=4,
            owner_portfolio_data_as_of="2026-07-23",
            parcel_geometry=geometry,
            top_features=[
                {
                    "name": "lot_area",
                    "value": 5000,
                    "contribution_logit": 0.8,
                    "contribution_pct": 0.3,
                }
            ],
        ),
        _row("3020000026", acquisition_rank=26),
    ]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    client = TestClient(app)

    public = client.get("/v1/parcel-intel/parcel/3020000001")
    assert public.status_code == 200, public.text
    assert public.json()["owner_name"] is None
    assert public.json()["owner_name_source"] is None
    assert public.json()["owner_type"] is None
    assert public.json()["owner_entity_type"] is None
    assert public.json()["owner_portfolio_id"] is None
    assert public.json()["owner_portfolio_match_method"] is None
    assert public.json()["owner_portfolio_lot_count"] is None
    assert public.json()["owner_portfolio_borough_count"] is None
    assert public.json()["owner_portfolio_total_lot_area_sqft"] is None
    assert public.json()["owner_portfolio_candidate_count"] is None
    assert public.json()["owner_portfolio_data_as_of"] is None
    assert public.json()["tax_lien_sale_year"] is None
    assert public.json()["tax_lien_sale_date"] is None
    assert public.json()["dob_safety_active_count"] == 0
    assert public.json()["ecb_active_count"] == 0
    assert public.json()["hpd_open_count"] == 0
    assert public.json()["critical_violation_count"] is None
    assert public.json()["violation_data_as_of"] is None
    assert public.json()["firm07_floodplain"] is None
    assert public.json()["pfirm15_floodplain"] is None
    assert public.json()["floodplain_1pct"] is None
    assert public.json()["floodplain_data_as_of"] is None
    assert public.json()["top_features"] == []
    assert public.json()["parcel_geometry"] == geometry
    assert "s-maxage=600" in public.headers["cache-control"]

    hidden = client.get("/v1/parcel-intel/parcel/3020000026")
    assert hidden.status_code == 404

    _authed()
    private = client.get("/v1/parcel-intel/parcel/3020000001")
    assert private.status_code == 200, private.text
    assert private.json()["owner_name"] == "ACME REALTY LLC"
    assert private.json()["owner_name_source"] == "acris"
    assert private.json()["owner_type"] == "P"
    assert private.json()["owner_entity_type"] == "llc"
    assert private.json()["owner_portfolio_id"] == "acme-portfolio"
    assert private.json()["owner_portfolio_match_method"] == (
        "exact_normalized_pluto_owner_name"
    )
    assert private.json()["owner_portfolio_lot_count"] == 9
    assert private.json()["owner_portfolio_borough_count"] == 3
    assert private.json()["owner_portfolio_total_lot_area_sqft"] == 72000.0
    assert private.json()["owner_portfolio_candidate_count"] == 4
    assert private.json()["owner_portfolio_data_as_of"] == "2026-07-23"
    assert private.json()["tax_lien_sale_year"] == 2025
    assert private.json()["tax_lien_sale_date"] == "2025-06-01"
    assert private.json()["dob_safety_active_count"] == 4
    assert private.json()["ecb_class_1_count"] == 2
    assert private.json()["ecb_balance_due"] == -3125.0
    assert private.json()["hpd_class_c_count"] == 1
    assert private.json()["critical_violation_count"] == 3
    assert private.json()["violation_data_as_of"] == "2026-07-23"
    assert private.json()["firm07_floodplain"] is False
    assert private.json()["pfirm15_floodplain"] is True
    assert private.json()["floodplain_1pct"] is True
    assert private.json()["floodplain_data_as_of"] == "2026-07-23"
    assert len(private.json()["top_features"]) == 1
    assert private.headers["cache-control"] == "private, no-store"


def test_parcel_intel_rejects_unknown_borough(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyne"})
    assert r.status_code == 404
    assert "borough" in r.json()["detail"].lower()


def test_parcel_intel_clamps_top_to_1000(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    # FastAPI Query(le=1000) returns 422 for top=1001.
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn", "top": 1001})
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


def test_parcel_intel_sweep_returns_top_features_when_authed(monkeypatch) -> None:
    """When the publisher injects per-row SHAP attributions, the engine must
    surface them through the response without truncation or reshaping.
    SHAP attributions are a premium field, so this requires auth."""
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
    _authed()

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
    """Older publishes (no top_features field at all) must still deserialize.
    Authed request so the empty list reflects the schema default rather
    than anonymous stripping."""
    _set_required_env(monkeypatch)
    row_without = _row("3020000002")
    row_without.pop("top_features")  # simulate v1 sweep
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": [row_without]})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    _authed()

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
    fake = FakeGcs(
        {
            "parcel-intel/v1/manifest.json": json.dumps(
                _manifest(["brooklyn"], generated_at="2026-05-08T00:00:00+00:00")
            ).encode("utf-8"),
            "parcel-intel/v1/brooklyn.jsonl": (json.dumps(rows_v1[0]) + "\n").encode("utf-8"),
        }
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    first = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert first.json()["rows"][0]["address"] == "ROW V1"

    # Simulate a republish: bump generated_at + swap rows.
    fake._store["parcel-intel/v1/manifest.json"] = json.dumps(
        _manifest(["brooklyn"], generated_at="2026-05-09T00:00:00+00:00")
    ).encode("utf-8")
    fake._store["parcel-intel/v1/brooklyn.jsonl"] = (json.dumps(rows_v2[0]) + "\n").encode("utf-8")

    second = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert second.json()["rows"][0]["address"] == "ROW V2"


def test_atomic_manifest_reads_immutable_generation_not_legacy_files(
    monkeypatch,
) -> None:
    _set_required_env(monkeypatch)
    atomic_row = _row("3020000001", address="ATOMIC GENERATION")
    legacy_row = _row("3020000002", address="LEGACY FILE")
    fake = _make_atomic_fake_gcs(
        ["brooklyn"], {"brooklyn": [atomic_row]}
    )
    fake._store["parcel-intel/v1/brooklyn.jsonl"] = (
        json.dumps(legacy_row) + "\n"
    ).encode("utf-8")
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    sweep = client.get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )
    map_response = client.get(
        "/v1/parcel-intel/map", params={"top_per_borough": 1}
    )

    assert sweep.status_code == 200, sweep.text
    assert sweep.json()["rows"][0]["address"] == "ATOMIC GENERATION"
    assert map_response.status_code == 200, map_response.text
    assert map_response.json()["rows"][0]["address"] == "ATOMIC GENERATION"
    assert "parcel-intel/v1/brooklyn.jsonl" not in fake.requests
    assert "parcel-intel/v1/map.jsonl" not in fake.requests


def test_atomic_artifact_checksum_mismatch_fails_closed(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_atomic_fake_gcs(
        ["brooklyn"],
        {"brooklyn": [_row("3020000001", address="ORIGINAL")]},
    )
    manifest = json.loads(fake._store["parcel-intel/v1/manifest.json"])
    object_name = manifest["artifacts"]["brooklyn.jsonl"]["object_name"]
    fake._store[object_name] = (
        json.dumps(_row("3020000002", address="TAMPERED")) + "\n"
    ).encode("utf-8")
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Parcel intelligence artifact integrity check failed"
    )


def test_atomic_manifest_rejects_untrusted_artifact_prefix(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_atomic_fake_gcs(["brooklyn"])
    manifest = json.loads(fake._store["parcel-intel/v1/manifest.json"])
    manifest["artifact_prefix"] = "parcel-intel/v1/generations/../../private"
    fake._store["parcel-intel/v1/manifest.json"] = json.dumps(manifest).encode(
        "utf-8"
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Parcel intelligence manifest is invalid"
    assert all("../" not in object_name for object_name in fake.requests)


def test_index_rejects_partial_atomic_manifest(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    manifest = _manifest(["brooklyn"])
    manifest["artifact_generation"] = (
        "20260723T230308737433Z-aaaaaaaaaaaa"
    )
    fake = FakeGcs(
        {
            "parcel-intel/v1/manifest.json": json.dumps(manifest).encode(
                "utf-8"
            )
        }
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get("/v1/parcel-intel/index")

    assert response.status_code == 503
    assert response.json()["detail"] == "Parcel intelligence manifest is invalid"


def test_atomic_manifest_missing_referenced_borough_fails_closed(
    monkeypatch,
) -> None:
    _set_required_env(monkeypatch)
    fake = _make_atomic_fake_gcs(["brooklyn"])
    manifest = json.loads(fake._store["parcel-intel/v1/manifest.json"])
    object_name = manifest["artifacts"]["brooklyn.jsonl"]["object_name"]
    del fake._store[object_name]
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Parcel intelligence referenced artifact is missing"
    )


def test_atomic_cache_uses_generation_even_when_generated_at_is_unchanged(
    monkeypatch,
) -> None:
    _set_required_env(monkeypatch)
    generated_at = "2026-07-23T23:03:08.737433+00:00"
    fake = _make_atomic_fake_gcs(
        ["brooklyn"],
        {"brooklyn": [_row("3020000001", address="GENERATION ONE")]},
        generation="20260723T230308737433Z-aaaaaaaaaaaa",
        generated_at=generated_at,
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    client = TestClient(app)

    first = client.get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )
    assert first.json()["rows"][0]["address"] == "GENERATION ONE"

    second_store = _make_atomic_fake_gcs(
        ["brooklyn"],
        {"brooklyn": [_row("3020000002", address="GENERATION TWO")]},
        generation="20260723T230308737433Z-bbbbbbbbbbbb",
        generated_at=generated_at,
    )._store
    fake._store.update(second_store)

    second = client.get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn", "top": 1},
    )
    assert second.status_code == 200, second.text
    assert second.json()["rows"][0]["address"] == "GENERATION TWO"


# --- Tiered access: public preview + authenticated full feed ---


def test_anon_sweep_capped_at_25_rows(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows = [_row(f"30200001{i:02d}") for i in range(30)]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn", "top": 1000})
    assert r.status_code == 200, r.text
    # Silently clamped, not an error.
    assert len(r.json()["rows"]) == 25
    # Anonymous responses keep the public edge-cache headers.
    assert "s-maxage=600" in r.headers["cache-control"]


def test_anon_sweep_strips_premium_fields(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    feats = [
        {"name": "lot_area", "value": 5000, "contribution_logit": 0.85, "contribution_pct": 0.31},
    ]
    rows = [
        _row(
            "3020000001",
            score_calibrated_p10=0.62,
            score_calibrated_p90=0.97,
            top_features=feats,
            change_added_count=3,
            change_demolished_count=1,
            change_modified_count=2,
            change_latest_imagery_year=2024,
            observed_imagery_year=2024,
            recent_change=True,
            owner_name="ACME REALTY LLC",
            owner_name_source="acris",
            owner_type="P",
            owner_entity_type="llc",
            owner_portfolio_id="acme-portfolio",
            owner_portfolio_match_method="exact_normalized_pluto_owner_name",
            owner_portfolio_lot_count=9,
            owner_portfolio_borough_count=3,
            owner_portfolio_total_lot_area_sqft=72000.0,
            owner_portfolio_candidate_count=4,
            owner_portfolio_data_as_of="2026-07-23",
            assemblage_id="assembly-1",
            assemblage_lot_count=2,
            assemblage_combined_lot_area_sqft=10000,
            assemblage_combined_buildable_sqft=40000,
            assemblage_member_bbls=["3020000001", "3020000002"],
        )
    ]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    served = r.json()["rows"][0]
    # Non-premium fields still flow through.
    assert served["bbl"] == "3020000001"
    assert served["score_calibrated"] == 0.9
    # Premium fields are stripped/defaulted for anonymous callers.
    assert served["score_calibrated_p10"] is None
    assert served["score_calibrated_p90"] is None
    assert served["top_features"] == []
    assert served["change_added_count"] == 0
    assert served["change_demolished_count"] == 0
    assert served["change_modified_count"] == 0
    assert served["change_latest_imagery_year"] is None
    assert served["observed_imagery_year"] is None
    assert served["recent_change"] is False
    assert served["owner_name"] is None
    assert served["owner_name_source"] is None
    assert served["owner_type"] is None
    assert served["owner_entity_type"] is None
    assert served["owner_portfolio_id"] is None
    assert served["owner_portfolio_match_method"] is None
    assert served["owner_portfolio_lot_count"] is None
    assert served["owner_portfolio_borough_count"] is None
    assert served["owner_portfolio_total_lot_area_sqft"] is None
    assert served["owner_portfolio_candidate_count"] is None
    assert served["owner_portfolio_data_as_of"] is None
    assert served["assemblage_id"] is None
    assert served["assemblage_lot_count"] is None
    assert served["assemblage_combined_lot_area_sqft"] is None
    assert served["assemblage_combined_buildable_sqft"] is None
    assert served["assemblage_member_bbls"] == []


def test_authed_sweep_full_rows_and_no_store_header(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows = [
        _row(
            f"30200002{i:02d}",
            score_calibrated_p10=0.6,
            score_calibrated_p90=0.95,
            owner_name="ACME REALTY LLC",
            owner_entity_type="llc",
            owner_portfolio_id="acme-portfolio",
            owner_portfolio_match_method="exact_normalized_pluto_owner_name",
            owner_portfolio_lot_count=9,
            owner_portfolio_borough_count=3,
            owner_portfolio_total_lot_area_sqft=72000.0,
            owner_portfolio_candidate_count=4,
            owner_portfolio_data_as_of="2026-07-23",
            recent_change=True,
        )
        for i in range(30)
    ]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    _authed()

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn", "top": 1000})
    assert r.status_code == 200, r.text
    body = r.json()
    # Full feed: no anonymous cap.
    assert len(body["rows"]) == 30
    served = body["rows"][0]
    assert served["score_calibrated_p10"] == 0.6
    assert served["score_calibrated_p90"] == 0.95
    assert served["owner_name"] == "ACME REALTY LLC"
    assert served["owner_entity_type"] == "llc"
    assert served["owner_portfolio_id"] == "acme-portfolio"
    assert served["owner_portfolio_lot_count"] == 9
    assert served["owner_portfolio_borough_count"] == 3
    assert served["owner_portfolio_total_lot_area_sqft"] == 72000.0
    assert served["owner_portfolio_candidate_count"] == 4
    assert served["recent_change"] is True
    # Authenticated payloads must never sit in a shared cache.
    assert r.headers["cache-control"] == "private, no-store"


def test_invalid_bearer_on_sweep_is_401_not_anon_downgrade(monkeypatch) -> None:
    """maybe_auth must reject bad credentials rather than silently serving
    the anonymous tier. Mock verifier is enabled by conftest env."""
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"])
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get(
        "/v1/parcel-intel/sweep",
        params={"borough": "brooklyn"},
        headers={"Authorization": "Bearer not-a-valid-token"},
    )
    assert r.status_code == 401


# --- Robust read path (corrupt manifest / bad JSONL / bad rows) ---


def test_corrupt_manifest_returns_503(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = FakeGcs(
        {
            "parcel-intel/v1/manifest.json": b"{this is not JSON",
        }
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/index")
    assert r.status_code == 503
    assert r.json()["detail"] == "Parcel intelligence manifest is invalid"

    # The sweep goes through the same manifest refresh.
    r2 = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r2.status_code == 503


def test_bad_jsonl_line_is_skipped_rest_served(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    good1 = _row("3020000001")
    good2 = _row("3020000002")
    payload = json.dumps(good1) + "\n" + "{corrupt line!!\n" + json.dumps(good2) + "\n"
    fake = FakeGcs(
        {
            "parcel-intel/v1/manifest.json": json.dumps(_manifest(["brooklyn"])).encode("utf-8"),
            "parcel-intel/v1/brooklyn.jsonl": payload.encode("utf-8"),
        }
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    bbls = [row["bbl"] for row in r.json()["rows"]]
    assert bbls == ["3020000001", "3020000002"]


def test_invalid_row_is_skipped_rest_served(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    good = _row("3020000001")
    bad = _row("3020000002", score_calibrated="not-a-number")
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": [good, bad]})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    bbls = [row["bbl"] for row in r.json()["rows"]]
    assert bbls == ["3020000001"]


# --- Freshness (index age_days / stale) ---


def test_index_flags_stale_data(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"], generated_at="2026-01-01T00:00:00+00:00")
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/index")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["stale"] is True
    assert body["age_days"] is not None
    assert body["age_days"] > 45


def test_index_fresh_data_not_stale(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    fake = _make_fake_gcs(["brooklyn"], generated_at=_fresh_iso())
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/index")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["stale"] is False
    assert body["age_days"] is not None
    assert body["age_days"] < 1.0


# --- Change-signal + owner schema fields (deploy-order prerequisite) ---


def test_change_and_owner_fields_round_trip_when_authed(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    rows = [
        _row(
            "3020000001",
            change_added_count=4,
            change_demolished_count=1,
            change_modified_count=7,
            change_latest_imagery_year=2024,
            recent_change=True,
            owner_name="100 E 21 ST OWNER LLC",
        )
    ]
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": rows})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    _authed()

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    served = r.json()["rows"][0]
    assert served["change_added_count"] == 4
    assert served["change_demolished_count"] == 1
    assert served["change_modified_count"] == 7
    assert served["change_latest_imagery_year"] == 2024
    assert served["recent_change"] is True
    assert served["owner_name"] == "100 E 21 ST OWNER LLC"


def test_change_and_owner_fields_default_when_absent(monkeypatch) -> None:
    """Old JSONL publishes (no change/owner fields) must still validate and
    serve schema defaults."""
    _set_required_env(monkeypatch)
    old_row = _row("3020000003")  # _row never sets the new fields
    for key in (
        "change_added_count",
        "change_demolished_count",
        "change_modified_count",
        "change_latest_imagery_year",
        "recent_change",
        "owner_name",
    ):
        assert key not in old_row
    fake = _make_fake_gcs(["brooklyn"], {"brooklyn": [old_row]})
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    _authed()

    client = TestClient(app)
    r = client.get("/v1/parcel-intel/sweep", params={"borough": "brooklyn"})
    assert r.status_code == 200, r.text
    served = r.json()["rows"][0]
    assert served["change_added_count"] == 0
    assert served["change_demolished_count"] == 0
    assert served["change_modified_count"] == 0
    assert served["change_latest_imagery_year"] is None
    assert served["recent_change"] is False
    assert served["owner_name"] is None

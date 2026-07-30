from __future__ import annotations

import gzip
import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_intel as parcel_intel_routes
from app.services.parcel_official_dossier import (
    ACRIS_DATASET_IDS,
    DOSSIER_PREFIX,
    DOSSIER_SCHEMA,
    PUBLICATION_SCHEMA,
    ROW_FIELDS,
    ParcelOfficialDossierStore,
)


class FakeGcs:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.store = store
        self.requests: list[str] = []

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        self.requests.append(object_name)
        if object_name not in self.store:
            raise FileNotFoundError(object_name)
        return (
            self.store[object_name],
            (
                "application/json"
                if object_name.endswith("manifest.json")
                else "application/gzip"
            ),
        )


def _row(
    bbl: str = "3058920038",
    *,
    pluto_owner: str | None = "GEFFEN MANAGEMENT LLC",
    acris_owner: str | None = "GEFFEN MANAGEMENT LLC",
) -> dict:
    return {
        "b": bbl,
        "a": "464 OVINGTON AVENUE",
        "po": pluto_owner,
        "ao": acris_owner,
        "sd": "2022-06-15T00:00:00",
        "sp": 1_460_000.0,
        "yh": 4,
        "la": 9260.0,
        "ba": 3006.0,
        "u": 2,
        "nf": 2.0,
        "yb": 1899,
        "lu": "1",
        "bc": "B3",
        "z1": "R6A",
        "z2": None,
        "bf": 0.32,
        "rf": 3.0,
        "cf": 0.0,
        "ff": 3.0,
        "al": 32_400.0,
        "ab": None,
        "at": 39_420.0,
        "f07": False,
        "f15": False,
        "er": True,
        "ek": "e_designation",
        "en": "E-839",
    }


def _store(rows: list[dict]) -> dict[str, bytes]:
    generation = "20260727T005131244552Z-9624c5a2e365"
    artifact_prefix = f"{DOSSIER_PREFIX}/generations/{generation}"
    by_shard: dict[str, list[dict]] = {}
    for row in rows:
        bbl = row["b"]
        shard = hashlib.sha256(bbl.encode("ascii")).hexdigest()[:2]
        by_shard.setdefault(shard, []).append(row)

    artifacts: dict[str, dict] = {}
    store: dict[str, bytes] = {}
    fallback = {
        "sha256": "a" * 64,
        "size_bytes": 1,
        "uncompressed_sha256": "b" * 64,
        "uncompressed_size_bytes": 1,
        "row_count": 1,
        "acris_covered_row_count": 0,
    }
    for value in range(256):
        shard = f"{value:02x}"
        artifacts[shard] = {
            **fallback,
            "object_name": (
                f"{artifact_prefix}/shards/{shard}.jsonl.gz"
            ),
        }
    for shard, shard_rows in by_shard.items():
        body = (
            "".join(
                json.dumps(row, separators=(",", ":"), sort_keys=True)
                + "\n"
                for row in sorted(shard_rows, key=lambda item: item["b"])
            )
        ).encode()
        compressed = gzip.compress(body, mtime=0)
        object_name = f"{artifact_prefix}/shards/{shard}.jsonl.gz"
        store[object_name] = compressed
        artifacts[shard] = {
            "object_name": object_name,
            "sha256": hashlib.sha256(compressed).hexdigest(),
            "size_bytes": len(compressed),
            "uncompressed_sha256": hashlib.sha256(body).hexdigest(),
            "uncompressed_size_bytes": len(body),
            "row_count": len(shard_rows),
            "acris_covered_row_count": len(shard_rows),
        }

    manifest = {
        "schema": DOSSIER_SCHEMA,
        "publication_schema": PUBLICATION_SCHEMA,
        "generated_at": "2026-07-27T00:51:31+00:00",
        "artifact_generation": generation,
        "artifact_prefix": artifact_prefix,
        "shard_key": "sha256_bbl",
        "shard_prefix_length": 2,
        "compression": "gzip",
        "row_fields": ROW_FIELDS,
        "sources": {
            "pluto": {
                "dataset_id": "64uk-42ks",
                "retrieved_at": "2026-07-26T01:46:34+00:00",
            },
            "acris": {
                "dataset_ids": ACRIS_DATASET_IDS,
                "feature_source_updated_at": "2026-07-15T02:23:16+00:00",
            },
        },
        "stats": {
            "rendered_rows": 858_602,
            "rejected_pluto_rows": 0,
            "rejected_acris_rows": 0,
            "shard_count": 256,
        },
        "privacy": {
            "access_contract": "private_authenticated_single_parcel_only",
            "contact_fields_published": False,
            "beneficial_owner_inference_published": False,
            "candidate_membership_published": False,
            "model_fields_published": False,
            "workflow_fields_published": False,
            "permitted_row_fields": ROW_FIELDS,
        },
        "quality_gate": {"passed": True, "failures": []},
        "artifacts": artifacts,
    }
    store[f"{DOSSIER_PREFIX}/manifest.json"] = json.dumps(manifest).encode()
    return store


def _other_bbl_in_same_shard(bbl: str) -> str:
    target = hashlib.sha256(bbl.encode("ascii")).hexdigest()[:2]
    for value in range(1, 1_000_000):
        candidate = f"3{value:09d}"
        if (
            candidate != bbl
            and hashlib.sha256(candidate.encode("ascii")).hexdigest()[:2]
            == target
        ):
            return candidate
    raise AssertionError("failed to find test BBL in target shard")


def _authenticate(auth_override) -> None:
    context = auth_override()
    app.dependency_overrides[
        parcel_intel_routes.require_parcel_read_auth
    ] = lambda: context


@pytest.fixture(autouse=True)
def _reset_store() -> None:
    parcel_intel_routes._OFFICIAL_DOSSIERS = ParcelOfficialDossierStore()
    yield
    parcel_intel_routes._OFFICIAL_DOSSIERS = ParcelOfficialDossierStore()
    app.dependency_overrides = {}


def test_official_dossier_returns_source_specific_facts_privately(
    auth_override,
) -> None:
    _authenticate(auth_override)
    fake = FakeGcs(_store([_row()]))
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/official-parcel/3058920038"
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    vary = {
        value.strip().lower()
        for value in response.headers["vary"].split(",")
        if value.strip()
    }
    assert {
        "authorization",
        "x-api-key",
        "x-citylens-parcel-smoke-key",
    } <= vary
    payload = response.json()
    assert payload["schema_version"] == "citylens/parcel-official-dossier@v1"
    assert payload["bbl"] == "3058920038"
    assert payload["borough"] == "brooklyn"
    assert payload["address"] == "464 OVINGTON AVENUE"
    assert payload["owner_source_status"] == "match"
    assert payload["last_sale_price"] == 1_460_000
    assert payload["zoning_district_1"] == "R6A"
    assert payload["environmental_review_required"] is True
    assert payload["property_facts_dataset_id"] == "64uk-42ks"
    assert payload["ownership_dataset_ids"] == ACRIS_DATASET_IDS
    assert payload["official_links"]["zola"].endswith("/3/5892/38")
    rendered = json.dumps(payload).lower()
    for forbidden in ("score", "rank", "phone", "email", "seller intent"):
        assert forbidden not in rendered


def test_sales_comparables_route_uses_private_authenticated_dossier(
    auth_override,
) -> None:
    _authenticate(auth_override)
    fake = FakeGcs(_store([_row()]))
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    class FakeComparables:
        calls: list[tuple[str, dict]] = []

        def get(self, bbl: str, *, dossier_row: dict) -> dict:
            self.calls.append((bbl, dossier_row))
            return {
                "schema_version": (
                    "citylens/parcel-sales-comparables@v1"
                ),
                "status": "available",
                "subject_bbl": bbl,
                "search_zip_code": "11209",
                "query_window_start": "2023-01-01",
                "source_candidate_count": 12,
                "eligible_candidate_count": 4,
                "source_limit_reached": False,
                "comparables": [
                    {
                        "bbl": "3058900040",
                        "address": "450 OVINGTON AVENUE",
                        "sale_date": "2025-10-15",
                        "sale_price": 1_500_000,
                        "distance_miles": 0.2,
                        "lot_area_sqft": 9_000,
                        "gross_area_sqft": 3_200,
                        "residential_units": 2,
                        "commercial_units": 0,
                        "total_units": 2,
                        "year_built": 1910,
                        "building_class": "B2",
                        "building_class_category": (
                            "01 ONE FAMILY DWELLINGS"
                        ),
                        "price_per_land_sqft": 166.67,
                        "price_per_gross_sqft": 468.75,
                        "match_reasons": [
                            "Same building-class family",
                            "Lot area within 15%",
                            "Within 0.2 miles",
                        ],
                    }
                ],
                "summary": {
                    "comparable_count": 1,
                    "median_sale_price": 1_500_000,
                    "median_price_per_land_sqft": 166.67,
                    "median_price_per_gross_sqft": 468.75,
                    "minimum_sale_price": 1_500_000,
                    "maximum_sale_price": 1_500_000,
                },
                "source_name": (
                    "NYC Department of Finance annualized property sales"
                ),
                "source_dataset_id": "w2pb-icbu",
                "source_url": (
                    "https://data.cityofnewyork.us/"
                    "City-Government/NYC-Citywide-Annualized-"
                    "Calendar-Sales-Update/w2pb-icbu"
                ),
                "source_data_updated_at": "2026-06-09T14:00:00Z",
                "source_retrieved_at": "2026-07-30T12:00:00Z",
                "selection_method": "Transparent bounded screen.",
                "interpretation": "Not an appraisal.",
            }

    service = FakeComparables()
    app.dependency_overrides[
        parcel_intel_routes.get_sales_comparables
    ] = lambda: service

    response = TestClient(app).get(
        "/v1/parcel-intel/official-parcel/"
        "3058920038/sales-comparables"
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    payload = response.json()
    assert payload["status"] == "available"
    assert payload["summary"]["comparable_count"] == 1
    assert payload["comparables"][0]["bbl"] == "3058900040"
    assert service.calls[0][0] == "3058920038"
    assert service.calls[0][1]["a"] == "464 OVINGTON AVENUE"


def test_owner_disagreement_is_preserved_not_reconciled(
    auth_override,
) -> None:
    _authenticate(auth_override)
    fake = FakeGcs(
        _store(
            [
                _row(
                    pluto_owner="PLUTO OWNER LLC",
                    acris_owner="ACRIS OWNER LLC",
                )
            ]
        )
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    payload = TestClient(app).get(
        "/v1/parcel-intel/official-parcel/3058920038"
    ).json()

    assert payload["owner_source_status"] == "different"
    assert payload["pluto_owner_name"] == "PLUTO OWNER LLC"
    assert payload["acris_owner_name"] == "ACRIS OWNER LLC"


def test_official_dossier_requires_auth_and_marks_early_error_private() -> None:
    response = TestClient(app).get(
        "/v1/parcel-intel/official-parcel/3058920038"
    )

    assert response.status_code == 401
    assert response.headers["cache-control"] == "private, no-store"


def test_missing_tax_lot_is_explicit(auth_override) -> None:
    _authenticate(auth_override)
    missing_bbl = "3058920039"
    fake = FakeGcs(
        _store([_row(), _row(_other_bbl_in_same_shard(missing_bbl))])
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        f"/v1/parcel-intel/official-parcel/{missing_bbl}"
    )

    assert response.status_code == 404
    assert "current official PLUTO snapshot" in response.json()["detail"]
    assert response.headers["cache-control"] == "private, no-store"


def test_dossier_integrity_failure_fails_closed(auth_override) -> None:
    _authenticate(auth_override)
    store = _store([_row()])
    generation = "20260727T005131244552Z-9624c5a2e365"
    shard = hashlib.sha256(b"3058920038").hexdigest()[:2]
    object_name = (
        f"{DOSSIER_PREFIX}/generations/{generation}/shards/"
        f"{shard}.jsonl.gz"
    )
    store[object_name] += b"tampered"
    fake = FakeGcs(store)
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).get(
        "/v1/parcel-intel/official-parcel/3058920038"
    )

    assert response.status_code == 503
    assert "integrity check" in response.json()["detail"]


def test_read_only_parcel_smoke_key_can_verify_dossier_but_not_workflow(
    monkeypatch,
) -> None:
    smoke_key = "production-smoke-test-key"
    monkeypatch.setenv(
        "CITYLENS_PARCEL_SMOKE_API_KEY_HASHES",
        hashlib.sha256(smoke_key.encode()).hexdigest(),
    )
    fake = FakeGcs(_store([_row()]))
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    headers = {"X-CityLens-Parcel-Smoke-Key": smoke_key}
    client = TestClient(app)

    dossier = client.get(
        "/v1/parcel-intel/official-parcel/3058920038",
        headers=headers,
    )
    workflow = client.get(
        "/v1/parcel-intel/workflow",
        headers=headers,
    )

    assert dossier.status_code == 200
    assert dossier.json()["bbl"] == "3058920038"
    assert workflow.status_code == 401

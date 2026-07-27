from __future__ import annotations

import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_intel as parcel_intel_routes
from app.services.parcel_address_resolver import (
    NORMALIZATION_SCHEMA,
    PUBLICATION_SCHEMA,
    RESOLVER_PREFIX,
    RESOLVER_SCHEMA,
    ParcelAddressResolver,
    normalize_resolver_address,
)


class FakeGcs:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.store = store
        self.requests: list[str] = []

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        self.requests.append(object_name)
        if object_name not in self.store:
            raise FileNotFoundError(object_name)
        content_type = (
            "application/json"
            if object_name.endswith("manifest.json")
            else "application/x-ndjson"
        )
        return self.store[object_name], content_type


def _resolver_store(
    address_bbls: dict[str, list[str]],
) -> dict[str, bytes]:
    generation = "20260727T000234316462Z-1824ab6b25f2"
    artifact_prefix = f"{RESOLVER_PREFIX}/generations/{generation}"
    by_shard: dict[str, list[dict]] = {}
    for address, bbls in address_bbls.items():
        normalized = normalize_resolver_address(address).value
        address_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        for bbl in bbls:
            by_shard.setdefault(address_hash[:2], []).append(
                {"h": address_hash, "b": bbl, "s": "pad"}
            )

    store: dict[str, bytes] = {}
    artifacts: dict[str, dict] = {}
    for shard, rows in sorted(by_shard.items()):
        rows.sort(key=lambda row: (row["h"], row["b"]))
        body = (
            "".join(
                json.dumps(row, separators=(",", ":"), sort_keys=True)
                + "\n"
                for row in rows
            )
        ).encode("utf-8")
        object_name = f"{artifact_prefix}/shards/{shard}.jsonl"
        store[object_name] = body
        artifacts[shard] = {
            "object_name": object_name,
            "sha256": hashlib.sha256(body).hexdigest(),
            "size_bytes": len(body),
            "row_count": len(rows),
            "address_hash_count": len({row["h"] for row in rows}),
        }

    manifest = {
        "schema": RESOLVER_SCHEMA,
        "publication_schema": PUBLICATION_SCHEMA,
        "normalization_schema": NORMALIZATION_SCHEMA,
        "generated_at": "2026-07-27T00:02:34.316462+00:00",
        "artifact_generation": generation,
        "artifact_prefix": artifact_prefix,
        "shard_prefix_length": 2,
        "hash_algorithm": "sha256",
        "row_fields": ["h", "b", "s"],
        "source": {
            "name": "NYC Property Address Directory with PLUTO fallback",
            "official_sources": [
                {
                    "dataset_key": "pad",
                    "dataset_id": "bc8t-ecyu",
                    "retrieved_at": "2026-07-26T23:37:33+00:00",
                }
            ],
        },
        "privacy": {
            "plaintext_addresses_published": False,
            "candidate_membership_published": False,
            "permitted_row_fields": ["h", "b", "s"],
        },
        "quality_gate": {"passed": True, "failures": []},
        "artifacts": artifacts,
    }
    store[f"{RESOLVER_PREFIX}/manifest.json"] = json.dumps(manifest).encode()
    return store


@pytest.fixture(autouse=True)
def _reset_resolver() -> None:
    parcel_intel_routes._ADDRESS_RESOLVER = ParcelAddressResolver()
    yield
    parcel_intel_routes._ADDRESS_RESOLVER = ParcelAddressResolver()
    app.dependency_overrides = {}


@pytest.mark.parametrize(
    ("raw", "expected", "unit_removed", "locality_removed"),
    [
        (
            "464 Ovington Ave., Brooklyn, NY 11209",
            "464 OVINGTON AVENUE",
            False,
            True,
        ),
        (
            "464 Ovington Avenue Apt 2A, Brooklyn, NY 11209",
            "464 OVINGTON AVENUE",
            True,
            True,
        ),
        (
            "464 Ovington Avenue Apt. #2A, Brooklyn, NY 11209",
            "464 OVINGTON AVENUE",
            True,
            True,
        ),
        (
            "464 Ovington Ave Brooklyn NY 11209",
            "464 OVINGTON AVENUE",
            False,
            True,
        ),
        (
            "1 New York Avenue",
            "1 NEW YORK AVENUE",
            False,
            False,
        ),
        (
            "12-14 31st Ave, Queens",
            "12-14 31 AVENUE",
            False,
            True,
        ),
    ],
)
def test_normalization_matches_publisher_contract(
    raw: str,
    expected: str,
    unit_removed: bool,
    locality_removed: bool,
) -> None:
    result = normalize_resolver_address(raw)
    assert result.value == expected
    assert result.unit_removed is unit_removed
    assert result.locality_removed is locality_removed


def test_unique_address_resolution_is_private_and_source_bound(
    auth_override,
) -> None:
    auth_override()
    fake = FakeGcs(
        _resolver_store(
            {"464 OVINGTON AVENUE": ["3058920038"]}
        )
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake
    client = TestClient(app)

    response = client.post(
        "/v1/parcel-intel/resolve-address",
        json={
            "schema_version": (
                "citylens/parcel-address-resolve-request@v1"
            ),
            "address": "464 Ovington Ave., Brooklyn, NY 11209",
        },
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert "Authorization" in response.headers["vary"]
    payload = response.json()
    assert payload["match_status"] == "unique"
    assert payload["candidate_count"] == 1
    assert payload["truncated"] is False
    assert payload["candidates"] == [
        {"bbl": "3058920038", "borough": "brooklyn"}
    ]
    assert payload["source_dataset_id"] == "bc8t-ecyu"
    assert payload["locality_ignored"] is True
    rendered = json.dumps(payload).upper()
    assert "OVINGTON" not in rendered
    assert "464 " not in rendered


def test_ambiguous_resolution_returns_every_bounded_bbl_without_guessing(
    auth_override,
) -> None:
    auth_override()
    fake = FakeGcs(
        _resolver_store(
            {
                "10 TEST STREET": [
                    "1000010001",
                    "1000010002",
                    "1000010003",
                ]
            }
        )
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).post(
        "/v1/parcel-intel/resolve-address",
        json={"address": "10 Test St, Manhattan, NY"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["match_status"] == "ambiguous"
    assert payload["candidate_count"] == 3
    assert payload["truncated"] is False
    assert [row["bbl"] for row in payload["candidates"]] == [
        "1000010001",
        "1000010002",
        "1000010003",
    ]
    assert "did not choose one automatically" in payload["interpretation"]


def test_not_found_is_explicit_and_never_fuzzy_matches(
    auth_override,
) -> None:
    auth_override()
    fake = FakeGcs(
        _resolver_store({"10 TEST STREET": ["1000010001"]})
    )
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).post(
        "/v1/parcel-intel/resolve-address",
        json={"address": "11 Test Street, Manhattan, NY"},
    )

    assert response.status_code == 200
    assert response.json()["match_status"] == "not_found"
    assert response.json()["candidate_count"] == 0
    assert response.json()["candidates"] == []
    assert "did not substitute a similar address" in (
        response.json()["interpretation"]
    )


def test_resolver_requires_auth_and_marks_early_errors_private() -> None:
    response = TestClient(app).post(
        "/v1/parcel-intel/resolve-address",
        json={"address": "10 Test Street, Manhattan, NY"},
    )

    assert response.status_code == 401
    assert response.headers["cache-control"] == "private, no-store"


def test_invalid_address_does_not_load_private_resolver(
    auth_override,
) -> None:
    auth_override()
    fake = FakeGcs(_resolver_store({}))
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).post(
        "/v1/parcel-intel/resolve-address",
        json={"address": "Brooklyn, NY"},
    )

    assert response.status_code == 422
    assert fake.requests == []


def test_integrity_mismatch_fails_closed(auth_override) -> None:
    auth_override()
    store = _resolver_store({"10 TEST STREET": ["1000010001"]})
    manifest = json.loads(
        store[f"{RESOLVER_PREFIX}/manifest.json"]
    )
    shard = next(iter(manifest["artifacts"]))
    store[manifest["artifacts"][shard]["object_name"]] += b"tamper"
    fake = FakeGcs(store)
    app.dependency_overrides[parcel_intel_routes.get_gcs] = lambda: fake

    response = TestClient(app).post(
        "/v1/parcel-intel/resolve-address",
        json={"address": "10 Test Street, Manhattan, NY"},
    )

    assert response.status_code == 503
    assert "integrity" in response.json()["detail"].lower()

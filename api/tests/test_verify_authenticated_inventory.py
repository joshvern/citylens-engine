from __future__ import annotations

import json
from copy import deepcopy

from scripts import verify_authenticated_inventory as verifier
from scripts.verify_authenticated_inventory import (
    validate_authenticated_detail,
    validate_authenticated_map,
)

BOROUGHS = (
    "manhattan",
    "brooklyn",
    "queens",
    "bronx",
    "staten_island",
)


def _map_payload() -> dict:
    return {
        "access_scope": "authenticated_full",
        "requested_top_per_borough": 1,
        "returned_count": 5,
        "available_count": 5,
        "inventory_complete": True,
        "generated_at": "2026-07-26T00:00:00Z",
        "rows": [
            {
                "bbl": f"{index}000000001",
                "borough": borough,
                "owner_name": f"Owner {index}",
                "lat": 40.55 + index * 0.05,
                "lng": -74.2 + index * 0.08,
            }
            for index, borough in enumerate(BOROUGHS, start=1)
        ],
    }


def _map_headers() -> dict[str, str]:
    return {
        "cache-control": "private, no-store",
        "content-encoding": "gzip",
        "vary": ("Authorization, X-API-Key, X-CityLens-Parcel-Smoke-Key, Accept-Encoding"),
        "x-citylens-inventory-scope": "authenticated_full",
        "x-citylens-inventory-count": "5",
        "x-citylens-inventory-available": "5",
    }


def test_authenticated_map_validator_accepts_complete_private_inventory() -> None:
    assert (
        validate_authenticated_map(
            _map_payload(),
            _map_headers(),
            expected_total=5,
            expected_per_borough=1,
        )
        == []
    )


def test_authenticated_map_validator_rejects_downgraded_or_ambiguous_data() -> None:
    payload = deepcopy(_map_payload())
    payload["access_scope"] = "public_preview"
    payload["inventory_complete"] = False
    payload["rows"][1]["bbl"] = payload["rows"][0]["bbl"]
    payload["rows"][2]["lat"] = None
    headers = deepcopy(_map_headers())
    headers["cache-control"] = "public, max-age=600"
    headers["vary"] = "Accept-Encoding"

    failures = validate_authenticated_map(
        payload,
        headers,
        expected_total=5,
        expected_per_borough=1,
    )

    assert any("access scope" in failure for failure in failures)
    assert any("not marked complete" in failure for failure in failures)
    assert any("BBLs are not unique" in failure for failure in failures)
    assert any("plausible NYC coordinates" in failure for failure in failures)
    assert any("not private, no-store" in failure for failure in failures)
    assert any("every supported credential" in failure for failure in failures)


def test_authenticated_detail_validator_accepts_private_owner_context() -> None:
    payload = {
        "bbl": "1000000001",
        "owner_name": "Owner 1",
        "decision_audit": {"readiness": {"status": "ready"}},
    }
    headers = {"cache-control": "private, no-store"}

    assert (
        validate_authenticated_detail(
            payload,
            headers,
            expected_bbl="1000000001",
            expected_owner="Owner 1",
        )
        == []
    )


def test_authenticated_detail_validator_rejects_preview_or_public_cache() -> None:
    payload = {
        "bbl": "1000000002",
        "owner_name": "Different owner",
        "decision_audit": {"readiness": {"status": "limited_preview"}},
    }
    failures = validate_authenticated_detail(
        payload,
        {"cache-control": "public, max-age=600"},
        expected_bbl="1000000001",
        expected_owner="Owner 1",
    )

    assert any("response BBL" in failure for failure in failures)
    assert any("owner context" in failure for failure in failures)
    assert any("preview-limited" in failure for failure in failures)
    assert any("not private, no-store" in failure for failure in failures)


def test_report_omits_smoke_key_owner_and_selected_parcel(
    monkeypatch,
) -> None:
    map_payload = _map_payload()
    map_headers = _map_headers()
    detail_payload = {
        "bbl": "1000000001",
        "owner_name": "Owner 1",
        "decision_audit": {"readiness": {"status": "ready"}},
    }
    detail_headers = {"cache-control": "private, no-store"}

    def fake_request(url: str, *, smoke_key: str, timeout: float):
        assert smoke_key == "do-not-report-this"
        assert timeout == 1.0
        if url.endswith("top_per_borough=1"):
            return 200, map_headers, map_payload, 0.2
        return 200, detail_headers, detail_payload, 0.1

    monkeypatch.setattr(verifier, "_request_json", fake_request)

    report = verifier.run_checks(
        api_base="https://api.citylens.dev",
        smoke_key="do-not-report-this",
        timeout=1.0,
        expected_total=5,
        expected_per_borough=1,
    )
    rendered = json.dumps(report)

    assert report["passed"] is True
    assert report["inventory"]["returned_count"] == 5
    assert "do-not-report-this" not in rendered
    assert "Owner 1" not in rendered
    assert "1000000001" not in rendered

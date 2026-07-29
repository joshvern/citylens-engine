#!/usr/bin/env python3
"""Verify the real authenticated Parcel Intelligence production contract.

The credential is read only from ``CITYLENS_PARCEL_SMOKE_KEY`` and sent in the
least-privilege ``X-CityLens-Parcel-Smoke-Key`` header. It is never accepted as
a command-line argument, written to the report, or printed.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

BOROUGHS = (
    "manhattan",
    "brooklyn",
    "queens",
    "bronx",
    "staten_island",
)
EXPECTED_TOTAL = 5_000
REQUESTED_TOP_PER_BOROUGH = 1_000
SMOKE_HEADER = "X-CityLens-Parcel-Smoke-Key"
OFFICIAL_DOSSIER_SMOKE_BBL = "3058920038"
_DOSSIER_GENERATION_RE = re.compile(
    r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
)


def _expect(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def _is_mappable_nyc_row(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    lat = value.get("lat")
    lng = value.get("lng")
    return (
        isinstance(lat, (int, float))
        and not isinstance(lat, bool)
        and isinstance(lng, (int, float))
        and not isinstance(lng, bool)
        and 40.45 <= float(lat) <= 41.0
        and -74.30 <= float(lng) <= -73.65
    )


def expected_borough_counts_from_index(
    payload: dict[str, Any],
    *,
    expected_total: int = EXPECTED_TOTAL,
) -> tuple[dict[str, int], list[str]]:
    """Read the authenticated inventory distribution from its publication receipt."""
    failures: list[str] = []
    quality_gate = payload.get("quality_gate")
    _expect(
        isinstance(quality_gate, dict),
        "index: quality gate is missing",
        failures,
    )
    quality_gate = quality_gate if isinstance(quality_gate, dict) else {}
    selection_policy = quality_gate.get("selection_policy")
    _expect(
        isinstance(selection_policy, dict),
        "index: selection policy is missing",
        failures,
    )
    selection_policy = (
        selection_policy if isinstance(selection_policy, dict) else {}
    )
    _expect(
        selection_policy.get("schema")
        == "citylens-parcel-intel/selection-policy@v1",
        "index: selection policy schema is invalid",
        failures,
    )
    _expect(
        selection_policy.get("passed") is True
        and selection_policy.get("failures") == [],
        "index: selection policy did not pass",
        failures,
    )
    _expect(
        selection_policy.get("target_count") == expected_total
        and selection_policy.get("selected_count") == expected_total,
        (
            "index: selection policy does not declare the expected "
            f"{expected_total:,}-row inventory"
        ),
        failures,
    )

    by_borough = selection_policy.get("by_borough")
    _expect(
        isinstance(by_borough, dict)
        and set(by_borough) == set(BOROUGHS),
        "index: selection policy does not cover exactly five NYC boroughs",
        failures,
    )
    by_borough = by_borough if isinstance(by_borough, dict) else {}
    expected_counts: dict[str, int] = {}
    for borough in BOROUGHS:
        receipt = by_borough.get(borough)
        receipt = receipt if isinstance(receipt, dict) else {}
        selected_count = receipt.get("selected_count")
        valid_count = (
            isinstance(selected_count, int)
            and not isinstance(selected_count, bool)
            and selected_count > 0
        )
        _expect(
            valid_count
            and receipt.get("requested_minimum_satisfied") is True,
            f"index: {borough} selection receipt is invalid",
            failures,
        )
        if valid_count:
            expected_counts[borough] = selected_count

    _expect(
        len(expected_counts) == len(BOROUGHS)
        and sum(expected_counts.values()) == expected_total,
        (
            "index: selected borough counts do not sum to the expected "
            f"{expected_total:,}-row inventory"
        ),
        failures,
    )

    boroughs = payload.get("boroughs")
    boroughs = boroughs if isinstance(boroughs, list) else []
    index_counts = {
        row.get("slug"): row.get("count")
        for row in boroughs
        if isinstance(row, dict) and row.get("slug") in BOROUGHS
    }
    _expect(
        index_counts == expected_counts,
        "index: borough summaries do not match the selection policy receipt",
        failures,
    )
    return expected_counts, failures


def _request_json(
    url: str,
    *,
    smoke_key: str,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any], float]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            SMOKE_HEADER: smoke_key,
            "User-Agent": "citylens-authenticated-production-smoke/1",
        },
    )
    started = time.monotonic()
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            headers = {key.lower(): value for key, value in response.headers.items()}
            body = response.read()
    except HTTPError as exc:
        status = exc.code
        headers = {key.lower(): value for key, value in exc.headers.items()}
        body = exc.read()
    except URLError as exc:
        raise RuntimeError(f"Network request failed: {exc.reason}") from exc
    elapsed = round(time.monotonic() - started, 3)
    if headers.get("content-encoding", "").lower() == "gzip":
        body = gzip.decompress(body)
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Production API returned non-JSON (HTTP {status})") from exc
    if not isinstance(payload, dict):
        raise TypeError(f"Production API returned non-object JSON (HTTP {status})")
    return status, headers, payload, elapsed


def validate_authenticated_map(
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    expected_total: int = EXPECTED_TOTAL,
    requested_top_per_borough: int = REQUESTED_TOP_PER_BOROUGH,
    expected_borough_counts: dict[str, int] | None = None,
) -> list[str]:
    failures: list[str] = []
    rows = payload.get("rows")
    _expect(isinstance(rows, list), "map: rows is not a list", failures)
    rows = rows if isinstance(rows, list) else []

    _expect(
        payload.get("access_scope") == "authenticated_full",
        "map: access scope is not authenticated_full",
        failures,
    )
    _expect(
        payload.get("requested_top_per_borough") == requested_top_per_borough,
        "map: request receipt does not match the full borough limit",
        failures,
    )
    _expect(
        payload.get("returned_count") == expected_total,
        f"map: receipt does not report {expected_total:,} returned rows",
        failures,
    )
    _expect(
        payload.get("available_count") == expected_total,
        f"map: receipt does not report {expected_total:,} available rows",
        failures,
    )
    _expect(
        payload.get("inventory_complete") is True,
        "map: authenticated inventory is not marked complete",
        failures,
    )
    _expect(
        len(rows) == expected_total,
        f"map: expected {expected_total:,} rows, got {len(rows):,}",
        failures,
    )

    bbls = [
        row.get("bbl")
        for row in rows
        if isinstance(row, dict) and isinstance(row.get("bbl"), str)
    ]
    _expect(
        len(bbls) == expected_total,
        "map: one or more rows lack a string BBL",
        failures,
    )
    _expect(
        len(set(bbls)) == expected_total,
        "map: BBLs are not unique",
        failures,
    )
    borough_counts = Counter(
        row.get("borough") for row in rows if isinstance(row, dict)
    )
    _expect(
        set(borough_counts) == set(BOROUGHS),
        "map: rows do not cover exactly five NYC boroughs",
        failures,
    )
    if expected_borough_counts is None:
        failures.append(
            "map: published borough distribution was not available to verify"
        )
    else:
        for borough in BOROUGHS:
            expected_count = expected_borough_counts.get(borough)
            _expect(
                borough_counts[borough] == expected_count,
                (
                    f"map: {borough} has {borough_counts[borough]:,} rows, "
                    f"published selection has {expected_count!r}"
                ),
                failures,
            )
    mappable_rows = [row for row in rows if _is_mappable_nyc_row(row)]
    _expect(
        len(mappable_rows) == expected_total,
        (
            f"map: expected {expected_total:,} rows with plausible NYC "
            f"coordinates, got {len(mappable_rows):,}"
        ),
        failures,
    )
    _expect(
        any(
            isinstance(row, dict)
            and isinstance(row.get("owner_name"), str)
            and bool(row["owner_name"].strip())
            for row in rows
        ),
        "map: authenticated owner context is absent from every row",
        failures,
    )

    cache_control = headers.get("cache-control", "").lower()
    _expect(
        "private" in cache_control and "no-store" in cache_control,
        "map: authenticated response is not private, no-store",
        failures,
    )
    vary = {
        value.strip().lower()
        for value in headers.get("vary", "").split(",")
        if value.strip()
    }
    _expect(
        {
            "authorization",
            "x-api-key",
            "x-citylens-parcel-smoke-key",
        }
        <= vary,
        "map: response does not vary on every supported credential",
        failures,
    )
    _expect(
        headers.get("x-citylens-inventory-scope") == "authenticated_full",
        "map: diagnostic scope header is not authenticated_full",
        failures,
    )
    _expect(
        headers.get("x-citylens-inventory-count") == str(expected_total),
        "map: diagnostic returned-count header is incorrect",
        failures,
    )
    _expect(
        headers.get("x-citylens-inventory-available") == str(expected_total),
        "map: diagnostic available-count header is incorrect",
        failures,
    )
    _expect(
        headers.get("content-encoding", "").lower() == "gzip",
        "map: full inventory was not delivered with gzip",
        failures,
    )
    return failures


def validate_authenticated_detail(
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    expected_bbl: str,
    expected_owner: str,
) -> list[str]:
    failures: list[str] = []
    _expect(
        payload.get("bbl") == expected_bbl,
        "parcel detail: response BBL does not match the selected map row",
        failures,
    )
    _expect(
        payload.get("owner_name") == expected_owner,
        "parcel detail: authenticated owner context does not match the map",
        failures,
    )
    audit = payload.get("decision_audit")
    _expect(
        isinstance(audit, dict),
        "parcel detail: decision audit is missing",
        failures,
    )
    if isinstance(audit, dict):
        readiness = audit.get("readiness")
        _expect(
            isinstance(readiness, dict)
            and readiness.get("status") != "limited_preview",
            "parcel detail: authenticated readiness is still preview-limited",
            failures,
        )
    cache_control = headers.get("cache-control", "").lower()
    _expect(
        "private" in cache_control and "no-store" in cache_control,
        "parcel detail: authenticated response is not private, no-store",
        failures,
    )
    return failures


def validate_official_dossier(
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    expected_bbl: str,
) -> list[str]:
    """Validate useful source facts without returning them in the report."""

    failures: list[str] = []
    _expect(
        payload.get("schema_version")
        == "citylens/parcel-official-dossier@v1",
        "official dossier: schema is invalid",
        failures,
    )
    _expect(
        payload.get("bbl") == expected_bbl,
        "official dossier: response BBL does not match the request",
        failures,
    )
    _expect(
        payload.get("borough") == "brooklyn",
        "official dossier: reference borough is invalid",
        failures,
    )
    _expect(
        payload.get("address") == "464 OVINGTON AVENUE",
        "official dossier: reference official address is invalid",
        failures,
    )
    _expect(
        payload.get("property_facts_dataset_id") == "64uk-42ks",
        "official dossier: PLUTO dataset identity is invalid",
        failures,
    )
    _expect(
        payload.get("ownership_dataset_ids")
        == {
            "master": "bnx9-e6tj",
            "legals": "8h5j-fqxa",
            "parties": "636b-3b5g",
        },
        "official dossier: ACRIS dataset identities are invalid",
        failures,
    )
    _expect(
        payload.get("owner_source_status")
        in {
            "match",
            "different",
            "pluto_only",
            "acris_only",
            "unavailable",
        },
        "official dossier: owner-source status is invalid",
        failures,
    )
    _expect(
        any(
            isinstance(payload.get(key), str)
            and bool(payload[key].strip())
            for key in ("pluto_owner_name", "acris_owner_name")
        ),
        "official dossier: both recorded-owner sources are empty",
        failures,
    )
    _expect(
        isinstance(payload.get("lot_area_sqft"), (int, float))
        and not isinstance(payload.get("lot_area_sqft"), bool)
        and float(payload["lot_area_sqft"]) > 0,
        "official dossier: lot area is unavailable or invalid",
        failures,
    )
    _expect(
        isinstance(payload.get("zoning_district_1"), str)
        and bool(payload["zoning_district_1"].strip()),
        "official dossier: mapped zoning reference is unavailable",
        failures,
    )
    generation = payload.get("dossier_generation")
    _expect(
        isinstance(generation, str)
        and _DOSSIER_GENERATION_RE.fullmatch(generation) is not None,
        "official dossier: generation is invalid",
        failures,
    )
    for key in (
        "property_facts_retrieved_at",
        "ownership_features_updated_at",
    ):
        value = payload.get(key)
        try:
            parsed = datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
            valid_date = (
                parsed.tzinfo is not None
                and parsed <= datetime.now(timezone.utc)
            )
        except ValueError:
            valid_date = False
        _expect(
            valid_date,
            f"official dossier: {key} is invalid or future-dated",
            failures,
        )
    links = payload.get("official_links")
    _expect(
        isinstance(links, dict)
        and set(links) == {"zola", "acris", "dob_bis"}
        and all(
            isinstance(value, str) and value.startswith("https://")
            for value in links.values()
        ),
        "official dossier: official links are invalid",
        failures,
    )
    forbidden_fields = {
        "score",
        "rank",
        "lead_membership",
        "phone",
        "email",
        "contact",
        "beneficial_owner",
        "workflow",
        "seller_intent",
    }
    _expect(
        not (forbidden_fields & set(payload)),
        "official dossier: a prohibited inference or workflow field leaked",
        failures,
    )
    interpretation = payload.get("interpretation")
    _expect(
        isinstance(interpretation, str)
        and all(
            phrase in interpretation.lower()
            for phrase in (
                "not a citylens lead",
                "not",
                "title report",
                "seller-intent",
            )
        ),
        "official dossier: evidence limitations are incomplete",
        failures,
    )
    cache_control = headers.get("cache-control", "").lower()
    _expect(
        "private" in cache_control and "no-store" in cache_control,
        "official dossier: authenticated response is not private, no-store",
        failures,
    )
    vary = {
        value.strip().lower()
        for value in headers.get("vary", "").split(",")
        if value.strip()
    }
    _expect(
        {
            "authorization",
            "x-api-key",
            "x-citylens-parcel-smoke-key",
        }
        <= vary,
        "official dossier: response does not vary on every credential",
        failures,
    )
    return failures


def run_checks(
    *,
    api_base: str,
    smoke_key: str,
    timeout: float,
    expected_total: int = EXPECTED_TOTAL,
    requested_top_per_borough: int = REQUESTED_TOP_PER_BOROUGH,
) -> dict[str, Any]:
    failures: list[str] = []
    timings: dict[str, float] = {}
    index_url = f"{api_base.rstrip('/')}/v1/parcel-intel/index"
    map_url = (
        f"{api_base.rstrip('/')}/v1/parcel-intel/map?"
        f"{urlencode({'top_per_borough': requested_top_per_borough})}"
    )
    try:
        index_status, _, index_payload, index_elapsed = _request_json(
            index_url,
            smoke_key=smoke_key,
            timeout=timeout,
        )
        timings["index"] = index_elapsed
        _expect(
            index_status == 200,
            f"index: expected HTTP 200, got {index_status}",
            failures,
        )
        expected_borough_counts: dict[str, int] = {}
        if index_status == 200:
            expected_borough_counts, index_failures = (
                expected_borough_counts_from_index(
                    index_payload,
                    expected_total=expected_total,
                )
            )
            failures.extend(index_failures)

        map_status, map_headers, map_payload, map_elapsed = _request_json(
            map_url,
            smoke_key=smoke_key,
            timeout=timeout,
        )
        timings["map"] = map_elapsed
        _expect(
            map_status == 200, f"map: expected HTTP 200, got {map_status}", failures
        )
        if map_status == 200:
            failures.extend(
                validate_authenticated_map(
                    map_payload,
                    map_headers,
                    expected_total=expected_total,
                    requested_top_per_borough=requested_top_per_borough,
                    expected_borough_counts=expected_borough_counts,
                )
            )

        rows_value = map_payload.get("rows")
        rows = rows_value if isinstance(rows_value, list) else []
        detail_row = next(
            (
                row
                for row in rows
                if isinstance(row, dict)
                and isinstance(row.get("bbl"), str)
                and isinstance(row.get("owner_name"), str)
                and bool(row["owner_name"].strip())
            ),
            None,
        )
        _expect(
            isinstance(detail_row, dict),
            "parcel detail: no owner-backed row was available to verify",
            failures,
        )
        if isinstance(detail_row, dict):
            bbl = str(detail_row["bbl"])
            detail_url = f"{api_base.rstrip('/')}/v1/parcel-intel/parcel/{quote(bbl)}"
            detail_status, detail_headers, detail_payload, detail_elapsed = (
                _request_json(
                    detail_url,
                    smoke_key=smoke_key,
                    timeout=timeout,
                )
            )
            timings["parcel_detail"] = detail_elapsed
            _expect(
                detail_status == 200,
                f"parcel detail: expected HTTP 200, got {detail_status}",
                failures,
            )
            if detail_status == 200:
                failures.extend(
                    validate_authenticated_detail(
                        detail_payload,
                        detail_headers,
                        expected_bbl=bbl,
                        expected_owner=str(detail_row["owner_name"]),
                    )
                )

        dossier_url = (
            f"{api_base.rstrip('/')}/v1/parcel-intel/official-parcel/"
            f"{quote(OFFICIAL_DOSSIER_SMOKE_BBL)}"
        )
        (
            dossier_status,
            dossier_headers,
            dossier_payload,
            dossier_elapsed,
        ) = _request_json(
            dossier_url,
            smoke_key=smoke_key,
            timeout=timeout,
        )
        timings["official_dossier"] = dossier_elapsed
        _expect(
            dossier_status == 200,
            (
                "official dossier: expected HTTP 200, "
                f"got {dossier_status}"
            ),
            failures,
        )
        if dossier_status == 200:
            failures.extend(
                validate_official_dossier(
                    dossier_payload,
                    dossier_headers,
                    expected_bbl=OFFICIAL_DOSSIER_SMOKE_BBL,
                )
            )
    except (RuntimeError, TypeError) as exc:
        failures.append(str(exc))
        index_payload = {}
        expected_borough_counts = {}
        map_payload = {}
        dossier_payload = {}

    index_quality_gate = index_payload.get("quality_gate")
    index_quality_gate = (
        index_quality_gate if isinstance(index_quality_gate, dict) else {}
    )
    index_selection_policy = index_quality_gate.get("selection_policy")
    index_selection_policy = (
        index_selection_policy
        if isinstance(index_selection_policy, dict)
        else {}
    )
    returned_rows = map_payload.get("rows")
    returned_rows = returned_rows if isinstance(returned_rows, list) else []
    returned_borough_counts = Counter(
        row.get("borough") for row in returned_rows if isinstance(row, dict)
    )
    return {
        "schema_version": "citylens/authenticated-production-verification@v1",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "api_base": api_base.rstrip("/"),
        "passed": not failures,
        "failures": failures,
        "inventory": {
            "access_scope": map_payload.get("access_scope"),
            "returned_count": map_payload.get("returned_count"),
            "available_count": map_payload.get("available_count"),
            "inventory_complete": map_payload.get("inventory_complete"),
            "mappable_count": sum(
                1
                for row in returned_rows
                if _is_mappable_nyc_row(row)
            ),
            "generated_at": map_payload.get("generated_at"),
            "selection_policy": index_selection_policy.get("policy_id"),
            "published_borough_counts": expected_borough_counts,
            "returned_borough_counts": {
                borough: returned_borough_counts[borough]
                for borough in BOROUGHS
            },
        },
        "official_dossier": {
            "verified": (
                dossier_payload.get("schema_version")
                == "citylens/parcel-official-dossier@v1"
            ),
            "generation": dossier_payload.get("dossier_generation"),
            "property_facts_retrieved_at": dossier_payload.get(
                "property_facts_retrieved_at"
            ),
            "ownership_features_updated_at": dossier_payload.get(
                "ownership_features_updated_at"
            ),
        },
        "timings_seconds": timings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the authenticated 5,000-row Parcel Intelligence "
            "production contract with a read-only smoke credential."
        )
    )
    parser.add_argument(
        "--api-base",
        default="https://api.citylens.dev",
        help="API origin to verify.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON report path.",
    )
    args = parser.parse_args()

    smoke_key = os.getenv("CITYLENS_PARCEL_SMOKE_KEY", "").strip()
    if not smoke_key:
        print(
            "CITYLENS_PARCEL_SMOKE_KEY is required and must be supplied "
            "through the environment.",
            file=sys.stderr,
        )
        return 2

    report = run_checks(
        api_base=args.api_base,
        smoke_key=smoke_key,
        timeout=args.timeout,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

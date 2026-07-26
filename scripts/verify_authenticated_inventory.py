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
EXPECTED_PER_BOROUGH = 1_000
SMOKE_HEADER = "X-CityLens-Parcel-Smoke-Key"


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
    expected_per_borough: int = EXPECTED_PER_BOROUGH,
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
        payload.get("requested_top_per_borough") == expected_per_borough,
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
    for borough in BOROUGHS:
        _expect(
            borough_counts[borough] == expected_per_borough,
            (
                f"map: {borough} has {borough_counts[borough]:,} rows, "
                f"expected {expected_per_borough:,}"
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


def run_checks(
    *,
    api_base: str,
    smoke_key: str,
    timeout: float,
    expected_total: int = EXPECTED_TOTAL,
    expected_per_borough: int = EXPECTED_PER_BOROUGH,
) -> dict[str, Any]:
    failures: list[str] = []
    timings: dict[str, float] = {}
    map_url = (
        f"{api_base.rstrip('/')}/v1/parcel-intel/map?"
        f"{urlencode({'top_per_borough': expected_per_borough})}"
    )
    try:
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
                    expected_per_borough=expected_per_borough,
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
    except (RuntimeError, TypeError) as exc:
        failures.append(str(exc))
        map_payload = {}

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
                for row in (
                    map_payload.get("rows")
                    if isinstance(map_payload.get("rows"), list)
                    else []
                )
                if _is_mappable_nyc_row(row)
            ),
            "generated_at": map_payload.get("generated_at"),
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

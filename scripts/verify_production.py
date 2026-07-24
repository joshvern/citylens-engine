#!/usr/bin/env python3
"""Adversarial public-contract verification for the deployed CityLens stack.

This intentionally uses only unauthenticated endpoints and the Python standard
library. It is safe to run from GitHub Actions without secrets and verifies that
premium parcel/workflow data does *not* cross the public boundary.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BOROUGHS = ("manhattan", "brooklyn", "queens", "bronx", "staten_island")
BBL_PREFIX = {
    "manhattan": "1",
    "bronx": "2",
    "brooklyn": "3",
    "queens": "4",
    "staten_island": "5",
}
PRIVATE_NULL_FIELDS = (
    "score_calibrated_p10",
    "score_calibrated_p90",
    "owner_name",
    "owner_name_source",
    "owner_type",
    "owner_entity_type",
    "owner_portfolio_id",
    "owner_portfolio_match_method",
    "owner_portfolio_lot_count",
    "owner_portfolio_borough_count",
    "owner_portfolio_total_lot_area_sqft",
    "owner_portfolio_candidate_count",
    "owner_portfolio_data_as_of",
    "change_latest_imagery_year",
    "observed_imagery_year",
    "tax_lien_sale_date",
    "tax_lien_sale_year",
    "tax_lien_water_debt_only",
    "tax_lien_data_as_of",
    "dob_safety_latest_issue_date",
    "ecb_latest_issue_date",
    "hpd_latest_inspection_date",
    "critical_violation_count",
    "violation_data_as_of",
    "firm07_floodplain",
    "pfirm15_floodplain",
    "floodplain_1pct",
    "floodplain_data_as_of",
)


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes
    elapsed_seconds: float


def _request(
    url: str,
    *,
    timeout: float,
    accept_gzip: bool = False,
    attempts: int = 3,
) -> HttpResult:
    headers = {
        "Accept": "application/json",
        "User-Agent": "citylens-production-verifier/1.0",
    }
    if accept_gzip:
        headers["Accept-Encoding"] = "gzip"
    last_error: Exception | None = None
    for attempt in range(attempts):
        started = time.monotonic()
        try:
            with urlopen(Request(url, headers=headers), timeout=timeout) as response:
                return HttpResult(
                    status=int(response.status),
                    headers={key.lower(): value for key, value in response.headers.items()},
                    body=response.read(),
                    elapsed_seconds=time.monotonic() - started,
                )
        except HTTPError as exc:
            body = exc.read()
            if exc.code < 500 or attempt == attempts - 1:
                return HttpResult(
                    status=int(exc.code),
                    headers={key.lower(): value for key, value in exc.headers.items()},
                    body=body,
                    elapsed_seconds=time.monotonic() - started,
                )
            last_error = exc
        except (TimeoutError, URLError) as exc:
            last_error = exc
        if attempt < attempts - 1:
            time.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"request failed after {attempts} attempts: {url}: {last_error}")


def _json(result: HttpResult, label: str, failures: list[str]) -> dict[str, Any]:
    if result.status != 200:
        failures.append(f"{label}: expected HTTP 200, got {result.status}")
        return {}
    body = result.body
    if result.headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError:
            failures.append(f"{label}: response declared gzip but could not be decompressed")
            return {}
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        failures.append(f"{label}: response was not valid JSON")
        return {}
    if not isinstance(parsed, dict):
        failures.append(f"{label}: expected a JSON object")
        return {}
    return parsed


def _expect(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_index(
    index: dict[str, Any],
    *,
    max_age_days: float,
    now: datetime,
) -> list[str]:
    failures: list[str] = []
    generated_at = _parse_timestamp(index.get("generated_at"))
    _expect(generated_at is not None, "index: generated_at is missing or invalid", failures)
    if generated_at is not None:
        age_days = max((now - generated_at).total_seconds(), 0.0) / 86400
        _expect(
            age_days <= max_age_days,
            f"index: feed is {age_days:.1f} days old (limit {max_age_days:.1f})",
            failures,
        )
    _expect(index.get("stale") is False, "index: API marks the feed stale", failures)

    boroughs = index.get("boroughs")
    _expect(isinstance(boroughs, list), "index: boroughs is not a list", failures)
    borough_rows = boroughs if isinstance(boroughs, list) else []
    by_slug = {
        row.get("slug"): row for row in borough_rows if isinstance(row, dict)
    }
    _expect(set(by_slug) == set(BOROUGHS), "index: expected exactly five NYC boroughs", failures)
    for slug in BOROUGHS:
        row = by_slug.get(slug) or {}
        _expect(row.get("count") == 1000, f"index: {slug} count is not 1000", failures)

    quality = index.get("quality_gate")
    _expect(isinstance(quality, dict), "index: quality_gate is missing", failures)
    quality = quality if isinstance(quality, dict) else {}
    _expect(quality.get("passed") is True, "index: quality gate did not pass", failures)
    _expect(quality.get("failures") == [], "index: quality gate has failures", failures)
    _expect(
        quality.get("citywide_acquisition_eligible_count") == 5000,
        "index: expected 5,000 eligible citywide leads",
        failures,
    )
    _expect(
        quality.get("citywide_rank_sequence_valid") is True,
        "index: citywide rank sequence is invalid",
        failures,
    )
    quality_boroughs = quality.get("boroughs")
    quality_boroughs = quality_boroughs if isinstance(quality_boroughs, dict) else {}
    for slug in BOROUGHS:
        row = quality_boroughs.get(slug)
        _expect(isinstance(row, dict), f"index: {slug} quality report missing", failures)
        row = row if isinstance(row, dict) else {}
        _expect(row.get("passed") is True, f"index: {slug} quality gate failed", failures)
        _expect(
            row.get("row_count") == 1000,
            f"index: {slug} quality row_count is not 1000",
            failures,
        )
        for field in (
            "project_leakage_count",
            "land_use_project_leakage_count",
            "duplicate_bbl_count",
            "invalid_owner_leakage_count",
            "non_private_owner_leakage_count",
            "negative_unused_floor_area_count",
            "invalid_owner_portfolio_count",
        ):
            _expect(row.get(field) == 0, f"index: {slug} {field} is not zero", failures)
        for field in ("owner_coverage", "geometry_coverage", "floodplain_coverage"):
            _expect(row.get(field) == 1.0, f"index: {slug} {field} is not complete", failures)

    generation_diff = index.get("generation_diff")
    _expect(
        isinstance(generation_diff, dict),
        "index: generation_diff is missing",
        failures,
    )
    generation_diff = (
        generation_diff if isinstance(generation_diff, dict) else {}
    )
    _expect(
        generation_diff.get("schema")
        == "citylens-parcel-intel/generation-diff@v1",
        "index: generation_diff schema is invalid",
        failures,
    )
    _expect(
        generation_diff.get("status") in {"initial_generation", "compared"},
        "index: generation_diff status is invalid",
        failures,
    )
    diff_gate = generation_diff.get("gate")
    _expect(
        isinstance(diff_gate, dict),
        "index: generation_diff gate is missing",
        failures,
    )
    diff_gate = diff_gate if isinstance(diff_gate, dict) else {}
    _expect(
        diff_gate.get("passed") is True,
        "index: generation_diff gate did not pass",
        failures,
    )
    if diff_gate.get("thresholds_passed") is not True:
        _expect(
            diff_gate.get("override_applied") is True
            and isinstance(diff_gate.get("override_reason"), str)
            and bool(diff_gate["override_reason"].strip()),
            "index: failed drift thresholds lack a reviewed override reason",
            failures,
        )
    diff_candidate = generation_diff.get("candidate")
    diff_candidate = diff_candidate if isinstance(diff_candidate, dict) else {}
    _expect(
        diff_candidate.get("row_count") == 5000,
        "index: generation_diff candidate row count is not 5,000",
        failures,
    )

    model = index.get("model_metadata")
    model = model if isinstance(model, dict) else {}
    _expect(
        model.get("label_definition") == "dob_nb_job_filing",
        "index: unexpected model label definition",
        failures,
    )
    _expect(
        model.get("evaluation_mode") == "rolling_origin",
        "index: model is not governed by rolling-origin evaluation",
        failures,
    )
    _expect(
        model.get("training_origins") == [2018, 2020, 2022],
        "index: unexpected training origins",
        failures,
    )
    _expect(
        model.get("calibration_origin") == 2024,
        "index: unexpected calibration origin",
        failures,
    )
    _expect(
        model.get("inference_feature_snapshot") == "current",
        "index: inference feature snapshot is not current",
        failures,
    )
    _expect(
        model.get("prospective_2026_validated") is False,
        "index: prospective 2026 validation flag must remain false until matured",
        failures,
    )
    return failures


def _validate_public_row(row: dict[str, Any], label: str) -> list[str]:
    failures: list[str] = []
    _expect(
        isinstance(row.get("bbl"), str) and len(row["bbl"]) == 10 and row["bbl"].isdigit(),
        f"{label}: invalid BBL",
        failures,
    )
    _expect(row.get("acquisition_eligible") is True, f"{label}: lead is not eligible", failures)
    _expect(
        row.get("acquisition_status") == "eligible",
        f"{label}: acquisition status is not eligible",
        failures,
    )
    _expect(
        row.get("opportunity_category") not in {"active_project", "completed_project"},
        f"{label}: active/completed project leaked into eligible leads",
        failures,
    )
    for field in PRIVATE_NULL_FIELDS:
        _expect(row.get(field) is None, f"{label}: private field {field} was exposed", failures)
    _expect(row.get("top_features", []) == [], f"{label}: SHAP features were exposed", failures)
    for field in ("change_added_count", "change_demolished_count", "change_modified_count"):
        _expect(row.get(field, 0) == 0, f"{label}: private field {field} was exposed", failures)
    _expect(row.get("recent_change", False) is False, f"{label}: recent_change was exposed", failures)
    for field in ("dob_safety_active_count", "ecb_active_count", "hpd_open_count"):
        _expect(row.get(field, 0) == 0, f"{label}: private field {field} was exposed", failures)
    return failures


def validate_map(
    payload: dict[str, Any],
    *,
    expected_generated_at: str | None,
) -> list[str]:
    failures: list[str] = []
    _expect(
        payload.get("generated_at") == expected_generated_at,
        "map: generation does not match index",
        failures,
    )
    rows = payload.get("rows")
    _expect(isinstance(rows, list), "map: rows is not a list", failures)
    rows = rows if isinstance(rows, list) else []
    _expect(len(rows) == 125, f"map: expected 125 public rows, got {len(rows)}", failures)
    bbls: set[str] = set()
    citywide_ranks: set[int] = set()
    counts = {slug: 0 for slug in BOROUGHS}
    acquisition_ranks = {slug: set() for slug in BOROUGHS}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            failures.append(f"map: row {index} is not an object")
            continue
        label = f"map row {index}"
        failures.extend(_validate_public_row(row, label))
        bbl = row.get("bbl")
        if isinstance(bbl, str):
            _expect(bbl not in bbls, f"{label}: duplicate BBL {bbl}", failures)
            bbls.add(bbl)
        slug = row.get("borough")
        _expect(slug in BOROUGHS, f"{label}: unknown borough {slug}", failures)
        if slug in counts:
            counts[slug] += 1
            rank = row.get("acquisition_rank")
            if isinstance(rank, int):
                acquisition_ranks[slug].add(rank)
        citywide_rank = row.get("citywide_rank")
        _expect(
            isinstance(citywide_rank, int) and citywide_rank > 0,
            f"{label}: invalid citywide rank",
            failures,
        )
        if isinstance(citywide_rank, int):
            _expect(
                citywide_rank not in citywide_ranks,
                f"{label}: duplicate citywide rank {citywide_rank}",
                failures,
            )
            citywide_ranks.add(citywide_rank)
    for slug in BOROUGHS:
        _expect(counts[slug] == 25, f"map: {slug} does not have 25 rows", failures)
        _expect(
            acquisition_ranks[slug] == set(range(1, 26)),
            f"map: {slug} acquisition ranks are not 1-25",
            failures,
        )
    return failures


def validate_sweep(
    payload: dict[str, Any],
    *,
    slug: str,
    expected_generated_at: str | None,
) -> list[str]:
    failures: list[str] = []
    _expect(payload.get("borough") == slug, f"sweep {slug}: response borough mismatch", failures)
    _expect(
        payload.get("generated_at") == expected_generated_at,
        f"sweep {slug}: generation does not match index",
        failures,
    )
    rows = payload.get("rows")
    _expect(isinstance(rows, list), f"sweep {slug}: rows is not a list", failures)
    rows = rows if isinstance(rows, list) else []
    _expect(len(rows) == 1, f"sweep {slug}: expected one row", failures)
    if rows and isinstance(rows[0], dict):
        row = rows[0]
        failures.extend(_validate_public_row(row, f"sweep {slug} row"))
        _expect(row.get("acquisition_rank") == 1, f"sweep {slug}: top rank is not 1", failures)
        _expect(
            str(row.get("bbl") or "").startswith(BBL_PREFIX[slug]),
            f"sweep {slug}: BBL borough prefix mismatch",
            failures,
        )
    _expect(
        (payload.get("quality_gate") or {}).get("passed") is True,
        f"sweep {slug}: quality gate did not pass",
        failures,
    )
    return failures


def run_checks(
    *,
    api_base: str,
    web_base: str,
    max_age_days: float,
    timeout: float,
) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    timings: dict[str, float] = {}
    api_base = api_base.rstrip("/")
    web_base = web_base.rstrip("/")

    health_result = _request(f"{api_base}/v1/health", timeout=timeout)
    health = _json(health_result, "health", failures)
    timings["health"] = round(health_result.elapsed_seconds, 3)
    _expect(health.get("ok") is True, "health: ok is not true", failures)

    ready_result = _request(f"{api_base}/v1/health/ready", timeout=timeout)
    ready = _json(ready_result, "readiness", failures)
    timings["readiness"] = round(ready_result.elapsed_seconds, 3)
    _expect(ready.get("ok") is True, "readiness: API is not ready", failures)
    _expect(ready.get("firestore") is True, "readiness: Firestore is unavailable", failures)
    parcel_ready = ready.get("parcel_intel") or {}
    _expect(parcel_ready.get("present") is True, "readiness: parcel feed is absent", failures)
    _expect(parcel_ready.get("stale") is False, "readiness: parcel feed is stale", failures)

    index_result = _request(f"{api_base}/v1/parcel-intel/index", timeout=timeout)
    index = _json(index_result, "index", failures)
    timings["index"] = round(index_result.elapsed_seconds, 3)
    failures.extend(
        validate_index(
            index,
            max_age_days=max_age_days,
            now=datetime.now(timezone.utc),
        )
    )
    generated_at = index.get("generated_at")

    map_result = _request(
        f"{api_base}/v1/parcel-intel/map?{urlencode({'top_per_borough': 25})}",
        timeout=timeout,
        accept_gzip=True,
    )
    timings["map"] = round(map_result.elapsed_seconds, 3)
    _expect(
        map_result.headers.get("content-encoding", "").lower() == "gzip",
        "map: expected gzip delivery",
        failures,
    )
    _expect(
        "public" in map_result.headers.get("cache-control", "").lower(),
        "map: anonymous response is not publicly cacheable",
        failures,
    )
    map_payload = _json(map_result, "map", failures)
    failures.extend(
        validate_map(
            map_payload,
            expected_generated_at=generated_at if isinstance(generated_at, str) else None,
        )
    )

    for slug in BOROUGHS:
        result = _request(
            f"{api_base}/v1/parcel-intel/sweep?{urlencode({'borough': slug, 'top': 1})}",
            timeout=timeout,
        )
        timings[f"sweep_{slug}"] = round(result.elapsed_seconds, 3)
        payload = _json(result, f"sweep {slug}", failures)
        failures.extend(
            validate_sweep(
                payload,
                slug=slug,
                expected_generated_at=(
                    generated_at if isinstance(generated_at, str) else None
                ),
            )
        )

    for label, path in (
        ("workflow list", "/v1/parcel-intel/workflow"),
        ("workflow analytics", "/v1/parcel-intel/workflow/analytics"),
        ("workflow events", "/v1/parcel-intel/workflow/3020960069/events"),
    ):
        result = _request(f"{api_base}{path}", timeout=timeout)
        timings[label.replace(" ", "_")] = round(result.elapsed_seconds, 3)
        _expect(result.status == 401, f"{label}: anonymous request returned {result.status}", failures)

    web_result = _request(
        f"{web_base}/parcel-intel",
        timeout=timeout,
    )
    timings["web_parcel_intel"] = round(web_result.elapsed_seconds, 3)
    _expect(web_result.status == 200, f"web: /parcel-intel returned {web_result.status}", failures)
    html = web_result.body.decode("utf-8", errors="replace")
    for expected in (
        "Find the sites worth pursuing this week",
        "Citywide opportunity explorer",
        "See the whole market",
    ):
        _expect(expected in html, f"web: missing expected copy: {expected}", failures)

    summary = {
        "schema_version": "citylens/production-verification@v1",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "api_base": api_base,
        "web_base": web_base,
        "feed_generated_at": generated_at,
        "max_age_days": max_age_days,
        "checks": 12,
        "timings_seconds": timings,
        "passed": not failures,
        "failures": failures,
    }
    return failures, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base", default="https://api.citylens.dev")
    parser.add_argument("--web-base", default="https://www.citylens.dev")
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=35.0,
        help="Fail before the API's 45-day stale boundary to leave remediation time.",
    )
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--output", help="Optional path for the JSON verification report.")
    args = parser.parse_args()

    try:
        failures, summary = run_checks(
            api_base=args.api_base,
            web_base=args.web_base,
            max_age_days=args.max_age_days,
            timeout=args.timeout,
        )
    except Exception as exc:
        failures = [f"verifier crashed: {type(exc).__name__}: {exc}"]
        summary = {
            "schema_version": "citylens/production-verification@v1",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "passed": False,
            "failures": failures,
        }
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.write("\n")
    for failure in failures:
        safe = failure.replace("\n", " ").replace("%", "%25").replace("\r", "%0D")
        print(f"::error title=CityLens production verification::{safe}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

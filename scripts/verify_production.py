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
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    "score_raw",
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
    "environmental_review_required",
    "environmental_designation_number",
    "environmental_designation_kind",
    "environmental_designation_data_as_of",
    "mandatory_inclusionary_housing",
    "mih_options",
    "mih_area_count",
    "mih_data_as_of",
    "nearest_transit_complex_id",
    "nearest_transit_station_name",
    "nearest_transit_station_distance_m",
    "nearest_transit_routes",
    "nearest_transit_ada_status",
    "transit_station_count_400m",
    "transit_station_count_800m",
    "transit_access_tier",
    "transit_data_as_of",
)
REQUIRED_SOURCE_SLAS = (
    "property_facts",
    "ownership",
    "constraints",
    "project_activity",
    "land_use_activity",
    "owner_portfolio",
    "tax_lien_sale_history",
    "current_violations",
    "floodplain_screen",
    "environmental_review",
    "mandatory_inclusionary_housing",
    "transit_access",
)
SOURCE_MAX_AGE_CAP_DAYS = {
    "project_activity": 8.0,
    "land_use_activity": 8.0,
}
EXPECTED_WORKFLOW_HORIZONS = (
    ("owner_contacted", 30),
    ("qualified", 90),
    ("offer_submitted", 180),
    ("under_contract", 270),
    ("closed", 365),
)
EXPECTED_PERMISSIONS_POLICY = (
    "browsing-topics=(), camera=(), geolocation=(), microphone=(), payment=()"
)
REQUIRED_WEB_COPY = (
    "Find the sites worth pursuing this week",
    "Citywide opportunity explorer",
    "Filter the five-borough market",
    "Qualification receipt",
    "not model accuracy, seller intent, transaction probability",
    "Search parcels",
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
    method: str = "GET",
    json_body: dict[str, Any] | None = None,
) -> HttpResult:
    headers = {
        "Accept": "application/json",
        "User-Agent": "citylens-production-verifier/1.0",
    }
    if accept_gzip:
        headers["Accept-Encoding"] = "gzip"
    data = None
    if json_body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(json_body, separators=(",", ":")).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(attempts):
        started = time.monotonic()
        try:
            with urlopen(
                Request(url, headers=headers, data=data, method=method),
                timeout=timeout,
            ) as response:
                return HttpResult(
                    status=int(response.status),
                    headers={
                        key.lower(): value for key, value in response.headers.items()
                    },
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
            failures.append(
                f"{label}: response declared gzip but could not be decompressed"
            )
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


def _wilson_95(successes: int, trials: int) -> tuple[float, float]:
    z = 1.959963984540054
    proportion = successes / trials
    z_squared = z**2
    denominator = 1.0 + z_squared / trials
    center = (proportion + z_squared / (2 * trials)) / denominator
    margin = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / trials + z_squared / (4 * trials**2)
        )
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def validate_historical_benchmark_receipt(
    receipt: Any,
    *,
    label: str = "index",
) -> list[str]:
    """Validate exact historical counts, intervals, and claim boundaries."""

    failures: list[str] = []
    _expect(
        isinstance(receipt, dict),
        f"{label}: historical benchmark receipt is missing",
        failures,
    )
    receipt = receipt if isinstance(receipt, dict) else {}
    _expect(
        receipt.get("schema") == "citylens_historical_benchmark_receipt@v1",
        f"{label}: historical benchmark receipt schema is invalid",
        failures,
    )
    _expect(
        receipt.get("target") == "dob_nb_job_filing"
        and receipt.get("feature_origin") == 2024
        and receipt.get("outcome_window") == "2025-2025",
        f"{label}: historical benchmark cohort identity is invalid",
        failures,
    )
    rows = receipt.get("evaluation_rows")
    positives = receipt.get("observed_positive_rows")
    base_rate = receipt.get("base_rate")
    counts_valid = (
        isinstance(rows, int)
        and not isinstance(rows, bool)
        and rows >= 1000
        and isinstance(positives, int)
        and not isinstance(positives, bool)
        and 0 <= positives <= rows
    )
    _expect(
        counts_valid,
        f"{label}: historical benchmark counts are invalid",
        failures,
    )
    _expect(
        counts_valid
        and isinstance(base_rate, (int, float))
        and not isinstance(base_rate, bool)
        and math.isclose(float(base_rate), positives / rows, abs_tol=1e-12),
        f"{label}: historical benchmark base rate disagrees with counts",
        failures,
    )
    for key, expected_k in (("top_100", 100), ("top_1000", 1000)):
        metric = receipt.get(key)
        _expect(
            isinstance(metric, dict),
            f"{label}: historical benchmark {key} is missing",
            failures,
        )
        metric = metric if isinstance(metric, dict) else {}
        evaluated = metric.get("evaluated_rows")
        hits = metric.get("observed_hits")
        precision = metric.get("precision")
        interval = metric.get("precision_95ci")
        metric_counts_valid = (
            counts_valid
            and metric.get("k") == expected_k
            and evaluated == min(expected_k, rows)
            and isinstance(hits, int)
            and not isinstance(hits, bool)
            and 0 <= hits <= evaluated
            and hits <= positives
        )
        _expect(
            metric_counts_valid,
            f"{label}: historical benchmark {key} counts are invalid",
            failures,
        )
        _expect(
            metric_counts_valid
            and isinstance(precision, (int, float))
            and not isinstance(precision, bool)
            and math.isclose(
                float(precision),
                hits / evaluated,
                abs_tol=1e-12,
            ),
            f"{label}: historical benchmark {key} precision disagrees with counts",
            failures,
        )
        expected_interval = _wilson_95(hits, evaluated) if metric_counts_valid else None
        _expect(
            expected_interval is not None
            and isinstance(interval, list)
            and len(interval) == 2
            and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in interval
            )
            and all(
                math.isclose(
                    float(observed),
                    expected,
                    abs_tol=1e-12,
                )
                for observed, expected in zip(
                    interval,
                    expected_interval,
                    strict=True,
                )
            ),
            f"{label}: historical benchmark {key} interval is invalid",
            failures,
        )
    interval_meta = receipt.get("interval")
    interval_meta = interval_meta if isinstance(interval_meta, dict) else {}
    limitations = str(interval_meta.get("limitations") or "").lower()
    _expect(
        interval_meta.get("method") == "wilson_score_observed_top_k"
        and interval_meta.get("confidence_level") == 0.95
        and interval_meta.get("scope") == "fixed_historical_ranked_list",
        f"{label}: historical interval method is invalid",
        failures,
    )
    for phrase in ("model", "spatial", "current"):
        _expect(
            phrase in limitations,
            f"{label}: historical interval limitations omit {phrase}",
            failures,
        )
    _expect(
        receipt.get("evidence_status") == "development_exposed"
        and receipt.get("not_current_accuracy") is True
        and receipt.get("not_parcel_confidence") is True,
        f"{label}: historical benchmark claim boundaries are invalid",
        failures,
    )
    return failures


def validate_historical_borough_benchmark_receipt(
    receipt: Any,
    *,
    label: str = "index",
) -> list[str]:
    """Validate five exact historical within-borough top-100 cohorts."""

    failures: list[str] = []
    _expect(
        isinstance(receipt, dict),
        f"{label}: historical borough benchmark receipt is missing",
        failures,
    )
    receipt = receipt if isinstance(receipt, dict) else {}
    _expect(
        receipt.get("schema") == "citylens_historical_borough_benchmark_receipt@v1"
        and receipt.get("target") == "dob_nb_job_filing"
        and receipt.get("feature_origin") == 2024
        and receipt.get("outcome_window") == "2025-2025"
        and receipt.get("ranking_scope") == "historical_within_borough_model_order",
        f"{label}: historical borough cohort identity is invalid",
        failures,
    )
    citywide_rows = receipt.get("citywide_evaluation_rows")
    citywide_positives = receipt.get("citywide_observed_positive_rows")
    citywide_counts_valid = (
        isinstance(citywide_rows, int)
        and not isinstance(citywide_rows, bool)
        and citywide_rows >= 500
        and isinstance(citywide_positives, int)
        and not isinstance(citywide_positives, bool)
        and 0 <= citywide_positives <= citywide_rows
    )
    _expect(
        citywide_counts_valid,
        f"{label}: historical borough citywide counts are invalid",
        failures,
    )
    boroughs = receipt.get("boroughs")
    boroughs = boroughs if isinstance(boroughs, dict) else {}
    expected_boroughs = {
        "manhattan",
        "bronx",
        "brooklyn",
        "queens",
        "staten_island",
    }
    _expect(
        set(boroughs) == expected_boroughs,
        f"{label}: historical borough membership is invalid",
        failures,
    )
    observed_rows = 0
    observed_positives = 0
    for slug in sorted(expected_boroughs):
        cohort = boroughs.get(slug)
        _expect(
            isinstance(cohort, dict),
            f"{label}: historical {slug} cohort is missing",
            failures,
        )
        cohort = cohort if isinstance(cohort, dict) else {}
        rows = cohort.get("evaluation_rows")
        positives = cohort.get("observed_positive_rows")
        base_rate = cohort.get("base_rate")
        counts_valid = (
            isinstance(rows, int)
            and not isinstance(rows, bool)
            and rows >= 100
            and isinstance(positives, int)
            and not isinstance(positives, bool)
            and 0 <= positives <= rows
        )
        _expect(
            counts_valid,
            f"{label}: historical {slug} counts are invalid",
            failures,
        )
        if counts_valid:
            observed_rows += rows
            observed_positives += positives
        _expect(
            counts_valid
            and isinstance(base_rate, (int, float))
            and not isinstance(base_rate, bool)
            and math.isclose(
                float(base_rate),
                positives / rows,
                abs_tol=1e-12,
            ),
            f"{label}: historical {slug} base rate disagrees with counts",
            failures,
        )
        top_100 = cohort.get("top_100")
        _expect(
            isinstance(top_100, dict),
            f"{label}: historical {slug} top-100 is missing",
            failures,
        )
        top_100 = top_100 if isinstance(top_100, dict) else {}
        evaluated = top_100.get("evaluated_rows")
        hits = top_100.get("observed_hits")
        precision = top_100.get("precision")
        interval = top_100.get("precision_95ci")
        top_counts_valid = (
            counts_valid
            and top_100.get("k") == 100
            and evaluated == min(100, rows)
            and isinstance(hits, int)
            and not isinstance(hits, bool)
            and 0 <= hits <= evaluated
            and hits <= positives
        )
        _expect(
            top_counts_valid,
            f"{label}: historical {slug} top-100 counts are invalid",
            failures,
        )
        _expect(
            top_counts_valid
            and isinstance(precision, (int, float))
            and not isinstance(precision, bool)
            and math.isclose(
                float(precision),
                hits / evaluated,
                abs_tol=1e-12,
            ),
            f"{label}: historical {slug} top-100 precision disagrees with counts",
            failures,
        )
        expected_interval = _wilson_95(hits, evaluated) if top_counts_valid else None
        _expect(
            expected_interval is not None
            and isinstance(interval, list)
            and len(interval) == 2
            and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in interval
            )
            and all(
                math.isclose(float(actual), expected, abs_tol=1e-12)
                for actual, expected in zip(
                    interval,
                    expected_interval,
                    strict=True,
                )
            ),
            f"{label}: historical {slug} top-100 interval is invalid",
            failures,
        )

    _expect(
        citywide_counts_valid
        and observed_rows == citywide_rows
        and observed_positives == citywide_positives,
        f"{label}: historical borough counts do not reconcile citywide",
        failures,
    )
    interval_meta = receipt.get("interval")
    interval_meta = interval_meta if isinstance(interval_meta, dict) else {}
    limitations = str(interval_meta.get("limitations") or "").lower()
    _expect(
        interval_meta.get("method") == "wilson_score_observed_top_k"
        and interval_meta.get("confidence_level") == 0.95
        and interval_meta.get("scope") == "fixed_historical_borough_ranked_list",
        f"{label}: historical borough interval method is invalid",
        failures,
    )
    for phrase in ("model", "spatial", "current", "parcel"):
        _expect(
            phrase in limitations,
            f"{label}: historical borough limitations omit {phrase}",
            failures,
        )
    source = receipt.get("source_receipt")
    source = source if isinstance(source, dict) else {}
    expected_source_keys = {
        "schema",
        "report_file_name",
        "report_schema",
        "report_sha256",
        "report_size_bytes",
        "source_model_sha256",
        "metadata_only_attachment",
    }
    _expect(
        set(source) == expected_source_keys
        and source.get("schema") == "citylens_borough_benchmark_attachment@v1"
        and isinstance(source.get("report_file_name"), str)
        and bool(source["report_file_name"].strip())
        and isinstance(source.get("report_schema"), str)
        and bool(source["report_schema"].strip())
        and re.fullmatch(r"[0-9a-f]{64}", str(source.get("report_sha256"))) is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(source.get("source_model_sha256")),
        )
        is not None
        and isinstance(source.get("report_size_bytes"), int)
        and source["report_size_bytes"] >= 1
        and source.get("metadata_only_attachment") is True,
        f"{label}: historical borough source receipt is invalid",
        failures,
    )
    _expect(
        receipt.get("evidence_status") == "development_exposed"
        and receipt.get("not_current_accuracy") is True
        and receipt.get("not_parcel_confidence") is True,
        f"{label}: historical borough claim boundaries are invalid",
        failures,
    )
    return failures


def validate_web_copy(html: str) -> list[str]:
    """Verify durable map-first product semantics without pinning a tagline."""

    failures: list[str] = []
    for expected in REQUIRED_WEB_COPY:
        _expect(
            expected in html,
            f"web: missing expected copy: {expected}",
            failures,
        )
    return failures


def validate_pilot_probe_response(
    status: int,
    headers: dict[str, str],
) -> list[str]:
    """Accept validation rejection or an already-enforced public rate limit.

    The smoke probe deliberately submits an empty body and must never create a
    pilot request. Repeated verifier runs can exhaust the endpoint's small
    public token bucket, so HTTP 429 is an equally safe terminal response.
    """

    failures: list[str] = []
    _expect(
        status in {422, 429},
        f"pilot requests: invalid anonymous submission returned {status}",
        failures,
    )
    _expect(
        "no-store" in headers.get("cache-control", "").lower(),
        "pilot requests: rejection response is cacheable",
        failures,
    )
    return failures


def validate_security_headers(
    headers: dict[str, str],
    *,
    label: str,
    browser_page: bool,
) -> list[str]:
    """Validate the deployed baseline without constraining resource origins."""
    failures: list[str] = []
    _expect(
        headers.get("x-content-type-options", "").lower() == "nosniff",
        f"{label}: X-Content-Type-Options is not nosniff",
        failures,
    )
    _expect(
        headers.get("x-frame-options", "").upper() == "DENY",
        f"{label}: X-Frame-Options is not DENY",
        failures,
    )
    _expect(
        headers.get("x-xss-protection") == "0",
        f"{label}: X-XSS-Protection must explicitly disable the legacy filter",
        failures,
    )
    _expect(
        headers.get("permissions-policy") == EXPECTED_PERMISSIONS_POLICY,
        f"{label}: Permissions-Policy does not match the production contract",
        failures,
    )
    expected_referrer = (
        "strict-origin-when-cross-origin" if browser_page else "no-referrer"
    )
    _expect(
        headers.get("referrer-policy") == expected_referrer,
        f"{label}: Referrer-Policy is not {expected_referrer}",
        failures,
    )
    hsts = headers.get("strict-transport-security", "")
    try:
        max_age = int(
            next(
                part.split("=", 1)[1]
                for part in hsts.split(";")
                if part.strip().lower().startswith("max-age=")
            )
        )
    except (StopIteration, ValueError):
        max_age = 0
    _expect(
        max_age >= 31_536_000,
        f"{label}: HSTS max-age is below one year",
        failures,
    )
    csp = headers.get("content-security-policy", "")
    for directive in (
        "object-src 'none'",
        "frame-ancestors 'none'",
    ):
        _expect(
            directive in csp,
            f"{label}: CSP is missing {directive}",
            failures,
        )
    expected_base = "base-uri 'self'" if browser_page else "base-uri 'none'"
    _expect(
        expected_base in csp,
        f"{label}: CSP is missing {expected_base}",
        failures,
    )
    if browser_page:
        _expect(
            "form-action 'self'" in csp,
            f"{label}: CSP is missing form-action 'self'",
            failures,
        )
        _expect(
            "x-powered-by" not in headers,
            f"{label}: X-Powered-By exposes framework metadata",
            failures,
        )
    return failures


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


def _has_explicit_timezone(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_prospective_validation(
    status: Any,
    *,
    feed_generation: Any,
    health: Any,
    now: datetime | None = None,
) -> list[str]:
    """Adversarially verify the parcel-free live-cohort public contract."""
    failures: list[str] = []
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    _expect(
        isinstance(status, dict),
        "index: prospective validation status is missing",
        failures,
    )
    if not isinstance(status, dict):
        return failures
    _expect(
        status.get("schema")
        == "citylens-parcel-intel/prospective-validation-status@v1",
        "index: prospective validation schema is invalid",
        failures,
    )
    _expect(
        isinstance(feed_generation, str)
        and status.get("cohort_id") == feed_generation
        and status.get("source_generation") == feed_generation,
        "index: prospective validation does not match the active feed",
        failures,
    )
    _expect(
        status.get("label_definition") == "dob_nb_job_filing",
        "index: prospective validation target is invalid",
        failures,
    )
    measurement_status = status.get("measurement_status")
    _expect(
        measurement_status in {"awaiting_post_issue_data", "collecting", "mature"},
        "index: prospective measurement status is invalid",
        failures,
    )
    issued_at = _parse_timestamp(status.get("issued_at"))
    matures_at = _parse_timestamp(status.get("matures_at"))
    try:
        observation_start = datetime.fromisoformat(
            str(status.get("observation_starts_on"))
        ).date()
        observed_through = datetime.fromisoformat(
            str(status.get("observed_through"))
        ).date()
    except ValueError:
        observation_start = None
        observed_through = None
    _expect(
        issued_at is not None
        and matures_at is not None
        and (matures_at - issued_at).days == 365,
        "index: prospective maturity horizon is invalid",
        failures,
    )
    _expect(
        _has_explicit_timezone(status.get("issued_at"))
        and _has_explicit_timezone(status.get("matures_at")),
        "index: prospective timestamps are not timezone-aware",
        failures,
    )
    _expect(
        issued_at is not None
        and observation_start is not None
        and observation_start.toordinal() == issued_at.date().toordinal() + 1,
        "index: prospective observation start is invalid",
        failures,
    )
    if measurement_status == "awaiting_post_issue_data":
        _expect(
            observed_through is not None
            and observation_start is not None
            and observed_through < observation_start,
            "index: awaiting prospective status has an invalid source date",
            failures,
        )
    elif measurement_status == "collecting":
        _expect(
            observed_through is not None
            and observation_start is not None
            and matures_at is not None
            and observation_start <= observed_through < matures_at.date(),
            "index: collecting prospective status has an invalid source date",
            failures,
        )
    elif measurement_status == "mature":
        _expect(
            observed_through is not None
            and matures_at is not None
            and observed_through >= matures_at.date(),
            "index: mature prospective status is premature",
            failures,
        )
    expected_elapsed_days = None
    if issued_at is not None and observed_through is not None:
        expected_elapsed_days = min(
            365,
            max(0, (observed_through - issued_at.date()).days),
        )
    elapsed_days = status.get("elapsed_days")
    maturity_fraction = status.get("maturity_fraction")
    _expect(
        expected_elapsed_days is not None
        and elapsed_days == expected_elapsed_days
        and isinstance(maturity_fraction, (int, float))
        and not isinstance(maturity_fraction, bool)
        and abs(maturity_fraction - expected_elapsed_days / 365) <= 1e-12,
        "index: prospective maturity telemetry is inconsistent",
        failures,
    )

    metrics = status.get("metrics")
    _expect(
        isinstance(metrics, dict) and set(metrics) == {"top_100", "top_1000"},
        "index: prospective metrics are incomplete",
        failures,
    )
    for name, expected_count in (("top_100", 100), ("top_1000", 1000)):
        metric = metrics.get(name) if isinstance(metrics, dict) else None
        _expect(
            isinstance(metric, dict)
            and metric.get("eligible_parcels") == expected_count,
            f"index: prospective {name} population is invalid",
            failures,
        )
        if not isinstance(metric, dict):
            continue
        hits = metric.get("observed_nb_filing_hits")
        lower_bound = metric.get("observed_precision_lower_bound")
        final_precision = metric.get("final_precision")
        final_interval = metric.get("final_precision_95ci")
        if measurement_status == "awaiting_post_issue_data":
            _expect(
                hits is None
                and lower_bound is None
                and final_precision is None
                and final_interval is None,
                f"index: prospective {name} exposes premature metrics",
                failures,
            )
        elif measurement_status == "collecting":
            _expect(
                isinstance(hits, int)
                and not isinstance(hits, bool)
                and 0 <= hits <= expected_count
                and isinstance(lower_bound, (int, float))
                and not isinstance(lower_bound, bool)
                and abs(lower_bound - hits / expected_count) <= 1e-12
                and final_precision is None
                and final_interval is None,
                f"index: prospective {name} lower bound is invalid",
                failures,
            )
        elif measurement_status == "mature":
            _expect(
                isinstance(hits, int)
                and not isinstance(hits, bool)
                and 0 <= hits <= expected_count
                and isinstance(lower_bound, (int, float))
                and not isinstance(lower_bound, bool)
                and abs(lower_bound - hits / expected_count) <= 1e-12
                and isinstance(final_precision, (int, float))
                and not isinstance(final_precision, bool)
                and abs(final_precision - lower_bound) <= 1e-12
                and isinstance(final_interval, list)
                and len(final_interval) == 2,
                f"index: prospective {name} final metric is incomplete",
                failures,
            )
            _expect(
                isinstance(final_interval, list)
                and len(final_interval) == 2
                and all(
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and 0 <= value <= 1
                    for value in final_interval
                )
                and isinstance(final_precision, (int, float))
                and final_interval[0] <= final_precision <= final_interval[1],
                f"index: prospective {name} confidence interval is invalid",
                failures,
            )

    historical = status.get("historical_benchmark")
    _expect(
        isinstance(historical, dict)
        and historical.get("not_current_cohort_accuracy") is True,
        "index: historical and live validation scopes are conflated",
        failures,
    )
    sources = status.get("official_sources")
    _expect(
        isinstance(sources, list)
        and len(sources) == 2
        and {item.get("dataset_id") for item in sources if isinstance(item, dict)}
        == {"ic3t-wcy2", "w9ak-ipjd"},
        "index: prospective official DOB sources are incomplete",
        failures,
    )
    _expect(
        isinstance(sources, list)
        and all(
            isinstance(item, dict)
            and _parse_timestamp(item.get("rows_updated_at")) is not None
            and _has_explicit_timezone(item.get("rows_updated_at"))
            for item in sources
        ),
        "index: prospective official DOB source timestamps are invalid",
        failures,
    )
    source_timestamps = [
        parsed
        for item in (sources if isinstance(sources, list) else [])
        if isinstance(item, dict)
        for parsed in [_parse_timestamp(item.get("rows_updated_at"))]
        if parsed is not None
    ]
    report_reference = status.get("report_reference")
    observation_id = (
        report_reference.get("observation_id")
        if isinstance(report_reference, dict)
        else None
    )
    report_sha256 = (
        report_reference.get("sha256") if isinstance(report_reference, dict) else None
    )
    _expect(
        isinstance(report_reference, dict)
        and isinstance(observation_id, str)
        and len(observation_id) == 21
        and observation_id[8] == "-"
        and observation_id[:8].isdigit()
        and all(character in "0123456789abcdef" for character in observation_id[9:])
        and observed_through is not None
        and observation_id.startswith(observed_through.strftime("%Y%m%d") + "-")
        and isinstance(report_sha256, str)
        and len(report_sha256) == 64
        and all(character in "0123456789abcdef" for character in report_sha256),
        "index: prospective report reference is invalid",
        failures,
    )

    _expect(
        isinstance(health, dict),
        "index: prospective validation health is missing",
        failures,
    )
    health = health if isinstance(health, dict) else {}
    max_lag_days = health.get("max_observation_lag_days")
    observation_lag_days = health.get("observation_lag_days")
    expected_lag_days = (
        (current.date() - observed_through).days
        if observed_through is not None
        else None
    )
    expected_health_status = (
        "stale"
        if expected_lag_days is not None and expected_lag_days > 8
        else "current"
    )
    expected_health_reason = (
        "observation_lag_exceeded" if expected_health_status == "stale" else "current"
    )
    try:
        next_monitor_due_on = datetime.fromisoformat(
            str(health.get("next_monitor_due_on"))
        ).date()
    except ValueError:
        next_monitor_due_on = None
    oldest_source_updated_at = _parse_timestamp(
        health.get("oldest_official_source_updated_at")
    )
    _expect(
        expected_lag_days is not None
        and expected_lag_days >= 0
        and max_lag_days == 8
        and observation_lag_days == expected_lag_days
        and health.get("status") == expected_health_status
        and health.get("reason") == expected_health_reason
        and observed_through is not None
        and next_monitor_due_on == observed_through + timedelta(days=8)
        and len(source_timestamps) == 2
        and oldest_source_updated_at == min(source_timestamps),
        "index: prospective validation freshness telemetry is inconsistent",
        failures,
    )
    _expect(
        health.get("status") == "current",
        "index: prospective validation monitor is stale",
        failures,
    )

    forbidden = {
        "address",
        "bbl",
        "email",
        "matched_filings",
        "owner",
        "owner_name",
        "phone",
        "rank",
        "score",
        "score_calibrated",
    }

    def scan(value: Any, path: tuple[Any, ...] = ()) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).lower() in forbidden:
                    failures.append(
                        "index: prospective validation exposed private "
                        f"field {'.'.join(map(str, path + (key,)))}"
                    )
                scan(item, path + (key,))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                scan(item, path + (index,))

    scan(status)
    return failures


def validate_workflow_methodology(data: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    _expect(
        data.get("schema_version")
        == "citylens/parcel-workflow-analytics-methodology@v2",
        "workflow methodology: unexpected schema version",
        failures,
    )
    _expect(
        data.get("analytics_schema_version") == "citylens/parcel-workflow-analytics@v3",
        "workflow methodology: uncertainty-aware analytics v3 is not active",
        failures,
    )
    _expect(
        data.get("model_accuracy_claim") is False,
        "workflow methodology: outcomes must not claim model accuracy",
        failures,
    )
    horizons = data.get("horizons")
    observed = []
    if isinstance(horizons, list):
        for row in horizons:
            if isinstance(row, dict):
                observed.append((row.get("milestone"), row.get("horizon_days")))
    _expect(
        tuple(observed) == EXPECTED_WORKFLOW_HORIZONS,
        "workflow methodology: fixed horizons do not match the production contract",
        failures,
    )
    _expect(
        isinstance(data.get("minimum_rate_denominator"), int)
        and data["minimum_rate_denominator"] >= 10,
        "workflow methodology: minimum rate denominator must be at least 10",
        failures,
    )
    _expect(
        data.get("confidence_level") == 0.95
        and "Wilson" in str(data.get("uncertainty_semantics") or "")
        and "omitted from the API" in str(
            data.get("uncertainty_semantics") or ""
        ),
        "workflow methodology: API-level maturity-safe interval contract is not active",
        failures,
    )
    return failures


def evaluate_source_slas(
    index: dict[str, Any],
    *,
    now: datetime,
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Evaluate each decision-relevant source against its published SLA.

    The publisher owns the SLA durations in ``data_sources``. The production
    verifier independently recomputes age from ``retrieved_at`` so a stale
    source cannot pass merely because its cached ``age_days`` or ``stale`` flag
    is incorrect.
    """

    failures: list[str] = []
    warnings: list[str] = []
    sources = index.get("data_sources")
    if not isinstance(sources, dict):
        return (
            ["index: data_sources is missing"],
            warnings,
            {
                "passed": False,
                "warning_count": 0,
                "breach_count": len(REQUIRED_SOURCE_SLAS),
                "sources": {},
            },
        )

    source_report: dict[str, Any] = {}
    for key in REQUIRED_SOURCE_SLAS:
        raw = sources.get(key)
        if not isinstance(raw, dict):
            failures.append(f"index: source SLA {key} is missing")
            source_report[key] = {"status": "missing"}
            continue

        retrieved_at = _parse_timestamp(raw.get("retrieved_at"))
        max_age = raw.get("max_age_days")
        if retrieved_at is None:
            failures.append(
                f"index: source SLA {key} retrieved_at is missing or invalid"
            )
        if (
            not isinstance(max_age, (int, float))
            or isinstance(max_age, bool)
            or max_age <= 0
        ):
            failures.append(
                f"index: source SLA {key} max_age_days is missing or invalid"
            )
            max_age = None
        if max_age is not None and key in SOURCE_MAX_AGE_CAP_DAYS:
            max_age = min(float(max_age), SOURCE_MAX_AGE_CAP_DAYS[key])

        age_days = (
            max((now - retrieved_at).total_seconds(), 0.0) / 86400
            if retrieved_at is not None
            else None
        )
        warning_lead_days = (
            min(7.0, max(2.0, float(max_age) * 0.2)) if max_age is not None else None
        )
        remaining_days = (
            float(max_age) - age_days
            if max_age is not None and age_days is not None
            else None
        )

        status = "current"
        if retrieved_at is not None and retrieved_at > now:
            status = "breached"
            failures.append(f"index: source SLA {key} retrieved_at is in the future")
        elif raw.get("stale") is True or (
            age_days is not None and max_age is not None and age_days > float(max_age)
        ):
            status = "breached"
            failures.append(
                f"index: source SLA {key} is stale"
                + (
                    f" ({age_days:.1f} days old; limit {float(max_age):.1f})"
                    if age_days is not None and max_age is not None
                    else ""
                )
            )
        elif (
            remaining_days is not None
            and warning_lead_days is not None
            and remaining_days <= warning_lead_days
        ):
            status = "warning"
            warnings.append(
                f"index: source SLA {key} has {remaining_days:.1f} days remaining"
            )

        source_report[key] = {
            "source": raw.get("source"),
            "retrieved_at": raw.get("retrieved_at"),
            "age_days": round(age_days, 2) if age_days is not None else None,
            "max_age_days": float(max_age) if max_age is not None else None,
            "remaining_days": (
                round(remaining_days, 2) if remaining_days is not None else None
            ),
            "status": status,
        }

    return (
        failures,
        warnings,
        {
            "passed": not failures,
            "warning_count": len(warnings),
            "breach_count": sum(
                row.get("status") in {"breached", "missing"}
                for row in source_report.values()
            ),
            "sources": source_report,
        },
    )


def validate_index(
    index: dict[str, Any],
    *,
    max_age_days: float,
    now: datetime,
) -> list[str]:
    failures: list[str] = []
    generated_at = _parse_timestamp(index.get("generated_at"))
    _expect(
        generated_at is not None, "index: generated_at is missing or invalid", failures
    )
    if generated_at is not None:
        age_days = max((now - generated_at).total_seconds(), 0.0) / 86400
        _expect(
            age_days <= max_age_days,
            f"index: feed is {age_days:.1f} days old (limit {max_age_days:.1f})",
            failures,
        )
    _expect(index.get("stale") is False, "index: API marks the feed stale", failures)
    source_failures, _, _ = evaluate_source_slas(index, now=now)
    failures.extend(source_failures)

    boroughs = index.get("boroughs")
    _expect(isinstance(boroughs, list), "index: boroughs is not a list", failures)
    borough_rows = boroughs if isinstance(boroughs, list) else []
    by_slug = {row.get("slug"): row for row in borough_rows if isinstance(row, dict)}
    _expect(
        set(by_slug) == set(BOROUGHS),
        "index: expected exactly five NYC boroughs",
        failures,
    )

    quality = index.get("quality_gate")
    _expect(isinstance(quality, dict), "index: quality_gate is missing", failures)
    quality = quality if isinstance(quality, dict) else {}
    selection_policy = quality.get("selection_policy")
    selection_policy = selection_policy if isinstance(selection_policy, dict) else None
    if selection_policy is None:
        # Transitional support for the already-active v5 equal-borough feed.
        for slug in BOROUGHS:
            row = by_slug.get(slug) or {}
            _expect(
                row.get("count") == 1000,
                f"index: legacy {slug} count is not 1000",
                failures,
            )
    else:
        _expect(
            selection_policy.get("schema")
            == "citylens-parcel-intel/selection-policy@v1",
            "index: selection policy schema is invalid",
            failures,
        )
        _expect(
            selection_policy.get("policy_id") == "borough_floor_250",
            "index: selection policy ID is not borough_floor_250",
            failures,
        )
        _expect(
            selection_policy.get("target_count") == 5000
            and selection_policy.get("selected_count") == 5000
            and selection_policy.get("eligible_selected_count") == 5000,
            "index: selection policy did not produce 5,000 eligible leads",
            failures,
        )
        _expect(
            selection_policy.get("minimum_per_borough") == 250
            and selection_policy.get("effective_minimum_per_borough") == 250,
            "index: selection policy borough floor is invalid",
            failures,
        )
        _expect(
            selection_policy.get("passed") is True
            and selection_policy.get("failures") == [],
            "index: selection policy receipt did not pass",
            failures,
        )
        membership_sha = selection_policy.get("membership_sha256")
        _expect(
            isinstance(membership_sha, str)
            and len(membership_sha) == 64
            and all(character in "0123456789abcdef" for character in membership_sha),
            "index: selection membership digest is invalid",
            failures,
        )
        selection_boroughs = selection_policy.get("by_borough")
        selection_boroughs = (
            selection_boroughs if isinstance(selection_boroughs, dict) else {}
        )
        _expect(
            sum(int((by_slug.get(slug) or {}).get("count") or 0) for slug in BOROUGHS)
            == 5000,
            "index: selected borough counts do not sum to 5,000",
            failures,
        )
        for slug in BOROUGHS:
            index_count = (by_slug.get(slug) or {}).get("count")
            policy_borough = selection_boroughs.get(slug)
            policy_borough = policy_borough if isinstance(policy_borough, dict) else {}
            _expect(
                isinstance(index_count, int)
                and index_count >= 250
                and policy_borough.get("selected_count") == index_count
                and policy_borough.get("requested_minimum_satisfied") is True,
                f"index: {slug} selection count/floor receipt is invalid",
                failures,
            )
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
    land_use_reconciliation = quality.get("land_use_reconciliation")
    _expect(
        isinstance(land_use_reconciliation, dict),
        "index: land-use source reconciliation is missing",
        failures,
    )
    land_use_reconciliation = (
        land_use_reconciliation if isinstance(land_use_reconciliation, dict) else {}
    )
    _expect(
        land_use_reconciliation.get("schema")
        == "citylens-parcel-intel/land-use-reconciliation@v1",
        "index: land-use reconciliation schema is invalid",
        failures,
    )
    _expect(
        land_use_reconciliation.get("source_schema")
        == "citylens-parcel-intel/zap-activity@v1",
        "index: land-use reconciliation source schema is invalid",
        failures,
    )
    source_sha256 = land_use_reconciliation.get("source_sha256")
    _expect(
        isinstance(source_sha256, str)
        and len(source_sha256) == 64
        and all(character in "0123456789abcdef" for character in source_sha256),
        "index: land-use reconciliation source digest is invalid",
        failures,
    )
    source_blocked_count = land_use_reconciliation.get("source_blocked_bbl_count")
    _expect(
        isinstance(source_blocked_count, int) and source_blocked_count > 0,
        "index: land-use reconciliation has no blocked source BBLs",
        failures,
    )
    _expect(
        land_use_reconciliation.get("declared_blocked_bbl_count")
        == source_blocked_count,
        "index: land-use reconciliation blocked counts disagree",
        failures,
    )
    private_current_project_count = land_use_reconciliation.get(
        "private_current_project_count"
    )
    non_parcel_applicable_project_count = land_use_reconciliation.get(
        "non_parcel_applicable_project_count"
    )
    non_parcel_applicable_project_ids = land_use_reconciliation.get(
        "non_parcel_applicable_project_ids"
    )
    blocking_project_count = land_use_reconciliation.get("blocking_project_count")
    joined_blocking_project_count = land_use_reconciliation.get(
        "joined_blocking_project_count"
    )
    unjoined_blocking_project_count = land_use_reconciliation.get(
        "unjoined_blocking_project_count"
    )
    _expect(
        isinstance(private_current_project_count, int)
        and not isinstance(private_current_project_count, bool)
        and private_current_project_count > 0,
        "index: land-use reconciliation private project count is invalid",
        failures,
    )
    _expect(
        isinstance(non_parcel_applicable_project_count, int)
        and not isinstance(non_parcel_applicable_project_count, bool)
        and non_parcel_applicable_project_count >= 0,
        "index: land-use reconciliation non-parcel project count is invalid",
        failures,
    )
    _expect(
        isinstance(non_parcel_applicable_project_ids, list)
        and all(
            isinstance(project_id, str) and bool(project_id.strip())
            for project_id in non_parcel_applicable_project_ids
        )
        and len(set(non_parcel_applicable_project_ids))
        == len(non_parcel_applicable_project_ids)
        and len(non_parcel_applicable_project_ids)
        == non_parcel_applicable_project_count,
        "index: land-use reconciliation non-parcel project IDs are invalid",
        failures,
    )
    _expect(
        isinstance(blocking_project_count, int)
        and not isinstance(blocking_project_count, bool)
        and blocking_project_count > 0,
        "index: land-use reconciliation has no blocking projects",
        failures,
    )
    _expect(
        isinstance(joined_blocking_project_count, int)
        and not isinstance(joined_blocking_project_count, bool)
        and joined_blocking_project_count >= 0,
        "index: land-use reconciliation joined project count is invalid",
        failures,
    )
    _expect(
        isinstance(unjoined_blocking_project_count, int)
        and not isinstance(unjoined_blocking_project_count, bool)
        and unjoined_blocking_project_count >= 0,
        "index: land-use reconciliation unjoined project count is invalid",
        failures,
    )
    _expect(
        isinstance(blocking_project_count, int)
        and isinstance(joined_blocking_project_count, int)
        and isinstance(unjoined_blocking_project_count, int)
        and joined_blocking_project_count + unjoined_blocking_project_count
        == blocking_project_count,
        "index: land-use reconciliation project counts disagree",
        failures,
    )
    _expect(
        isinstance(private_current_project_count, int)
        and isinstance(non_parcel_applicable_project_count, int)
        and isinstance(blocking_project_count, int)
        and blocking_project_count + non_parcel_applicable_project_count
        == private_current_project_count,
        "index: land-use reconciliation project scope counts disagree",
        failures,
    )
    unjoined_blocking_project_ids = land_use_reconciliation.get(
        "unjoined_blocking_project_ids"
    )
    _expect(
        isinstance(unjoined_blocking_project_ids, list)
        and all(
            isinstance(project_id, str) and bool(project_id.strip())
            for project_id in unjoined_blocking_project_ids
        )
        and len(set(unjoined_blocking_project_ids))
        == len(unjoined_blocking_project_ids)
        and len(unjoined_blocking_project_ids) == unjoined_blocking_project_count,
        "index: land-use reconciliation unresolved project IDs are invalid",
        failures,
    )
    minimum_project_bbl_crosswalk_coverage = land_use_reconciliation.get(
        "minimum_project_bbl_crosswalk_coverage"
    )
    _expect(
        isinstance(minimum_project_bbl_crosswalk_coverage, (int, float))
        and not isinstance(minimum_project_bbl_crosswalk_coverage, bool)
        and minimum_project_bbl_crosswalk_coverage == 1.0,
        "index: land-use reconciliation coverage floor is not 1.0",
        failures,
    )
    project_bbl_crosswalk_coverage = land_use_reconciliation.get(
        "project_bbl_crosswalk_coverage"
    )
    _expect(
        isinstance(project_bbl_crosswalk_coverage, (int, float))
        and not isinstance(project_bbl_crosswalk_coverage, bool)
        and project_bbl_crosswalk_coverage == 1.0,
        "index: land-use project-to-BBL coverage is below 100%",
        failures,
    )
    _expect(
        land_use_reconciliation.get("project_detail_source")
        == ("https://zap-api-production.herokuapp.com/projects/{project_id}"),
        "index: land-use project-detail source is invalid",
        failures,
    )
    project_detail_retrieved_at = _parse_timestamp(
        land_use_reconciliation.get("project_detail_retrieved_at")
    )
    _expect(
        project_detail_retrieved_at is not None,
        "index: land-use project-detail timestamp is invalid",
        failures,
    )
    if project_detail_retrieved_at is not None:
        project_detail_age_days = (
            max((now - project_detail_retrieved_at).total_seconds(), 0.0) / 86400
        )
        _expect(
            project_detail_age_days <= 45,
            (
                "index: land-use project details are "
                f"{project_detail_age_days:.1f} days old"
            ),
            failures,
        )
    project_detail_supplemental_relation_count = land_use_reconciliation.get(
        "project_detail_supplemental_relation_count"
    )
    _expect(
        isinstance(project_detail_supplemental_relation_count, int)
        and not isinstance(project_detail_supplemental_relation_count, bool)
        and project_detail_supplemental_relation_count >= 0,
        "index: land-use supplemental relation count is invalid",
        failures,
    )
    _expect(
        land_use_reconciliation.get("project_detail_fetch_failure_count") == 0,
        "index: land-use project-detail refresh has failures",
        failures,
    )
    _expect(
        land_use_reconciliation.get("project_detail_fetch_failure_ids") == [],
        "index: land-use project-detail failure IDs are not empty",
        failures,
    )
    official_document_schema = land_use_reconciliation.get(
        "official_document_supplement_schema"
    )
    _expect(
        official_document_schema
        == ("citylens-parcel-intel/zap-official-document-bbl-supplements@v1"),
        "index: official ZAP document supplement schema is invalid",
        failures,
    )
    official_document_reviewed_at = _parse_timestamp(
        land_use_reconciliation.get("official_document_supplement_reviewed_at")
    )
    _expect(
        official_document_reviewed_at is not None,
        "index: official ZAP document review timestamp is invalid",
        failures,
    )
    if official_document_reviewed_at is not None:
        official_document_review_age_days = (
            now - official_document_reviewed_at
        ).total_seconds() / 86400
        _expect(
            -1 <= official_document_review_age_days <= 365,
            (
                "index: official ZAP document review age is "
                f"{official_document_review_age_days:.1f} days"
            ),
            failures,
        )
    official_document_sha256 = land_use_reconciliation.get(
        "official_document_supplement_sha256"
    )
    _expect(
        isinstance(official_document_sha256, str)
        and len(official_document_sha256) == 64
        and all(
            character in "0123456789abcdef" for character in official_document_sha256
        ),
        "index: official ZAP document supplement digest is invalid",
        failures,
    )
    official_document_source_count = land_use_reconciliation.get(
        "official_document_source_count"
    )
    official_document_relation_count = land_use_reconciliation.get(
        "official_document_supplemental_relation_count"
    )
    official_document_project_count = land_use_reconciliation.get(
        "official_document_supplemental_project_count"
    )
    official_document_project_ids = land_use_reconciliation.get(
        "official_document_supplemental_project_ids"
    )
    _expect(
        isinstance(official_document_source_count, int)
        and not isinstance(official_document_source_count, bool)
        and official_document_source_count >= 0,
        "index: official ZAP document source count is invalid",
        failures,
    )
    _expect(
        isinstance(official_document_relation_count, int)
        and not isinstance(official_document_relation_count, bool)
        and official_document_relation_count >= 0,
        "index: official ZAP document relation count is invalid",
        failures,
    )
    _expect(
        isinstance(official_document_project_count, int)
        and not isinstance(official_document_project_count, bool)
        and official_document_project_count >= 0,
        "index: official ZAP document project count is invalid",
        failures,
    )
    _expect(
        isinstance(official_document_project_ids, list)
        and all(
            isinstance(project_id, str) and bool(project_id.strip())
            for project_id in official_document_project_ids
        )
        and len(set(official_document_project_ids))
        == len(official_document_project_ids)
        and len(official_document_project_ids) == official_document_project_count,
        "index: official ZAP document project IDs are invalid",
        failures,
    )
    _expect(
        isinstance(official_document_source_count, int)
        and isinstance(official_document_project_count, int)
        and official_document_source_count >= official_document_project_count,
        "index: official ZAP document sources are below project count",
        failures,
    )
    _expect(
        isinstance(official_document_relation_count, int)
        and isinstance(official_document_project_count, int)
        and official_document_relation_count >= official_document_project_count,
        "index: official ZAP document relations are below project count",
        failures,
    )
    current_tax_lot_candidate_count = land_use_reconciliation.get(
        "current_tax_lot_reconciliation_candidate_count"
    )
    current_tax_lot_relation_count = land_use_reconciliation.get(
        "current_tax_lot_reconciled_relation_count"
    )
    current_tax_lot_project_count = land_use_reconciliation.get(
        "current_tax_lot_reconciled_project_count"
    )
    current_tax_lot_project_ids = land_use_reconciliation.get(
        "current_tax_lot_reconciled_project_ids"
    )
    current_tax_lot_unmatched_count = land_use_reconciliation.get(
        "current_tax_lot_unmatched_user_input_count"
    )
    current_tax_lot_universe_count = land_use_reconciliation.get(
        "current_tax_lot_universe_count"
    )
    current_tax_lot_index_sha256 = land_use_reconciliation.get(
        "current_tax_lot_index_sha256"
    )
    _expect(
        isinstance(current_tax_lot_candidate_count, int)
        and not isinstance(current_tax_lot_candidate_count, bool)
        and current_tax_lot_candidate_count > 0,
        "index: current-tax-lot reconciliation has no candidates",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_relation_count, int)
        and not isinstance(current_tax_lot_relation_count, bool)
        and current_tax_lot_relation_count > 0
        and isinstance(current_tax_lot_candidate_count, int)
        and current_tax_lot_relation_count <= current_tax_lot_candidate_count,
        "index: current-tax-lot reconciled relation count is invalid",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_project_count, int)
        and not isinstance(current_tax_lot_project_count, bool)
        and current_tax_lot_project_count > 0
        and isinstance(current_tax_lot_relation_count, int)
        and current_tax_lot_project_count <= current_tax_lot_relation_count,
        "index: current-tax-lot reconciled project count is invalid",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_project_ids, list)
        and all(
            isinstance(project_id, str) and bool(project_id.strip())
            for project_id in current_tax_lot_project_ids
        )
        and len(set(current_tax_lot_project_ids)) == len(current_tax_lot_project_ids)
        and len(current_tax_lot_project_ids) == current_tax_lot_project_count,
        "index: current-tax-lot reconciled project IDs are invalid",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_unmatched_count, int)
        and not isinstance(current_tax_lot_unmatched_count, bool)
        and current_tax_lot_unmatched_count >= 0,
        "index: current-tax-lot unmatched input count is invalid",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_universe_count, int)
        and not isinstance(current_tax_lot_universe_count, bool)
        and current_tax_lot_universe_count >= 800_000,
        "index: current PLUTO tax-lot universe is invalid",
        failures,
    )
    _expect(
        isinstance(current_tax_lot_index_sha256, str)
        and len(current_tax_lot_index_sha256) == 64
        and all(
            character in "0123456789abcdef"
            for character in current_tax_lot_index_sha256
        ),
        "index: current PLUTO tax-lot digest is invalid",
        failures,
    )
    _expect(
        isinstance(land_use_reconciliation.get("candidate_blocked_bbl_count"), int)
        and land_use_reconciliation["candidate_blocked_bbl_count"] > 0,
        "index: land-use reconciliation exercised no blocked candidates",
        failures,
    )
    _expect(
        land_use_reconciliation.get("published_leakage_count") == 0,
        "index: authoritative ZAP-blocked BBL leaked into published leads",
        failures,
    )
    _expect(
        land_use_reconciliation.get("passed") is True,
        "index: land-use reconciliation did not pass",
        failures,
    )
    _expect(
        land_use_reconciliation.get("failures") == [],
        "index: land-use reconciliation has failures",
        failures,
    )
    ranking_tie_audit = quality.get("ranking_tie_audit")
    _expect(
        isinstance(ranking_tie_audit, dict),
        "index: ranking tie audit is missing",
        failures,
    )
    ranking_tie_audit = ranking_tie_audit if isinstance(ranking_tie_audit, dict) else {}
    _expect(
        ranking_tie_audit.get("schema") == "citylens-parcel-intel/ranking-tie-audit@v1",
        "index: ranking tie audit schema is invalid",
        failures,
    )
    _expect(
        ranking_tie_audit.get("primary_field") == "score_calibrated"
        and ranking_tie_audit.get("tiebreaker_field") == "score_raw"
        and ranking_tie_audit.get("tiebreaker_scope")
        == "equal_calibrated_probability_only"
        and ranking_tie_audit.get("tiebreaker_is_public") is False
        and (
            (
                selection_policy is None
                and ranking_tie_audit.get("deterministic_fallback")
                == ["model_rank", "bbl"]
            )
            or (
                selection_policy is not None
                and ranking_tie_audit.get("deterministic_fallback") == ["bbl"]
                and ranking_tie_audit.get("borough_deterministic_fallback")
                == ["model_rank", "bbl"]
            )
        ),
        "index: ranking tie policy is invalid",
        failures,
    )
    _expect(
        ranking_tie_audit.get("passed") is True
        and ranking_tie_audit.get("failures") == [],
        "index: ranking tie audit did not pass",
        failures,
    )
    ranking_tie_boroughs = ranking_tie_audit.get("boroughs")
    ranking_tie_boroughs = (
        ranking_tie_boroughs if isinstance(ranking_tie_boroughs, dict) else {}
    )
    for slug in BOROUGHS:
        tie_stats = ranking_tie_boroughs.get(slug)
        tie_stats = tie_stats if isinstance(tie_stats, dict) else {}
        expected_borough_count = (by_slug.get(slug) or {}).get("count")
        _expect(
            tie_stats.get("row_count") == expected_borough_count
            and tie_stats.get("tiebreaker_count") == expected_borough_count
            and tie_stats.get("tiebreaker_coverage") == 1.0,
            f"index: {slug} ranking tie-break coverage is incomplete",
            failures,
        )
    citywide_tie_stats = ranking_tie_audit.get("citywide")
    citywide_tie_stats = (
        citywide_tie_stats if isinstance(citywide_tie_stats, dict) else {}
    )
    _expect(
        citywide_tie_stats.get("row_count") == 5000
        and citywide_tie_stats.get("tiebreaker_count") == 5000
        and citywide_tie_stats.get("tiebreaker_coverage") == 1.0,
        "index: citywide ranking tie-break coverage is incomplete",
        failures,
    )
    quality_boroughs = quality.get("boroughs")
    quality_boroughs = quality_boroughs if isinstance(quality_boroughs, dict) else {}
    for slug in BOROUGHS:
        row = quality_boroughs.get(slug)
        _expect(
            isinstance(row, dict), f"index: {slug} quality report missing", failures
        )
        row = row if isinstance(row, dict) else {}
        _expect(
            row.get("passed") is True, f"index: {slug} quality gate failed", failures
        )
        _expect(
            row.get("row_count") == (by_slug.get(slug) or {}).get("count"),
            f"index: {slug} quality row_count does not match manifest",
            failures,
        )
        for field in (
            "project_leakage_count",
            "land_use_project_leakage_count",
            "authoritative_zap_bbl_leakage_count",
            "duplicate_bbl_count",
            "invalid_owner_leakage_count",
            "non_private_owner_leakage_count",
            "negative_unused_floor_area_count",
            "invalid_owner_portfolio_count",
        ):
            _expect(row.get(field) == 0, f"index: {slug} {field} is not zero", failures)
        for field in (
            "owner_coverage",
            "geometry_coverage",
            "floodplain_coverage",
            "environmental_review_coverage",
            "mih_coverage",
            "transit_coverage",
        ):
            _expect(
                row.get(field) == 1.0,
                f"index: {slug} {field} is not complete",
                failures,
            )

    generation_diff = index.get("generation_diff")
    _expect(
        isinstance(generation_diff, dict),
        "index: generation_diff is missing",
        failures,
    )
    generation_diff = generation_diff if isinstance(generation_diff, dict) else {}
    _expect(
        generation_diff.get("schema") == "citylens-parcel-intel/generation-diff@v1",
        "index: generation_diff schema is invalid",
        failures,
    )
    _expect(
        generation_diff.get("status") == "compared",
        "index: generation_diff is not in comparison mode",
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
    feature_drift = generation_diff.get("inference_feature_drift")
    _expect(
        isinstance(feature_drift, dict),
        "index: inference feature drift report is missing",
        failures,
    )
    feature_drift = feature_drift if isinstance(feature_drift, dict) else {}
    _expect(
        feature_drift.get("schema")
        == "citylens-parcel-intel/inference-feature-drift@v1",
        "index: inference feature drift schema is invalid",
        failures,
    )
    _expect(
        feature_drift.get("status") == "compared",
        "index: inference feature drift is not in comparison mode",
        failures,
    )
    feature_candidate = feature_drift.get("candidate")
    feature_candidate = feature_candidate if isinstance(feature_candidate, dict) else {}
    _expect(
        feature_candidate.get("row_count") == 5000,
        "index: inference feature row count is not 5,000",
        failures,
    )
    _expect(
        feature_candidate.get("column_count") == 142,
        "index: inference feature column count is not 142",
        failures,
    )
    feature_fingerprint = feature_candidate.get("feature_spec_sha256")
    _expect(
        isinstance(feature_fingerprint, str)
        and len(feature_fingerprint) == 64
        and all(character in "0123456789abcdef" for character in feature_fingerprint),
        "index: inference feature fingerprint is invalid",
        failures,
    )
    feature_gate = feature_drift.get("gate")
    _expect(
        isinstance(feature_gate, dict),
        "index: inference feature drift gate is missing",
        failures,
    )
    feature_gate = feature_gate if isinstance(feature_gate, dict) else {}
    feature_failures = feature_gate.get("failures")
    feature_failures = (
        [str(item) for item in feature_failures]
        if isinstance(feature_failures, list)
        else []
    )
    diff_failures = diff_gate.get("failures")
    diff_failures = (
        {str(item) for item in diff_failures}
        if isinstance(diff_failures, list)
        else set()
    )
    reviewed_feature_override = (
        feature_gate.get("passed") is False
        and bool(feature_failures)
        and set(feature_failures).issubset(diff_failures)
        and diff_gate.get("passed") is True
        and diff_gate.get("thresholds_passed") is False
        and diff_gate.get("override_applied") is True
        and isinstance(diff_gate.get("override_reason"), str)
        and bool(diff_gate["override_reason"].strip())
    )
    _expect(
        feature_gate.get("passed") is True or reviewed_feature_override,
        "index: inference feature drift gate did not pass",
        failures,
    )
    replay = index.get("inference_replay")
    _expect(
        isinstance(replay, dict),
        "index: inference score replay is missing",
        failures,
    )
    replay = replay if isinstance(replay, dict) else {}
    _expect(
        replay.get("schema") == "citylens-parcel-intel/inference-replay@v1",
        "index: inference score replay schema is invalid",
        failures,
    )
    _expect(
        replay.get("passed") is True
        and replay.get("status") == "matched"
        and replay.get("row_count") == 5000
        and replay.get("mismatch_count") == 0,
        "index: inference score replay did not match all 5,000 rows",
        failures,
    )
    _expect(
        replay.get("maximum_absolute_error") == 0.0,
        "index: inference score replay has non-zero maximum error",
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
    evaluation_evidence = model.get("evaluation_evidence")
    evaluation_evidence = (
        evaluation_evidence if isinstance(evaluation_evidence, dict) else {}
    )
    _expect(
        evaluation_evidence.get("schema") == "citylens_model_evaluation_evidence@v1",
        "index: model evaluation evidence is missing or invalid",
        failures,
    )
    _expect(
        evaluation_evidence.get("status") == "development_exposed",
        "index: accepted historical benchmark exposure is misclassified",
        failures,
    )
    _expect(
        evaluation_evidence.get("independent_for_future_selection") is False
        and evaluation_evidence.get("production_promotion_eligible") is False,
        "index: exposed benchmark is incorrectly eligible for model selection",
        failures,
    )
    failures.extend(
        validate_historical_benchmark_receipt(
            model.get("historical_benchmark_receipt"),
            label="index",
        )
    )
    borough_receipt = model.get("historical_borough_benchmark_receipt")
    if borough_receipt is not None:
        failures.extend(
            validate_historical_borough_benchmark_receipt(
                borough_receipt,
                label="index",
            )
        )
    performance_scope = model.get("performance_scope")
    _expect(
        isinstance(performance_scope, str)
        and "historical rolling benchmark" in performance_scope.lower()
        and "untouched" not in performance_scope.lower(),
        "index: historical benchmark scope overstates evaluation independence",
        failures,
    )
    model_ranking_policy = model.get("ranking_policy")
    model_ranking_policy = (
        model_ranking_policy if isinstance(model_ranking_policy, dict) else {}
    )
    _expect(
        model_ranking_policy.get("primary_field") == "score_calibrated"
        and model_ranking_policy.get("tiebreaker_field") == "score_raw"
        and model_ranking_policy.get("tiebreaker_scope")
        == "equal_calibrated_probability_only"
        and model_ranking_policy.get("tiebreaker_is_public") is False
        and (
            (
                selection_policy is None
                and model_ranking_policy.get("deterministic_fallback")
                == ["model_rank", "bbl"]
            )
            or (
                selection_policy is not None
                and model_ranking_policy.get("deterministic_fallback") == ["bbl"]
                and model_ranking_policy.get("borough_rank_fallback")
                == ["model_rank", "bbl"]
            )
        ),
        "index: model ranking policy is invalid",
        failures,
    )
    failures.extend(
        validate_prospective_validation(
            index.get("prospective_validation"),
            feed_generation=index.get("feed_generation"),
            health=index.get("prospective_validation_health"),
            now=now,
        )
    )
    return failures


def _validate_public_row(row: dict[str, Any], label: str) -> list[str]:
    failures: list[str] = []
    _expect(
        isinstance(row.get("bbl"), str)
        and len(row["bbl"]) == 10
        and row["bbl"].isdigit(),
        f"{label}: invalid BBL",
        failures,
    )
    _expect(
        row.get("acquisition_eligible") is True,
        f"{label}: lead is not eligible",
        failures,
    )
    _expect(
        row.get("acquisition_status") == "eligible",
        f"{label}: acquisition status is not eligible",
        failures,
    )
    _expect(
        row.get("address_source") in {"nyc_pad", "nyc_pluto", "model_sweep"},
        f"{label}: address source provenance is missing or invalid",
        failures,
    )
    _expect(
        row.get("opportunity_category") not in {"active_project", "completed_project"},
        f"{label}: active/completed project leaked into eligible leads",
        failures,
    )
    for field in PRIVATE_NULL_FIELDS:
        _expect(
            row.get(field) is None,
            f"{label}: private field {field} was exposed",
            failures,
        )
    _expect(
        row.get("top_features", []) == [],
        f"{label}: SHAP features were exposed",
        failures,
    )
    for field in (
        "change_added_count",
        "change_demolished_count",
        "change_modified_count",
    ):
        _expect(
            row.get(field, 0) == 0,
            f"{label}: private field {field} was exposed",
            failures,
        )
    _expect(
        row.get("recent_change", False) is False,
        f"{label}: recent_change was exposed",
        failures,
    )
    for field in ("dob_safety_active_count", "ecb_active_count", "hpd_open_count"):
        _expect(
            row.get(field, 0) == 0,
            f"{label}: private field {field} was exposed",
            failures,
        )
    return failures


def validate_public_decision_audit(
    payload: dict[str, Any],
    *,
    model_metadata: dict[str, Any],
) -> list[str]:
    """Validate the public property-level decision explanation and redaction.

    The audit is deliberately narrower than the authenticated parcel record:
    it may explain the published historical model and deterministic eligibility
    policy, but it must not expose owner or diligence evidence.
    """

    failures: list[str] = []
    audit = payload.get("decision_audit")
    _expect(
        isinstance(audit, dict),
        "parcel detail: decision_audit is missing",
        failures,
    )
    audit = audit if isinstance(audit, dict) else {}
    _expect(
        audit.get("schema_version") == "citylens/parcel-decision-audit@v1",
        "parcel detail: decision audit schema is invalid",
        failures,
    )
    _expect(
        _parse_timestamp(audit.get("evidence_generated_at")) is not None
        and _has_explicit_timezone(audit.get("evidence_generated_at")),
        "parcel detail: decision audit evidence version is missing or invalid",
        failures,
    )
    _expect(
        audit.get("overall_status")
        in {"screened", "screened_with_flags", "excluded", "incomplete"},
        "parcel detail: decision audit status is invalid",
        failures,
    )
    _expect(
        isinstance(audit.get("overall_label"), str)
        and bool(audit["overall_label"].strip()),
        "parcel detail: decision audit label is missing",
        failures,
    )

    readiness = audit.get("readiness")
    _expect(
        isinstance(readiness, dict),
        "parcel detail: decision readiness block is missing",
        failures,
    )
    readiness = readiness if isinstance(readiness, dict) else {}
    _expect(
        readiness.get("status") == "limited_preview",
        "parcel detail: anonymous decision readiness is not a limited preview",
        failures,
    )
    _expect(
        isinstance(readiness.get("recommended_action"), str)
        and bool(readiness["recommended_action"].strip()),
        "parcel detail: decision readiness action is missing",
        failures,
    )
    for key in ("blockers", "review_items", "cleared_items"):
        _expect(
            isinstance(readiness.get(key), list),
            f"parcel detail: decision readiness {key} is invalid",
            failures,
        )
    _expect(
        readiness.get("blockers") == [],
        "parcel detail: public eligible lead unexpectedly has readiness blockers",
        failures,
    )
    readiness_disclaimer = str(readiness.get("disclaimer") or "").lower()
    for phrase in ("purchase recommendation", "seller-intent score"):
        _expect(
            phrase in readiness_disclaimer,
            f"parcel detail: readiness disclaimer omits {phrase}",
            failures,
        )
    public_readiness_text = " ".join(
        [
            str(readiness.get("label") or ""),
            str(readiness.get("recommended_action") or ""),
            *(str(value) for value in readiness.get("review_items", [])),
            *(str(value) for value in readiness.get("cleared_items", [])),
        ]
    ).lower()
    for phrase in (
        "tax-lien",
        "immediate-hazard",
        "floodplain",
        "environmental review",
        "mandatory inclusionary housing",
        "recent aerial change",
    ):
        _expect(
            phrase not in public_readiness_text,
            f"parcel detail: anonymous readiness exposed {phrase}",
            failures,
        )

    validation = audit.get("validation")
    _expect(
        isinstance(validation, dict),
        "parcel detail: historical validation block is missing",
        failures,
    )
    validation = validation if isinstance(validation, dict) else {}
    _expect(
        validation.get("target") == "dob_nb_job_filing",
        "parcel detail: historical validation target is incorrect",
        failures,
    )
    _expect(
        validation.get("prospective_validated") is False,
        "parcel detail: prospective validation must remain false",
        failures,
    )
    disclaimer = str(validation.get("disclaimer") or "").lower()
    for phrase in ("seller intent", "transaction probability"):
        _expect(
            phrase in disclaimer,
            f"parcel detail: audit disclaimer omits {phrase}",
            failures,
        )
    for audit_key, model_key in (
        ("precision_at_100", "precision_at_100"),
        ("precision_at_1000", "precision_at_1000"),
        ("base_rate", "spatial_cv_base_rate"),
    ):
        observed = validation.get(audit_key)
        expected = model_metadata.get(model_key)
        _expect(
            isinstance(observed, (int, float))
            and not isinstance(observed, bool)
            and isinstance(expected, (int, float))
            and not isinstance(expected, bool)
            and abs(float(observed) - float(expected)) <= 1e-12,
            f"parcel detail: {audit_key} does not match accepted model metadata",
            failures,
        )
    audit_receipt = validation.get("historical_benchmark_receipt")
    model_receipt = model_metadata.get("historical_benchmark_receipt")
    failures.extend(
        validate_historical_benchmark_receipt(
            audit_receipt,
            label="parcel detail",
        )
    )
    _expect(
        audit_receipt == model_receipt,
        "parcel detail: historical benchmark receipt does not match accepted model metadata",
        failures,
    )
    audit_borough_receipt = validation.get("historical_borough_benchmark_receipt")
    model_borough_receipt = model_metadata.get("historical_borough_benchmark_receipt")
    selected_borough_cohort = validation.get("historical_borough_cohort")
    if model_borough_receipt is None:
        _expect(
            audit_borough_receipt is None and selected_borough_cohort is None,
            "parcel detail: historical borough evidence was invented",
            failures,
        )
    else:
        failures.extend(
            validate_historical_borough_benchmark_receipt(
                audit_borough_receipt,
                label="parcel detail",
            )
        )
        _expect(
            audit_borough_receipt == model_borough_receipt,
            "parcel detail: historical borough receipt does not match accepted model metadata",
            failures,
        )
        borough_slug = payload.get("borough")
        model_boroughs = model_borough_receipt.get("boroughs")
        model_boroughs = model_boroughs if isinstance(model_boroughs, dict) else {}
        expected_cohort = (
            {
                "borough": borough_slug,
                "target": model_borough_receipt.get("target"),
                "feature_origin": model_borough_receipt.get("feature_origin"),
                "outcome_window": model_borough_receipt.get("outcome_window"),
                "evaluation_scope": model_borough_receipt.get("evaluation_scope"),
                "ranking_scope": model_borough_receipt.get("ranking_scope"),
                "cohort": model_boroughs[borough_slug],
                "interval": model_borough_receipt.get("interval"),
                "evidence_status": model_borough_receipt.get("evidence_status"),
                "not_current_accuracy": True,
                "not_parcel_confidence": True,
            }
            if borough_slug in model_boroughs
            else None
        )
        _expect(
            selected_borough_cohort == expected_cohort,
            "parcel detail: selected historical borough cohort is inconsistent",
            failures,
        )

    checks = audit.get("checks")
    _expect(
        isinstance(checks, list),
        "parcel detail: decision audit checks are missing",
        failures,
    )
    check_rows = checks if isinstance(checks, list) else []
    by_key = {
        row.get("key"): row
        for row in check_rows
        if isinstance(row, dict) and isinstance(row.get("key"), str)
    }
    required_keys = {
        "historical_model",
        "address_identity",
        "acquisition_eligibility",
        "current_project_clearance",
        "property_facts",
        "ownership",
        "current_diligence",
        "transit_access",
    }
    _expect(
        required_keys.issubset(by_key),
        "parcel detail: decision audit is missing required evidence layers",
        failures,
    )
    for key in required_keys:
        check = by_key.get(key, {})
        _expect(
            isinstance(check.get("summary"), str) and bool(check["summary"].strip()),
            f"parcel detail: audit check {key} has no summary",
            failures,
        )
        _expect(
            isinstance(check.get("source"), str) and bool(check["source"].strip()),
            f"parcel detail: audit check {key} has no source",
            failures,
        )

    historical = by_key.get("historical_model", {})
    address_identity = by_key.get("address_identity", {})
    eligibility = by_key.get("acquisition_eligibility", {})
    diligence = by_key.get("current_diligence", {})
    transit = by_key.get("transit_access", {})
    ownership = by_key.get("ownership", {})
    _expect(
        historical.get("layer") == "model_signal"
        and historical.get("affects_model_rank") is True
        and historical.get("affects_acquisition_eligibility") is False,
        "parcel detail: historical model role is ambiguous",
        failures,
    )
    _expect(
        address_identity.get("layer") == "source_freshness"
        and address_identity.get("affects_model_rank") is False
        and address_identity.get("affects_acquisition_eligibility") is False,
        "parcel detail: address enrichment role is ambiguous",
        failures,
    )
    if payload.get("address_source") == "nyc_pad":
        _expect(
            address_identity.get("status") == "verified"
            and "PAD" in str(address_identity.get("source") or ""),
            "parcel detail: PAD address provenance is not verified",
            failures,
        )
    _expect(
        eligibility.get("layer") == "eligibility_gate"
        and eligibility.get("affects_model_rank") is False
        and eligibility.get("affects_acquisition_eligibility") is True,
        "parcel detail: acquisition gate role is ambiguous",
        failures,
    )
    _expect(
        diligence.get("layer") == "current_diligence"
        and diligence.get("affects_model_rank") is False
        and diligence.get("affects_acquisition_eligibility") is False,
        "parcel detail: diligence-only role is ambiguous",
        failures,
    )
    _expect(
        transit.get("layer") == "current_diligence"
        and transit.get("affects_model_rank") is False
        and transit.get("affects_acquisition_eligibility") is False,
        "parcel detail: transit diligence-only role is ambiguous",
        failures,
    )
    for key, check in (
        ("ownership", ownership),
        ("current_diligence", diligence),
        ("transit_access", transit),
    ):
        _expect(
            check.get("status") == "unavailable"
            and "sign in" in str(check.get("summary") or "").lower()
            and check.get("as_of") is None,
            f"parcel detail: anonymous {key} evidence was not safely withheld",
            failures,
        )

    limitations = audit.get("limitations")
    _expect(
        isinstance(limitations, list)
        and len(limitations) >= 2
        and any(
            "willingness to sell" in str(limitation).lower()
            for limitation in limitations
        ),
        "parcel detail: decision limitations are incomplete",
        failures,
    )
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
    _expect(
        payload.get("access_scope") == "public_preview",
        "map: anonymous response does not identify public-preview scope",
        failures,
    )
    _expect(
        payload.get("requested_top_per_borough") == 25,
        "map: request receipt does not record the 25-per-borough request",
        failures,
    )
    _expect(
        payload.get("returned_count") == 125,
        "map: response receipt does not record 125 returned rows",
        failures,
    )
    _expect(
        payload.get("available_count") == 5000,
        "map: response receipt does not record 5,000 available rows",
        failures,
    )
    _expect(
        payload.get("inventory_complete") is False,
        "map: anonymous preview incorrectly claims complete inventory",
        failures,
    )
    rows = payload.get("rows")
    _expect(isinstance(rows, list), "map: rows is not a list", failures)
    rows = rows if isinstance(rows, list) else []
    _expect(
        len(rows) == 125, f"map: expected 125 public rows, got {len(rows)}", failures
    )
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
    _expect(
        payload.get("borough") == slug,
        f"sweep {slug}: response borough mismatch",
        failures,
    )
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
        _expect(
            row.get("acquisition_rank") == 1,
            f"sweep {slug}: top rank is not 1",
            failures,
        )
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
    api_security_failures = validate_security_headers(
        health_result.headers,
        label="API",
        browser_page=False,
    )
    failures.extend(api_security_failures)

    ready_result = _request(f"{api_base}/v1/health/ready", timeout=timeout)
    ready = _json(ready_result, "readiness", failures)
    timings["readiness"] = round(ready_result.elapsed_seconds, 3)
    _expect(ready.get("ok") is True, "readiness: API is not ready", failures)
    _expect(
        ready.get("firestore") is True, "readiness: Firestore is unavailable", failures
    )
    parcel_ready = ready.get("parcel_intel") or {}
    _expect(
        parcel_ready.get("present") is True,
        "readiness: parcel feed is absent",
        failures,
    )
    _expect(
        parcel_ready.get("stale") is False, "readiness: parcel feed is stale", failures
    )

    index_result = _request(f"{api_base}/v1/parcel-intel/index", timeout=timeout)
    index = _json(index_result, "index", failures)
    timings["index"] = round(index_result.elapsed_seconds, 3)
    verified_at = datetime.now(timezone.utc)
    failures.extend(
        validate_index(
            index,
            max_age_days=max_age_days,
            now=verified_at,
        )
    )
    _, source_warnings, source_sla = evaluate_source_slas(
        index,
        now=verified_at,
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
    vary = {
        value.strip().lower()
        for value in map_result.headers.get("vary", "").split(",")
        if value.strip()
    }
    _expect(
        {
            "authorization",
            "x-api-key",
            "x-citylens-parcel-smoke-key",
        }
        <= vary,
        "map: cache key does not vary on authentication credentials",
        failures,
    )
    map_payload = _json(map_result, "map", failures)
    failures.extend(
        validate_map(
            map_payload,
            expected_generated_at=generated_at
            if isinstance(generated_at, str)
            else None,
        )
    )
    map_rows = map_payload.get("rows")
    public_bbl = (
        map_rows[0].get("bbl")
        if isinstance(map_rows, list) and map_rows and isinstance(map_rows[0], dict)
        else None
    )
    _expect(
        isinstance(public_bbl, str),
        "parcel detail: no public map BBL was available for verification",
        failures,
    )
    if isinstance(public_bbl, str):
        detail_result = _request(
            f"{api_base}/v1/parcel-intel/parcel/{public_bbl}",
            timeout=timeout,
        )
        timings["parcel_detail"] = round(detail_result.elapsed_seconds, 3)
        _expect(
            "public" in detail_result.headers.get("cache-control", "").lower(),
            "parcel detail: anonymous response is not publicly cacheable",
            failures,
        )
        detail = _json(detail_result, "parcel detail", failures)
        failures.extend(_validate_public_row(detail, "parcel detail"))
        model_metadata = index.get("model_metadata")
        failures.extend(
            validate_public_decision_audit(
                detail,
                model_metadata=(
                    model_metadata if isinstance(model_metadata, dict) else {}
                ),
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
        ("workflow item", "/v1/parcel-intel/workflow/3020960069"),
        ("workflow actions", "/v1/parcel-intel/workflow/actions"),
        ("workflow analytics", "/v1/parcel-intel/workflow/analytics"),
        ("workflow alerts", "/v1/parcel-intel/workflow/alerts"),
        ("workflow events", "/v1/parcel-intel/workflow/3020960069/events"),
    ):
        result = _request(f"{api_base}{path}", timeout=timeout)
        timings[label.replace(" ", "_")] = round(result.elapsed_seconds, 3)
        _expect(
            result.status == 401,
            f"{label}: anonymous request returned {result.status}",
            failures,
        )

    product_event_result = _request(
        f"{api_base}/v1/parcel-intel/product-events",
        timeout=timeout,
        method="POST",
        json_body={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "parcel_opened",
            "source": "direct",
        },
    )
    timings["product_events"] = round(product_event_result.elapsed_seconds, 3)
    _expect(
        product_event_result.status == 401,
        f"product events: anonymous request returned {product_event_result.status}",
        failures,
    )

    pilot_request_result = _request(
        f"{api_base}/v1/pilot-requests",
        timeout=timeout,
        method="POST",
        json_body={},
    )
    timings["pilot_requests"] = round(
        pilot_request_result.elapsed_seconds,
        3,
    )
    failures.extend(
        validate_pilot_probe_response(
            pilot_request_result.status,
            pilot_request_result.headers,
        )
    )

    methodology_result = _request(
        f"{api_base}/v1/parcel-intel/workflow/analytics/methodology",
        timeout=timeout,
    )
    timings["workflow_methodology"] = round(methodology_result.elapsed_seconds, 3)
    methodology = _json(methodology_result, "workflow methodology", failures)
    failures.extend(validate_workflow_methodology(methodology))

    web_result = _request(
        f"{web_base}/parcel-intel",
        timeout=timeout,
    )
    timings["web_parcel_intel"] = round(web_result.elapsed_seconds, 3)
    _expect(
        web_result.status == 200,
        f"web: /parcel-intel returned {web_result.status}",
        failures,
    )
    web_security_failures = validate_security_headers(
        web_result.headers,
        label="web",
        browser_page=True,
    )
    failures.extend(web_security_failures)
    html = web_result.body.decode("utf-8", errors="replace")
    failures.extend(validate_web_copy(html))

    summary = {
        "schema_version": "citylens/production-verification@v1",
        "verified_at": verified_at.isoformat(),
        "api_base": api_base,
        "web_base": web_base,
        "feed_generated_at": generated_at,
        "max_age_days": max_age_days,
        "checks": 18,
        "security_headers": {
            "api": {"passed": not api_security_failures},
            "web": {"passed": not web_security_failures},
        },
        "source_sla": source_sla,
        "warnings": source_warnings,
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
    parser.add_argument(
        "--output", help="Optional path for the JSON verification report."
    )
    args = parser.parse_args()

    try:
        failures, summary = run_checks(
            api_base=args.api_base,
            web_base=args.web_base,
            max_age_days=args.max_age_days,
            timeout=args.timeout,
        )
    except Exception as exc:  # noqa: BLE001 - verifier must emit crash JSON
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

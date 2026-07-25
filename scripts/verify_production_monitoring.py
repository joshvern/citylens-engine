#!/usr/bin/env python3
"""Verify that managed production uptime checks have fresh healthy observations."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

try:
    from scripts.configure_production_monitoring import (
        SPECS,
        Gcloud,
        JsonCommand,
        _single_by_display_name,
        _uptime_id,
    )
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from configure_production_monitoring import (  # type: ignore[no-redef]
        SPECS,
        Gcloud,
        JsonCommand,
        _single_by_display_name,
        _uptime_id,
    )

SCHEMA = "citylens/production-monitoring-verification@v1"


class TimeSeriesCommand(Protocol):
    def list_check_passed(
        self,
        *,
        project: str,
        check_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]: ...


class GoogleMonitoring:
    """Minimal authenticated Cloud Monitoring API client for operator checks."""

    def __init__(self) -> None:
        completed = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0 or not completed.stdout.strip():
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(
                "could not obtain a Google access token from gcloud: "
                f"{detail}"
            )
        self.token = completed.stdout.strip()

    def list_check_passed(
        self,
        *,
        project: str,
        check_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        metric_filter = (
            'metric.type="monitoring.googleapis.com/uptime_check/'
            f'check_passed" AND metric.label.check_id="{check_id}"'
        )
        query = urllib.parse.urlencode(
            {
                "filter": metric_filter,
                "interval.startTime": _format_time(start),
                "interval.endTime": _format_time(end),
                "view": "FULL",
                "pageSize": "1000",
            }
        )
        url = (
            "https://monitoring.googleapis.com/v3/"
            f"projects/{urllib.parse.quote(project, safe='')}/timeSeries?"
            f"{query}"
        )
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError(
                "Cloud Monitoring observation query failed"
            ) from exc
        rows = payload.get("timeSeries") or []
        if not isinstance(rows, list):
            raise TypeError("Cloud Monitoring timeSeries response is not a list")
        return rows


@dataclass(frozen=True)
class Observation:
    location: str
    passed: bool
    observed_at: datetime


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def latest_observations(rows: list[dict[str, Any]]) -> list[Observation]:
    latest: dict[str, Observation] = {}
    for row in rows:
        metric_labels = (row.get("metric") or {}).get("labels") or {}
        resource_labels = (row.get("resource") or {}).get("labels") or {}
        location = str(
            metric_labels.get("checker_location")
            or resource_labels.get("checker_location")
            or ""
        )
        if not location:
            continue
        for point in row.get("points") or []:
            observed_at_raw = (point.get("interval") or {}).get("endTime")
            bool_value = (point.get("value") or {}).get("boolValue")
            if not isinstance(observed_at_raw, str) or not isinstance(
                bool_value, bool
            ):
                continue
            observation = Observation(
                location=location,
                passed=bool_value,
                observed_at=_parse_time(observed_at_raw),
            )
            current = latest.get(location)
            if current is None or observation.observed_at > current.observed_at:
                latest[location] = observation
    return sorted(latest.values(), key=lambda value: value.location)


def verify(
    *,
    project: str,
    now: datetime,
    max_age: timedelta,
    minimum_regions: int,
    command: JsonCommand,
    time_series: TimeSeriesCommand,
) -> dict[str, Any]:
    uptime_rows = command.json("monitoring", "uptime", "list-configs")
    if not isinstance(uptime_rows, list):
        raise TypeError("gcloud monitoring list response is not a list")

    checks: list[dict[str, Any]] = []
    for spec in SPECS:
        check = _single_by_display_name(
            uptime_rows,
            spec.uptime_display_name,
            resource_label="uptime check",
        )
        if check is None:
            checks.append(
                {
                    "key": spec.key,
                    "healthy": False,
                    "failures": ["managed uptime check is missing"],
                    "observations": [],
                }
            )
            continue

        check_id = _uptime_id(check)
        rows = time_series.list_check_passed(
            project=project,
            check_id=check_id,
            start=now - max_age,
            end=now,
        )
        observations = latest_observations(rows)
        failures: list[str] = []
        if len(observations) < minimum_regions:
            failures.append(
                f"only {len(observations)} regions reported; "
                f"{minimum_regions} required"
            )
        for observation in observations:
            age = now - observation.observed_at
            if age > max_age:
                failures.append(
                    f"{observation.location} observation is "
                    f"{int(age.total_seconds())} seconds old"
                )
            if not observation.passed:
                failures.append(
                    f"{observation.location} latest observation failed"
                )
        checks.append(
            {
                "key": spec.key,
                "check_id": check_id,
                "healthy": not failures,
                "failures": failures,
                "observations": [
                    {
                        "location": observation.location,
                        "passed": observation.passed,
                        "observed_at": _format_time(
                            observation.observed_at
                        ),
                    }
                    for observation in observations
                ],
            }
        )

    return {
        "schema": SCHEMA,
        "project": project,
        "verified_at": _format_time(now),
        "maximum_observation_age_seconds": int(max_age.total_seconds()),
        "minimum_regions": minimum_regions,
        "healthy": all(check["healthy"] for check in checks),
        "checks": checks,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default=(
            os.environ.get("CITYLENS_GCP_PROJECT")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
        ),
        help="Google Cloud project ID",
    )
    parser.add_argument(
        "--maximum-age-minutes",
        type=int,
        default=20,
        help="Observation query/freshness window; default 20 minutes",
    )
    parser.add_argument(
        "--minimum-regions",
        type=int,
        default=3,
        help="Minimum distinct healthy probe locations per check; default 3",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.project:
        raise SystemExit(
            "--project or CITYLENS_GCP_PROJECT/GOOGLE_CLOUD_PROJECT is required"
        )
    if args.maximum_age_minutes < 5:
        raise SystemExit("--maximum-age-minutes must be at least 5")
    if args.minimum_regions < 1:
        raise SystemExit("--minimum-regions must be positive")
    result = verify(
        project=args.project,
        now=datetime.now(UTC),
        max_age=timedelta(minutes=args.maximum_age_minutes),
        minimum_regions=args.minimum_regions,
        command=Gcloud(args.project),
        time_series=GoogleMonitoring(),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

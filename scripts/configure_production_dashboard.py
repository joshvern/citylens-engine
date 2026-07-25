#!/usr/bin/env python3
"""Plan, validate, or apply the CityLens production operations dashboard."""

from __future__ import annotations

import argparse
import json
import os
from copy import deepcopy
from typing import Any

try:
    from scripts.configure_production_monitoring import Gcloud, JsonCommand
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from configure_production_monitoring import (  # type: ignore[no-redef]
        Gcloud,
        JsonCommand,
    )

SCHEMA = "citylens/production-dashboard-plan@v1"
DISPLAY_NAME = "CityLens Production Operations"


def _metric_filter(
    metric_type: str,
    resource_type: str,
    *predicates: str,
) -> str:
    parts = [
        f'metric.type="{metric_type}"',
        f'resource.type="{resource_type}"',
        *predicates,
    ]
    return " AND ".join(parts)


def _xy_chart(
    *,
    metric_filter: str,
    aligner: str,
    reducer: str,
    group_by: list[str] | None = None,
    unit: str | None = None,
    plot_type: str = "LINE",
) -> dict[str, Any]:
    aggregation: dict[str, Any] = {
        "alignmentPeriod": "300s",
        "perSeriesAligner": aligner,
        "crossSeriesReducer": reducer,
    }
    if group_by:
        aggregation["groupByFields"] = group_by
    query: dict[str, Any] = {
        "timeSeriesFilter": {
            "filter": metric_filter,
            "aggregation": aggregation,
        }
    }
    if unit:
        query["unitOverride"] = unit
    return {
        "chartOptions": {"mode": "COLOR"},
        "dataSets": [
            {
                "minAlignmentPeriod": "60s",
                "plotType": plot_type,
                "targetAxis": "Y1",
                "timeSeriesQuery": query,
            }
        ],
        "thresholds": [],
        "timeshiftDuration": "0s",
        "yAxis": {"label": "value", "scale": "LINEAR"},
    }


def _tile(
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    widget: dict[str, Any],
) -> dict[str, Any]:
    return {
        "xPos": x,
        "yPos": y,
        "width": width,
        "height": height,
        "widget": {"title": title, **widget},
    }


def build_dashboard(
    *,
    project: str,
    api_service: str = "citylens-api",
    worker_job: str = "citylens-worker",
) -> dict[str, Any]:
    api_resource = (
        f'resource.label."service_name"="{api_service}"'
    )
    worker_resource = (
        f'resource.label."job_name"="{worker_job}"'
    )
    api_uptime = (
        'metric.label."check_id"='
        '"citylens-api-readiness-and-data-freshness-t5AFAusNmxk"'
    )
    web_uptime = (
        'metric.label."check_id"='
        '"citylens-parcel-intelligence-web-NgV0chDhZDk"'
    )
    request_filter = _metric_filter(
        "run.googleapis.com/request_count",
        "cloud_run_revision",
        api_resource,
    )
    latency_filter = _metric_filter(
        "run.googleapis.com/request_latencies",
        "cloud_run_revision",
        api_resource,
    )
    instance_filter = _metric_filter(
        "run.googleapis.com/container/instance_count",
        "cloud_run_revision",
        api_resource,
    )
    job_filter = _metric_filter(
        "run.googleapis.com/job/completed_execution_count",
        "cloud_run_job",
        worker_resource,
    )
    uptime_filter = (
        'metric.type="monitoring.googleapis.com/uptime_check/check_passed" '
        'AND resource.type="uptime_url"'
    )
    firestore_metrics = (
        "firestore.googleapis.com/document/read_count",
        "firestore.googleapis.com/document/write_count",
        "firestore.googleapis.com/document/delete_count",
    )
    firestore_data_sets = [
        {
            "minAlignmentPeriod": "60s",
            "plotType": "LINE",
            "targetAxis": "Y1",
            "timeSeriesQuery": {
                "timeSeriesFilter": {
                    "filter": _metric_filter(
                        metric,
                        "firestore_instance",
                    ),
                    "aggregation": {
                        "alignmentPeriod": "300s",
                        "perSeriesAligner": "ALIGN_RATE",
                        "crossSeriesReducer": "REDUCE_SUM",
                    },
                },
                "unitOverride": "1/s",
            },
        }
        for metric in firestore_metrics
    ]
    text = (
        "## CityLens production operations\n\n"
        "Public contract: [API readiness]"
        "(https://api.citylens.dev/v1/health/ready) · "
        "[Parcel Intelligence](https://www.citylens.dev/parcel-intel) · "
        "[engine workflows](https://github.com/joshvern/citylens-engine/actions)"
        "\n\n"
        "Uptime and TLS policies require failures from at least two regions. "
        "The daily recovery workflow proves delete protection, PITR, backup "
        "schedules, artifact soft delete, and backup freshness. "
        "Runbooks: `README.md` and `docs/deploy_gcp.md`."
    )
    error_filter = (
        "severity>=ERROR AND "
        "((resource.type=\"cloud_run_revision\" AND "
        f"resource.labels.service_name=\"{api_service}\") OR "
        "(resource.type=\"cloud_run_job\" AND "
        f"resource.labels.job_name=\"{worker_job}\"))"
    )
    tiles = [
        _tile(
            x=0,
            y=0,
            width=48,
            height=5,
            title="Operator entry point",
            widget={"text": {"content": text, "format": "MARKDOWN"}},
        ),
        _tile(
            x=0,
            y=5,
            width=16,
            height=9,
            title="Open production incidents",
            widget={
                "incidentList": {
                    "monitoredResources": [],
                    "policyNames": [],
                }
            },
        ),
        _tile(
            x=16,
            y=5,
            width=32,
            height=9,
            title="API and worker errors",
            widget={
                "logsPanel": {
                    "filter": error_filter,
                    "resourceNames": [f"projects/{project}"],
                }
            },
        ),
        _tile(
            x=0,
            y=14,
            width=24,
            height=12,
            title="API request rate by response class",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=request_filter,
                    aligner="ALIGN_RATE",
                    reducer="REDUCE_SUM",
                    group_by=["metric.label.response_code_class"],
                    unit="1/s",
                    plot_type="STACKED_AREA",
                )
            },
        ),
        _tile(
            x=24,
            y=14,
            width=24,
            height=12,
            title="API request latency · p95",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=latency_filter,
                    aligner="ALIGN_PERCENTILE_95",
                    reducer="REDUCE_MAX",
                    unit="ms",
                )
            },
        ),
        _tile(
            x=0,
            y=26,
            width=16,
            height=10,
            title="API container instances by state",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=instance_filter,
                    aligner="ALIGN_MAX",
                    reducer="REDUCE_SUM",
                    group_by=["metric.label.state"],
                    unit="1",
                )
            },
        ),
        _tile(
            x=16,
            y=26,
            width=16,
            height=10,
            title="Worker executions by result",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=job_filter,
                    aligner="ALIGN_SUM",
                    reducer="REDUCE_SUM",
                    group_by=["metric.label.result"],
                    unit="1",
                    plot_type="STACKED_BAR",
                )
            },
        ),
        _tile(
            x=32,
            y=26,
            width=16,
            height=10,
            title="Firestore document operations",
            widget={
                "xyChart": {
                    "chartOptions": {"mode": "COLOR"},
                    "dataSets": firestore_data_sets,
                    "thresholds": [],
                    "timeshiftDuration": "0s",
                    "yAxis": {
                        "label": "operations / second",
                        "scale": "LINEAR",
                    },
                }
            },
        ),
        _tile(
            x=0,
            y=36,
            width=24,
            height=9,
            title="API readiness · fraction passing",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=f"{uptime_filter} AND {api_uptime}",
                    aligner="ALIGN_FRACTION_TRUE",
                    reducer="REDUCE_MEAN",
                    unit="1",
                )
            },
        ),
        _tile(
            x=24,
            y=36,
            width=24,
            height=9,
            title="Web availability · fraction passing",
            widget={
                "xyChart": _xy_chart(
                    metric_filter=f"{uptime_filter} AND {web_uptime}",
                    aligner="ALIGN_FRACTION_TRUE",
                    reducer="REDUCE_MEAN",
                    unit="1",
                )
            },
        ),
    ]
    return {
        "displayName": DISPLAY_NAME,
        "dashboardFilters": [],
        "labels": {
            "environment": "production",
            "managed_by": "citylens",
        },
        "mosaicLayout": {"columns": 48, "tiles": tiles},
    }


def _dashboard_id(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "")
    if "/dashboards/" not in name:
        raise ValueError("dashboard has no valid resource name")
    return name.rsplit("/", 1)[-1]


def _single_dashboard(
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    matches = [
        row for row in rows if row.get("displayName") == DISPLAY_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple dashboards use display name {DISPLAY_NAME!r}"
        )
    return matches[0] if matches else None


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"etag", "name", "id"}:
                continue
            canonical = _canonical(item)
            # The API omits protobuf defaults and adds an empty text style.
            # These representations are semantically equal, not drift.
            if canonical in ({}, []):
                continue
            if key in {"xPos", "yPos"} and canonical == 0:
                continue
            result[key] = canonical
        return result
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def configure(
    *,
    project: str,
    api_service: str,
    worker_job: str,
    apply: bool,
    validate_only: bool,
    command: JsonCommand,
) -> dict[str, Any]:
    desired = build_dashboard(
        project=project,
        api_service=api_service,
        worker_job=worker_job,
    )
    config = json.dumps(desired, separators=(",", ":"), sort_keys=True)
    if validate_only:
        command.json(
            "monitoring",
            "dashboards",
            "create",
            "--validate-only",
            f"--config={config}",
        )
        return {
            "schema": SCHEMA,
            "project": project,
            "applied": False,
            "action": "validated",
        }

    rows = command.json("monitoring", "dashboards", "list")
    if not isinstance(rows, list):
        raise TypeError("gcloud dashboard list response is not a list")
    existing = _single_dashboard(rows)
    if existing is None:
        if not apply:
            return {
                "schema": SCHEMA,
                "project": project,
                "applied": False,
                "action": "create",
            }
        created = command.json(
            "monitoring",
            "dashboards",
            "create",
            f"--config={config}",
        )
        return {
            "schema": SCHEMA,
            "project": project,
            "applied": True,
            "action": "created",
            "dashboard_id": _dashboard_id(created),
        }

    dashboard_id = _dashboard_id(existing)
    if _canonical(existing) == _canonical(desired):
        return {
            "schema": SCHEMA,
            "project": project,
            "applied": False,
            "action": "unchanged",
            "dashboard_id": dashboard_id,
        }
    if not apply:
        return {
            "schema": SCHEMA,
            "project": project,
            "applied": False,
            "action": "update",
            "dashboard_id": dashboard_id,
        }
    update = deepcopy(desired)
    update["name"] = existing["name"]
    if existing.get("etag"):
        update["etag"] = existing["etag"]
    command.json(
        "monitoring",
        "dashboards",
        "update",
        dashboard_id,
        (
            "--config="
            + json.dumps(update, separators=(",", ":"), sort_keys=True)
        ),
    )
    return {
        "schema": SCHEMA,
        "project": project,
        "applied": True,
        "action": "updated",
        "dashboard_id": dashboard_id,
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
        "--api-service",
        default=os.environ.get("API_SERVICE_NAME", "citylens-api"),
        help="Production API Cloud Run service",
    )
    parser.add_argument(
        "--worker-job",
        default=os.environ.get("CITYLENS_JOB_NAME", "citylens-worker"),
        help="Production worker Cloud Run job",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Create or update the managed dashboard",
    )
    mode.add_argument(
        "--validate-only",
        action="store_true",
        help="Ask Cloud Monitoring to validate without saving",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.project:
        raise SystemExit(
            "--project or CITYLENS_GCP_PROJECT/GOOGLE_CLOUD_PROJECT is required"
        )
    result = configure(
        project=args.project,
        api_service=args.api_service,
        worker_job=args.worker_job,
        apply=args.apply,
        validate_only=args.validate_only,
        command=Gcloud(args.project),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

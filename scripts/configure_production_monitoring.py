#!/usr/bin/env python3
"""Plan or apply CityLens production monitoring.

The command is dry-run by default. It manages two public HTTPS uptime checks
and their alert policies through the authenticated ``gcloud`` CLI:

* API readiness plus prospective-evidence freshness.
* Parcel Intelligence web availability plus product-content validation.

It also manages two Cloud Run metric alert policies in the same declarative
style (no uptime check involved):

* API service 5xx response rate above threshold over a 5-minute window.
* Worker job failed executions (> 0 in any 5-minute window).

Existing notification channels are preserved. New channels can be attached by
passing their fully qualified Cloud Monitoring resource names.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any, Protocol

SCHEMA = "citylens/production-monitoring-plan@v1"
REGIONS = (
    "asia-pacific",
    "europe",
    "south-america",
    "usa-iowa",
    "usa-oregon",
    "usa-virginia",
)
API_REGIONS = tuple(value.upper().replace("-", "_") for value in REGIONS)


@dataclass(frozen=True)
class MonitorSpec:
    key: str
    uptime_display_name: str
    policy_display_name: str
    service_label: str
    host: str
    path: str
    matcher: str
    failure_condition_name: str
    tls_condition_name: str
    policy_documentation: str


@dataclass(frozen=True)
class MetricAlertSpec:
    """One Cloud Monitoring metric alert policy (no uptime check).

    Mirrors MonitorSpec's declarative style for alerts driven directly by
    Cloud Run metrics rather than uptime probes.
    """

    key: str
    policy_display_name: str
    condition_name: str
    service_label: str
    metric_filter: str
    aligner: str
    reducer: str
    alignment_period: str
    comparison: str
    threshold: float
    duration: str
    policy_documentation: str


SPECS = (
    MonitorSpec(
        key="api",
        uptime_display_name="CityLens API readiness and data freshness",
        policy_display_name="CityLens API readiness or TLS failure",
        service_label="citylens-api",
        host="api.citylens.dev",
        path="/v1/health/ready",
        matcher='"status":"current"',
        failure_condition_name=(
            "API readiness fails from at least two regions"
        ),
        tls_condition_name="API TLS certificate expires within 15 days",
        policy_documentation=(
            "Independent production check for "
            "`https://api.citylens.dev/v1/health/ready`. The response must be "
            "HTTP 200, pass TLS validation, and report the prospective "
            "evidence monitor as `current`. Check Cloud Run, the active parcel "
            "feed, and the `production-smoke` workflow before resolving the "
            "incident."
        ),
    ),
    MonitorSpec(
        key="web",
        uptime_display_name="CityLens Parcel Intelligence web",
        policy_display_name=(
            "CityLens Parcel Intelligence web or TLS failure"
        ),
        service_label="citylens-web",
        host="www.citylens.dev",
        path="/parcel-intel",
        matcher="Find the sites worth pursuing this week.",
        failure_condition_name=(
            "Parcel Intelligence web fails from at least two regions"
        ),
        tls_condition_name=(
            "Parcel Intelligence TLS certificate expires within 15 days"
        ),
        policy_documentation=(
            "Independent production check for "
            "`https://www.citylens.dev/parcel-intel`. The response must be "
            "HTTP 200, pass TLS validation, and contain the core "
            "parcel-acquisition product heading. Check Vercel and the engine "
            "production verifier before resolving the incident."
        ),
    ),
)

METRIC_ALERT_SPECS = (
    MetricAlertSpec(
        key="api_5xx",
        policy_display_name="CityLens API 5xx error rate",
        condition_name=(
            "Cloud Run citylens-api 5xx rate exceeds 0.05/s over 5 minutes"
        ),
        service_label="citylens-api",
        metric_filter=(
            'metric.type="run.googleapis.com/request_count" '
            'AND resource.type="cloud_run_revision" '
            'AND resource.label.service_name="citylens-api" '
            'AND metric.label.response_code_class="5xx"'
        ),
        aligner="ALIGN_RATE",
        reducer="REDUCE_SUM",
        alignment_period="300s",
        comparison="COMPARISON_GT",
        # requests/second averaged over the alignment window: 0.05/s
        # sustained for 5 minutes is ~15 server errors — above background
        # noise, below a full outage the uptime policy already catches.
        threshold=0.05,
        duration="300s",
        policy_documentation=(
            "The `citylens-api` Cloud Run service is returning 5xx responses "
            "above 0.05/s sustained for 5 minutes. Check Cloud Run logs for "
            "the failing route (severity>=ERROR now carries Error Reporting "
            "payloads), recent deploys, and Firestore/GCS dependency health "
            "before resolving the incident."
        ),
    ),
    MetricAlertSpec(
        key="worker_job_failures",
        policy_display_name="CityLens worker job execution failure",
        condition_name=(
            "Cloud Run job citylens-worker completed a failed execution"
        ),
        service_label="citylens-worker",
        metric_filter=(
            'metric.type="run.googleapis.com/job/completed_execution_count" '
            'AND resource.type="cloud_run_job" '
            'AND resource.label.job_name="citylens-worker" '
            'AND metric.label.result="failed"'
        ),
        aligner="ALIGN_DELTA",
        reducer="REDUCE_SUM",
        alignment_period="300s",
        comparison="COMPARISON_GT",
        threshold=0,
        duration="0s",
        policy_documentation=(
            "A `citylens-worker` Cloud Run job execution finished as failed "
            "(pipeline crash, OOM, or task timeout). Check the execution "
            "logs, the run document in Firestore, and whether the stuck-run "
            "reconciler refunded the user's quota before resolving the "
            "incident."
        ),
    ),
)


class JsonCommand(Protocol):
    def json(self, *args: str) -> Any: ...


class Gcloud:
    def __init__(self, project: str) -> None:
        self.project = project

    def json(self, *args: str) -> Any:
        command = [
            "gcloud",
            *args,
            "--project",
            self.project,
            "--format=json",
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(
                f"gcloud command failed ({completed.returncode}): "
                f"{' '.join(command[:-1])}\n{detail}"
            )
        if not completed.stdout.strip():
            return {}
        return json.loads(completed.stdout)


def _labels(spec: MonitorSpec | MetricAlertSpec) -> dict[str, str]:
    return {
        "environment": "production",
        "managed_by": "citylens",
        "service": spec.service_label,
    }


def _csv(values: tuple[str, ...] | list[str]) -> str:
    return ",".join(values)


def _uptime_id(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "")
    if "/uptimeCheckConfigs/" not in name:
        raise ValueError("uptime check has no valid resource name")
    return name.rsplit("/", 1)[-1]


def _policy_id(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "")
    if "/alertPolicies/" not in name:
        raise ValueError("alert policy has no valid resource name")
    return name.rsplit("/", 1)[-1]


def _single_by_display_name(
    rows: list[dict[str, Any]],
    display_name: str,
    *,
    resource_label: str,
) -> dict[str, Any] | None:
    matches = [
        row for row in rows if row.get("displayName") == display_name
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple {resource_label} resources use display name "
            f"{display_name!r}"
        )
    return matches[0] if matches else None


def uptime_drift(
    row: dict[str, Any],
    *,
    project: str,
    spec: MonitorSpec,
) -> list[str]:
    reasons: list[str] = []
    resource = row.get("monitoredResource") or {}
    resource_labels = resource.get("labels") or {}
    http = row.get("httpCheck") or {}
    status_values = {
        value.get("statusValue")
        for value in http.get("acceptedResponseStatusCodes") or []
    }
    matchers = row.get("contentMatchers") or []
    labels = row.get("userLabels") or {}
    expected_labels = _labels(spec)

    expected = {
        "resource.type": (resource.get("type"), "uptime_url"),
        "resource.host": (resource_labels.get("host"), spec.host),
        "resource.project_id": (
            resource_labels.get("project_id"),
            project,
        ),
        "http.path": (http.get("path"), spec.path),
        "http.port": (http.get("port"), 443),
        "http.request_method": (http.get("requestMethod"), "GET"),
        "http.use_ssl": (http.get("useSsl"), True),
        "http.validate_ssl": (http.get("validateSsl"), True),
        "period": (row.get("period"), "300s"),
        "timeout": (row.get("timeout"), "10s"),
    }
    for field, (actual, desired) in expected.items():
        if actual != desired:
            reasons.append(f"{field}: {actual!r} != {desired!r}")
    if status_values != {200}:
        reasons.append(f"http.status_codes: {status_values!r} != {{200}}")
    if set(row.get("selectedRegions") or []) != set(API_REGIONS):
        reasons.append("selected_regions differ from the six-region contract")
    if matchers != [{"content": spec.matcher, "matcher": "CONTAINS_STRING"}]:
        reasons.append("content matcher differs from the product contract")
    for key, value in expected_labels.items():
        if labels.get(key) != value:
            reasons.append(
                f"user_labels.{key}: {labels.get(key)!r} != {value!r}"
            )
    return reasons


def _create_uptime_args(
    *,
    project: str,
    spec: MonitorSpec,
) -> list[str]:
    labels = _labels(spec)
    return [
        "monitoring",
        "uptime",
        "create",
        spec.uptime_display_name,
        "--resource-type=uptime-url",
        (
            "--resource-labels="
            f"host={spec.host},project_id={project}"
        ),
        "--protocol=https",
        "--port=443",
        f"--path={spec.path}",
        "--request-method=get",
        "--validate-ssl=true",
        "--status-codes=200",
        "--matcher-type=contains-string",
        f"--matcher-content={spec.matcher}",
        "--period=5",
        "--timeout=10",
        f"--regions={_csv(REGIONS)}",
        (
            "--user-labels="
            f"environment={labels['environment']},"
            f"managed_by={labels['managed_by']},"
            f"service={labels['service']}"
        ),
    ]


def _update_uptime_args(
    check_id: str,
    *,
    spec: MonitorSpec,
) -> list[str]:
    labels = _labels(spec)
    return [
        "monitoring",
        "uptime",
        "update",
        check_id,
        f"--display-name={spec.uptime_display_name}",
        f"--path={spec.path}",
        "--port=443",
        "--request-method=get",
        "--validate-ssl=true",
        "--set-status-codes=200",
        "--matcher-type=contains-string",
        f"--matcher-content={spec.matcher}",
        "--period=5",
        "--timeout=10",
        f"--set-regions={_csv(REGIONS)}",
        (
            "--update-user-labels="
            f"environment={labels['environment']},"
            f"managed_by={labels['managed_by']},"
            f"service={labels['service']}"
        ),
    ]


def ensure_uptime(
    command: JsonCommand,
    *,
    project: str,
    spec: MonitorSpec,
    existing: dict[str, Any] | None,
    apply: bool,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if existing is None:
        if not apply:
            return {"key": spec.key, "action": "create"}, None
        created = command.json(*_create_uptime_args(project=project, spec=spec))
        return {
            "key": spec.key,
            "action": "created",
            "check_id": _uptime_id(created),
        }, created

    check_id = _uptime_id(existing)
    drift = uptime_drift(existing, project=project, spec=spec)
    if not drift:
        return {
            "key": spec.key,
            "action": "unchanged",
            "check_id": check_id,
        }, existing

    resource = existing.get("monitoredResource") or {}
    resource_labels = resource.get("labels") or {}
    immutable_mismatch = (
        resource.get("type") != "uptime_url"
        or resource_labels.get("host") != spec.host
        or resource_labels.get("project_id") != project
        or not (existing.get("httpCheck") or {}).get("useSsl")
    )
    if immutable_mismatch:
        raise RuntimeError(
            f"uptime check {check_id} has immutable target drift; "
            "review its alert policy, then recreate it deliberately"
        )
    if not apply:
        return {
            "key": spec.key,
            "action": "update",
            "check_id": check_id,
            "drift": drift,
        }, existing
    updated = command.json(*_update_uptime_args(check_id, spec=spec))
    return {
        "key": spec.key,
        "action": "updated",
        "check_id": check_id,
        "drift": drift,
    }, updated


def build_policy(
    *,
    spec: MonitorSpec,
    check_id: str,
    notification_channels: list[str],
    name: str | None = None,
) -> dict[str, Any]:
    def metric_filter(metric: str) -> str:
        return (
            f'metric.type="monitoring.googleapis.com/uptime_check/{metric}" '
            f'AND metric.label.check_id="{check_id}" '
            'AND resource.type="uptime_url"'
        )

    policy: dict[str, Any] = {
        "displayName": spec.policy_display_name,
        "combiner": "OR",
        "enabled": True,
        "conditions": [
            {
                "displayName": spec.failure_condition_name,
                "conditionThreshold": {
                    "filter": metric_filter("check_passed"),
                    "aggregations": [
                        {
                            "alignmentPeriod": "300s",
                            "perSeriesAligner": "ALIGN_NEXT_OLDER",
                            "crossSeriesReducer": "REDUCE_COUNT_FALSE",
                            "groupByFields": ["resource.label.*"],
                        }
                    ],
                    "comparison": "COMPARISON_GT",
                    "thresholdValue": 1,
                    "duration": "60s",
                    "trigger": {"count": 1},
                },
            },
            {
                "displayName": spec.tls_condition_name,
                "conditionThreshold": {
                    "filter": metric_filter(
                        "time_until_ssl_cert_expires"
                    ),
                    "aggregations": [
                        {
                            "alignmentPeriod": "300s",
                            "perSeriesAligner": "ALIGN_NEXT_OLDER",
                            "crossSeriesReducer": "REDUCE_MEAN",
                            "groupByFields": ["resource.label.*"],
                        }
                    ],
                    "comparison": "COMPARISON_LT",
                    "thresholdValue": 15,
                    "duration": "300s",
                    "trigger": {"count": 1},
                },
            },
        ],
        "documentation": {
            "mimeType": "text/markdown",
            "content": spec.policy_documentation,
        },
        "userLabels": _labels(spec),
    }
    if notification_channels:
        policy["notificationChannels"] = sorted(set(notification_channels))
    if name:
        policy["name"] = name
    return policy


def build_metric_policy(
    *,
    spec: MetricAlertSpec,
    notification_channels: list[str],
    name: str | None = None,
) -> dict[str, Any]:
    policy: dict[str, Any] = {
        "displayName": spec.policy_display_name,
        "combiner": "OR",
        "enabled": True,
        "conditions": [
            {
                "displayName": spec.condition_name,
                "conditionThreshold": {
                    "filter": spec.metric_filter,
                    "aggregations": [
                        {
                            "alignmentPeriod": spec.alignment_period,
                            "perSeriesAligner": spec.aligner,
                            "crossSeriesReducer": spec.reducer,
                            "groupByFields": ["resource.label.*"],
                        }
                    ],
                    "comparison": spec.comparison,
                    "thresholdValue": spec.threshold,
                    "duration": spec.duration,
                    "trigger": {"count": 1},
                },
            }
        ],
        "documentation": {
            "mimeType": "text/markdown",
            "content": spec.policy_documentation,
        },
        "userLabels": _labels(spec),
    }
    if notification_channels:
        policy["notificationChannels"] = sorted(set(notification_channels))
    if name:
        policy["name"] = name
    return policy


def _canonical_policy(policy: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: policy.get(key)
        for key in (
            "displayName",
            "combiner",
            "enabled",
            "documentation",
            "userLabels",
        )
    }
    result["notificationChannels"] = sorted(
        policy.get("notificationChannels") or []
    )
    conditions = []
    for condition in policy.get("conditions") or []:
        conditions.append(
            {
                "displayName": condition.get("displayName"),
                "conditionThreshold": condition.get("conditionThreshold"),
            }
        )
    result["conditions"] = sorted(
        conditions,
        key=lambda value: str(value.get("displayName")),
    )
    return result


def _reconcile_policy(
    command: JsonCommand,
    *,
    key: str,
    desired: dict[str, Any],
    channels: list[str],
    existing: dict[str, Any] | None,
    apply: bool,
) -> dict[str, Any]:
    if existing is None:
        if not apply:
            return {"key": key, "action": "create"}
        created = command.json(
            "monitoring",
            "policies",
            "create",
            f"--policy={json.dumps(desired, separators=(',', ':'))}",
        )
        return {
            "key": key,
            "action": "created",
            "policy_id": _policy_id(created),
        }

    policy_id = _policy_id(existing)
    if _canonical_policy(existing) == _canonical_policy(desired):
        return {
            "key": key,
            "action": "unchanged",
            "policy_id": policy_id,
            "notification_channels": channels,
        }
    if not apply:
        return {
            "key": key,
            "action": "update",
            "policy_id": policy_id,
            "notification_channels": channels,
        }
    updated = command.json(
        "monitoring",
        "policies",
        "update",
        policy_id,
        f"--policy={json.dumps(desired, separators=(',', ':'))}",
    )
    return {
        "key": key,
        "action": "updated",
        "policy_id": _policy_id(updated),
        "notification_channels": channels,
    }


def _merged_channels(
    existing: dict[str, Any] | None,
    requested_channels: list[str],
) -> list[str]:
    existing_channels = (
        list(existing.get("notificationChannels") or []) if existing else []
    )
    return sorted(set(existing_channels + requested_channels))


def ensure_policy(
    command: JsonCommand,
    *,
    spec: MonitorSpec,
    check_id: str,
    existing: dict[str, Any] | None,
    requested_channels: list[str],
    apply: bool,
) -> dict[str, Any]:
    channels = _merged_channels(existing, requested_channels)
    desired = build_policy(
        spec=spec,
        check_id=check_id,
        notification_channels=channels,
        name=str(existing.get("name")) if existing else None,
    )
    return _reconcile_policy(
        command,
        key=spec.key,
        desired=desired,
        channels=channels,
        existing=existing,
        apply=apply,
    )


def ensure_metric_policy(
    command: JsonCommand,
    *,
    spec: MetricAlertSpec,
    existing: dict[str, Any] | None,
    requested_channels: list[str],
    apply: bool,
) -> dict[str, Any]:
    channels = _merged_channels(existing, requested_channels)
    desired = build_metric_policy(
        spec=spec,
        notification_channels=channels,
        name=str(existing.get("name")) if existing else None,
    )
    return _reconcile_policy(
        command,
        key=spec.key,
        desired=desired,
        channels=channels,
        existing=existing,
        apply=apply,
    )


def configure(
    *,
    project: str,
    apply: bool,
    notification_channels: list[str],
    command: JsonCommand | None = None,
) -> dict[str, Any]:
    command = command or Gcloud(project)
    uptime_rows = command.json(
        "monitoring", "uptime", "list-configs"
    )
    policy_rows = command.json("monitoring", "policies", "list")
    if not isinstance(uptime_rows, list) or not isinstance(policy_rows, list):
        raise TypeError("gcloud monitoring list response is not a list")

    uptime_results: list[dict[str, Any]] = []
    policy_results: list[dict[str, Any]] = []
    resolved_checks: dict[str, dict[str, Any]] = {}
    for spec in SPECS:
        existing = _single_by_display_name(
            uptime_rows,
            spec.uptime_display_name,
            resource_label="uptime check",
        )
        result, resolved = ensure_uptime(
            command,
            project=project,
            spec=spec,
            existing=existing,
            apply=apply,
        )
        uptime_results.append(result)
        if resolved is not None:
            resolved_checks[spec.key] = resolved

    for spec in SPECS:
        check = resolved_checks.get(spec.key)
        if check is None:
            policy_results.append(
                {
                    "key": spec.key,
                    "action": "blocked_until_uptime_check_exists",
                }
            )
            continue
        existing = _single_by_display_name(
            policy_rows,
            spec.policy_display_name,
            resource_label="alert policy",
        )
        policy_results.append(
            ensure_policy(
                command,
                spec=spec,
                check_id=_uptime_id(check),
                existing=existing,
                requested_channels=notification_channels,
                apply=apply,
            )
        )

    for metric_spec in METRIC_ALERT_SPECS:
        existing = _single_by_display_name(
            policy_rows,
            metric_spec.policy_display_name,
            resource_label="alert policy",
        )
        policy_results.append(
            ensure_metric_policy(
                command,
                spec=metric_spec,
                existing=existing,
                requested_channels=notification_channels,
                apply=apply,
            )
        )

    return {
        "schema": SCHEMA,
        "project": project,
        "applied": apply,
        "uptime_checks": uptime_results,
        "alert_policies": policy_results,
        "notification_channels_requested": sorted(
            set(notification_channels)
        ),
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
        "--apply",
        action="store_true",
        help="Apply changes; default behavior is a read-only plan",
    )
    parser.add_argument(
        "--notification-channel",
        action="append",
        default=[],
        help=(
            "Fully qualified Cloud Monitoring notification-channel resource "
            "to attach; repeatable and additive"
        ),
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
        apply=args.apply,
        notification_channels=args.notification_channel,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

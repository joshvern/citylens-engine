#!/usr/bin/env python3
"""Verify CityLens production recovery configuration and backup freshness."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

try:
    from scripts.configure_production_monitoring import Gcloud, JsonCommand
    from scripts.configure_production_recovery import configure
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from configure_production_monitoring import (  # type: ignore[no-redef]
        Gcloud,
        JsonCommand,
    )
    from configure_production_recovery import (  # type: ignore[no-redef]
        configure,
    )

SCHEMA = "citylens/production-recovery-verification@v1"


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _backup_database_matches(row: dict[str, Any], project: str) -> bool:
    expected = f"projects/{project}/databases/(default)"
    return row.get("database") == expected


def _ready_backups(
    rows: list[dict[str, Any]],
    *,
    project: str,
) -> list[dict[str, Any]]:
    result = [
        row
        for row in rows
        if row.get("state") == "READY"
        and _backup_database_matches(row, project)
        and isinstance(row.get("snapshotTime"), str)
        and isinstance(row.get("expireTime"), str)
    ]
    return sorted(
        result,
        key=lambda row: _parse_time(str(row["snapshotTime"])),
        reverse=True,
    )


def verify(
    *,
    project: str,
    bucket: str,
    location: str,
    now: datetime,
    maximum_backup_age: timedelta,
    initial_grace: timedelta,
    command: JsonCommand,
) -> dict[str, Any]:
    configuration = configure(
        project=project,
        bucket=bucket,
        apply=False,
        command=command,
    )
    backups = command.json(
        "firestore",
        "backups",
        "list",
        f"--location={location}",
    )
    if not isinstance(backups, list):
        raise TypeError("gcloud backup list response is not a list")

    actions = [
        configuration["firestore"]["action"],
        configuration["artifact_bucket_protection"]["action"],
        *[
            schedule["action"]
            for schedule in configuration["backup_schedules"]
        ],
    ]
    configuration_failures = [
        f"recovery configuration requires action: {action}"
        for action in actions
        if action != "unchanged"
    ]
    schedules_created = [
        _parse_time(str(schedule["created_at"]))
        for schedule in configuration["backup_schedules"]
        if schedule.get("created_at")
    ]
    oldest_schedule = min(schedules_created) if schedules_created else None
    ready = _ready_backups(backups, project=project)
    failures = list(configuration_failures)
    warnings: list[str] = []
    latest: dict[str, Any] | None = None
    status = "ready"

    if ready:
        latest = ready[0]
        snapshot_time = _parse_time(str(latest["snapshotTime"]))
        expire_time = _parse_time(str(latest["expireTime"]))
        if now - snapshot_time > maximum_backup_age:
            failures.append(
                "latest READY backup is "
                f"{int((now - snapshot_time).total_seconds())} seconds old"
            )
        if expire_time <= now:
            failures.append("latest READY backup is already expired")
    elif (
        oldest_schedule is not None
        and now - oldest_schedule <= initial_grace
        and not configuration_failures
    ):
        status = "collecting"
        warnings.append(
            "backup schedules are new and the first managed backup is pending"
        )
    else:
        failures.append("no READY backup exists for the production database")

    if failures:
        status = "failed"
    return {
        "schema": SCHEMA,
        "project": project,
        "database": "(default)",
        "artifact_bucket": bucket,
        "verified_at": _format_time(now),
        "status": status,
        "ready": status == "ready",
        "configuration_healthy": not configuration_failures,
        "maximum_backup_age_seconds": int(
            maximum_backup_age.total_seconds()
        ),
        "initial_backup_grace_seconds": int(initial_grace.total_seconds()),
        "latest_ready_backup": (
            {
                "name": latest.get("name"),
                "snapshot_time": latest.get("snapshotTime"),
                "expire_time": latest.get("expireTime"),
            }
            if latest
            else None
        ),
        "ready_backup_count": len(ready),
        "failures": failures,
        "warnings": warnings,
        "configuration": configuration,
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
        "--bucket",
        default=os.environ.get("CITYLENS_BUCKET"),
        help="Production artifact bucket name",
    )
    parser.add_argument(
        "--location",
        default=os.environ.get("CITYLENS_REGION", "us-central1"),
        help="Firestore backup location",
    )
    parser.add_argument(
        "--maximum-backup-age-hours",
        type=int,
        default=36,
        help="Maximum accepted READY backup age; default 36 hours",
    )
    parser.add_argument(
        "--initial-grace-hours",
        type=int,
        default=26,
        help="Grace before the first scheduled backup is required; default 26",
    )
    parser.add_argument(
        "--allow-collecting",
        action="store_true",
        help="Exit successfully while a new valid schedule awaits first backup",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.project:
        raise SystemExit(
            "--project or CITYLENS_GCP_PROJECT/GOOGLE_CLOUD_PROJECT is required"
        )
    if not args.bucket:
        raise SystemExit("--bucket or CITYLENS_BUCKET is required")
    if args.maximum_backup_age_hours < 24:
        raise SystemExit("--maximum-backup-age-hours must be at least 24")
    if args.initial_grace_hours < 24:
        raise SystemExit("--initial-grace-hours must be at least 24")
    result = verify(
        project=args.project,
        bucket=args.bucket,
        location=args.location,
        now=datetime.now(UTC),
        maximum_backup_age=timedelta(
            hours=args.maximum_backup_age_hours
        ),
        initial_grace=timedelta(hours=args.initial_grace_hours),
        command=Gcloud(args.project),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["ready"]:
        return 0
    if args.allow_collecting and result["status"] == "collecting":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

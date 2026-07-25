#!/usr/bin/env python3
"""Plan or apply the CityLens production data-recovery baseline.

The command is read-only by default. With ``--apply`` it enables Firestore
delete protection and PITR, creates or repairs daily and weekly backup
schedules, and enforces the artifact bucket's private soft-delete baseline.
It never deletes backups, schedules, objects, buckets, or databases.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

try:
    from scripts.configure_production_monitoring import Gcloud, JsonCommand
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from configure_production_monitoring import (  # type: ignore[no-redef]
        Gcloud,
        JsonCommand,
    )

SCHEMA = "citylens/production-recovery-plan@v1"
DATABASE = "(default)"
DAILY_RETENTION = "1209600s"
WEEKLY_RETENTION = "8467200s"
WEEKLY_DAY = "SUNDAY"
MINIMUM_SOFT_DELETE_SECONDS = 604800


@dataclass(frozen=True)
class ScheduleSpec:
    key: str
    recurrence: str
    retention: str
    day: str | None = None


SCHEDULES = (
    ScheduleSpec(
        key="daily",
        recurrence="daily",
        retention=DAILY_RETENTION,
    ),
    ScheduleSpec(
        key="weekly",
        recurrence="weekly",
        retention=WEEKLY_RETENTION,
        day=WEEKLY_DAY,
    ),
)


def _schedule_id(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "")
    if "/backupSchedules/" not in name:
        raise ValueError("backup schedule has no valid resource name")
    return name.rsplit("/", 1)[-1]


def database_drift(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    expected = {
        "deleteProtectionState": "DELETE_PROTECTION_ENABLED",
        "pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_ENABLED",
        "versionRetentionPeriod": "604800s",
    }
    for field, desired in expected.items():
        if row.get(field) != desired:
            reasons.append(f"{field}: {row.get(field)!r} != {desired!r}")
    return reasons


def _schedule_matches(row: dict[str, Any], spec: ScheduleSpec) -> bool:
    if spec.recurrence == "daily":
        return "dailyRecurrence" in row
    return "weeklyRecurrence" in row


def _single_schedule(
    rows: list[dict[str, Any]],
    spec: ScheduleSpec,
) -> dict[str, Any] | None:
    matches = [row for row in rows if _schedule_matches(row, spec)]
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple {spec.key} Firestore backup schedules exist"
        )
    return matches[0] if matches else None


def ensure_database(
    command: JsonCommand,
    *,
    row: dict[str, Any],
    apply: bool,
) -> dict[str, Any]:
    drift = database_drift(row)
    if not drift:
        return {"action": "unchanged"}
    if not apply:
        return {"action": "update", "drift": drift}
    command.json(
        "firestore",
        "databases",
        "update",
        f"--database={DATABASE}",
        "--delete-protection",
        "--enable-pitr",
        "--quiet",
    )
    return {"action": "updated", "drift": drift}


def ensure_schedule(
    command: JsonCommand,
    *,
    spec: ScheduleSpec,
    existing: dict[str, Any] | None,
    apply: bool,
) -> dict[str, Any]:
    if existing is None:
        if not apply:
            return {"key": spec.key, "action": "create"}
        args = [
            "firestore",
            "backups",
            "schedules",
            "create",
            f"--database={DATABASE}",
            f"--recurrence={spec.recurrence}",
            f"--retention={spec.retention}",
        ]
        if spec.day:
            args.append(f"--day-of-week={spec.day}")
        created = command.json(*args)
        return {
            "key": spec.key,
            "action": "created",
            "schedule_id": _schedule_id(created),
        }

    schedule_id = _schedule_id(existing)
    if spec.day:
        actual_day = (existing.get("weeklyRecurrence") or {}).get("day")
        if actual_day != spec.day:
            raise RuntimeError(
                f"{spec.key} backup schedule {schedule_id} runs on "
                f"{actual_day!r}, not {spec.day!r}; recurrence is immutable, "
                "so review retained backups and recreate it deliberately"
            )
    if existing.get("retention") == spec.retention:
        return {
            "key": spec.key,
            "action": "unchanged",
            "schedule_id": schedule_id,
            "created_at": existing.get("createTime"),
        }
    drift = [
        f"retention: {existing.get('retention')!r} != {spec.retention!r}"
    ]
    if not apply:
        return {
            "key": spec.key,
            "action": "update",
            "schedule_id": schedule_id,
            "drift": drift,
        }
    updated = command.json(
        "firestore",
        "backups",
        "schedules",
        "update",
        f"--database={DATABASE}",
        f"--backup-schedule={schedule_id}",
        f"--retention={spec.retention}",
    )
    return {
        "key": spec.key,
        "action": "updated",
        "schedule_id": _schedule_id(updated),
        "drift": drift,
    }


def bucket_drift(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if row.get("public_access_prevention") != "enforced":
        reasons.append("public access prevention is not enforced")
    if row.get("uniform_bucket_level_access") is not True:
        reasons.append("uniform bucket-level access is not enabled")
    soft_delete = row.get("soft_delete_policy") or {}
    raw_duration = soft_delete.get("retentionDurationSeconds")
    try:
        duration = int(raw_duration)
    except (TypeError, ValueError):
        duration = 0
    if duration < MINIMUM_SOFT_DELETE_SECONDS:
        reasons.append(
            "soft-delete retention is below the seven-day minimum"
        )
    return reasons


def ensure_bucket(
    command: JsonCommand,
    *,
    bucket: str,
    row: dict[str, Any],
    apply: bool,
) -> dict[str, Any]:
    drift = bucket_drift(row)
    if not drift:
        return {
            "action": "unchanged",
            "soft_delete_seconds": int(
                row["soft_delete_policy"]["retentionDurationSeconds"]
            ),
        }
    if not apply:
        return {"action": "update", "drift": drift}
    updated = command.json(
        "storage",
        "buckets",
        "update",
        f"gs://{bucket}",
        "--uniform-bucket-level-access",
        "--public-access-prevention",
        "--soft-delete-duration=7d",
    )
    remaining = bucket_drift(updated)
    if remaining:
        raise RuntimeError(
            "artifact bucket still violates recovery baseline after update: "
            + "; ".join(remaining)
        )
    return {
        "action": "updated",
        "drift": drift,
        "soft_delete_seconds": int(
            updated["soft_delete_policy"]["retentionDurationSeconds"]
        ),
    }


def configure(
    *,
    project: str,
    bucket: str,
    apply: bool,
    command: JsonCommand | None = None,
) -> dict[str, Any]:
    command = command or Gcloud(project)
    database = command.json(
        "firestore", "databases", "describe", f"--database={DATABASE}"
    )
    schedules = command.json(
        "firestore",
        "backups",
        "schedules",
        "list",
        f"--database={DATABASE}",
    )
    bucket_row = command.json(
        "storage", "buckets", "describe", f"gs://{bucket}"
    )
    if not isinstance(database, dict) or not isinstance(bucket_row, dict):
        raise TypeError("gcloud recovery resource response is not an object")
    if not isinstance(schedules, list):
        raise TypeError("gcloud backup schedule response is not a list")

    database_result = ensure_database(
        command,
        row=database,
        apply=apply,
    )
    schedule_results = [
        ensure_schedule(
            command,
            spec=spec,
            existing=_single_schedule(schedules, spec),
            apply=apply,
        )
        for spec in SCHEDULES
    ]
    bucket_result = ensure_bucket(
        command,
        bucket=bucket,
        row=bucket_row,
        apply=apply,
    )
    return {
        "schema": SCHEMA,
        "project": project,
        "database": DATABASE,
        "artifact_bucket": bucket,
        "applied": apply,
        "firestore": database_result,
        "backup_schedules": schedule_results,
        "artifact_bucket_protection": bucket_result,
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
        "--apply",
        action="store_true",
        help="Apply protective changes; default behavior is read-only",
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
    result = configure(
        project=args.project,
        bucket=args.bucket,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

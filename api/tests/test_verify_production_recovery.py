from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from scripts.configure_production_recovery import (
    DAILY_RETENTION,
    WEEKLY_RETENTION,
)
from scripts.verify_production_recovery import verify

NOW = datetime(2026, 7, 25, 4, 0, tzinfo=UTC)


def _database() -> dict[str, Any]:
    return {
        "deleteProtectionState": "DELETE_PROTECTION_ENABLED",
        "pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_ENABLED",
        "versionRetentionPeriod": "604800s",
    }


def _schedule(
    recurrence: str,
    *,
    created_at: datetime = NOW - timedelta(hours=1),
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": (
            "projects/citylens-001/databases/(default)/backupSchedules/"
            f"{recurrence}"
        ),
        "retention": (
            DAILY_RETENTION if recurrence == "daily" else WEEKLY_RETENTION
        ),
        "createTime": created_at.isoformat().replace("+00:00", "Z"),
    }
    if recurrence == "daily":
        row["dailyRecurrence"] = {}
    else:
        row["weeklyRecurrence"] = {"day": "SUNDAY"}
    return row


def _bucket() -> dict[str, Any]:
    return {
        "public_access_prevention": "enforced",
        "uniform_bucket_level_access": True,
        "soft_delete_policy": {"retentionDurationSeconds": "604800"},
    }


def _backup(
    *,
    snapshot: datetime = NOW - timedelta(hours=2),
    expire: datetime = NOW + timedelta(days=12),
) -> dict[str, Any]:
    return {
        "name": "projects/citylens-001/locations/us-central1/backups/backup",
        "database": "projects/citylens-001/databases/(default)",
        "state": "READY",
        "snapshotTime": snapshot.isoformat().replace("+00:00", "Z"),
        "expireTime": expire.isoformat().replace("+00:00", "Z"),
    }


class FakeCommand:
    def __init__(self, backups: list[dict[str, Any]], *, created_at: datetime) -> None:
        self.responses: list[Any] = [
            _database(),
            [
                _schedule("daily", created_at=created_at),
                _schedule("weekly", created_at=created_at),
            ],
            _bucket(),
            backups,
        ]

    def json(self, *args: str) -> Any:
        if not self.responses:
            raise AssertionError(f"unexpected command: {args}")
        return self.responses.pop(0)


def _verify(
    backups: list[dict[str, Any]],
    *,
    schedule_age: timedelta = timedelta(hours=1),
) -> dict[str, Any]:
    return verify(
        project="citylens-001",
        bucket="citylens-001-artifacts",
        location="us-central1",
        now=NOW,
        maximum_backup_age=timedelta(hours=36),
        initial_grace=timedelta(hours=26),
        command=FakeCommand(
            backups,
            created_at=NOW - schedule_age,
        ),
    )


def test_recent_ready_backup_passes() -> None:
    result = _verify([_backup()])
    assert result["status"] == "ready"
    assert result["ready"] is True
    assert result["configuration_healthy"] is True
    assert result["ready_backup_count"] == 1
    assert result["failures"] == []


def test_new_schedule_without_backup_is_collecting() -> None:
    result = _verify([])
    assert result["status"] == "collecting"
    assert result["ready"] is False
    assert result["failures"] == []
    assert result["warnings"]


def test_missing_backup_after_grace_fails() -> None:
    result = _verify([], schedule_age=timedelta(hours=27))
    assert result["status"] == "failed"
    assert result["failures"] == [
        "no READY backup exists for the production database"
    ]


def test_stale_or_expired_backup_fails() -> None:
    result = _verify(
        [
            _backup(
                snapshot=NOW - timedelta(hours=37),
                expire=NOW - timedelta(seconds=1),
            )
        ]
    )
    assert result["status"] == "failed"
    assert any("seconds old" in failure for failure in result["failures"])
    assert any("already expired" in failure for failure in result["failures"])

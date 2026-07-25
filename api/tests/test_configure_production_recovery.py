from __future__ import annotations

from typing import Any

from scripts.configure_production_recovery import (
    DAILY_RETENTION,
    SCHEDULES,
    WEEKLY_RETENTION,
    bucket_drift,
    configure,
    database_drift,
    ensure_bucket,
    ensure_schedule,
)


def _database() -> dict[str, Any]:
    return {
        "name": "projects/citylens-001/databases/(default)",
        "deleteProtectionState": "DELETE_PROTECTION_ENABLED",
        "pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_ENABLED",
        "versionRetentionPeriod": "604800s",
    }


def _daily() -> dict[str, Any]:
    return {
        "name": (
            "projects/citylens-001/databases/(default)/backupSchedules/daily"
        ),
        "dailyRecurrence": {},
        "retention": DAILY_RETENTION,
        "createTime": "2026-07-25T03:14:17Z",
    }


def _weekly() -> dict[str, Any]:
    return {
        "name": (
            "projects/citylens-001/databases/(default)/backupSchedules/weekly"
        ),
        "weeklyRecurrence": {"day": "SUNDAY"},
        "retention": WEEKLY_RETENTION,
        "createTime": "2026-07-25T03:14:18Z",
    }


def _bucket() -> dict[str, Any]:
    return {
        "name": "citylens-001-artifacts",
        "public_access_prevention": "enforced",
        "uniform_bucket_level_access": True,
        "soft_delete_policy": {
            "retentionDurationSeconds": "604800",
        },
    }


class FakeCommand:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, ...]] = []

    def json(self, *args: str) -> Any:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError(f"unexpected command: {args}")
        return self.responses.pop(0)


def test_recovery_contract_detects_database_and_bucket_drift() -> None:
    database = _database()
    assert database_drift(database) == []
    database["pointInTimeRecoveryEnablement"] = (
        "POINT_IN_TIME_RECOVERY_DISABLED"
    )
    assert any("pointInTime" in reason for reason in database_drift(database))

    bucket = _bucket()
    assert bucket_drift(bucket) == []
    bucket["public_access_prevention"] = "inherited"
    bucket["soft_delete_policy"]["retentionDurationSeconds"] = "3600"
    drift = bucket_drift(bucket)
    assert any("public access" in reason for reason in drift)
    assert any("seven-day" in reason for reason in drift)


def test_configure_dry_run_is_idempotent_and_non_mutating() -> None:
    command = FakeCommand(
        [_database(), [_daily(), _weekly()], _bucket()]
    )
    result = configure(
        project="citylens-001",
        bucket="citylens-001-artifacts",
        apply=False,
        command=command,
    )
    assert result["firestore"]["action"] == "unchanged"
    assert [row["action"] for row in result["backup_schedules"]] == [
        "unchanged",
        "unchanged",
    ]
    assert result["artifact_bucket_protection"]["action"] == "unchanged"
    assert len(command.calls) == 3


def test_schedule_create_and_update_use_bounded_retention() -> None:
    create = FakeCommand([_daily()])
    created = ensure_schedule(
        create,
        spec=SCHEDULES[0],
        existing=None,
        apply=True,
    )
    assert created["action"] == "created"
    assert "--recurrence=daily" in create.calls[0]
    assert "--retention=1209600s" in create.calls[0]

    existing = _weekly()
    existing["retention"] = "604800s"
    update = FakeCommand([_weekly()])
    updated = ensure_schedule(
        update,
        spec=SCHEDULES[1],
        existing=existing,
        apply=True,
    )
    assert updated["action"] == "updated"
    assert "--backup-schedule=weekly" in update.calls[0]
    assert "--retention=8467200s" in update.calls[0]


def test_bucket_apply_enforces_private_soft_delete_baseline() -> None:
    existing = _bucket()
    existing["uniform_bucket_level_access"] = False
    updated = _bucket()
    command = FakeCommand([updated])
    result = ensure_bucket(
        command,
        bucket="citylens-001-artifacts",
        row=existing,
        apply=True,
    )
    assert result["action"] == "updated"
    args = command.calls[0]
    assert "--uniform-bucket-level-access" in args
    assert "--public-access-prevention" in args
    assert "--soft-delete-duration=7d" in args


def test_duplicate_backup_schedule_fails_closed() -> None:
    command = FakeCommand(
        [_database(), [_daily(), _daily(), _weekly()], _bucket()]
    )
    try:
        configure(
            project="citylens-001",
            bucket="citylens-001-artifacts",
            apply=False,
            command=command,
        )
    except RuntimeError as exc:
        assert "multiple daily" in str(exc)
    else:
        raise AssertionError("duplicate schedules were accepted")


def test_wrong_weekly_day_fails_closed() -> None:
    existing = _weekly()
    existing["weeklyRecurrence"]["day"] = "MONDAY"
    try:
        ensure_schedule(
            FakeCommand([]),
            spec=SCHEDULES[1],
            existing=existing,
            apply=True,
        )
    except RuntimeError as exc:
        assert "recurrence is immutable" in str(exc)
    else:
        raise AssertionError("wrong weekly recurrence was accepted")

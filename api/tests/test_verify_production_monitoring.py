from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from scripts.configure_production_monitoring import SPECS
from scripts.verify_production_monitoring import (
    latest_observations,
    verify,
)

NOW = datetime(2026, 7, 25, 3, 0, tzinfo=UTC)


def _uptime(spec_index: int) -> dict[str, Any]:
    spec = SPECS[spec_index]
    return {
        "name": (
            "projects/citylens-001/uptimeCheckConfigs/"
            f"existing-{spec.key}"
        ),
        "displayName": spec.uptime_display_name,
    }


def _series(
    location: str,
    *,
    passed: bool = True,
    observed_at: datetime = NOW,
) -> dict[str, Any]:
    return {
        "metric": {"labels": {"checker_location": location}},
        "resource": {"type": "uptime_url"},
        "points": [
            {
                "interval": {
                    "endTime": observed_at.isoformat().replace(
                        "+00:00", "Z"
                    )
                },
                "value": {"boolValue": passed},
            }
        ],
    }


class FakeGcloud:
    def __init__(self, uptime_rows: list[dict[str, Any]]) -> None:
        self.uptime_rows = uptime_rows

    def json(self, *args: str) -> Any:
        assert args == ("monitoring", "uptime", "list-configs")
        return self.uptime_rows


class FakeTimeSeries:
    def __init__(self, rows_by_check: dict[str, list[dict[str, Any]]]) -> None:
        self.rows_by_check = rows_by_check
        self.calls: list[str] = []

    def list_check_passed(
        self,
        *,
        project: str,
        check_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        assert project == "citylens-001"
        assert start == NOW - timedelta(minutes=20)
        assert end == NOW
        self.calls.append(check_id)
        return self.rows_by_check[check_id]


def test_latest_observations_keeps_newest_point_per_location() -> None:
    old = NOW - timedelta(minutes=10)
    rows = [
        _series("usa-iowa", passed=False, observed_at=old),
        _series("usa-iowa", passed=True),
        _series("europe", passed=True),
        {"metric": {"labels": {}}, "points": []},
    ]
    result = latest_observations(rows)
    assert [(row.location, row.passed) for row in result] == [
        ("europe", True),
        ("usa-iowa", True),
    ]


def test_verify_requires_fresh_success_from_multiple_regions() -> None:
    observations = [
        _series("asia-pacific"),
        _series("europe"),
        _series("usa-iowa"),
    ]
    time_series = FakeTimeSeries(
        {
            "existing-api": observations,
            "existing-web": observations,
        }
    )
    result = verify(
        project="citylens-001",
        now=NOW,
        max_age=timedelta(minutes=20),
        minimum_regions=3,
        command=FakeGcloud([_uptime(0), _uptime(1)]),
        time_series=time_series,
    )
    assert result["healthy"] is True
    assert [check["healthy"] for check in result["checks"]] == [True, True]
    assert time_series.calls == ["existing-api", "existing-web"]


def test_verify_fails_closed_on_missing_stale_or_failed_evidence() -> None:
    result = verify(
        project="citylens-001",
        now=NOW,
        max_age=timedelta(minutes=20),
        minimum_regions=3,
        command=FakeGcloud([_uptime(0)]),
        time_series=FakeTimeSeries(
            {
                "existing-api": [
                    _series("europe", passed=False),
                    _series(
                        "usa-iowa",
                        observed_at=NOW - timedelta(minutes=21),
                    ),
                ]
            }
        ),
    )
    assert result["healthy"] is False
    api, web = result["checks"]
    assert api["healthy"] is False
    assert any("only 2 regions" in failure for failure in api["failures"])
    assert any("latest observation failed" in failure for failure in api["failures"])
    assert any("observation is 1260 seconds old" in failure for failure in api["failures"])
    assert web["failures"] == ["managed uptime check is missing"]

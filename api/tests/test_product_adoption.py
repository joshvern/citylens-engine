from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.services.product_adoption import build_product_adoption_report


def test_report_is_aggregate_only_and_uses_explicit_window() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": "private-user-a",
                "day": "2026-07-24",
                "events": {"parcel_opened": 3, "workflow_created": 1},
                "sources": {
                    "parcel_opened:map": 2,
                    "parcel_opened:ranking": 1,
                    "workflow_created:header": 1,
                },
                "bbl": "3020960069",
            },
            {
                "_user_id": "private-user-b",
                "day": "2026-07-23",
                "events": {"parcel_opened": 1, "workflow_updated": 2},
                "sources": {
                    "parcel_opened:direct": 1,
                    "workflow_updated:workflow": 2,
                },
            },
            {
                "_user_id": "private-user-a",
                "day": "2026-01-01",
                "events": {"parcel_opened": 99},
            },
        ],
        as_of=datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc),
        days=30,
    )

    assert report["window"] == {
        "days": 30,
        "start": "2026-06-25",
        "end": "2026-07-24",
    }
    assert report["active_users"] == 2
    assert report["active_user_days"] == 2
    assert report["total_events"] == 7
    assert report["events"] == {
        "parcel_opened": 4,
        "workflow_created": 1,
        "workflow_updated": 2,
    }
    assert report["parcel_open_to_workflow_create_rate"] == 0.25
    assert report["model_accuracy_claim"] is False
    assert report["excluded_or_invalid_rows"] == 1
    rendered = json.dumps(report)
    assert "private-user" not in rendered
    assert "3020960069" not in rendered


def test_report_handles_empty_window_without_false_rate() -> None:
    report = build_product_adoption_report(
        [],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
        days=7,
    )
    assert report["active_users"] == 0
    assert report["total_events"] == 0
    assert report["parcel_open_to_workflow_create_rate"] is None
    assert any("No qualifying" in warning for warning in report["warnings"])


@pytest.mark.parametrize("days", [0, 91])
def test_report_rejects_windows_outside_retention(days: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 90"):
        build_product_adoption_report([], days=days)

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.services.parcel_workflow_actions import (
    build_workflow_actions,
    normalize_workflow_action_payload,
)


def _item(as_of: datetime, **overrides):
    item = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "stage": "reviewing",
        "outcome": "unknown",
        "assignee": None,
        "next_action": None,
        "next_action_due_date": None,
        "saved_at": as_of - timedelta(days=45),
        "updated_at": as_of - timedelta(days=10),
        "snapshot": {
            "address": "100 E 21 STREET",
            "citywide_rank": 82,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
        },
    }
    item.update(overrides)
    return item


def test_action_queue_classifies_and_orders_open_work() -> None:
    as_of = datetime(2026, 7, 24, 14, 0, tzinfo=timezone.utc)
    rows = [
        _item(
            as_of,
            bbl="3020960001",
            assignee="Acquisitions",
            next_action="Call owner",
            next_action_due_date="2026-07-22",
        ),
        _item(
            as_of,
            bbl="3020960002",
            next_action="Review title",
            next_action_due_date="2026-07-24",
            outcome="owner_contacted",
        ),
        _item(
            as_of,
            bbl="3020960003",
            next_action="Prepare offer range",
            next_action_due_date="2026-07-28",
            outcome="qualified",
        ),
        _item(as_of, bbl="3020960004"),
        _item(
            as_of,
            bbl="3020960005",
            stage="pursue",
            outcome="closed",
            next_action="Stale task",
            next_action_due_date="2026-07-20",
        ),
    ]

    result = build_workflow_actions(rows, as_of=as_of)

    assert result["schema_version"] == "citylens/parcel-workflow-actions@v1"
    assert result["total_records"] == 5
    assert result["open_records"] == 4
    assert result["completed_records"] == 1
    assert result["overdue_count"] == 1
    assert result["due_today_count"] == 1
    assert result["due_soon_count"] == 1
    assert result["unscheduled_count"] == 1
    assert result["unassigned_count"] == 3
    assert result["outcome_update_due_count"] == 2
    assert [item["action_state"] for item in result["items"]] == [
        "overdue",
        "due_today",
        "due_soon",
        "unscheduled",
    ]
    assert result["items"][0]["days_overdue"] == 2
    assert result["items"][0]["address"] == "100 E 21 STREET"
    assert result["items"][0]["citywide_rank"] == 82


def test_action_payload_requires_a_task_for_a_date_and_clears_terminal_tasks() -> None:
    with pytest.raises(ValueError, match="next_action is required"):
        normalize_workflow_action_payload(
            {
                "stage": "reviewing",
                "outcome": "unknown",
                "next_action": " ",
                "next_action_due_date": "2026-07-25",
            }
        )

    normalized = normalize_workflow_action_payload(
        {
            "stage": "pursue",
            "outcome": "closed",
            "next_action": "This should not survive",
            "next_action_due_date": "2026-07-25",
        }
    )
    assert normalized["next_action"] is None
    assert normalized["next_action_due_date"] is None


def test_action_payload_serializes_python_dates_for_firestore() -> None:
    normalized = normalize_workflow_action_payload(
        {
            "stage": "reviewing",
            "outcome": "unknown",
            "next_action": "  Call the owner  ",
            "next_action_due_date": datetime(2026, 7, 25).date(),
        }
    )
    assert normalized["next_action"] == "Call the owner"
    assert normalized["next_action_due_date"] == "2026-07-25"

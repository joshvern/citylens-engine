from __future__ import annotations

from datetime import datetime, timezone

from app.services.parcel_workflow_analytics import (
    build_workflow_analytics,
    milestone_patch,
)


def test_progression_outcome_backfills_necessary_milestones_once() -> None:
    now = datetime(2026, 7, 23, tzinfo=timezone.utc)
    patch = milestone_patch(outcome="under_contract", existing={}, occurred_at=now)
    assert set(patch) == {
        "first_contacted_at",
        "first_meeting_scheduled_at",
        "first_qualified_at",
        "first_offer_submitted_at",
        "first_under_contract_at",
    }
    assert milestone_patch(
        outcome="under_contract",
        existing={"first_contacted_at": now},
        occurred_at=now,
    ).get("first_contacted_at") is None


def test_analytics_uses_explicit_denominators_and_preserves_archived_labels() -> None:
    now = datetime(2026, 7, 23, tzinfo=timezone.utc)
    rows = [
        {
            "borough": "queens",
            "stage": "pursue",
            "outcome": "closed",
            "saved_at": now,
            "archived_at": None,
            "event_count": 3,
            "first_contacted_at": now,
            "first_meeting_scheduled_at": now,
            "first_qualified_at": now,
            "first_offer_submitted_at": now,
            "first_under_contract_at": now,
            "first_closed_at": now,
            "snapshot": {
                "citywide_rank": 50,
                "opportunity_category": "vacant_site",
            },
        },
        {
            "borough": "queens",
            "stage": "pass",
            "outcome": "rejected",
            "saved_at": now,
            "archived_at": now,
            "event_count": 2,
            "first_rejected_at": now,
            "snapshot": {
                "citywide_rank": 600,
                "opportunity_category": "ground_up_candidate",
            },
        },
    ]
    analytics = build_workflow_analytics(rows)
    assert analytics["total_records"] == 2
    assert analytics["active_records"] == 1
    assert analytics["archived_records"] == 1
    assert analytics["funnel"]["closed"] == 1
    assert analytics["funnel"]["rejected"] == 1
    assert analytics["funnel"]["contacted_per_saved"] == {
        "numerator": 1,
        "denominator": 2,
        "rate": 0.5,
        "sufficient_denominator": False,
    }

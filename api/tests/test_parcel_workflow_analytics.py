from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.models.schemas import ParcelWorkflowAnalytics
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
    analytics = build_workflow_analytics(rows, as_of=now)
    assert analytics["total_records"] == 2
    assert analytics["active_records"] == 1
    assert analytics["archived_records"] == 1
    assert analytics["funnel"]["closed"] == 1
    assert analytics["funnel"]["rejected"] == 1
    assert analytics["funnel"]["contacted_per_saved"] == {
        "numerator": 1,
        "denominator": 2,
        "rate": None,
        "confidence_interval": None,
        "sufficient_denominator": False,
    }


def test_analytics_uses_fixed_horizons_and_excludes_late_backfills() -> None:
    as_of = datetime(2026, 7, 23, tzinfo=timezone.utc)
    rows: list[dict] = []
    for index in range(30):
        saved_at = as_of - timedelta(days=400 + index)
        row = {
            "borough": "brooklyn",
            "stage": "pursue",
            "outcome": "unknown",
            "saved_at": saved_at,
            "archived_at": None,
            "event_count": 2,
            "snapshot": {
                "citywide_rank": index + 1,
                "opportunity_category": "ground_up_candidate",
            },
        }
        if index < 12:
            row["outcome"] = "owner_contacted"
            row["first_contacted_at"] = saved_at + timedelta(days=20)
        if index < 8:
            row["outcome"] = "qualified"
            row["first_qualified_at"] = saved_at + timedelta(days=60)
        if index < 4:
            row["outcome"] = "closed"
            row["first_closed_at"] = saved_at + timedelta(days=300)
        rows.append(row)

    # This record eventually reached every inferred milestone but was entered
    # after each deadline. It remains visible in the operational funnel and
    # must not inflate fixed-horizon prospective rates.
    late_saved = as_of - timedelta(days=500)
    rows.append(
        {
            "borough": "queens",
            "stage": "pursue",
            "outcome": "closed",
            "saved_at": late_saved,
            "archived_at": None,
            "event_count": 3,
            "first_contacted_at": late_saved + timedelta(days=31),
            "first_qualified_at": late_saved + timedelta(days=91),
            "first_offer_submitted_at": late_saved + timedelta(days=181),
            "first_under_contract_at": late_saved + timedelta(days=271),
            "first_closed_at": late_saved + timedelta(days=366),
            "snapshot": {
                "citywide_rank": 700,
                "opportunity_category": "vacant_site",
            },
        }
    )

    analytics = build_workflow_analytics(rows, as_of=as_of)
    assert analytics["schema_version"] == "citylens/parcel-workflow-analytics@v3"
    assert analytics["measurement_status"] == "usable"
    assert analytics["valid_saved_at_records"] == 31
    assert analytics["oldest_followup_days"] == 500
    windows = {row["milestone"]: row for row in analytics["maturity_windows"]}
    assert windows["owner_contacted"] == {
        "milestone": "owner_contacted",
        "label": "Contacted within 30 days",
        "horizon_days": 30,
        "eligible_records": 31,
        "reached_within_horizon": 12,
        "pending_records": 0,
        "rate": 0.3871,
        "confidence_interval": {
            "confidence_level": 0.95,
            "lower": 0.2373,
            "upper": 0.5618,
        },
        "sufficient_denominator": True,
    }
    assert windows["qualified"]["reached_within_horizon"] == 8
    assert windows["closed"]["reached_within_horizon"] == 4
    assert analytics["funnel"]["closed"] == 5


def test_analytics_withholds_directional_status_until_observation_time_matures() -> None:
    as_of = datetime(2026, 7, 23, tzinfo=timezone.utc)
    rows = [
        {
            "borough": "bronx",
            "stage": "new",
            "outcome": "unknown",
            "saved_at": as_of - timedelta(days=5),
            "event_count": 1,
            "snapshot": {"citywide_rank": index + 1},
        }
        for index in range(40)
    ]
    rows.append(
        {
            "borough": "bronx",
            "stage": "new",
            "outcome": "unknown",
            "saved_at": "not-a-date",
            "event_count": 1,
            "snapshot": {"citywide_rank": 41},
        }
    )

    analytics = build_workflow_analytics(rows, as_of=as_of)
    assert analytics["measurement_status"] == "collecting"
    assert analytics["measurement_label"] == "Collecting observation time"
    assert analytics["valid_saved_at_records"] == 40
    assert analytics["maturity_windows"][0]["eligible_records"] == 0
    assert analytics["maturity_windows"][0]["confidence_interval"] is None
    assert any("invalid" in warning for warning in analytics["warnings"])


def test_analytics_omits_every_subthreshold_rate_from_the_api() -> None:
    as_of = datetime(2026, 7, 23, tzinfo=timezone.utc)
    rows: list[dict] = []
    for index in range(9):
        saved_at = as_of - timedelta(days=400)
        rows.append(
            {
                "borough": "brooklyn",
                "stage": "pursue",
                "outcome": "qualified" if index < 4 else "unknown",
                "saved_at": saved_at,
                "archived_at": None,
                "event_count": 2,
                "first_contacted_at": (
                    saved_at + timedelta(days=10) if index < 6 else None
                ),
                "first_qualified_at": (
                    saved_at + timedelta(days=45) if index < 4 else None
                ),
                "snapshot": {
                    "citywide_rank": index + 1,
                    "opportunity_category": "ground_up_candidate",
                },
            }
        )

    analytics = build_workflow_analytics(rows, as_of=as_of)

    assert analytics["measurement_status"] == "collecting"
    assert analytics["funnel"]["contacted_per_saved"] == {
        "numerator": 6,
        "denominator": 9,
        "rate": None,
        "confidence_interval": None,
        "sufficient_denominator": False,
    }
    assert all(
        window["rate"] is None
        and window["confidence_interval"] is None
        and window["sufficient_denominator"] is False
        for window in analytics["maturity_windows"]
    )
    rank_cohort = next(
        cohort
        for cohort in analytics["cohorts"]
        if cohort["dimension"] == "rank_band" and cohort["value"] == "1-100"
    )
    assert rank_cohort["contacted_rate_denominator"] == 9
    assert rank_cohort["qualified_rate_denominator"] == 9
    assert rank_cohort["close_rate_denominator"] == 9
    assert rank_cohort["contacted_rate"] is None
    assert rank_cohort["contacted_confidence_interval"] is None
    assert rank_cohort["qualified_rate"] is None
    assert rank_cohort["qualified_confidence_interval"] is None
    assert rank_cohort["close_rate"] is None
    assert rank_cohort["close_confidence_interval"] is None


def test_analytics_schema_rejects_a_subthreshold_rate_leak() -> None:
    as_of = datetime(2026, 7, 23, tzinfo=timezone.utc)
    payload = build_workflow_analytics(
        [
            {
                "borough": "brooklyn",
                "stage": "pursue",
                "outcome": "owner_contacted",
                "saved_at": as_of - timedelta(days=40),
                "event_count": 1,
                "first_contacted_at": as_of - timedelta(days=20),
                "snapshot": {"citywide_rank": 1},
            }
        ],
        as_of=as_of,
    )
    payload["maturity_windows"][0]["rate"] = 1.0
    payload["maturity_windows"][0]["confidence_interval"] = {
        "confidence_level": 0.95,
        "lower": 0.2,
        "upper": 1.0,
    }

    with pytest.raises(
        ValidationError,
        match="maturity-window rate and interval must be present only after maturity",
    ):
        ParcelWorkflowAnalytics.model_validate(payload)

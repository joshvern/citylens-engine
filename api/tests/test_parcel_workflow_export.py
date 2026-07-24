from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.services.parcel_workflow_export import build_workflow_outcome_export


def _workflow_row(
    *,
    bbl: str,
    saved_at: datetime,
    event_count: int = 1,
    **overrides,
) -> dict:
    row = {
        "bbl": bbl,
        "borough": "brooklyn",
        "stage": "reviewing",
        "outcome": "unknown",
        "decision_reason": "pursuing",
        "saved_at": saved_at,
        "archived_at": None,
        "event_count": event_count,
        "notes": "Do not export this private note",
        "tags": ["private-tag"],
        "assignee": "Acquisitions Team",
        "next_action": "Call private contact",
        "snapshot": {
            "address": "Private workflow address",
            "owner_name": "PRIVATE OWNER LLC",
            "feed_generated_at": "2026-01-01T00:00:00Z",
            "property_facts_as_of": "2026-01-01",
            "citywide_rank": 75,
            "acquisition_rank": 12,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
            "score_calibrated": 0.31,
        },
    }
    row.update(overrides)
    return row


def test_export_emits_only_mature_instrumented_labels_and_redacts_private_data() -> None:
    as_of = datetime(2026, 7, 24, tzinfo=timezone.utc)
    saved_at = as_of - timedelta(days=400)
    export = build_workflow_outcome_export(
        [
            _workflow_row(
                bbl="3020960069",
                saved_at=saved_at,
                outcome="qualified",
                first_contacted_at=saved_at + timedelta(days=5),
                first_qualified_at=saved_at + timedelta(days=120),
            )
        ],
        as_of=as_of,
    )

    assert export["schema_version"] == (
        "citylens/parcel-workflow-outcome-export@v1"
    )
    assert export["input_record_count"] == 1
    assert export["exported_record_count"] == 1
    assert export["event_history_observed_count"] == 1
    assert export["rank_snapshot_count"] == 1
    assert len(export["rows_sha256"]) == 64

    row = export["rows"][0]
    labels = {label["milestone"]: label for label in row["labels"]}
    assert labels["owner_contacted"]["state"] == "positive"
    assert labels["owner_contacted"]["value"] is True
    assert labels["owner_contacted"]["days_to_milestone"] == 5
    assert labels["qualified"]["state"] == "negative"
    assert labels["qualified"]["value"] is False
    assert labels["qualified"]["days_to_milestone"] == 120
    assert labels["closed"]["state"] == "negative"
    assert labels["closed"]["value"] is False
    assert row["decision_reason_category"] == "pursuing"

    serialized = json.dumps(export, default=str)
    for secret in (
        "Do not export",
        "private-tag",
        "Acquisitions Team",
        "Call private contact",
        "Private workflow address",
        "PRIVATE OWNER LLC",
    ):
        assert secret not in serialized


def test_export_normalizes_custom_decision_text_to_other() -> None:
    as_of = datetime(2026, 7, 24, tzinfo=timezone.utc)
    export = build_workflow_outcome_export(
        [
            _workflow_row(
                bbl="3020960069",
                saved_at=as_of - timedelta(days=40),
                decision_reason="Private custom explanation",
            )
        ],
        as_of=as_of,
    )

    assert export["rows"][0]["decision_reason_category"] == "other"
    assert "Private custom explanation" not in json.dumps(export, default=str)


def test_export_keeps_pending_and_uninstrumented_observations_null() -> None:
    as_of = datetime(2026, 7, 24, tzinfo=timezone.utc)
    recent = _workflow_row(
        bbl="3020960069",
        saved_at=as_of - timedelta(days=10),
    )
    legacy = _workflow_row(
        bbl="3020960070",
        saved_at=as_of - timedelta(days=500),
        event_count=0,
        outcome="closed",
    )
    invalid = _workflow_row(
        bbl="3020960071",
        saved_at="not-a-date",
    )

    export = build_workflow_outcome_export(
        [invalid, legacy, recent],
        as_of=as_of,
    )

    assert export["input_record_count"] == 3
    assert export["exported_record_count"] == 2
    assert export["excluded_invalid_saved_at_count"] == 1
    rows = {row["bbl"]: row for row in export["rows"]}
    assert {
        label["state"] for label in rows["3020960069"]["labels"]
    } == {"pending"}
    assert all(
        label["value"] is None
        for label in rows["3020960069"]["labels"]
    )
    assert {
        label["state"] for label in rows["3020960070"]["labels"]
    } == {"unavailable_history"}
    assert all(
        label["eligible"] is False
        and label["value"] is None
        for label in rows["3020960070"]["labels"]
    )


def test_export_digest_is_stable_across_input_order() -> None:
    as_of = datetime(2026, 7, 24, tzinfo=timezone.utc)
    rows = [
        _workflow_row(
            bbl="3020960070",
            saved_at=as_of - timedelta(days=50),
        ),
        _workflow_row(
            bbl="3020960069",
            saved_at=as_of - timedelta(days=50),
        ),
    ]

    forward = build_workflow_outcome_export(rows, as_of=as_of)
    reverse = build_workflow_outcome_export(
        list(reversed(rows)), as_of=as_of
    )

    assert forward["rows_sha256"] == reverse["rows_sha256"]
    assert [row["bbl"] for row in forward["rows"]] == [
        "3020960069",
        "3020960070",
    ]

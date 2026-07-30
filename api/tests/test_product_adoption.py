from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import get_args

import pytest
from scripts.report_product_adoption import (
    _read_pilot_request_rows,
    _read_saved_view_rows,
    _read_synthetic_actor_ids,
    _read_workflow_rows,
)

from app.models.schemas import PilotPlan, PilotRequestStatus
from app.services.product_adoption import (
    PILOT_PLAN_VALUES,
    PILOT_REQUEST_STATUS_VALUES,
    build_product_adoption_report,
)


def test_pilot_report_enums_match_the_public_admin_contract() -> None:
    assert PILOT_REQUEST_STATUS_VALUES == frozenset(
        get_args(PilotRequestStatus)
    )
    assert PILOT_PLAN_VALUES == frozenset(get_args(PilotPlan))


def test_report_is_aggregate_only_and_uses_explicit_window() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": "private-user-a",
                "day": "2026-07-24",
                "events": {
                    "parcel_opened": 3,
                    "official_dossier_opened": 2,
                    "comparison_opened": 2,
                    "thesis_composer_applied": 2,
                    "decision_audit_opened": 2,
                    "underwriting_opened": 2,
                    "underwriting_assumptions_changed": 1,
                    "saved_view_applied": 2,
                    "saved_view_comparison_opened": 1,
                    "saved_view_created": 1,
                    "saved_thesis_baseline_created": 1,
                    "saved_thesis_baseline_advanced": 1,
                    "saved_thesis_changes_opened": 2,
                    "workflow_created": 3,
                    "workflow_evidence_reviewed": 2,
                    "workflow_evidence_issue_submitted": 1,
                },
                "sources": {
                    "parcel_opened:map": 2,
                    "parcel_opened:ranking": 1,
                    "official_dossier_opened:official_dossier": 2,
                    "comparison_opened:comparison": 1,
                    "comparison_opened:decision_peers": 1,
                    "thesis_composer_applied:thesis_composer": 2,
                    "decision_audit_opened:decision_posture": 1,
                    "decision_audit_opened:audit_tab": 1,
                    "underwriting_opened:underwrite_tab": 2,
                    (
                        "underwriting_assumptions_changed:"
                        "base_assumptions"
                    ): 1,
                    "saved_view_applied:saved_views": 2,
                    "saved_view_comparison_opened:saved_views": 1,
                    "saved_view_created:saved_views": 1,
                    "saved_thesis_baseline_created:saved_views": 1,
                    "saved_thesis_baseline_advanced:saved_views": 1,
                    "saved_thesis_changes_opened:saved_views": 2,
                    "workflow_created:comparison": 1,
                    "workflow_created:decision_audit": 1,
                    "workflow_created:underwriting": 1,
                    "workflow_evidence_reviewed:workflow": 2,
                    "workflow_evidence_issue_submitted:workflow": 1,
                },
                "bbl": "3020960069",
            },
            {
                "_user_id": "private-user-b",
                "day": "2026-07-23",
                "events": {
                    "parcel_opened": 1,
                    "official_dossier_opened": 1,
                    "comparison_opened": 1,
                    "thesis_composer_applied": 1,
                    "decision_audit_opened": 1,
                    "underwriting_opened": 1,
                    "underwriting_assumptions_changed": 1,
                    "saved_view_applied": 1,
                    "saved_thesis_baseline_advanced": 1,
                    "saved_thesis_changes_opened": 1,
                    "workflow_updated": 2,
                    "workflow_evidence_reviewed": 1,
                },
                "sources": {
                    "parcel_opened:direct": 1,
                    "official_dossier_opened:official_dossier": 1,
                    "comparison_opened:comparison": 1,
                    "thesis_composer_applied:thesis_composer": 1,
                    "decision_audit_opened:audit_tab": 1,
                    "underwriting_opened:underwrite_tab": 1,
                    (
                        "underwriting_assumptions_changed:"
                        "base_assumptions"
                    ): 1,
                    "saved_view_applied:saved_views": 1,
                    "saved_thesis_baseline_advanced:saved_views": 1,
                    "saved_thesis_changes_opened:saved_views": 1,
                    "workflow_updated:workflow": 2,
                    "workflow_evidence_reviewed:workflow": 1,
                },
            },
            {
                "_user_id": "private-user-a",
                "day": "2026-01-01",
                "events": {"parcel_opened": 99},
            },
        ],
        workflow_rows=[
            {"_user_id": "private-user-a", "archived_at": None},
            {
                "_user_id": "private-user-b",
                "archived_at": "2026-07-20T00:00:00Z",
            },
            {"_user_id": "", "archived_at": None},
        ],
        saved_view_rows=[
            {
                "_user_id": "private-user-a",
                "schema_version": "citylens/parcel-saved-view@v2",
            },
            {
                "_user_id": "private-user-b",
                "schema_version": "citylens/parcel-saved-view@v2",
            },
            {"_user_id": "", "schema_version": "citylens/parcel-saved-view@v2"},
            {
                "_user_id": "private-user-c",
                "schema_version": "citylens/parcel-saved-view@v3",
            },
            {
                "_user_id": "private-user-c",
                "schema_version": "citylens/parcel-saved-search@v1",
            },
        ],
        pilot_request_rows=[
            {
                "status": "new",
                "plan": "acquisitions",
                "created_at": datetime(
                    2026, 7, 24, 11, 0, tzinfo=timezone.utc
                ),
                "work_email": "private@example.com",
            },
            {
                "status": "contacted",
                "plan": "concierge",
                "created_at": datetime(
                    2026, 6, 1, 11, 0, tzinfo=timezone.utc
                ),
            },
            {
                "status": "invented",
                "plan": "concierge",
                "created_at": datetime(
                    2026, 7, 24, 11, 0, tzinfo=timezone.utc
                ),
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
    assert report["total_events"] == 41
    assert report["events"] == {
        "comparison_opened": 3,
        "decision_audit_opened": 3,
        "official_dossier_opened": 3,
        "parcel_opened": 4,
        "saved_view_applied": 3,
        "saved_view_comparison_opened": 1,
        "saved_view_created": 1,
        "saved_thesis_baseline_advanced": 2,
        "saved_thesis_baseline_created": 1,
        "saved_thesis_changes_opened": 3,
        "thesis_composer_applied": 3,
        "underwriting_assumptions_changed": 2,
        "underwriting_opened": 3,
        "workflow_created": 3,
        "workflow_evidence_issue_submitted": 1,
        "workflow_evidence_reviewed": 3,
        "workflow_updated": 2,
    }
    assert report["parcel_open_to_workflow_create_rate"] == 0.75
    assert report["decision_audit_engagement"] == {
        "opened": 3,
        "users": 2,
        "workflow_creates": 1,
        "workflow_users": 1,
        "entry_points": {
            "decision_posture": 1,
            "audit_tab": 2,
        },
        "parcel_open_to_audit_rate": 0.75,
        "audit_to_workflow_create_rate": 0.333333,
        "evidence_gate": {
            "status": "collecting",
            "minimum_opens": 10,
            "minimum_users": 3,
            "opens_remaining": 7,
            "users_remaining": 1,
            "claim": (
                "Directional evidence-audit engagement only; opens are "
                "best-effort aggregate counters, not unique parcels, "
                "completed diligence, lead quality, or model accuracy."
            ),
        },
        "handoff_gate": {
            "status": "collecting",
            "minimum_workflow_creates": 5,
            "minimum_users": 3,
            "workflow_creates_remaining": 4,
            "users_remaining": 2,
            "claim": (
                "Canonical decision-audit-to-workflow handoffs only; the "
                "numerator is transactionally derived and contains no "
                "parcel IDs, actions, due dates, source facts, values, or "
                "notes. The directional rate is not diligence completion, "
                "lead quality, seller intent, or model accuracy."
            ),
        },
    }
    assert report["comparison_engagement"] == {
        "opened": 3,
        "users": 2,
        "workflow_creates": 1,
        "workflow_users": 1,
        "entry_points": {
            "comparison": 2,
            "decision_peers": 1,
        },
        "parcel_open_to_comparison_rate": 0.75,
        "comparison_to_workflow_create_rate": 0.333333,
        "evidence_gate": {
            "status": "collecting",
            "minimum_opens": 10,
            "minimum_users": 3,
            "opens_remaining": 7,
            "users_remaining": 1,
            "claim": (
                "Directional shortlist-comparison engagement only; opens "
                "are best-effort aggregate counters with no parcel IDs or "
                "values, not unique shortlists, completed diligence, "
                "lead quality, or model accuracy."
            ),
        },
        "handoff_gate": {
            "status": "collecting",
            "minimum_workflow_creates": 5,
            "minimum_users": 3,
            "workflow_creates_remaining": 4,
            "users_remaining": 2,
            "claim": (
                "Canonical comparison-to-workflow handoffs only; the "
                "numerator is transactionally derived and contains no "
                "parcel IDs, actions, due dates, values, or notes. The "
                "directional rate is not lead quality, seller intent, or "
                "model accuracy."
            ),
        },
    }
    assert report["underwriting_engagement"] == {
        "opened": 3,
        "open_users": 2,
        "first_adjustments": 2,
        "adjustment_users": 2,
        "workflow_creates": 1,
        "workflow_users": 1,
        "entry_points": {
            "underwrite_tab": 3,
            "base_assumptions": 2,
        },
        "directional_adjustment_to_open_ratio": 0.666667,
        "directional_open_to_workflow_rate": 0.333333,
        "evidence_gate": {
            "status": "collecting",
            "minimum_opens": 10,
            "minimum_open_users": 3,
            "minimum_first_adjustments": 5,
            "minimum_adjustment_users": 3,
            "opens_remaining": 7,
            "open_users_remaining": 1,
            "first_adjustments_remaining": 3,
            "adjustment_users_remaining": 1,
            "claim": (
                "Directional underwriting engagement only; opens and "
                "first adjustments are best-effort aggregate counters, "
                "not unique parcels, assumption values, saved scenarios, "
                "valuations, transactions, lead quality, or model "
                "accuracy."
            ),
        },
        "handoff_gate": {
            "status": "collecting",
            "minimum_workflow_creates": 5,
            "minimum_users": 3,
            "workflow_creates_remaining": 4,
            "users_remaining": 2,
            "claim": (
                "Canonical underwriting-to-workflow handoffs only; the "
                "numerator is transactionally derived and contains no "
                "parcel IDs, actions, due dates, assumptions, values, or "
                "notes. The directional rate is not valuation accuracy, "
                "lead quality, seller intent, or a transaction outcome."
            ),
        },
    }
    assert report["model_accuracy_claim"] is False
    assert report["excluded_or_invalid_rows"] == 1
    assert report["schema_version"] == "citylens/product-adoption-report@v18"
    assert report["measurement_governance"] == {
        "synthetic_actor_class": "synthetic_monitor",
        "synthetic_actors_excluded": 0,
        "product_usage_days_excluded": 0,
        "workflow_records_excluded": 0,
        "saved_view_records_excluded": 0,
        "identifiers_reported": False,
    }
    assert report["thesis_composer_engagement"] == {
        "applied": 3,
        "users": 2,
        "source": "thesis_composer_applied:thesis_composer",
        "evidence_gate": {
            "status": "collecting",
            "minimum_applies": 10,
            "minimum_users": 3,
            "applies_remaining": 7,
            "users_remaining": 1,
            "claim": (
                "Directional constrained-composer engagement only. "
                "Applies are best-effort aggregate counters containing "
                "no prompt text, parsed criteria, thresholds, geography, "
                "result count, BBL, address, owner, value, or source fact. "
                "They are not unique strategies, lead quality, seller "
                "intent, acquisition outcomes, or model accuracy."
            ),
        },
    }
    assert report["official_dossier_engagement"] == {
        "opened": 3,
        "users": 2,
        "source": "official_dossier_opened:official_dossier",
        "evidence_gate": {
            "status": "collecting",
            "minimum_opens": 10,
            "minimum_users": 3,
            "opens_remaining": 7,
            "users_remaining": 1,
            "claim": (
                "Directional dossier engagement only. Opens are best-effort "
                "aggregate counters containing no BBL, address, owner, source "
                "fact, readiness state, lead membership, or result. They are "
                "not diligence completion, lead quality, seller intent, or "
                "model accuracy."
            ),
        },
    }
    assert report["evidence_review_engagement"] == {
        "reviewed_versions": 3,
        "users": 2,
        "source": "workflow_evidence_reviewed:workflow",
        "evidence_gate": {
            "status": "collecting",
            "minimum_reviewed_versions": 10,
            "minimum_users": 3,
            "reviewed_versions_remaining": 7,
            "users_remaining": 1,
            "claim": (
                "Canonical source-bound review markers only. A marker "
                "means a user considered the exact cited evidence version; "
                "it does not establish completed or cleared diligence, "
                "lead quality, seller intent, or model accuracy."
            ),
        },
    }
    assert report["evidence_issue_engagement"] == {
        "submitted": 1,
        "users": 1,
        "source": "workflow_evidence_issue_submitted:workflow",
        "claim": (
            "Canonical aggregate evidence-governance submissions only. "
            "Counts exclude parcel IDs, cited values, sources, reasons, "
            "notes, request IDs, and resolution outcomes. A submission "
            "signals data-quality friction, not an incorrect official "
            "record, lead quality, seller intent, or model accuracy."
        ),
    }
    assert report["workflow_inventory"] == {
        "records": 2,
        "active": 1,
        "archived": 1,
        "users": 2,
        "excluded_or_invalid_rows": 1,
    }
    assert report["saved_view_inventory"] == {
        "records": 3,
        "users": 3,
        "monitored_records": 1,
        "monitored_users": 1,
        "excluded_or_invalid_rows": 2,
    }
    assert report["saved_view_reuse"]["created"] == 1
    assert report["saved_view_reuse"]["updated"] == 0
    assert report["saved_view_reuse"]["deleted"] == 0
    assert report["saved_view_reuse"]["applied"] == 3
    assert report["saved_view_reuse"]["comparisons"] == 1
    assert report["saved_view_reuse"]["event_users"] == 2
    assert report["saved_view_reuse"]["apply_users"] == 2
    assert report["saved_view_reuse"]["comparison_users"] == 1
    assert report["saved_view_reuse"]["evidence_gate"]["status"] == "collecting"
    assert report["thesis_monitor_engagement"] == {
        "monitored_views": 1,
        "monitored_view_users": 1,
        "baselines_created": 1,
        "baseline_creation_users": 1,
        "baselines_advanced": 2,
        "baseline_advance_users": 2,
        "change_reviews": 3,
        "change_review_users": 2,
        "evidence_gate": {
            "status": "collecting",
            "minimum_baseline_advances": 5,
            "minimum_baseline_advance_users": 3,
            "minimum_change_reviews": 10,
            "minimum_change_review_users": 3,
            "baseline_advances_remaining": 3,
            "baseline_advance_users_remaining": 1,
            "change_reviews_remaining": 7,
            "change_review_users_remaining": 1,
            "claim": (
                "Directional saved-thesis engagement only. Baseline "
                "lifecycle counters are transactionally derived and "
                "change-review opens are best-effort aggregate counters. "
                "They contain no view IDs, BBLs, filters, result counts, "
                "generations, addresses, owners, values, or notes and do "
                "not establish lead quality, seller intent, transaction "
                "evidence, acquisition outcomes, or model accuracy."
            ),
        },
    }
    assert report["pilot_intake"] == {
        "records": 2,
        "recent_requests": 1,
        "status_counts": {"contacted": 1, "new": 1},
        "plan_counts": {"acquisitions": 1, "concierge": 1},
        "new_requests_waiting": 1,
        "excluded_or_invalid_rows": 1,
        "privacy_scope": (
            "Aggregate counts only; excludes names, emails, companies, "
            "roles, boroughs, workflow summaries, request IDs, and "
            "network metadata."
        ),
    }
    assert report["activation_evidence_gate"] == {
        "status": "collecting",
        "minimum_workflow_records": 30,
        "minimum_workflow_users": 3,
        "records_remaining": 28,
        "users_remaining": 1,
        "claim": (
            "Directional activation evidence only; this gate does not "
            "establish lead quality, seller intent, or model accuracy."
        ),
    }
    assert any(
        "saved-view mutation counts are derived transactionally" in warning
        for warning in report["warnings"]
    )
    rendered = json.dumps(report)
    assert "private-user" not in rendered
    assert "3020960069" not in rendered
    assert "private@example.com" not in rendered


def test_report_handles_empty_window_without_false_rate() -> None:
    report = build_product_adoption_report(
        [],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
        days=7,
    )
    assert report["active_users"] == 0
    assert report["total_events"] == 0
    assert report["parcel_open_to_workflow_create_rate"] is None
    assert report["acquisition_funnel"]["market_explorer"]["opened"] == 0
    assert report["acquisition_funnel"]["market_explorer"]["users"] == 0
    assert report["acquisition_funnel"]["same_window_user_rates"] == {
        "explorer_to_parcel_review": None,
        "explorer_to_comparison": None,
        "explorer_to_workflow_create": None,
    }
    assert (
        report["acquisition_funnel"]["evidence_gate"]["status"]
        == "collecting"
    )
    assert report["decision_audit_engagement"]["opened"] == 0
    assert report["decision_audit_engagement"]["parcel_open_to_audit_rate"] is None
    assert report["decision_audit_engagement"]["workflow_creates"] == 0
    assert report["decision_audit_engagement"]["workflow_users"] == 0
    assert (
        report["decision_audit_engagement"][
            "audit_to_workflow_create_rate"
        ]
        is None
    )
    assert (
        report["decision_audit_engagement"]["evidence_gate"]["status"]
        == "collecting"
    )
    assert (
        report["decision_audit_engagement"]["handoff_gate"]["status"]
        == "collecting"
    )
    assert report["comparison_engagement"]["opened"] == 0
    assert report["comparison_engagement"]["workflow_creates"] == 0
    assert report["comparison_engagement"]["parcel_open_to_comparison_rate"] is None
    assert (
        report["comparison_engagement"]["comparison_to_workflow_create_rate"]
        is None
    )
    assert (
        report["comparison_engagement"]["evidence_gate"]["status"]
        == "collecting"
    )
    assert (
        report["comparison_engagement"]["handoff_gate"]["status"]
        == "collecting"
    )
    assert report["underwriting_engagement"]["opened"] == 0
    assert report["underwriting_engagement"]["first_adjustments"] == 0
    assert report["underwriting_engagement"]["workflow_creates"] == 0
    assert report["underwriting_engagement"]["workflow_users"] == 0
    assert (
        report["underwriting_engagement"][
            "directional_adjustment_to_open_ratio"
        ]
        is None
    )
    assert (
        report["underwriting_engagement"][
            "directional_open_to_workflow_rate"
        ]
        is None
    )
    assert (
        report["underwriting_engagement"]["evidence_gate"]["status"]
        == "collecting"
    )
    assert (
        report["underwriting_engagement"]["handoff_gate"]["status"]
        == "collecting"
    )
    assert report["workflow_inventory"]["records"] == 0
    assert report["evidence_issue_engagement"]["submitted"] == 0
    assert report["evidence_issue_engagement"]["users"] == 0
    assert report["saved_view_inventory"]["records"] == 0
    assert report["pilot_intake"]["records"] == 0
    assert report["saved_view_reuse"]["evidence_gate"]["status"] == "collecting"
    assert report["activation_evidence_gate"]["status"] == "collecting"
    assert any("No qualifying" in warning for warning in report["warnings"])


def test_acquisition_funnel_requires_mature_verified_market_activation() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": "user-a",
                "day": "2026-07-24",
                "events": {
                    "market_explorer_opened": 4,
                    "parcel_opened": 1,
                    "comparison_opened": 1,
                    "workflow_created": 1,
                },
                "sources": {
                    "market_explorer_opened:full_inventory": 4,
                    "parcel_opened:map": 1,
                    "comparison_opened:comparison": 1,
                    "workflow_created:comparison": 1,
                },
            },
            {
                "_user_id": "user-b",
                "day": "2026-07-24",
                "events": {
                    "market_explorer_opened": 3,
                    "parcel_opened": 1,
                    "comparison_opened": 1,
                },
                "sources": {
                    "market_explorer_opened:full_inventory": 3,
                    "parcel_opened:ranking": 1,
                    "comparison_opened:comparison": 1,
                },
            },
            {
                "_user_id": "user-c",
                "day": "2026-07-24",
                "events": {"market_explorer_opened": 3},
                "sources": {
                    "market_explorer_opened:full_inventory": 3,
                },
            },
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    funnel = report["acquisition_funnel"]
    assert funnel["market_explorer"] == {
        "opened": 10,
        "users": 3,
        "source": "market_explorer_opened:full_inventory",
    }
    assert funnel["parcel_review"] == {
        "opened": 2,
        "users": 2,
        "market_users_reached": 2,
    }
    assert funnel["comparison"] == {
        "opened": 2,
        "users": 2,
        "market_users_reached": 2,
    }
    assert funnel["workflow_create"] == {
        "created": 1,
        "users": 1,
        "market_users_reached": 1,
    }
    assert funnel["same_window_user_rates"] == {
        "explorer_to_parcel_review": 0.666667,
        "explorer_to_comparison": 0.666667,
        "explorer_to_workflow_create": 0.333333,
    }
    assert funnel["evidence_gate"]["status"] == "ready"
    assert funnel["evidence_gate"]["opens_remaining"] == 0
    assert funnel["evidence_gate"]["users_remaining"] == 0
    assert "parcel" not in json.dumps(funnel["market_explorer"])


def test_activation_gate_requires_records_across_multiple_users() -> None:
    rows = [
        {"_user_id": f"user-{index % 3}", "archived_at": None}
        for index in range(30)
    ]
    report = build_product_adoption_report([], workflow_rows=rows)

    assert report["workflow_inventory"]["records"] == 30
    assert report["workflow_inventory"]["users"] == 3
    assert report["activation_evidence_gate"]["status"] == "ready"
    assert report["activation_evidence_gate"]["records_remaining"] == 0
    assert report["activation_evidence_gate"]["users_remaining"] == 0


def test_saved_view_reuse_gate_requires_applies_across_multiple_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {"saved_view_applied": 1},
                "sources": {"saved_view_applied:saved_views": 1},
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    gate = report["saved_view_reuse"]["evidence_gate"]
    assert report["saved_view_reuse"]["applied"] == 10
    assert report["saved_view_reuse"]["apply_users"] == 3
    assert gate["status"] == "ready"
    assert gate["applies_remaining"] == 0
    assert gate["users_remaining"] == 0


def test_thesis_monitor_gate_requires_advances_and_reviews_across_users() -> None:
    rows = [
        {
            "_user_id": f"user-{index % 3}",
            "day": "2026-07-24",
            "events": {
                "saved_thesis_baseline_advanced": (
                    1 if index < 5 else 0
                ),
                "saved_thesis_changes_opened": 1,
            },
            "sources": {
                "saved_thesis_changes_opened:saved_views": 1,
                **(
                    {"saved_thesis_baseline_advanced:saved_views": 1}
                    if index < 5
                    else {}
                ),
            },
        }
        for index in range(10)
    ]
    report = build_product_adoption_report(
        rows,
        saved_view_rows=[
            {
                "_user_id": f"user-{index}",
                "schema_version": "citylens/parcel-saved-view@v3",
            }
            for index in range(3)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["thesis_monitor_engagement"]
    assert engagement["monitored_views"] == 3
    assert engagement["monitored_view_users"] == 3
    assert engagement["baselines_advanced"] == 5
    assert engagement["baseline_advance_users"] == 3
    assert engagement["change_reviews"] == 10
    assert engagement["change_review_users"] == 3
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["evidence_gate"]["baseline_advances_remaining"] == 0
    assert engagement["evidence_gate"]["change_reviews_remaining"] == 0


def test_decision_audit_gate_requires_opens_across_multiple_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {
                    "parcel_opened": 1,
                    "decision_audit_opened": 1,
                },
                "sources": {
                    "parcel_opened:ranking": 1,
                    (
                        "decision_audit_opened:decision_posture"
                        if index % 2 == 0
                        else "decision_audit_opened:audit_tab"
                    ): 1,
                },
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["decision_audit_engagement"]
    assert engagement["opened"] == 10
    assert engagement["users"] == 3
    assert engagement["entry_points"] == {
        "decision_posture": 5,
        "audit_tab": 5,
    }
    assert engagement["parcel_open_to_audit_rate"] == 1.0
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["evidence_gate"]["opens_remaining"] == 0
    assert engagement["evidence_gate"]["users_remaining"] == 0


def test_thesis_composer_gate_counts_only_coarse_applies_across_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {"thesis_composer_applied": 1},
                "sources": {
                    "thesis_composer_applied:thesis_composer": 1,
                },
                "prompt": "private text must never enter the report",
                "criteria": ["private criterion"],
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["thesis_composer_engagement"]
    assert engagement["applied"] == 10
    assert engagement["users"] == 3
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["evidence_gate"]["applies_remaining"] == 0
    assert engagement["evidence_gate"]["users_remaining"] == 0
    serialized = json.dumps(report)
    assert "private text" not in serialized
    assert "private criterion" not in serialized


def test_comparison_gate_requires_opens_across_multiple_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {
                    "parcel_opened": 2,
                    "comparison_opened": 1,
                },
                "sources": {
                    "parcel_opened:ranking": 2,
                    "comparison_opened:comparison": 1,
                },
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["comparison_engagement"]
    assert engagement["opened"] == 10
    assert engagement["users"] == 3
    assert engagement["workflow_creates"] == 0
    assert engagement["workflow_users"] == 0
    assert engagement["entry_points"] == {
        "comparison": 10,
        "decision_peers": 0,
    }
    assert engagement["parcel_open_to_comparison_rate"] == 0.5
    assert engagement["comparison_to_workflow_create_rate"] == 0
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["handoff_gate"]["status"] == "collecting"
    assert engagement["evidence_gate"]["opens_remaining"] == 0
    assert engagement["evidence_gate"]["users_remaining"] == 0


def test_comparison_handoff_gate_requires_canonical_creates_across_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {
                    "comparison_opened": 1,
                    "workflow_created": 1,
                },
                "sources": {
                    "comparison_opened:comparison": 1,
                    "workflow_created:comparison": 1,
                },
            }
            for index in range(5)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["comparison_engagement"]
    assert engagement["opened"] == 5
    assert engagement["workflow_creates"] == 5
    assert engagement["workflow_users"] == 3
    assert engagement["comparison_to_workflow_create_rate"] == 1.0
    assert engagement["evidence_gate"]["status"] == "collecting"
    assert engagement["handoff_gate"]["status"] == "ready"
    assert engagement["handoff_gate"]["workflow_creates_remaining"] == 0
    assert engagement["handoff_gate"]["users_remaining"] == 0


def test_underwriting_gate_requires_opens_and_adjustments_across_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {
                    "underwriting_opened": 1,
                    "underwriting_assumptions_changed": 1,
                    "workflow_created": 1 if index < 5 else 0,
                },
                "sources": {
                    "underwriting_opened:underwrite_tab": 1,
                    (
                        "underwriting_assumptions_changed:"
                        "base_assumptions"
                    ): 1,
                    "workflow_created:underwriting": (
                        1 if index < 5 else 0
                    ),
                },
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["underwriting_engagement"]
    assert engagement["opened"] == 10
    assert engagement["open_users"] == 3
    assert engagement["first_adjustments"] == 10
    assert engagement["adjustment_users"] == 3
    assert engagement["entry_points"] == {
        "underwrite_tab": 10,
        "base_assumptions": 10,
    }
    assert engagement["directional_adjustment_to_open_ratio"] == 1.0
    assert engagement["workflow_creates"] == 5
    assert engagement["workflow_users"] == 3
    assert engagement["directional_open_to_workflow_rate"] == 0.5
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["evidence_gate"]["opens_remaining"] == 0
    assert engagement["evidence_gate"]["open_users_remaining"] == 0
    assert engagement["evidence_gate"]["first_adjustments_remaining"] == 0
    assert engagement["evidence_gate"]["adjustment_users_remaining"] == 0
    assert engagement["handoff_gate"]["status"] == "ready"
    assert engagement["handoff_gate"]["workflow_creates_remaining"] == 0
    assert engagement["handoff_gate"]["users_remaining"] == 0


def test_decision_audit_handoff_requires_canonical_creates_across_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {
                    "decision_audit_opened": 1,
                    "workflow_created": 1 if index < 5 else 0,
                },
                "sources": {
                    "decision_audit_opened:audit_tab": 1,
                    "workflow_created:decision_audit": (
                        1 if index < 5 else 0
                    ),
                },
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    engagement = report["decision_audit_engagement"]
    assert engagement["opened"] == 10
    assert engagement["users"] == 3
    assert engagement["workflow_creates"] == 5
    assert engagement["workflow_users"] == 3
    assert engagement["audit_to_workflow_create_rate"] == 0.5
    assert engagement["evidence_gate"]["status"] == "ready"
    assert engagement["handoff_gate"]["status"] == "ready"
    assert engagement["handoff_gate"]["workflow_creates_remaining"] == 0
    assert engagement["handoff_gate"]["users_remaining"] == 0


def test_workflow_inventory_query_reads_only_archive_state_and_user_parent() -> None:
    class FakeUserReference:
        id = "private-user-a"

    class FakeCollectionReference:
        parent = FakeUserReference()

    class FakeDocumentReference:
        parent = FakeCollectionReference()

    class FakeSnapshot:
        reference = FakeDocumentReference()

        @staticmethod
        def to_dict() -> dict[str, str]:
            return {
                "archived_at": "2026-07-20T00:00:00Z",
                "bbl": "3020960069",
                "notes": "must never enter the report",
            }

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None

        def select(self, field_paths: list[str]) -> FakeQuery:
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection_group(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    rows = _read_workflow_rows(client)  # type: ignore[arg-type]

    assert client.collection_id == "parcel_workflow"
    assert client.query.field_paths == ["archived_at"]
    assert rows == [
        {
            "_user_id": "private-user-a",
            "archived_at": "2026-07-20T00:00:00Z",
        }
    ]
    assert "3020960069" not in json.dumps(rows)
    assert "must never enter" not in json.dumps(rows)


def test_saved_view_inventory_query_reads_only_schema_and_user_parent() -> None:
    class FakeUserReference:
        id = "private-user-a"

    class FakeCollectionReference:
        parent = FakeUserReference()

    class FakeDocumentReference:
        parent = FakeCollectionReference()

    class FakeSnapshot:
        reference = FakeDocumentReference()

        @staticmethod
        def to_dict() -> dict[str, str]:
            return {
                "schema_version": "citylens/parcel-saved-view@v3",
                "name": "must never enter the report",
                "query": "confidential owner",
                "owner": "PRIVATE OWNER LLC",
            }

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None

        def select(self, field_paths: list[str]) -> FakeQuery:
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection_group(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    rows = _read_saved_view_rows(client)  # type: ignore[arg-type]

    assert client.collection_id == "parcel_saved_searches"
    assert client.query.field_paths == ["schema_version"]
    assert rows == [
        {
            "_user_id": "private-user-a",
            "schema_version": "citylens/parcel-saved-view@v3",
        }
    ]
    rendered = json.dumps(rows)
    assert "must never enter" not in rendered
    assert "confidential owner" not in rendered
    assert "PRIVATE OWNER" not in rendered


def test_pilot_intake_query_reads_only_aggregate_fields() -> None:
    class FakeSnapshot:
        @staticmethod
        def to_dict() -> dict[str, object]:
            return {
                "status": "new",
                "plan": "acquisitions",
                "created_at": datetime(
                    2026, 7, 24, 12, 0, tzinfo=timezone.utc
                ),
                "name": "Private Person",
                "work_email": "private@example.com",
                "company": "PRIVATE COMPANY",
                "workflow_summary": "must never enter the report",
            }

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None

        def select(self, field_paths: list[str]) -> "FakeQuery":
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    rows = _read_pilot_request_rows(client)  # type: ignore[arg-type]

    assert client.collection_id == "pilot_requests"
    assert client.query.field_paths == ["status", "plan", "created_at"]
    assert rows == [
        {
            "status": "new",
            "plan": "acquisitions",
            "created_at": datetime(
                2026, 7, 24, 12, 0, tzinfo=timezone.utc
            ),
        }
    ]
    rendered = json.dumps(rows, default=str)
    assert "Private Person" not in rendered
    assert "private@example.com" not in rendered
    assert "PRIVATE COMPANY" not in rendered
    assert "must never enter" not in rendered


def test_synthetic_monitor_is_excluded_before_aggregation() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": "real-user",
                "day": "2026-07-27",
                "events": {"parcel_opened": 2},
                "sources": {"parcel_opened:map": 2},
            },
            {
                "_user_id": "synthetic-user",
                "day": "2026-07-27",
                "events": {
                    "market_explorer_opened": 99,
                    "parcel_opened": 99,
                    "thesis_composer_applied": 99,
                },
                "sources": {
                    "market_explorer_opened:full_inventory": 99,
                    "parcel_opened:map": 99,
                    "thesis_composer_applied:thesis_composer": 99,
                },
            },
        ],
        workflow_rows=[
            {"_user_id": "real-user", "archived_at": None},
            {"_user_id": "synthetic-user", "archived_at": None},
        ],
        saved_view_rows=[
            {
                "_user_id": "real-user",
                "schema_version": "citylens/parcel-saved-view@v3",
            },
            {
                "_user_id": "synthetic-user",
                "schema_version": "citylens/parcel-saved-view@v3",
            },
        ],
        excluded_user_ids={"synthetic-user"},
        as_of=datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc),
    )

    assert report["active_users"] == 1
    assert report["active_user_days"] == 1
    assert report["events"] == {"parcel_opened": 2}
    assert report["workflow_inventory"]["records"] == 1
    assert report["saved_view_inventory"]["records"] == 1
    assert report["acquisition_funnel"]["market_explorer"]["opened"] == 0
    assert report["acquisition_funnel"]["market_explorer"]["users"] == 0
    assert report["thesis_composer_engagement"]["applied"] == 0
    assert report["measurement_governance"] == {
        "synthetic_actor_class": "synthetic_monitor",
        "synthetic_actors_excluded": 1,
        "product_usage_days_excluded": 1,
        "workflow_records_excluded": 1,
        "saved_view_records_excluded": 1,
        "identifiers_reported": False,
    }
    rendered = json.dumps(report)
    assert "synthetic-user" not in rendered
    assert "real-user" not in rendered
    assert any(
        "synthetic-monitor actors were excluded" in warning
        for warning in report["warnings"]
    )


def test_synthetic_actor_query_reads_only_governance_class() -> None:
    class FakeSnapshot:
        id = "private-synthetic-user"

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None
            self.filter = None

        def where(self, *, filter: object) -> "FakeQuery":
            self.filter = filter
            return self

        def select(self, field_paths: list[str]) -> "FakeQuery":
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    user_ids = _read_synthetic_actor_ids(client)  # type: ignore[arg-type]

    assert client.collection_id == "users"
    assert client.query.field_paths == ["adoption_measurement_class"]
    assert client.query.filter.field_path == "adoption_measurement_class"
    assert client.query.filter.op_string == "=="
    assert client.query.filter.value == "synthetic_monitor"
    assert user_ids == {"private-synthetic-user"}


@pytest.mark.parametrize("days", [0, 91])
def test_report_rejects_windows_outside_retention(days: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 90"):
        build_product_adoption_report([], days=days)

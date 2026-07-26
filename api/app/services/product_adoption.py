from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

PILOT_REQUEST_STATUS_VALUES = frozenset(
    {"new", "contacted", "qualified", "declined", "converted", "spam"}
)
PILOT_PLAN_VALUES = frozenset({"acquisitions", "concierge"})


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _as_utc(value).date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _positive_counts(value: Any) -> Counter[str]:
    if not isinstance(value, dict):
        return Counter()
    result: Counter[str] = Counter()
    for key, raw_count in value.items():
        if not isinstance(key, str):
            continue
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            continue
        if count > 0:
            result[key] += count
    return result


def build_product_adoption_report(
    rows: Iterable[dict[str, Any]],
    *,
    workflow_rows: Iterable[dict[str, Any]] = (),
    saved_view_rows: Iterable[dict[str, Any]] = (),
    pilot_request_rows: Iterable[dict[str, Any]] = (),
    as_of: datetime | None = None,
    days: int = 30,
) -> dict[str, Any]:
    """Build a privacy-preserving, aggregate product-adoption report.

    Input rows may include an internal ``_user_id`` solely to count unique
    active users. The returned report never includes row-level records or
    identifiers.
    """

    if days < 1 or days > 90:
        raise ValueError("days must be between 1 and 90")

    generated_at = _as_utc(as_of or datetime.now(timezone.utc))
    window_end = generated_at.date()
    window_start = window_end - timedelta(days=days - 1)
    events: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    active_users: set[str] = set()
    active_user_days = 0
    rejected_rows = 0
    saved_view_event_users: set[str] = set()
    saved_view_apply_users: set[str] = set()
    saved_view_comparison_users: set[str] = set()
    decision_audit_users: set[str] = set()
    comparison_users: set[str] = set()
    comparison_workflow_users: set[str] = set()
    underwriting_open_users: set[str] = set()
    underwriting_adjustment_users: set[str] = set()
    evidence_review_users: set[str] = set()
    evidence_issue_users: set[str] = set()

    for row in rows:
        day = _parse_day(row.get("day"))
        if day is None or day < window_start or day > window_end:
            rejected_rows += 1
            continue
        row_events = _positive_counts(row.get("events"))
        row_sources = _positive_counts(row.get("sources"))
        if not row_events:
            rejected_rows += 1
            continue
        events.update(row_events)
        sources.update(row_sources)
        active_user_days += 1
        user_id = row.get("_user_id")
        if isinstance(user_id, str) and user_id:
            active_users.add(user_id)
            if any(
                row_events.get(event, 0) > 0
                for event in (
                    "saved_view_created",
                    "saved_view_updated",
                    "saved_view_deleted",
                    "saved_view_applied",
                    "saved_view_comparison_opened",
                )
            ):
                saved_view_event_users.add(user_id)
            if row_events.get("saved_view_applied", 0) > 0:
                saved_view_apply_users.add(user_id)
            if row_events.get("saved_view_comparison_opened", 0) > 0:
                saved_view_comparison_users.add(user_id)
            if row_events.get("decision_audit_opened", 0) > 0:
                decision_audit_users.add(user_id)
            if row_events.get("comparison_opened", 0) > 0:
                comparison_users.add(user_id)
            if row_sources.get("workflow_created:comparison", 0) > 0:
                comparison_workflow_users.add(user_id)
            if row_events.get("underwriting_opened", 0) > 0:
                underwriting_open_users.add(user_id)
            if row_events.get("underwriting_assumptions_changed", 0) > 0:
                underwriting_adjustment_users.add(user_id)
            if row_events.get("workflow_evidence_reviewed", 0) > 0:
                evidence_review_users.add(user_id)
            if row_events.get("workflow_evidence_issue_submitted", 0) > 0:
                evidence_issue_users.add(user_id)

    workflow_users: set[str] = set()
    active_workflows = 0
    archived_workflows = 0
    rejected_workflow_rows = 0
    for row in workflow_rows:
        user_id = row.get("_user_id")
        if not isinstance(user_id, str) or not user_id:
            rejected_workflow_rows += 1
            continue
        workflow_users.add(user_id)
        if row.get("archived_at") is None:
            active_workflows += 1
        else:
            archived_workflows += 1

    workflow_records = active_workflows + archived_workflows
    saved_view_users: set[str] = set()
    saved_view_records = 0
    rejected_saved_view_rows = 0
    for row in saved_view_rows:
        user_id = row.get("_user_id")
        if (
            not isinstance(user_id, str)
            or not user_id
            or row.get("schema_version") != "citylens/parcel-saved-view@v2"
        ):
            rejected_saved_view_rows += 1
            continue
        saved_view_users.add(user_id)
        saved_view_records += 1

    pilot_statuses: Counter[str] = Counter()
    pilot_plans: Counter[str] = Counter()
    recent_pilot_requests = 0
    rejected_pilot_rows = 0
    for row in pilot_request_rows:
        request_status = row.get("status")
        plan = row.get("plan")
        created_at = row.get("created_at")
        if (
            request_status not in PILOT_REQUEST_STATUS_VALUES
            or plan not in PILOT_PLAN_VALUES
            or not isinstance(created_at, datetime)
        ):
            rejected_pilot_rows += 1
            continue
        pilot_statuses[str(request_status)] += 1
        pilot_plans[str(plan)] += 1
        created_day = _as_utc(created_at).date()
        if window_start <= created_day <= window_end:
            recent_pilot_requests += 1

    minimum_workflow_records = 30
    minimum_workflow_users = 3
    activation_ready = (
        workflow_records >= minimum_workflow_records
        and len(workflow_users) >= minimum_workflow_users
    )
    parcel_opens = events.get("parcel_opened", 0)
    workflow_creates = events.get("workflow_created", 0)
    saved_view_applies = events.get("saved_view_applied", 0)
    saved_view_comparison_opens = events.get(
        "saved_view_comparison_opened", 0
    )
    decision_audit_opens = events.get("decision_audit_opened", 0)
    comparison_opens = events.get("comparison_opened", 0)
    comparison_workflow_creates = sources.get(
        "workflow_created:comparison", 0
    )
    underwriting_opens = events.get("underwriting_opened", 0)
    underwriting_adjustments = events.get(
        "underwriting_assumptions_changed", 0
    )
    evidence_reviews = events.get("workflow_evidence_reviewed", 0)
    evidence_issue_submissions = events.get(
        "workflow_evidence_issue_submitted", 0
    )
    minimum_decision_audit_opens = 10
    minimum_decision_audit_users = 3
    decision_audit_engagement_ready = (
        decision_audit_opens >= minimum_decision_audit_opens
        and len(decision_audit_users) >= minimum_decision_audit_users
    )
    minimum_comparison_opens = 10
    minimum_comparison_users = 3
    comparison_engagement_ready = (
        comparison_opens >= minimum_comparison_opens
        and len(comparison_users) >= minimum_comparison_users
    )
    minimum_comparison_workflow_creates = 5
    minimum_comparison_workflow_users = 3
    comparison_handoff_ready = (
        comparison_workflow_creates >= minimum_comparison_workflow_creates
        and len(comparison_workflow_users)
        >= minimum_comparison_workflow_users
    )
    minimum_underwriting_opens = 10
    minimum_underwriting_open_users = 3
    minimum_underwriting_adjustments = 5
    minimum_underwriting_adjustment_users = 3
    underwriting_engagement_ready = (
        underwriting_opens >= minimum_underwriting_opens
        and len(underwriting_open_users) >= minimum_underwriting_open_users
        and underwriting_adjustments >= minimum_underwriting_adjustments
        and len(underwriting_adjustment_users)
        >= minimum_underwriting_adjustment_users
    )
    minimum_evidence_reviews = 10
    minimum_evidence_review_users = 3
    evidence_review_engagement_ready = (
        evidence_reviews >= minimum_evidence_reviews
        and len(evidence_review_users) >= minimum_evidence_review_users
    )
    minimum_saved_view_applies = 10
    minimum_saved_view_apply_users = 3
    saved_view_reuse_ready = (
        saved_view_applies >= minimum_saved_view_applies
        and len(saved_view_apply_users) >= minimum_saved_view_apply_users
    )
    warnings: list[str] = [
        (
            "Parcel opens are directional client-side counters; workflow "
            "lifecycle and saved-view mutation counts are derived "
            "transactionally from canonical mutations. Saved-view applies "
            "and saved-screen comparison opens are directional, value-minimized "
            "client-side counters. Decision-audit/underwriting interactions "
            "are directional client-side counters. Comparison workspace opens "
            "are also directional and contain no parcel identifiers or values. "
            "Evidence-review "
            "markers are transactionally derived but mean only that one exact "
            "cited version was considered. Evidence-issue submissions are "
            "also transactionally derived and aggregate only; they signal "
            "data-quality friction without revealing the parcel, citation, "
            "reason, or note. None is model accuracy, completed or cleared "
            "diligence, a valuation, or a unique-parcel count."
        )
    ]
    if not events:
        warnings.append("No qualifying product-adoption events were observed.")
    if not activation_ready:
        warnings.append(
            "Activation evidence is still collecting: "
            f"{workflow_records}/{minimum_workflow_records} canonical workflow "
            f"records across {len(workflow_users)}/{minimum_workflow_users} users."
        )
    if not saved_view_reuse_ready:
        warnings.append(
            "Saved-view reuse evidence is still collecting: "
            f"{saved_view_applies}/{minimum_saved_view_applies} applies across "
            f"{len(saved_view_apply_users)}/{minimum_saved_view_apply_users} users."
        )
    if not decision_audit_engagement_ready:
        warnings.append(
            "Decision-audit engagement evidence is still collecting: "
            f"{decision_audit_opens}/{minimum_decision_audit_opens} opens across "
            f"{len(decision_audit_users)}/{minimum_decision_audit_users} users."
        )
    if not comparison_engagement_ready:
        warnings.append(
            "Comparison engagement evidence is still collecting: "
            f"{comparison_opens}/{minimum_comparison_opens} opens across "
            f"{len(comparison_users)}/{minimum_comparison_users} users."
        )
    if not comparison_handoff_ready:
        warnings.append(
            "Comparison-to-workflow evidence is still collecting: "
            f"{comparison_workflow_creates}/"
            f"{minimum_comparison_workflow_creates} canonical advances across "
            f"{len(comparison_workflow_users)}/"
            f"{minimum_comparison_workflow_users} users."
        )
    if not underwriting_engagement_ready:
        warnings.append(
            "Underwriting engagement evidence is still collecting: "
            f"{underwriting_opens}/{minimum_underwriting_opens} opens across "
            f"{len(underwriting_open_users)}/"
            f"{minimum_underwriting_open_users} users and "
            f"{underwriting_adjustments}/{minimum_underwriting_adjustments} "
            "first-adjustment events across "
            f"{len(underwriting_adjustment_users)}/"
            f"{minimum_underwriting_adjustment_users} users."
        )
    if not evidence_review_engagement_ready:
        warnings.append(
            "Source-bound evidence-review engagement is still collecting: "
            f"{evidence_reviews}/{minimum_evidence_reviews} review markers across "
            f"{len(evidence_review_users)}/{minimum_evidence_review_users} users."
        )
    if pilot_statuses.get("new", 0):
        warnings.append(
            f"{pilot_statuses['new']} pilot request(s) are waiting for review."
        )

    return {
        "schema_version": "citylens/product-adoption-report@v11",
        "generated_at": generated_at.isoformat(),
        "window": {
            "days": days,
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
        },
        "measurement_scope": "authenticated web product adoption",
        "model_accuracy_claim": False,
        "active_users": len(active_users),
        "active_user_days": active_user_days,
        "total_events": sum(events.values()),
        "events": dict(sorted(events.items())),
        "sources": dict(sorted(sources.items())),
        "parcel_open_to_workflow_create_rate": (
            round(workflow_creates / parcel_opens, 6)
            if parcel_opens > 0
            else None
        ),
        "decision_audit_engagement": {
            "opened": decision_audit_opens,
            "users": len(decision_audit_users),
            "entry_points": {
                "decision_posture": sources.get(
                    "decision_audit_opened:decision_posture", 0
                ),
                "audit_tab": sources.get(
                    "decision_audit_opened:audit_tab", 0
                ),
            },
            "parcel_open_to_audit_rate": (
                round(decision_audit_opens / parcel_opens, 6)
                if parcel_opens > 0
                else None
            ),
            "evidence_gate": {
                "status": (
                    "ready"
                    if decision_audit_engagement_ready
                    else "collecting"
                ),
                "minimum_opens": minimum_decision_audit_opens,
                "minimum_users": minimum_decision_audit_users,
                "opens_remaining": max(
                    0, minimum_decision_audit_opens - decision_audit_opens
                ),
                "users_remaining": max(
                    0,
                    minimum_decision_audit_users
                    - len(decision_audit_users),
                ),
                "claim": (
                    "Directional evidence-audit engagement only; opens are "
                    "best-effort aggregate counters, not unique parcels, "
                    "completed diligence, lead quality, or model accuracy."
                ),
            },
        },
        "comparison_engagement": {
            "opened": comparison_opens,
            "users": len(comparison_users),
            "workflow_creates": comparison_workflow_creates,
            "workflow_users": len(comparison_workflow_users),
            "entry_points": {
                "comparison": sources.get(
                    "comparison_opened:comparison", 0
                ),
                "decision_peers": sources.get(
                    "comparison_opened:decision_peers", 0
                ),
            },
            "parcel_open_to_comparison_rate": (
                round(comparison_opens / parcel_opens, 6)
                if parcel_opens > 0
                else None
            ),
            "comparison_to_workflow_create_rate": (
                round(comparison_workflow_creates / comparison_opens, 6)
                if comparison_opens > 0
                else None
            ),
            "evidence_gate": {
                "status": (
                    "ready" if comparison_engagement_ready else "collecting"
                ),
                "minimum_opens": minimum_comparison_opens,
                "minimum_users": minimum_comparison_users,
                "opens_remaining": max(
                    0, minimum_comparison_opens - comparison_opens
                ),
                "users_remaining": max(
                    0, minimum_comparison_users - len(comparison_users)
                ),
                "claim": (
                    "Directional shortlist-comparison engagement only; opens "
                    "are best-effort aggregate counters with no parcel IDs or "
                    "values, not unique shortlists, completed diligence, "
                    "lead quality, or model accuracy."
                ),
            },
            "handoff_gate": {
                "status": (
                    "ready" if comparison_handoff_ready else "collecting"
                ),
                "minimum_workflow_creates": (
                    minimum_comparison_workflow_creates
                ),
                "minimum_users": minimum_comparison_workflow_users,
                "workflow_creates_remaining": max(
                    0,
                    minimum_comparison_workflow_creates
                    - comparison_workflow_creates,
                ),
                "users_remaining": max(
                    0,
                    minimum_comparison_workflow_users
                    - len(comparison_workflow_users),
                ),
                "claim": (
                    "Canonical comparison-to-workflow handoffs only; the "
                    "numerator is transactionally derived and contains no "
                    "parcel IDs, actions, due dates, values, or notes. The "
                    "directional rate is not lead quality, seller intent, or "
                    "model accuracy."
                ),
            },
        },
        "underwriting_engagement": {
            "opened": underwriting_opens,
            "open_users": len(underwriting_open_users),
            "first_adjustments": underwriting_adjustments,
            "adjustment_users": len(underwriting_adjustment_users),
            "entry_points": {
                "underwrite_tab": sources.get(
                    "underwriting_opened:underwrite_tab", 0
                ),
                "base_assumptions": sources.get(
                    "underwriting_assumptions_changed:base_assumptions", 0
                ),
            },
            "directional_adjustment_to_open_ratio": (
                round(underwriting_adjustments / underwriting_opens, 6)
                if underwriting_opens > 0
                else None
            ),
            "evidence_gate": {
                "status": (
                    "ready" if underwriting_engagement_ready else "collecting"
                ),
                "minimum_opens": minimum_underwriting_opens,
                "minimum_open_users": minimum_underwriting_open_users,
                "minimum_first_adjustments": minimum_underwriting_adjustments,
                "minimum_adjustment_users": (
                    minimum_underwriting_adjustment_users
                ),
                "opens_remaining": max(
                    0, minimum_underwriting_opens - underwriting_opens
                ),
                "open_users_remaining": max(
                    0,
                    minimum_underwriting_open_users
                    - len(underwriting_open_users),
                ),
                "first_adjustments_remaining": max(
                    0,
                    minimum_underwriting_adjustments
                    - underwriting_adjustments,
                ),
                "adjustment_users_remaining": max(
                    0,
                    minimum_underwriting_adjustment_users
                    - len(underwriting_adjustment_users),
                ),
                "claim": (
                    "Directional underwriting engagement only; opens and "
                    "first adjustments are best-effort aggregate counters, "
                    "not unique parcels, assumption values, saved scenarios, "
                    "valuations, transactions, lead quality, or model "
                    "accuracy."
                ),
            },
        },
        "evidence_review_engagement": {
            "reviewed_versions": evidence_reviews,
            "users": len(evidence_review_users),
            "source": "workflow_evidence_reviewed:workflow",
            "evidence_gate": {
                "status": (
                    "ready"
                    if evidence_review_engagement_ready
                    else "collecting"
                ),
                "minimum_reviewed_versions": minimum_evidence_reviews,
                "minimum_users": minimum_evidence_review_users,
                "reviewed_versions_remaining": max(
                    0, minimum_evidence_reviews - evidence_reviews
                ),
                "users_remaining": max(
                    0,
                    minimum_evidence_review_users
                    - len(evidence_review_users),
                ),
                "claim": (
                    "Canonical source-bound review markers only. A marker "
                    "means a user considered the exact cited evidence version; "
                    "it does not establish completed or cleared diligence, "
                    "lead quality, seller intent, or model accuracy."
                ),
            },
        },
        "evidence_issue_engagement": {
            "submitted": evidence_issue_submissions,
            "users": len(evidence_issue_users),
            "source": "workflow_evidence_issue_submitted:workflow",
            "claim": (
                "Canonical aggregate evidence-governance submissions only. "
                "Counts exclude parcel IDs, cited values, sources, reasons, "
                "notes, request IDs, and resolution outcomes. A submission "
                "signals data-quality friction, not an incorrect official "
                "record, lead quality, seller intent, or model accuracy."
            ),
        },
        "excluded_or_invalid_rows": rejected_rows,
        "workflow_inventory": {
            "records": workflow_records,
            "active": active_workflows,
            "archived": archived_workflows,
            "users": len(workflow_users),
            "excluded_or_invalid_rows": rejected_workflow_rows,
        },
        "saved_view_inventory": {
            "records": saved_view_records,
            "users": len(saved_view_users),
            "excluded_or_invalid_rows": rejected_saved_view_rows,
        },
        "saved_view_reuse": {
            "created": events.get("saved_view_created", 0),
            "updated": events.get("saved_view_updated", 0),
            "deleted": events.get("saved_view_deleted", 0),
            "applied": saved_view_applies,
            "comparisons": saved_view_comparison_opens,
            "event_users": len(saved_view_event_users),
            "apply_users": len(saved_view_apply_users),
            "comparison_users": len(saved_view_comparison_users),
            "evidence_gate": {
                "status": "ready" if saved_view_reuse_ready else "collecting",
                "minimum_applies": minimum_saved_view_applies,
                "minimum_apply_users": minimum_saved_view_apply_users,
                "applies_remaining": max(
                    0, minimum_saved_view_applies - saved_view_applies
                ),
                "users_remaining": max(
                    0,
                    minimum_saved_view_apply_users
                    - len(saved_view_apply_users),
                ),
                "claim": (
                    "Directional repeat-use evidence only; applies are "
                    "best-effort client counters, not unique views, leads, "
                    "users, or model outcomes."
                ),
            },
        },
        "pilot_intake": {
            "records": sum(pilot_statuses.values()),
            "recent_requests": recent_pilot_requests,
            "status_counts": dict(sorted(pilot_statuses.items())),
            "plan_counts": dict(sorted(pilot_plans.items())),
            "new_requests_waiting": pilot_statuses.get("new", 0),
            "excluded_or_invalid_rows": rejected_pilot_rows,
            "privacy_scope": (
                "Aggregate counts only; excludes names, emails, companies, "
                "roles, boroughs, workflow summaries, request IDs, and "
                "network metadata."
            ),
        },
        "activation_evidence_gate": {
            "status": "ready" if activation_ready else "collecting",
            "minimum_workflow_records": minimum_workflow_records,
            "minimum_workflow_users": minimum_workflow_users,
            "records_remaining": max(
                0, minimum_workflow_records - workflow_records
            ),
            "users_remaining": max(
                0, minimum_workflow_users - len(workflow_users)
            ),
            "claim": (
                "Directional activation evidence only; this gate does not "
                "establish lead quality, seller intent, or model accuracy."
            ),
        },
        "warnings": warnings,
    }

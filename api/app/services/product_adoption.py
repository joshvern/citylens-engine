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
    decision_audit_users: set[str] = set()

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
                )
            ):
                saved_view_event_users.add(user_id)
            if row_events.get("saved_view_applied", 0) > 0:
                saved_view_apply_users.add(user_id)
            if row_events.get("decision_audit_opened", 0) > 0:
                decision_audit_users.add(user_id)

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
    decision_audit_opens = events.get("decision_audit_opened", 0)
    minimum_decision_audit_opens = 10
    minimum_decision_audit_users = 3
    decision_audit_engagement_ready = (
        decision_audit_opens >= minimum_decision_audit_opens
        and len(decision_audit_users) >= minimum_decision_audit_users
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
            "and decision-audit opens are directional client-side counters. "
            "None is model accuracy, completed diligence, or a unique-parcel "
            "count."
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
    if pilot_statuses.get("new", 0):
        warnings.append(
            f"{pilot_statuses['new']} pilot request(s) are waiting for review."
        )

    return {
        "schema_version": "citylens/product-adoption-report@v5",
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
            "event_users": len(saved_view_event_users),
            "apply_users": len(saved_view_apply_users),
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

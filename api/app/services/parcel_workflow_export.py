from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

OUTCOME_EXPORT_SCHEMA = "citylens/parcel-workflow-outcome-export@v1"
METHODOLOGY_SCHEMA = "citylens/parcel-workflow-analytics-methodology@v2"
MILESTONE_WINDOWS: tuple[tuple[str, int], ...] = (
    ("owner_contacted", 30),
    ("qualified", 90),
    ("offer_submitted", 180),
    ("under_contract", 270),
    ("closed", 365),
)
MILESTONE_FIELDS = {
    "owner_contacted": "first_contacted_at",
    "qualified": "first_qualified_at",
    "offer_submitted": "first_offer_submitted_at",
    "under_contract": "first_under_contract_at",
    "closed": "first_closed_at",
}
EXCLUDED_PRIVATE_FIELDS = (
    "address",
    "owner_name",
    "assignee",
    "notes",
    "tags",
    "next_action",
    "next_action_due_date",
    "reminder_snoozed_until",
    "decision_reason_raw",
)
DECISION_REASON_CATEGORIES = frozenset(
    {
        "pursuing",
        "owner_unresponsive",
        "pricing_gap",
        "insufficient_capacity",
        "zoning_constraints",
        "ownership_complexity",
        "active_project",
        "bad_data",
        "other",
    }
)


def _as_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _label(
    *,
    item: dict[str, Any],
    saved_at: datetime,
    as_of: datetime,
    milestone: str,
    horizon_days: int,
    event_history_observed: bool,
) -> dict[str, Any]:
    raw_reached_at = item.get(MILESTONE_FIELDS[milestone])
    reached_at = _as_utc_datetime(raw_reached_at)
    history_invalid = raw_reached_at is not None and (
        reached_at is None or reached_at < saved_at or reached_at > as_of
    )
    days_to_milestone = (
        int((reached_at - saved_at).total_seconds() // 86_400)
        if reached_at is not None and not history_invalid
        else None
    )
    if not event_history_observed or history_invalid:
        return {
            "milestone": milestone,
            "horizon_days": horizon_days,
            "state": "unavailable_history",
            "eligible": False,
            "value": None,
            "reached_at": reached_at if not history_invalid else None,
            "days_to_milestone": days_to_milestone,
        }
    if as_of < saved_at + timedelta(days=horizon_days):
        return {
            "milestone": milestone,
            "horizon_days": horizon_days,
            "state": "pending",
            "eligible": False,
            "value": None,
            "reached_at": reached_at,
            "days_to_milestone": days_to_milestone,
        }
    positive = (
        reached_at is not None
        and reached_at <= saved_at + timedelta(days=horizon_days)
    )
    return {
        "milestone": milestone,
        "horizon_days": horizon_days,
        "state": "positive" if positive else "negative",
        "eligible": True,
        "value": positive,
        "reached_at": reached_at,
        "days_to_milestone": days_to_milestone,
    }


def _canonical_rows_sha256(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        rows,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: (
            value.astimezone(timezone.utc).isoformat()
            if isinstance(value, datetime)
            else str(value)
        ),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_workflow_outcome_export(
    items: list[dict[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Build a privacy-minimized, right-censoring-safe outcome dataset.

    The export is deliberately not a dump of workflow documents. It contains
    only immutable saved-model context and fixed-horizon labels. Pending rows
    remain null, and legacy rows without observed event history cannot become
    training negatives.
    """

    generated_at = _as_utc_datetime(as_of) if as_of is not None else datetime.now(
        timezone.utc
    )
    if generated_at is None:
        raise ValueError("as_of must be a valid datetime")

    rows: list[dict[str, Any]] = []
    excluded_invalid_saved_at = 0
    for item in items:
        saved_at = _as_utc_datetime(item.get("saved_at"))
        if saved_at is None or saved_at > generated_at:
            excluded_invalid_saved_at += 1
            continue
        archived_at = _as_utc_datetime(item.get("archived_at"))
        event_count = max(0, int(item.get("event_count") or 0))
        event_history_observed = event_count > 0
        raw_decision_reason = str(item.get("decision_reason") or "").strip()
        decision_reason_category = (
            raw_decision_reason
            if raw_decision_reason in DECISION_REASON_CATEGORIES
            else "other" if raw_decision_reason else None
        )
        snapshot = (
            item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}
        )
        rows.append(
            {
                "bbl": str(item.get("bbl") or ""),
                "borough": str(item.get("borough") or ""),
                "saved_at": saved_at,
                "archived_at": archived_at,
                "followup_days": int(
                    (generated_at - saved_at).total_seconds() // 86_400
                ),
                "stage": str(item.get("stage") or "new"),
                "outcome": str(item.get("outcome") or "unknown"),
                "decision_reason_category": decision_reason_category,
                "event_history_observed": event_history_observed,
                "event_count": event_count,
                "feed_generated_at": snapshot.get("feed_generated_at"),
                "property_facts_as_of": snapshot.get("property_facts_as_of"),
                "citywide_rank": snapshot.get("citywide_rank"),
                "acquisition_rank": snapshot.get("acquisition_rank"),
                "priority_tier": snapshot.get("priority_tier"),
                "opportunity_category": snapshot.get("opportunity_category"),
                "saved_model_score": snapshot.get("score_calibrated"),
                "labels": [
                    _label(
                        item=item,
                        saved_at=saved_at,
                        as_of=generated_at,
                        milestone=milestone,
                        horizon_days=horizon_days,
                        event_history_observed=event_history_observed,
                    )
                    for milestone, horizon_days in MILESTONE_WINDOWS
                ],
            }
        )
    rows.sort(key=lambda row: (row["saved_at"], row["bbl"]))
    return {
        "schema_version": OUTCOME_EXPORT_SCHEMA,
        "methodology_schema_version": METHODOLOGY_SCHEMA,
        "generated_at": generated_at,
        "input_record_count": len(items),
        "exported_record_count": len(rows),
        "excluded_invalid_saved_at_count": excluded_invalid_saved_at,
        "event_history_observed_count": sum(
            row["event_history_observed"] for row in rows
        ),
        "rank_snapshot_count": sum(
            row["citywide_rank"] is not None
            or row["acquisition_rank"] is not None
            for row in rows
        ),
        "rows_sha256": _canonical_rows_sha256(rows),
        "label_semantics": (
            "A label is eligible only after its complete fixed observation "
            "window and only when immutable workflow event history is present. "
            "Pending or uninstrumented labels are null, never negative."
        ),
        "score_semantics": (
            "saved_model_score is the historical next-year DOB new-building "
            "filing score captured when the lead was saved; it is not seller "
            "intent or acquisition probability."
        ),
        "privacy_contract": (
            "Authenticated user-scoped export. Free text, people, contact "
            "details, addresses, owner names, reminders, and task assignments "
            "are excluded."
        ),
        "excluded_private_fields": list(EXCLUDED_PRIVATE_FIELDS),
        "rows": rows,
    }

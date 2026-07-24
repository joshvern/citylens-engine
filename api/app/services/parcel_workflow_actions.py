from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any

ACTION_SCHEMA = "citylens/parcel-workflow-actions@v1"
OUTCOME_UPDATE_AFTER_DAYS = 30
_TERMINAL_OUTCOMES = {"closed", "rejected", "lost"}
_TERMINAL_STAGES = {"pass"}
_STATE_ORDER = {
    "overdue": 0,
    "due_today": 1,
    "due_soon": 2,
    "unscheduled": 3,
    "scheduled": 4,
}


def normalize_workflow_action_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize action fields before persistence.

    Terminal workflow records do not retain stale reminders. Open records may
    carry an action without a date (and will be reported as unscheduled), but a
    date without a concrete action is rejected.
    """

    normalized = dict(payload)
    next_action = str(normalized.get("next_action") or "").strip() or None
    due_value = normalized.get("next_action_due_date")
    terminal = (
        str(normalized.get("stage") or "") in _TERMINAL_STAGES
        or str(normalized.get("outcome") or "") in _TERMINAL_OUTCOMES
    )
    if terminal:
        normalized["next_action"] = None
        normalized["next_action_due_date"] = None
        return normalized
    if due_value is not None and next_action is None:
        raise ValueError("next_action is required when a due date is set")
    normalized["next_action"] = next_action
    if isinstance(due_value, date):
        normalized["next_action_due_date"] = due_value.isoformat()
    return normalized


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def workflow_is_terminal(item: dict[str, Any]) -> bool:
    return (
        str(item.get("stage") or "") in _TERMINAL_STAGES
        or str(item.get("outcome") or "") in _TERMINAL_OUTCOMES
    )


def workflow_reminder_fingerprint(item: dict[str, Any]) -> str:
    """Return a stable identity for the current follow-up commitment.

    Snoozing applies only while these decision-relevant workflow fields remain
    unchanged. Editing the action, due date, assignee, stage, or outcome
    invalidates the old snooze without requiring a cleanup write.
    """

    values = (
        item.get("bbl"),
        item.get("stage"),
        item.get("outcome"),
        item.get("assignee"),
        item.get("next_action"),
        item.get("next_action_due_date"),
    )
    encoded = "\x1f".join(str(value or "").strip() for value in values)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _coverage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def build_workflow_actions(
    items: list[dict[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    generated_at = as_of or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    generated_at = generated_at.astimezone(timezone.utc)
    today = generated_at.date()
    open_items = [item for item in items if not workflow_is_terminal(item)]
    completed_count = len(items) - len(open_items)
    output: list[dict[str, Any]] = []

    for item in open_items:
        saved_at = _as_datetime(item.get("saved_at"))
        updated_at = _as_datetime(item.get("updated_at"))
        if saved_at is None or updated_at is None:
            # Persisted workflow rows should always have these server-owned
            # timestamps. Invalid legacy rows stay out of the action response
            # rather than producing misleading ages or Pydantic failures.
            continue
        next_action = str(item.get("next_action") or "").strip() or None
        due_date = _as_date(item.get("next_action_due_date"))
        days_since_update = max((today - updated_at.date()).days, 0)
        days_since_save = max((today - saved_at.date()).days, 0)
        needs_assignee = not bool(str(item.get("assignee") or "").strip())
        needs_outcome_update = (
            str(item.get("outcome") or "unknown") == "unknown"
            and days_since_save >= OUTCOME_UPDATE_AFTER_DAYS
        )
        if next_action is None or due_date is None:
            state = "unscheduled"
            days_overdue = 0
        elif due_date < today:
            state = "overdue"
            days_overdue = (today - due_date).days
        elif due_date == today:
            state = "due_today"
            days_overdue = 0
        elif due_date <= today + timedelta(days=7):
            state = "due_soon"
            days_overdue = 0
        else:
            state = "scheduled"
            days_overdue = 0

        snapshot = (
            item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}
        )
        snoozed_until = _as_datetime(item.get("reminder_snoozed_until"))
        is_snoozed = bool(
            snoozed_until
            and snoozed_until > generated_at
            and item.get("reminder_fingerprint")
            == workflow_reminder_fingerprint(item)
        )
        output.append(
            {
                "bbl": str(item.get("bbl") or ""),
                "borough": str(item.get("borough") or ""),
                "address": snapshot.get("address"),
                "stage": str(item.get("stage") or "new"),
                "outcome": str(item.get("outcome") or "unknown"),
                "assignee": item.get("assignee"),
                "next_action": next_action,
                "next_action_due_date": due_date,
                "action_state": state,
                "days_overdue": days_overdue,
                "days_since_update": days_since_update,
                "needs_assignee": needs_assignee,
                "needs_outcome_update": needs_outcome_update,
                "requires_attention": (
                    state
                    in {"overdue", "due_today", "due_soon", "unscheduled"}
                    or needs_assignee
                    or needs_outcome_update
                ),
                # Never expose an expired or invalidated stored timestamp as
                # an effective reminder state.
                "reminder_snoozed_until": snoozed_until if is_snoozed else None,
                "is_snoozed": is_snoozed,
                "citywide_rank": snapshot.get("citywide_rank"),
                "priority_tier": snapshot.get("priority_tier"),
                "opportunity_category": snapshot.get("opportunity_category"),
                "saved_at": saved_at,
                "updated_at": updated_at,
            }
        )

    def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        due = item.get("next_action_due_date")
        due_sort = due if isinstance(due, date) else date.max
        rank = item.get("citywide_rank")
        rank_sort = int(rank) if isinstance(rank, int) else 1_000_001
        return (
            _STATE_ORDER[str(item["action_state"])],
            due_sort,
            rank_sort,
            str(item["bbl"]),
        )

    output.sort(key=_sort_key)
    state_counts = {
        state: sum(1 for item in output if item["action_state"] == state)
        for state in _STATE_ORDER
    }
    complete_plan_count = sum(
        1
        for item in output
        if item["next_action"] and item["next_action_due_date"]
    )
    assigned_count = sum(1 for item in output if not item["needs_assignee"])
    outcome_current_count = sum(
        1 for item in output if not item["needs_outcome_update"]
    )
    attention_items = [item for item in output if item["requires_attention"]]
    return {
        "schema_version": ACTION_SCHEMA,
        "generated_at": generated_at,
        "total_records": len(items),
        "open_records": len(open_items),
        "completed_records": completed_count,
        "overdue_count": state_counts["overdue"],
        "due_today_count": state_counts["due_today"],
        "due_soon_count": state_counts["due_soon"],
        "scheduled_count": state_counts["scheduled"],
        "unscheduled_count": state_counts["unscheduled"],
        "unassigned_count": sum(1 for item in output if item["needs_assignee"]),
        "outcome_update_due_count": sum(
            1 for item in output if item["needs_outcome_update"]
        ),
        "attention_count": sum(
            1 for item in attention_items if not item["is_snoozed"]
        ),
        "snoozed_count": sum(
            1 for item in attention_items if item["is_snoozed"]
        ),
        "complete_plan_count": complete_plan_count,
        "plan_coverage_rate": _coverage(complete_plan_count, len(output)),
        "assigned_count": assigned_count,
        "assignee_coverage_rate": _coverage(assigned_count, len(output)),
        "outcome_current_count": outcome_current_count,
        "outcome_current_rate": _coverage(outcome_current_count, len(output)),
        "items": output,
    }

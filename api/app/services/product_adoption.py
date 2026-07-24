from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable


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
    minimum_workflow_records = 30
    minimum_workflow_users = 3
    activation_ready = (
        workflow_records >= minimum_workflow_records
        and len(workflow_users) >= minimum_workflow_users
    )
    parcel_opens = events.get("parcel_opened", 0)
    workflow_creates = events.get("workflow_created", 0)
    warnings: list[str] = [
        (
            "Parcel opens are directional client-side counters; workflow "
            "lifecycle counts are derived transactionally from canonical "
            "workflow mutations. Neither is model accuracy or a "
            "unique-parcel count."
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

    return {
        "schema_version": "citylens/product-adoption-report@v2",
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
        "excluded_or_invalid_rows": rejected_rows,
        "workflow_inventory": {
            "records": workflow_records,
            "active": active_workflows,
            "archived": archived_workflows,
            "users": len(workflow_users),
            "excluded_or_invalid_rows": rejected_workflow_rows,
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

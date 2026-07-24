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

    parcel_opens = events.get("parcel_opened", 0)
    workflow_creates = events.get("workflow_created", 0)
    warnings: list[str] = [
        (
            "These are directional client-side adoption counters, not model "
            "accuracy, unique-parcel counts, or a replacement for canonical "
            "workflow records."
        )
    ]
    if not events:
        warnings.append("No qualifying product-adoption events were observed.")

    return {
        "schema_version": "citylens/product-adoption-report@v1",
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
        "warnings": warnings,
    }

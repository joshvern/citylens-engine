from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

MINIMUM_COHORT_SIZE = 30
MINIMUM_RATE_DENOMINATOR = 10

_PROGRESSION = (
    "owner_contacted",
    "meeting_scheduled",
    "qualified",
    "offer_submitted",
    "under_contract",
    "closed",
)
_MILESTONE_FIELDS = {
    "owner_contacted": "first_contacted_at",
    "meeting_scheduled": "first_meeting_scheduled_at",
    "qualified": "first_qualified_at",
    "offer_submitted": "first_offer_submitted_at",
    "under_contract": "first_under_contract_at",
    "closed": "first_closed_at",
    "rejected": "first_rejected_at",
    "lost": "first_lost_at",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def milestone_patch(
    *,
    outcome: str | None,
    existing: dict[str, Any],
    occurred_at: datetime,
) -> dict[str, datetime]:
    """Return immutable first-seen milestone timestamps for an outcome.

    Progression outcomes imply every preceding commercial milestone. A direct
    update to ``under_contract``, for example, necessarily means the owner was
    contacted and an offer existed. Terminal rejection/loss outcomes are
    recorded independently.
    """

    normalized = str(outcome or "unknown")
    reached: Iterable[str]
    if normalized in _PROGRESSION:
        reached = _PROGRESSION[: _PROGRESSION.index(normalized) + 1]
    elif normalized in {"rejected", "lost"}:
        reached = (normalized,)
    else:
        reached = ()
    return {
        _MILESTONE_FIELDS[name]: occurred_at
        for name in reached
        if existing.get(_MILESTONE_FIELDS[name]) is None
    }


def _has_milestone(item: dict[str, Any], name: str) -> bool:
    field = _MILESTONE_FIELDS[name]
    if item.get(field) is not None:
        return True
    # Backwards-compatible inference for workflow rows created before event
    # instrumentation. These are counted, but analytics reports history
    # coverage separately so consumers know the timestamp was not observed.
    outcome = str(item.get("outcome") or "unknown")
    if name in _PROGRESSION and outcome in _PROGRESSION:
        return _PROGRESSION.index(outcome) >= _PROGRESSION.index(name)
    return outcome == name


def _rate(numerator: int, denominator: int) -> dict[str, Any]:
    return {
        "numerator": numerator,
        "denominator": denominator,
        "rate": round(numerator / denominator, 4) if denominator else None,
        "sufficient_denominator": denominator >= MINIMUM_RATE_DENOMINATOR,
    }


def _rank_band(item: dict[str, Any]) -> str:
    snapshot = item.get("snapshot") or {}
    rank = snapshot.get("citywide_rank") or snapshot.get("acquisition_rank")
    try:
        value = int(rank)
    except (TypeError, ValueError):
        return "unknown"
    if value <= 100:
        return "1-100"
    if value <= 500:
        return "101-500"
    if value <= 1_000:
        return "501-1000"
    return "1001+"


def _cohort_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dimensions: tuple[tuple[str, Any], ...] = (
        ("borough", lambda row: str(row.get("borough") or "unknown")),
        ("rank_band", _rank_band),
        (
            "opportunity",
            lambda row: str(
                (row.get("snapshot") or {}).get("opportunity_category") or "unknown"
            ),
        ),
    )
    rows: list[dict[str, Any]] = []
    for dimension, getter in dimensions:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            grouped.setdefault(getter(item), []).append(item)
        for value, cohort in sorted(grouped.items()):
            total = len(cohort)
            contacted = sum(_has_milestone(item, "owner_contacted") for item in cohort)
            qualified_ = sum(_has_milestone(item, "qualified") for item in cohort)
            offer = sum(_has_milestone(item, "offer_submitted") for item in cohort)
            contract = sum(_has_milestone(item, "under_contract") for item in cohort)
            closed = sum(_has_milestone(item, "closed") for item in cohort)
            rejected = sum(_has_milestone(item, "rejected") for item in cohort)
            lost = sum(_has_milestone(item, "lost") for item in cohort)
            rows.append(
                {
                    "dimension": dimension,
                    "value": value,
                    "total": total,
                    "contacted": contacted,
                    "qualified": qualified_,
                    "offer_submitted": offer,
                    "under_contract": contract,
                    "closed": closed,
                    "rejected": rejected,
                    "lost": lost,
                    "contacted_rate": round(contacted / total, 4) if total else None,
                    "qualified_rate": (
                        round(qualified_ / contacted, 4) if contacted else None
                    ),
                    "close_rate": round(closed / total, 4) if total else None,
                }
            )
    return rows


def build_workflow_analytics(items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)
    active = sum(not bool(item.get("archived_at")) for item in items)
    event_history = sum(int(item.get("event_count") or 0) > 0 for item in items)
    rank_snapshots = sum(
        bool(
            (item.get("snapshot") or {}).get("citywide_rank")
            or (item.get("snapshot") or {}).get("acquisition_rank")
        )
        for item in items
    )

    milestones = {
        name: sum(_has_milestone(item, name) for item in items)
        for name in (*_PROGRESSION, "rejected", "lost")
    }
    if total < MINIMUM_COHORT_SIZE:
        status = "collecting"
        label = "Collecting prospective outcomes"
    elif event_history < total * 0.8 or rank_snapshots < total * 0.8:
        status = "directional"
        label = "Directional prospective evidence"
    else:
        status = "usable"
        label = "Usable prospective workflow evidence"

    warnings = [
        "These are user-entered prospective workflow outcomes, not model accuracy.",
        (
            "Rates describe saved leads only and do not estimate seller intent "
            "or transaction probability."
        ),
    ]
    if total < MINIMUM_COHORT_SIZE:
        warnings.append(
            f"At least {MINIMUM_COHORT_SIZE} saved leads are required before "
            "cohort rates are directional."
        )
    if event_history < total:
        warnings.append(
            f"{total - event_history} legacy record(s) lack immutable event "
            "history; current outcomes were inferred."
        )
    if rank_snapshots < total:
        warnings.append(
            f"{total - rank_snapshots} record(s) lack a saved rank snapshot "
            "and are excluded from rank-band interpretation."
        )

    return {
        "schema_version": "citylens/parcel-workflow-analytics@v1",
        "generated_at": utcnow(),
        "measurement_status": status,
        "measurement_label": label,
        "total_records": total,
        "active_records": active,
        "archived_records": total - active,
        "event_history_records": event_history,
        "rank_snapshot_records": rank_snapshots,
        "minimum_cohort_size": MINIMUM_COHORT_SIZE,
        "minimum_rate_denominator": MINIMUM_RATE_DENOMINATOR,
        "stage_counts": dict(Counter(str(item.get("stage") or "new") for item in items)),
        "outcome_counts": dict(
            Counter(str(item.get("outcome") or "unknown") for item in items)
        ),
        "decision_reason_counts": dict(
            Counter(
                str(item.get("decision_reason"))
                for item in items
                if item.get("decision_reason")
            )
        ),
        "funnel": {
            "saved": total,
            "contacted": milestones["owner_contacted"],
            "meeting_scheduled": milestones["meeting_scheduled"],
            "qualified": milestones["qualified"],
            "offer_submitted": milestones["offer_submitted"],
            "under_contract": milestones["under_contract"],
            "closed": milestones["closed"],
            "rejected": milestones["rejected"],
            "lost": milestones["lost"],
            "contacted_per_saved": _rate(milestones["owner_contacted"], total),
            "qualified_per_contacted": _rate(
                milestones["qualified"], milestones["owner_contacted"]
            ),
            "offer_per_qualified": _rate(
                milestones["offer_submitted"], milestones["qualified"]
            ),
            "contract_per_offer": _rate(
                milestones["under_contract"], milestones["offer_submitted"]
            ),
            "close_per_contract": _rate(
                milestones["closed"], milestones["under_contract"]
            ),
        },
        "cohorts": _cohort_rows(items),
        "warnings": warnings,
    }

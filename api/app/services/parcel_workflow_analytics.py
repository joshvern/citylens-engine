from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import median
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
_MATURITY_WINDOWS: tuple[tuple[str, str, int], ...] = (
    ("owner_contacted", "Contacted within 30 days", 30),
    ("qualified", "Qualified within 90 days", 90),
    ("offer_submitted", "Offer submitted within 180 days", 180),
    ("under_contract", "Under contract within 270 days", 270),
    ("closed", "Closed within 365 days", 365),
)


def workflow_analytics_methodology() -> dict[str, Any]:
    """Return the public, data-free prospective measurement contract."""

    return {
        "schema_version": (
            "citylens/parcel-workflow-analytics-methodology@v1"
        ),
        "analytics_schema_version": "citylens/parcel-workflow-analytics@v2",
        "horizons": [
            {
                "milestone": milestone,
                "label": label,
                "horizon_days": horizon_days,
            }
            for milestone, label, horizon_days in _MATURITY_WINDOWS
        ],
        "minimum_cohort_size": MINIMUM_COHORT_SIZE,
        "minimum_rate_denominator": MINIMUM_RATE_DENOMINATOR,
        "selection_scope": (
            "User-saved leads only; rates do not estimate all ranked parcels, "
            "seller intent, or transaction probability."
        ),
        "timestamp_semantics": (
            "Eligibility starts at immutable saved_at. Outcomes use the first "
            "recorded milestone timestamp; late backfills are not counted as "
            "within-horizon outcomes."
        ),
        "model_accuracy_claim": False,
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


def _as_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _followup_days(item: dict[str, Any], *, as_of: datetime) -> int | None:
    saved_at = _as_utc_datetime(item.get("saved_at"))
    if saved_at is None or saved_at > as_of:
        return None
    return int((as_of - saved_at).total_seconds() // 86_400)


def _is_mature(
    item: dict[str, Any], *, horizon_days: int, as_of: datetime
) -> bool:
    days = _followup_days(item, as_of=as_of)
    return days is not None and days >= horizon_days


def _reached_within_horizon(
    item: dict[str, Any],
    *,
    milestone: str,
    horizon_days: int,
) -> bool:
    saved_at = _as_utc_datetime(item.get("saved_at"))
    reached_at = _as_utc_datetime(item.get(_MILESTONE_FIELDS[milestone]))
    if saved_at is None or reached_at is None or reached_at < saved_at:
        return False
    return reached_at <= saved_at + timedelta(days=horizon_days)


def _maturity_window(
    items: list[dict[str, Any]],
    *,
    milestone: str,
    label: str,
    horizon_days: int,
    as_of: datetime,
) -> dict[str, Any]:
    valid = [
        item for item in items if _followup_days(item, as_of=as_of) is not None
    ]
    eligible = [
        item
        for item in valid
        if _is_mature(item, horizon_days=horizon_days, as_of=as_of)
    ]
    reached = sum(
        _reached_within_horizon(
            item,
            milestone=milestone,
            horizon_days=horizon_days,
        )
        for item in eligible
    )
    return {
        "milestone": milestone,
        "label": label,
        "horizon_days": horizon_days,
        "eligible_records": len(eligible),
        "reached_within_horizon": reached,
        "pending_records": len(valid) - len(eligible),
        "rate": round(reached / len(eligible), 4) if eligible else None,
        "sufficient_denominator": len(eligible) >= MINIMUM_RATE_DENOMINATOR,
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


def _cohort_rows(
    items: list[dict[str, Any]], *, as_of: datetime
) -> list[dict[str, Any]]:
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
            contacted_eligible = [
                item
                for item in cohort
                if _is_mature(item, horizon_days=30, as_of=as_of)
            ]
            qualified_eligible = [
                item
                for item in cohort
                if _is_mature(item, horizon_days=90, as_of=as_of)
            ]
            close_eligible = [
                item
                for item in cohort
                if _is_mature(item, horizon_days=365, as_of=as_of)
            ]
            contacted_in_window = sum(
                _reached_within_horizon(
                    item,
                    milestone="owner_contacted",
                    horizon_days=30,
                )
                for item in contacted_eligible
            )
            qualified_in_window = sum(
                _reached_within_horizon(
                    item,
                    milestone="qualified",
                    horizon_days=90,
                )
                for item in qualified_eligible
            )
            closed_in_window = sum(
                _reached_within_horizon(
                    item,
                    milestone="closed",
                    horizon_days=365,
                )
                for item in close_eligible
            )
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
                    "contacted_rate_denominator": len(contacted_eligible),
                    "qualified_rate_denominator": len(qualified_eligible),
                    "close_rate_denominator": len(close_eligible),
                    "contacted_rate": (
                        round(contacted_in_window / len(contacted_eligible), 4)
                        if contacted_eligible
                        else None
                    ),
                    "qualified_rate": (
                        round(qualified_in_window / len(qualified_eligible), 4)
                        if qualified_eligible
                        else None
                    ),
                    "close_rate": (
                        round(closed_in_window / len(close_eligible), 4)
                        if close_eligible
                        else None
                    ),
                }
            )
    return rows


def build_workflow_analytics(
    items: list[dict[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _as_utc_datetime(as_of) if as_of is not None else utcnow()
    if generated_at is None:
        raise ValueError("as_of must be a valid datetime")
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
    followup_days = sorted(
        days
        for item in items
        if (days := _followup_days(item, as_of=generated_at)) is not None
    )
    maturity_windows = [
        _maturity_window(
            items,
            milestone=milestone,
            label=label,
            horizon_days=horizon_days,
            as_of=generated_at,
        )
        for milestone, label, horizon_days in _MATURITY_WINDOWS
    ]
    any_directional_window = any(
        window["eligible_records"] >= MINIMUM_RATE_DENOMINATOR
        for window in maturity_windows
    )
    close_window = maturity_windows[-1]
    if total < MINIMUM_COHORT_SIZE or not any_directional_window:
        status = "collecting"
        label = "Collecting observation time"
    elif (
        event_history < total * 0.8
        or rank_snapshots < total * 0.8
        or len(followup_days) < total * 0.8
    ):
        status = "directional"
        label = "Directional maturity-qualified evidence"
    elif close_window["eligible_records"] < MINIMUM_COHORT_SIZE:
        status = "directional"
        label = "Directional maturity-qualified evidence"
    else:
        status = "usable"
        label = "Usable maturity-qualified evidence"

    warnings = [
        "These are user-entered prospective workflow outcomes, not model accuracy.",
        (
            "Rates describe saved leads only and do not estimate seller intent "
            "or transaction probability."
        ),
        (
            "Fixed-horizon rates use immutable save time and first-recorded "
            "milestone time; late backfills are not treated as on-time outcomes."
        ),
        (
            "Operational funnel counts are lifetime milestones. Use the "
            "maturity windows—not the raw funnel—to evaluate prospective rates."
        ),
    ]
    if total < MINIMUM_COHORT_SIZE:
        warnings.append(
            f"At least {MINIMUM_COHORT_SIZE} saved leads are required before "
            "cohort rates are directional."
        )
    if not any_directional_window:
        warnings.append(
            f"At least {MINIMUM_RATE_DENOMINATOR} leads must complete an "
            "observation window before any prospective rate is directional."
        )
    if len(followup_days) < total:
        warnings.append(
            f"{total - len(followup_days)} record(s) have a missing, invalid, "
            "or future saved_at and are excluded from fixed-horizon rates."
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
        "schema_version": "citylens/parcel-workflow-analytics@v2",
        "generated_at": generated_at,
        "measurement_status": status,
        "measurement_label": label,
        "total_records": total,
        "active_records": active,
        "archived_records": total - active,
        "event_history_records": event_history,
        "rank_snapshot_records": rank_snapshots,
        "valid_saved_at_records": len(followup_days),
        "oldest_followup_days": followup_days[-1] if followup_days else None,
        "median_followup_days": (
            round(float(median(followup_days)), 1) if followup_days else None
        ),
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
        "maturity_windows": maturity_windows,
        "cohorts": _cohort_rows(items, as_of=generated_at),
        "warnings": warnings,
    }

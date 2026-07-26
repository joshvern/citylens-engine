"""Decision-relevant changes for watched Parcel Intelligence leads.

The comparison is deliberately conservative.  It compares the immutable
snapshot captured when a user saved a lead with the current, authenticated
citywide feed.  A parcel that disappears from the eligible feed is reported as
requiring verification; it is not described as sold, built, or otherwise
resolved without an authoritative current record.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

ALERT_SCHEMA = "citylens/parcel-workflow-alerts@v2"
RANK_MOVE_THRESHOLD = 100

_SEVERITY_ORDER = {"urgent": 0, "high": 1, "medium": 2, "low": 3}
_SOURCE_URLS = {
    "property_facts": "https://zola.planning.nyc.gov/",
    "ownership": "https://a836-acris.nyc.gov/CP/",
    "project_activity": (
        "https://a810-bisweb.nyc.gov/bisweb/"
        "PropertyProfileOverviewServlet"
    ),
    "land_use_activity": "https://zap.planning.nyc.gov/",
    "constraints": "https://nyclandmarks.lunaimaging.com/",
}
_REASON_SOURCE = {
    "approved_land_use_project": "land_use_activity",
    "active_land_use_project": "land_use_activity",
    "active_project": "project_activity",
    "completed_project": "project_activity",
    "recently_acquired": "ownership",
    "individual_landmark": "constraints",
    "historic_district": "constraints",
    "public_owner": "property_facts",
    "institutional_owner": "property_facts",
    "public_or_tax_exempt_owner_type": "property_facts",
    "administrative_or_condo_lot": "property_facts",
    "missing_current_pluto": "property_facts",
    "missing_address": "property_facts",
    "missing_owner": "property_facts",
    "missing_parcel_geometry": "property_facts",
    "missing_zoning": "property_facts",
    "missing_lot_area": "property_facts",
    "missing_allowed_far": "property_facts",
}


def _normalized_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).casefold()
    return normalized or None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _changed(before: Any, after: Any) -> bool:
    """Compare only when the saved snapshot contains an observed value."""

    if before is None:
        return False
    if isinstance(before, str) or isinstance(after, str):
        return _normalized_text(before) != _normalized_text(after)
    return before != after


def _alert(
    *,
    bbl: str,
    borough: str,
    code: str,
    severity: str,
    title: str,
    detail: str,
    field: str,
    before: Any,
    after: Any,
    current_disposition: str | None = None,
    reason_codes: list[str] | None = None,
    recommended_action: str | None = None,
    source_evidence: list[dict[str, Any]] | None = None,
    parcel_available: bool = True,
) -> dict[str, Any]:
    return {
        "bbl": bbl,
        "borough": borough,
        "code": code,
        "severity": severity,
        "title": title,
        "detail": detail,
        "field": field,
        "before": before,
        "after": after,
        "current_disposition": current_disposition,
        "reason_codes": reason_codes or [],
        "recommended_action": recommended_action,
        "source_evidence": source_evidence or [],
        "parcel_available": parcel_available,
    }


def _screening_source_evidence(
    screening: dict[str, Any],
    *,
    data_sources: dict[str, Any],
) -> list[dict[str, Any]]:
    reasons = list(screening.get("acquisition_exclusion_reasons") or [])
    source_keys: list[str] = []
    for reason in reasons:
        key = _REASON_SOURCE.get(str(reason))
        if key and key not in source_keys:
            source_keys.append(key)
    if not source_keys:
        source_keys.append("property_facts")

    evidence: list[dict[str, Any]] = []
    for key in source_keys:
        metadata = data_sources.get(key)
        metadata = metadata if isinstance(metadata, dict) else {}
        as_of_field = {
            "property_facts": "property_facts_as_of",
            "ownership": "ownership_as_of",
            "project_activity": "project_activity_as_of",
            "land_use_activity": "land_use_activity_as_of",
            "constraints": "property_facts_as_of",
        }[key]
        project_source = key in {
            "project_activity",
            "land_use_activity",
        }
        evidence.append(
            {
                "source": str(
                    metadata.get("source")
                    or {
                        "property_facts": "NYC PLUTO",
                        "ownership": "NYC ACRIS",
                        "project_activity": "NYC DOB",
                        "land_use_activity": "NYC ZAP",
                        "constraints": "NYC LPC",
                    }[key]
                ),
                "as_of": (
                    screening.get(as_of_field)
                    or metadata.get("retrieved_at")
                ),
                "url": (
                    screening.get("latest_project_url")
                    if project_source
                    and screening.get("latest_project_url")
                    else _SOURCE_URLS[key]
                ),
                "supports": ", ".join(
                    reason
                    for reason in reasons
                    if _REASON_SOURCE.get(str(reason)) == key
                )
                or "current screening status",
            }
        )
    return evidence


def _screening_exit_alert(
    *,
    item: dict[str, Any],
    screening: dict[str, Any],
    data_sources: dict[str, Any],
) -> dict[str, Any]:
    bbl = str(item.get("bbl") or "")
    borough = str(item.get("borough") or screening.get("borough") or "")
    reasons = [
        str(reason)
        for reason in (
            screening.get("acquisition_exclusion_reasons") or []
        )
    ]
    if screening.get("acquisition_eligible") is True:
        rank = screening.get("acquisition_rank")
        detail = (
            "The parcel still passes the current acquisition screen but ranks "
            "below the published borough inventory cutoff."
        )
        if isinstance(rank, int):
            detail += f" Its current borough acquisition rank is {rank:,}."
        return _alert(
            bbl=bbl,
            borough=borough,
            code="eligible_below_published_cutoff",
            severity="medium",
            title="Still eligible, below the current published cutoff",
            detail=detail,
            field="published_opportunity_set",
            before=True,
            after=False,
            current_disposition="eligible_below_cutoff",
            recommended_action=(
                "Keep the parcel watched and re-review it if its rank or "
                "screening facts change; do not treat this as a disqualification."
            ),
            source_evidence=_screening_source_evidence(
                screening,
                data_sources=data_sources,
            ),
            parcel_available=False,
        )

    status = str(screening.get("acquisition_status") or "screened_out")
    project_id = screening.get("latest_project_job_number")
    if status == "active_project":
        title = "Current project activity now screens out this lead"
        detail = (
            "The current source-backed screen identifies active or recently "
            "approved project activity."
        )
        if project_id:
            detail += f" Official project {project_id} is attached."
        action = (
            "Review the cited project record and disposition the lead as "
            "competing-sponsor activity unless verified control evidence "
            "supports a different conclusion."
        )
        severity = "urgent"
    elif status == "completed_project":
        title = "Completed redevelopment now screens out this lead"
        detail = (
            "Current DOB or property evidence indicates the redevelopment "
            "opportunity is no longer uncommitted."
        )
        action = (
            "Verify completion in the cited records, then close or reject the "
            "lead with the confirmed disposition."
        )
        severity = "urgent"
    elif status == "constrained":
        title = "Current acquisition constraints screen out this lead"
        detail = (
            "One or more current ownership, landmark, lot, or recent-sale "
            "conditions now fail the acquisition screen."
        )
        action = (
            "Review each cited constraint before rejecting the lead; the "
            "screen is conservative and is not a legal or title conclusion."
        )
        severity = "high"
    else:
        title = "Current source gaps screen out this lead"
        detail = (
            "Required current parcel facts are incomplete, so the lead no "
            "longer qualifies for the published opportunity inventory."
        )
        action = (
            "Resolve the missing official facts before restoring or advancing "
            "the lead."
        )
        severity = "high"

    return _alert(
        bbl=bbl,
        borough=borough,
        code="screened_out_of_current_feed",
        severity=severity,
        title=title,
        detail=detail,
        field="acquisition_eligible",
        before=True,
        after=False,
        current_disposition="screened_out",
        reason_codes=reasons,
        recommended_action=action,
        source_evidence=_screening_source_evidence(
            screening,
            data_sources=data_sources,
        ),
        parcel_available=False,
    )


def _row_alerts(
    item: dict[str, Any],
    current: dict[str, Any],
) -> list[dict[str, Any]]:
    bbl = str(item.get("bbl") or "")
    borough = str(item.get("borough") or current.get("borough") or "")
    snapshot = item.get("snapshot")
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    alerts: list[dict[str, Any]] = []

    before_owner = snapshot.get("owner_name")
    after_owner = current.get("owner_name")
    if (
        _normalized_text(before_owner) is not None
        and _normalized_text(after_owner) is not None
        and _normalized_text(before_owner) != _normalized_text(after_owner)
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="owner_changed",
                severity="high",
                title="Owner name changed",
                detail=(
                    "Current PLUTO owner text differs from the saved lead. "
                    "Verify the deed and ownership chain before outreach."
                ),
                field="owner_name",
                before=before_owner,
                after=after_owner,
            )
        )

    before_sale = snapshot.get("last_sale_year")
    after_sale = current.get("last_sale_year")
    if (
        isinstance(before_sale, int)
        and isinstance(after_sale, int)
        and after_sale > before_sale
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="newer_sale_record",
                severity="high",
                title="Newer sale year appeared",
                detail=(
                    "The current feed contains a more recent ACRIS sale year "
                    "than the saved baseline."
                ),
                field="last_sale_year",
                before=before_sale,
                after=after_sale,
            )
        )

    before_zoning = snapshot.get("zoning_district_1")
    after_zoning = current.get("zoning_district_1")
    if _changed(before_zoning, after_zoning):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="zoning_changed",
                severity="high",
                title="Primary zoning changed",
                detail=(
                    "The primary zoning district differs from the saved "
                    "baseline. Re-run zoning and feasibility diligence."
                ),
                field="zoning_district_1",
                before=before_zoning,
                after=after_zoning,
            )
        )

    before_opportunity = snapshot.get("opportunity_category")
    after_opportunity = current.get("opportunity_category")
    if _changed(before_opportunity, after_opportunity):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="opportunity_changed",
                severity="medium",
                title="Opportunity classification changed",
                detail=(
                    "The current parcel classification differs from the saved "
                    "lead. Review the current facts before advancing it."
                ),
                field="opportunity_category",
                before=before_opportunity,
                after=after_opportunity,
            )
        )

    before_tier = snapshot.get("priority_tier")
    after_tier = current.get("priority_tier")
    if _changed(before_tier, after_tier):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="priority_tier_changed",
                severity="medium",
                title="Priority tier changed",
                detail="The lead moved to a different current priority tier.",
                field="priority_tier",
                before=before_tier,
                after=after_tier,
            )
        )

    before_rank = snapshot.get("citywide_rank")
    after_rank = current.get("citywide_rank")
    if (
        isinstance(before_rank, int)
        and isinstance(after_rank, int)
        and abs(after_rank - before_rank) >= RANK_MOVE_THRESHOLD
    ):
        direction = "improved" if after_rank < before_rank else "declined"
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="material_rank_move",
                severity="medium",
                title=f"Citywide rank {direction}",
                detail=(
                    f"The current citywide rank moved by "
                    f"{abs(after_rank - before_rank):,} places."
                ),
                field="citywide_rank",
                before=before_rank,
                after=after_rank,
            )
        )

    before_lien = snapshot.get("tax_lien_sale_year")
    after_lien = current.get("tax_lien_sale_year")
    if _changed(before_lien, after_lien):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="tax_lien_history_changed",
                severity="medium",
                title="Tax-lien sale history changed",
                detail=(
                    "The official final-sale history differs from the saved "
                    "baseline. It is historical diligence context, not proof "
                    "of current debt."
                ),
                field="tax_lien_sale_year",
                before=before_lien,
                after=after_lien,
            )
        )

    before_violations = snapshot.get("critical_violation_count")
    after_violations = current.get("critical_violation_count")
    if (
        isinstance(before_violations, int)
        and isinstance(after_violations, int)
        and before_violations != after_violations
    ):
        severity = "high" if after_violations > before_violations else "low"
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="critical_violations_changed",
                severity=severity,
                title="Immediate-hazard count changed",
                detail=(
                    "The current official violation overlay differs from the "
                    "saved baseline. Verify the underlying agency records."
                ),
                field="critical_violation_count",
                before=before_violations,
                after=after_violations,
            )
        )

    before_flood = snapshot.get("floodplain_1pct")
    after_flood = current.get("floodplain_1pct")
    if isinstance(before_flood, bool) and isinstance(after_flood, bool) and (
        before_flood != after_flood
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="flood_overlay_changed",
                severity="medium",
                title="Floodplain overlay changed",
                detail=(
                    "The parcel-level 1% floodplain overlay differs from the "
                    "saved baseline. Review the dated FEMA/PLUTO source."
                ),
                field="floodplain_1pct",
                before=before_flood,
                after=after_flood,
            )
        )

    before_environmental = snapshot.get("environmental_review_required")
    after_environmental = current.get("environmental_review_required")
    before_designation = snapshot.get("environmental_designation_number")
    after_designation = current.get("environmental_designation_number")
    if (
        isinstance(before_environmental, bool)
        and isinstance(after_environmental, bool)
        and (
            before_environmental != after_environmental
            or (
                before_environmental
                and _changed(before_designation, after_designation)
            )
        )
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="environmental_review_changed",
                severity="high" if after_environmental else "low",
                title="Environmental designation changed",
                detail=(
                    "The current PLUTO E-designation or restrictive declaration "
                    "differs from the saved baseline. Verify the instrument's "
                    "air, noise, or hazardous-materials requirements and OER "
                    "notices for the proposed work."
                ),
                field="environmental_designation_number",
                before=before_designation,
                after=after_designation,
            )
        )

    before_mih = snapshot.get("mandatory_inclusionary_housing")
    after_mih = current.get("mandatory_inclusionary_housing")
    if isinstance(before_mih, bool) and isinstance(after_mih, bool) and (
        before_mih != after_mih
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="mih_overlay_changed",
                severity="high" if after_mih else "low",
                title="MIH mapped-area screen changed",
                detail=(
                    "The current NYC Planning Mandatory Inclusionary Housing "
                    "mapped-area overlap differs from the saved baseline. "
                    "Verify current Appendix F and project-specific "
                    "applicability before underwriting."
                ),
                field="mandatory_inclusionary_housing",
                before=before_mih,
                after=after_mih,
            )
        )

    before_transit_id = snapshot.get("nearest_transit_complex_id")
    after_transit_id = current.get("nearest_transit_complex_id")
    before_transit_tier = snapshot.get("transit_access_tier")
    after_transit_tier = current.get("transit_access_tier")
    if (
        _normalized_text(before_transit_id) is not None
        and _normalized_text(after_transit_id) is not None
        and (
            _changed(before_transit_id, after_transit_id)
            or _changed(before_transit_tier, after_transit_tier)
        )
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="transit_access_changed",
                severity="medium",
                title="Transit proximity context changed",
                detail=(
                    "The nearest MTA station complex or straight-line access "
                    "tier differs from the saved baseline. Review the dated "
                    "station source; this is not a walking-route measurement."
                ),
                field="nearest_transit_complex_id",
                before={
                    "complex_id": before_transit_id,
                    "station_name": snapshot.get(
                        "nearest_transit_station_name"
                    ),
                    "distance_m": snapshot.get(
                        "nearest_transit_station_distance_m"
                    ),
                    "tier": before_transit_tier,
                },
                after={
                    "complex_id": after_transit_id,
                    "station_name": current.get(
                        "nearest_transit_station_name"
                    ),
                    "distance_m": current.get(
                        "nearest_transit_station_distance_m"
                    ),
                    "tier": after_transit_tier,
                },
            )
        )

    before_change = snapshot.get("recent_change")
    after_change = current.get("recent_change")
    if isinstance(before_change, bool) and isinstance(after_change, bool) and (
        before_change != after_change
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="imagery_change_signal_changed",
                severity="medium",
                title="Imagery change signal changed",
                detail=(
                    "The dated aerial-change overlay differs from the saved "
                    "baseline. Inspect the imagery evidence before relying on it."
                ),
                field="recent_change",
                before=before_change,
                after=after_change,
            )
        )

    before_portfolio = snapshot.get("owner_portfolio_lot_count")
    after_portfolio = current.get("owner_portfolio_lot_count")
    if (
        isinstance(before_portfolio, int)
        and isinstance(after_portfolio, int)
        and before_portfolio != after_portfolio
    ):
        alerts.append(
            _alert(
                bbl=bbl,
                borough=borough,
                code="owner_portfolio_size_changed",
                severity="low",
                title="Exact-name owner portfolio changed",
                detail=(
                    "The count of current PLUTO lots sharing this exact legal "
                    "owner name differs from the saved baseline."
                ),
                field="owner_portfolio_lot_count",
                before=before_portfolio,
                after=after_portfolio,
            )
        )

    return alerts


def build_workflow_alerts(
    items: Iterable[dict[str, Any]],
    current_rows: Iterable[dict[str, Any]],
    *,
    feed_generated_at: str | None,
    screening_rows: dict[str, dict[str, Any]] | None = None,
    data_sources: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return authenticated in-app changes for active watched leads."""

    screening_rows = screening_rows or {}
    data_sources = data_sources or {}
    watched = [
        item
        for item in items
        if item.get("watching") is True and item.get("archived_at") is None
    ]
    current_by_bbl = {
        str(row.get("bbl")): row
        for row in current_rows
        if isinstance(row, dict) and row.get("bbl")
    }
    alerts: list[dict[str, Any]] = []
    changed_bbls: set[str] = set()
    removed_count = 0
    resolved_exit_count = 0
    unresolved_exit_count = 0
    screened_out_count = 0
    eligible_below_cutoff_count = 0
    missing_snapshot_count = 0

    for item in watched:
        bbl = str(item.get("bbl") or "")
        borough = str(item.get("borough") or "")
        snapshot = item.get("snapshot")
        if not isinstance(snapshot, dict) or not snapshot.get("feed_generated_at"):
            missing_snapshot_count += 1
        current = current_by_bbl.get(bbl)
        if current is None:
            removed_count += 1
            changed_bbls.add(bbl)
            screening = screening_rows.get(bbl)
            if isinstance(screening, dict):
                exit_alert = _screening_exit_alert(
                    item=item,
                    screening=screening,
                    data_sources=data_sources,
                )
                alerts.append(exit_alert)
                resolved_exit_count += 1
                if (
                    exit_alert.get("current_disposition")
                    == "eligible_below_cutoff"
                ):
                    eligible_below_cutoff_count += 1
                else:
                    screened_out_count += 1
            else:
                unresolved_exit_count += 1
                alerts.append(
                    _alert(
                        bbl=bbl,
                        borough=borough,
                        code="removed_from_current_feed",
                        severity="urgent",
                        title="No longer in the current eligible feed",
                        detail=(
                            "The parcel was saved previously but is absent from "
                            "the current acquisition-qualified inventory. Verify "
                            "current DOB, ZAP, ownership, sale, and constraint "
                            "records before continuing. This alert does not assert "
                            "why it was removed."
                        ),
                        field="acquisition_eligible",
                        before=True,
                        after=False,
                        current_disposition="not_evaluated",
                        recommended_action=(
                            "Keep the lead on hold until current official records "
                            "explain why it is absent from the evaluated candidate "
                            "set."
                        ),
                        parcel_available=False,
                    )
                )
            continue
        row_alerts = _row_alerts(item, current)
        if row_alerts:
            changed_bbls.add(bbl)
            alerts.extend(row_alerts)

    alerts.sort(
        key=lambda item: (
            _SEVERITY_ORDER.get(str(item.get("severity")), 99),
            str(item.get("bbl") or ""),
            str(item.get("code") or ""),
        )
    )
    severity_counts = Counter(str(item["severity"]) for item in alerts)
    warnings: list[str] = []
    if missing_snapshot_count:
        warnings.append(
            f"{missing_snapshot_count} watched lead(s) predate complete baseline "
            "snapshots; only current-feed removal can be assessed reliably for them."
        )

    return {
        "schema_version": ALERT_SCHEMA,
        "generated_at": datetime.now(timezone.utc),
        "feed_generated_at": feed_generated_at,
        "watched_count": len(watched),
        "changed_lead_count": len(changed_bbls),
        "alert_count": len(alerts),
        "removed_from_feed_count": removed_count,
        "resolved_exit_count": resolved_exit_count,
        "unresolved_exit_count": unresolved_exit_count,
        "screened_out_count": screened_out_count,
        "eligible_below_cutoff_count": eligible_below_cutoff_count,
        "severity_counts": {
            severity: severity_counts.get(severity, 0)
            for severity in ("urgent", "high", "medium", "low")
        },
        "alerts": alerts,
        "warnings": warnings,
    }

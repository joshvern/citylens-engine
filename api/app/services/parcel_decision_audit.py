from __future__ import annotations

from typing import Any

from ..models.schemas import ParcelDecisionAudit, ParcelIntelRow

AUDIT_SCHEMA = "citylens/parcel-decision-audit@v1"


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _joined_dates(*values: Any) -> str | None:
    dates = list(dict.fromkeys(value for value in map(_as_text, values) if value))
    return " / ".join(dates) if dates else None


def build_parcel_decision_audit(
    row: ParcelIntelRow,
    manifest: dict[str, Any] | None,
    *,
    premium_access: bool,
) -> ParcelDecisionAudit:
    """Separate historical ranking evidence from current decision evidence.

    The audit is built at read time so its language and privacy behavior do not
    depend on the publisher or browser. It never changes the stored feed row,
    model score, or acquisition eligibility.
    """

    metadata = (manifest or {}).get("model_metadata")
    model = metadata if isinstance(metadata, dict) else {}
    target = _as_text(model.get("label_definition")) or "dob_nb_job_filing"
    evaluation_scope = (
        _as_text(model.get("performance_scope"))
        or "Forward-only historical evaluation scope unavailable"
    )
    prospective_validated = model.get("prospective_2026_validated") is True
    validation = {
        "target": target,
        "evaluation_scope": evaluation_scope,
        "precision_at_100": _as_float(model.get("precision_at_100")),
        "precision_at_1000": _as_float(model.get("precision_at_1000")),
        "base_rate": _as_float(model.get("spatial_cv_base_rate")),
        "prospective_validated": prospective_validated,
        "disclaimer": (
            "Historical next-year DOB new-building filing performance is not "
            "seller intent, transaction probability, or acquisition conversion."
        ),
    }

    checks: list[dict[str, Any]] = [
        {
            "key": "historical_model",
            "layer": "model_signal",
            "label": "Historical redevelopment signal",
            "status": "informational",
            "summary": (
                "The accepted rolling-origin model orders structurally similar "
                "sites using historical PLUTO and DOB evidence. Treat the score "
                "as screening order, not a parcel-specific probability."
            ),
            "source": _as_text(model.get("metrics_source"))
            or "Accepted CityLens model bundle",
            "as_of": _as_text(model.get("label_window")),
            "affects_model_rank": True,
            "affects_acquisition_eligibility": False,
        }
    ]

    exclusion_reasons = [
        str(reason).replace("_", " ")
        for reason in row.acquisition_exclusion_reasons
        if str(reason).strip()
    ]
    if row.acquisition_eligible is True:
        eligibility_status = "verified"
        eligibility_summary = (
            "This published lead passed the current project, ownership, "
            "constraint, geometry, explanation, and capacity gates."
        )
    elif row.acquisition_status in {"active_project", "completed_project"}:
        eligibility_status = "excluded"
        eligibility_summary = (
            "Current official project evidence indicates the site is already "
            "active or completed and should not be treated as an acquisition lead."
        )
    elif row.acquisition_status == "incomplete_data":
        eligibility_status = "unavailable"
        eligibility_summary = (
            "Required current evidence is incomplete, so the parcel is not "
            "eligible for acquisition ranking."
        )
    else:
        eligibility_status = "excluded"
        eligibility_summary = (
            "The parcel does not pass the current acquisition eligibility policy."
        )
    if exclusion_reasons:
        eligibility_summary = (
            f"{eligibility_summary} Recorded reason"
            f"{'s' if len(exclusion_reasons) != 1 else ''}: "
            f"{'; '.join(exclusion_reasons)}."
        )
    checks.append(
        {
            "key": "acquisition_eligibility",
            "layer": "eligibility_gate",
            "label": "Current acquisition gate",
            "status": eligibility_status,
            "summary": eligibility_summary,
            "source": "CityLens deterministic acquisition policy",
            "as_of": _joined_dates(
                row.property_facts_as_of,
                row.project_activity_as_of,
                row.land_use_activity_as_of,
            ),
            "affects_model_rank": False,
            "affects_acquisition_eligibility": True,
        }
    )

    project_excluded = row.acquisition_status in {
        "active_project",
        "completed_project",
    }
    checks.append(
        {
            "key": "current_project_clearance",
            "layer": "eligibility_gate",
            "label": "DOB and ZAP project clearance",
            "status": "excluded" if project_excluded else "verified",
            "summary": (
                (
                    "Current DOB or ZAP evidence matched an active/completed "
                    "project exclusion."
                )
                if project_excluded
                else (
                    "No current DOB or private ZAP project exclusion matched "
                    "this published lead. Recheck the linked official records "
                    "before outreach."
                )
            ),
            "source": "NYC DOB and NYC Planning ZAP",
            "as_of": _joined_dates(
                row.project_activity_as_of, row.land_use_activity_as_of
            ),
            "affects_model_rank": False,
            "affects_acquisition_eligibility": True,
        }
    )

    checks.append(
        {
            "key": "property_facts",
            "layer": "source_freshness",
            "label": "Current property facts",
            "status": "verified" if row.property_facts_current else "unavailable",
            "summary": (
                "Current PLUTO tax-lot facts matched this BBL."
                if row.property_facts_current
                else (
                    "A current PLUTO match was not confirmed. Capacity and "
                    "existing-building facts require manual verification."
                )
            ),
            "source": "NYC PLUTO",
            "as_of": row.property_facts_as_of,
            "affects_model_rank": False,
            "affects_acquisition_eligibility": True,
        }
    )

    if not premium_access:
        ownership_status = "unavailable"
        ownership_summary = (
            "Sign in to review current owner provenance and exact-name "
            "portfolio context."
        )
    elif _as_text(row.owner_name):
        ownership_status = "verified"
        ownership_summary = (
            "A current deed/PLUTO legal owner is available. Exact-name portfolio "
            "matches do not infer beneficial ownership or related LLCs."
        )
    else:
        ownership_status = "unavailable"
        ownership_summary = (
            "No usable private-owner name is available for this parcel."
        )
    checks.append(
        {
            "key": "ownership",
            "layer": "source_freshness",
            "label": "Ownership provenance",
            "status": ownership_status,
            "summary": ownership_summary,
            "source": (
                "NYC PLUTO"
                if row.owner_name_source == "pluto"
                else "NYC ACRIS / NYC PLUTO"
            ),
            "as_of": row.ownership_as_of if premium_access else None,
            "affects_model_rank": False,
            "affects_acquisition_eligibility": True,
        }
    )

    diligence_signals: list[str] = []
    if premium_access:
        if row.tax_lien_sale_year:
            diligence_signals.append("historical final tax-lien sale")
        if (row.critical_violation_count or 0) > 0:
            diligence_signals.append("immediate-hazard violation")
        if row.floodplain_1pct is True:
            diligence_signals.append("1% floodplain overlap")
        if row.environmental_review_required is True:
            diligence_signals.append("environmental review instrument")
        if row.recent_change:
            diligence_signals.append("recent aerial change")
    if not premium_access:
        diligence_status = "unavailable"
        diligence_summary = (
            "Sign in to review current post-score diligence overlays."
        )
    elif diligence_signals:
        diligence_status = "review"
        diligence_summary = (
            "Review before underwriting: " + "; ".join(diligence_signals) + "."
        )
    else:
        diligence_status = "informational"
        diligence_summary = (
            "No joined post-score diligence flag is present in this feed. "
            "Absence of a flag is not completed legal, engineering, or site diligence."
        )
    checks.append(
        {
            "key": "current_diligence",
            "layer": "current_diligence",
            "label": "Current diligence overlays",
            "status": diligence_status,
            "summary": diligence_summary,
            "source": (
                "NYC DOF, DOB Safety, OATH/ECB, HPD, PLUTO/FEMA, OER context, "
                "and CityLens imagery"
            ),
            "as_of": (
                _joined_dates(
                    row.tax_lien_data_as_of,
                    row.violation_data_as_of,
                    row.floodplain_data_as_of,
                    row.environmental_designation_data_as_of,
                )
                if premium_access
                else None
            ),
            "affects_model_rank": False,
            "affects_acquisition_eligibility": False,
        }
    )

    warnings = [str(value).strip() for value in row.data_warnings if str(value).strip()]
    if row.acquisition_eligible is not True:
        overall_status = "excluded"
        overall_label = "Not an acquisition lead"
    elif not row.property_facts_current or warnings:
        overall_status = "incomplete"
        overall_label = "Screened, but evidence is incomplete"
    elif diligence_signals:
        overall_status = "screened_with_flags"
        overall_label = "Eligible lead with diligence flags"
    else:
        overall_status = "screened"
        overall_label = "Eligible lead after current gates"

    return ParcelDecisionAudit.model_validate(
        {
            "schema_version": AUDIT_SCHEMA,
            "overall_status": overall_status,
            "overall_label": overall_label,
            "validation": validation,
            "checks": checks,
            "limitations": [
                (
                    "The historical target is a next-year DOB new-building "
                    "filing, not owner willingness to sell."
                ),
                (
                    "Current eligibility gates reduce known false positives but "
                    "can lag newly filed, amended, withdrawn, or completed records."
                ),
                (
                    "No joined diligence flag means no flag in the cited snapshot, "
                    "not a clean title, buildable site, or completed investigation."
                ),
            ],
        }
    )

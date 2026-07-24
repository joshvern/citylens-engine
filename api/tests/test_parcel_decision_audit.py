from __future__ import annotations

from app.models.schemas import ParcelIntelRow
from app.services.parcel_decision_audit import build_parcel_decision_audit


def _row(**overrides) -> ParcelIntelRow:
    payload = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "address": "100 E 21 STREET",
        "acquisition_eligible": True,
        "acquisition_status": "eligible",
        "property_facts_current": True,
        "property_facts_as_of": "2026-07-24",
        "project_activity_as_of": "2026-07-22",
        "land_use_activity_as_of": "2026-07-24",
        "ownership_as_of": "2026-07-15",
        "owner_name": "TEST OWNER LLC",
        "owner_name_source": "acris",
    }
    payload.update(overrides)
    return ParcelIntelRow.model_validate(payload)


def _manifest() -> dict:
    return {
        "model_metadata": {
            "label_definition": "dob_nb_job_filing",
            "performance_scope": "2024 PLUTO to 2025 DOB NB filings",
            "metrics_source": "accepted_model_bundle.rolling_validation",
            "label_window": "2025-2025",
            "precision_at_100": 0.34,
            "precision_at_1000": 0.104,
            "spatial_cv_base_rate": 0.0012439591,
            "prospective_2026_validated": False,
        }
    }


def test_decision_audit_separates_model_gate_and_diligence_evidence() -> None:
    audit = build_parcel_decision_audit(
        _row(
            tax_lien_sale_year=2025,
            critical_violation_count=2,
            floodplain_1pct=True,
            environmental_review_required=True,
            mandatory_inclusionary_housing=True,
            mih_options=["Option 1"],
            mih_data_as_of="2026-07-24",
            nearest_transit_station_name="Church Av",
            nearest_transit_station_distance_m=420,
            nearest_transit_routes=["B", "Q"],
            nearest_transit_ada_status="full",
            transit_station_count_800m=2,
            transit_access_tier="walkable",
            transit_data_as_of="2026-07-24",
            recent_change=True,
        ),
        _manifest(),
        premium_access=True,
    )

    assert audit.schema_version == "citylens/parcel-decision-audit@v1"
    assert audit.overall_status == "screened_with_flags"
    assert audit.validation.precision_at_100 == 0.34
    assert audit.validation.precision_at_1000 == 0.104
    assert audit.validation.prospective_validated is False
    checks = {check.key: check for check in audit.checks}
    assert checks["historical_model"].affects_model_rank is True
    assert checks["acquisition_eligibility"].affects_acquisition_eligibility is True
    assert checks["current_diligence"].affects_model_rank is False
    assert checks["current_diligence"].affects_acquisition_eligibility is False
    assert checks["current_diligence"].status == "review"
    assert "historical final tax-lien sale" in checks["current_diligence"].summary
    assert checks["transit_access"].status == "verified"
    assert "420 m straight-line" in checks["transit_access"].summary
    assert checks["transit_access"].affects_model_rank is False
    assert audit.readiness.status == "review_required"
    assert any(
        "floodplain exposure" in item for item in audit.readiness.review_items
    )
    assert any(
        "MIH applicability" in item for item in audit.readiness.review_items
    )
    assert "purchase recommendation" in audit.readiness.disclaimer


def test_public_decision_audit_does_not_summarize_private_signals() -> None:
    audit = build_parcel_decision_audit(
        _row(
            owner_name=None,
            tax_lien_sale_year=2025,
            critical_violation_count=2,
            floodplain_1pct=True,
            environmental_review_required=True,
            mandatory_inclusionary_housing=True,
            recent_change=True,
        ),
        _manifest(),
        premium_access=False,
    )

    checks = {check.key: check for check in audit.checks}
    assert checks["ownership"].status == "unavailable"
    assert checks["current_diligence"].status == "unavailable"
    assert checks["transit_access"].status == "unavailable"
    assert "Sign in" in checks["current_diligence"].summary
    assert "tax-lien" not in checks["current_diligence"].summary
    assert audit.overall_status == "screened"
    assert audit.readiness.status == "limited_preview"
    assert audit.readiness.review_items == [
        "Protected ownership and diligence evidence is withheld in this preview."
    ]
    assert "tax-lien" not in " ".join(audit.readiness.review_items)
    assert "mandatory inclusionary housing" not in " ".join(
        audit.readiness.review_items
    ).lower()


def test_current_project_exclusion_dominates_overall_audit_status() -> None:
    audit = build_parcel_decision_audit(
        _row(
            acquisition_eligible=False,
            acquisition_status="active_project",
            acquisition_exclusion_reasons=["approved_land_use_project"],
            latest_project_type="land_use_entitlement",
        ),
        _manifest(),
        premium_access=True,
    )

    assert audit.overall_status == "excluded"
    assert audit.overall_label == "Not an acquisition lead"
    checks = {check.key: check for check in audit.checks}
    assert checks["acquisition_eligibility"].status == "excluded"
    assert checks["current_project_clearance"].status == "excluded"
    assert "approved land use project" in checks["acquisition_eligibility"].summary
    assert audit.readiness.status == "blocked"
    assert audit.readiness.blockers == ["approved land use project"]
    assert "Keep this parcel out of acquisition outreach" in (
        audit.readiness.recommended_action
    )


def test_clean_private_audit_proposes_initial_review_without_predictive_claim() -> None:
    audit = build_parcel_decision_audit(
        _row(max_floor_area_sqft=8_000),
        _manifest(),
        premium_access=True,
    )

    assert audit.readiness.status == "initial_review_ready"
    assert audit.readiness.blockers == []
    assert audit.readiness.review_items == []
    assert "owner/title review" in audit.readiness.recommended_action
    assert "seller-intent score" in audit.readiness.disclaimer

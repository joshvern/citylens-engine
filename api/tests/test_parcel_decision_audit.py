from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.schemas import ParcelIntelIndex, ParcelIntelRow
from app.services.parcel_decision_audit import build_parcel_decision_audit


def _row(**overrides) -> ParcelIntelRow:
    payload = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "address": "100 E 21 STREET",
        "address_source": "nyc_pad",
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


def _borough_receipt() -> dict:
    borough_rows = {
        "manhattan": (33_718, 36, 3, [0.010254524024038925, 0.0845193642905276]),
        "bronx": (78_837, 202, 24, [0.16691325556948772, 0.33232336349814473]),
        "brooklyn": (245_853, 240, 10, [0.0552291370606751, 0.17436566150491345]),
        "queens": (301_132, 329, 23, [0.15843265880303448, 0.3215438302287378]),
        "staten_island": (
            108_974,
            149,
            13,
            [0.07757167427240512, 0.20980351440076428],
        ),
    }
    boroughs = {}
    for slug, (rows, positives, hits, interval) in borough_rows.items():
        boroughs[slug] = {
            "evaluation_rows": rows,
            "observed_positive_rows": positives,
            "base_rate": positives / rows,
            "top_100": {
                "k": 100,
                "evaluated_rows": 100,
                "observed_hits": hits,
                "precision": hits / 100,
                "precision_95ci": interval,
            },
        }
    return {
        "schema": "citylens_historical_borough_benchmark_receipt@v1",
        "target": "dob_nb_job_filing",
        "feature_origin": 2024,
        "outcome_window": "2025-2025",
        "evaluation_scope": "rolling_origin_latest_out_of_time",
        "ranking_scope": "historical_within_borough_model_order",
        "citywide_evaluation_rows": 768_514,
        "citywide_observed_positive_rows": 956,
        "boroughs": boroughs,
        "interval": {
            "method": "wilson_score_observed_top_k",
            "confidence_level": 0.95,
            "scope": "fixed_historical_borough_ranked_list",
            "limitations": (
                "Does not include model-selection uncertainty, spatial "
                "dependence, dataset shift, current acquisition outcomes, "
                "or a parcel-specific probability."
            ),
        },
        "source_receipt": {
            "schema": "citylens_borough_benchmark_attachment@v1",
            "report_file_name": "rolling_origin_1y_attested.json",
            "report_schema": "citylens_rolling_origin_backtest@v2",
            "report_sha256": "a" * 64,
            "report_size_bytes": 21_910,
            "source_model_sha256": "b" * 64,
            "metadata_only_attachment": True,
        },
        "evidence_status": "development_exposed",
        "not_current_accuracy": True,
        "not_parcel_confidence": True,
    }


def _manifest() -> dict:
    return {
        "generated_at": "2026-07-24T02:43:29Z",
        "model_metadata": {
            "label_definition": "dob_nb_job_filing",
            "performance_scope": "2024 PLUTO to 2025 DOB NB filings",
            "metrics_source": "accepted_model_bundle.rolling_validation",
            "label_window": "2025-2025",
            "precision_at_100": 0.34,
            "precision_at_1000": 0.104,
            "spatial_cv_base_rate": 0.0012439591,
            "historical_benchmark_receipt": {
                "schema": "citylens_historical_benchmark_receipt@v1",
                "target": "dob_nb_job_filing",
                "feature_origin": 2024,
                "outcome_window": "2025-2025",
                "evaluation_scope": "rolling_origin_latest_out_of_time",
                "evaluation_rows": 768514,
                "observed_positive_rows": 956,
                "base_rate": 956 / 768514,
                "auc": 0.9232830323176429,
                "pr_auc": 0.054015618548797745,
                "top_100": {
                    "k": 100,
                    "evaluated_rows": 100,
                    "observed_hits": 34,
                    "precision": 0.34,
                    "precision_95ci": [
                        0.25461520797348164,
                        0.43722271145275377,
                    ],
                },
                "top_1000": {
                    "k": 1000,
                    "evaluated_rows": 1000,
                    "observed_hits": 104,
                    "precision": 0.104,
                    "precision_95ci": [
                        0.08657102809826807,
                        0.12445976462229157,
                    ],
                },
                "interval": {
                    "method": "wilson_score_observed_top_k",
                    "confidence_level": 0.95,
                    "scope": "fixed_historical_ranked_list",
                    "limitations": (
                        "Observed binomial uncertainty only; not model "
                        "selection, spatial dependence, or current outcomes."
                    ),
                },
                "evidence_status": "development_exposed",
                "not_current_accuracy": True,
                "not_parcel_confidence": True,
            },
            "historical_borough_benchmark_receipt": _borough_receipt(),
            "prospective_2026_validated": False,
            "evaluation_evidence": {
                "status": "development_exposed",
            },
        },
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
    assert audit.evidence_generated_at == "2026-07-24T02:43:29Z"
    assert audit.overall_status == "screened_with_flags"
    assert audit.validation.precision_at_100 == 0.34
    assert audit.validation.precision_at_1000 == 0.104
    receipt = audit.validation.historical_benchmark_receipt
    assert receipt is not None
    assert receipt.top_100.observed_hits == 34
    assert receipt.top_1000.observed_hits == 104
    assert receipt.not_current_accuracy is True
    assert receipt.not_parcel_confidence is True
    borough_receipt = audit.validation.historical_borough_benchmark_receipt
    assert borough_receipt is not None
    assert set(borough_receipt.boroughs) == {
        "manhattan",
        "bronx",
        "brooklyn",
        "queens",
        "staten_island",
    }
    cohort = audit.validation.historical_borough_cohort
    assert cohort is not None
    assert cohort.borough == "brooklyn"
    assert cohort.cohort.evaluation_rows == 245_853
    assert cohort.cohort.top_100.observed_hits == 10
    assert cohort.cohort.top_100.precision == 0.1
    assert cohort.not_current_accuracy is True
    assert cohort.not_parcel_confidence is True
    assert audit.validation.prospective_validated is False
    assert "not an independent current-accuracy estimate" in (audit.validation.disclaimer)
    checks = {check.key: check for check in audit.checks}
    assert checks["historical_model"].affects_model_rank is True
    assert (
        checks["historical_model"].source
        == "CityLens rolling-origin validation using NYC PLUTO and DOB filings"
    )
    assert checks["historical_model"].as_of == "2025"
    assert checks["address_identity"].status == "verified"
    assert "matched to this BBL" in checks["address_identity"].summary
    assert checks["address_identity"].affects_model_rank is False
    assert checks["address_identity"].affects_acquisition_eligibility is False
    assert checks["acquisition_eligibility"].affects_acquisition_eligibility is True
    assert checks["current_diligence"].affects_model_rank is False
    assert checks["current_diligence"].affects_acquisition_eligibility is False
    assert checks["current_diligence"].status == "review"
    assert "historical final tax-lien sale" in checks["current_diligence"].summary
    assert checks["transit_access"].status == "verified"
    assert "420 m straight-line" in checks["transit_access"].summary
    assert checks["transit_access"].affects_model_rank is False
    assert audit.readiness.status == "review_required"
    assert any("floodplain exposure" in item for item in audit.readiness.review_items)
    assert any("MIH applicability" in item for item in audit.readiness.review_items)
    assert "purchase recommendation" in audit.readiness.disclaimer


def test_index_rejects_internally_inconsistent_historical_receipt() -> None:
    metadata = _manifest()["model_metadata"]
    metadata["historical_benchmark_receipt"]["top_100"]["observed_hits"] = 99

    with pytest.raises(ValidationError, match="precision does not match"):
        ParcelIntelIndex.model_validate({"model_metadata": metadata})


def test_decision_audit_rejects_malformed_historical_receipt() -> None:
    manifest = _manifest()
    manifest["model_metadata"]["historical_benchmark_receipt"]["not_parcel_confidence"] = False

    with pytest.raises(ValidationError):
        build_parcel_decision_audit(
            _row(),
            manifest,
            premium_access=True,
        )


def test_index_rejects_internally_inconsistent_borough_receipt() -> None:
    metadata = _manifest()["model_metadata"]
    metadata["historical_borough_benchmark_receipt"]["boroughs"]["queens"]["top_100"][
        "observed_hits"
    ] = 24

    with pytest.raises(ValidationError, match="precision does not match"):
        ParcelIntelIndex.model_validate({"model_metadata": metadata})


def test_decision_audit_omits_selected_cohort_for_unknown_borough() -> None:
    audit = build_parcel_decision_audit(
        _row(borough=None),
        _manifest(),
        premium_access=True,
    )

    assert audit.validation.historical_borough_benchmark_receipt is not None
    assert audit.validation.historical_borough_cohort is None


def test_decision_audit_formats_multi_year_model_window_for_people() -> None:
    manifest = _manifest()
    manifest["model_metadata"]["label_window"] = "2018-2025"

    audit = build_parcel_decision_audit(
        _row(),
        manifest,
        premium_access=True,
    )

    historical = next(check for check in audit.checks if check.key == "historical_model")
    assert historical.as_of == "2018–2025"


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
    assert "mandatory inclusionary housing" not in " ".join(audit.readiness.review_items).lower()


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
    assert "Keep this parcel out of acquisition outreach" in (audit.readiness.recommended_action)


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

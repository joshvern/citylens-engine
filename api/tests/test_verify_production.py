from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from scripts.verify_production import (
    evaluate_source_slas,
    validate_index,
    validate_map,
    validate_public_decision_audit,
    validate_security_headers,
    validate_sweep,
    validate_workflow_methodology,
)


def _security_headers(*, browser_page: bool) -> dict[str, str]:
    headers = {
        "content-security-policy": (
            "base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
            "form-action 'self'"
            if browser_page
            else "base-uri 'none'; object-src 'none'; frame-ancestors 'none'"
        ),
        "permissions-policy": (
            "browsing-topics=(), camera=(), geolocation=(), microphone=(), "
            "payment=()"
        ),
        "referrer-policy": (
            "strict-origin-when-cross-origin"
            if browser_page
            else "no-referrer"
        ),
        "strict-transport-security": "max-age=63072000",
        "x-content-type-options": "nosniff",
        "x-frame-options": "DENY",
        "x-xss-protection": "0",
    }
    return headers


def test_security_header_validator_covers_api_and_browser_contracts() -> None:
    assert (
        validate_security_headers(
            _security_headers(browser_page=False),
            label="API",
            browser_page=False,
        )
        == []
    )
    assert (
        validate_security_headers(
            _security_headers(browser_page=True),
            label="web",
            browser_page=True,
        )
        == []
    )

    bad = _security_headers(browser_page=True)
    bad["x-powered-by"] = "Next.js"
    bad["strict-transport-security"] = "max-age=60"
    bad["content-security-policy"] = "object-src 'none'"
    failures = validate_security_headers(
        bad,
        label="web",
        browser_page=True,
    )
    assert any("HSTS" in failure for failure in failures)
    assert any("frame-ancestors" in failure for failure in failures)
    assert any("X-Powered-By" in failure for failure in failures)


def _quality_row() -> dict:
    return {
        "passed": True,
        "failures": [],
        "row_count": 1000,
        "project_leakage_count": 0,
        "land_use_project_leakage_count": 0,
        "authoritative_zap_bbl_leakage_count": 0,
        "duplicate_bbl_count": 0,
        "invalid_owner_leakage_count": 0,
        "non_private_owner_leakage_count": 0,
        "negative_unused_floor_area_count": 0,
        "invalid_owner_portfolio_count": 0,
        "owner_coverage": 1.0,
        "geometry_coverage": 1.0,
        "floodplain_coverage": 1.0,
        "environmental_review_coverage": 1.0,
        "mih_coverage": 1.0,
        "transit_coverage": 1.0,
    }


def _index() -> dict:
    data_sources = {
        key: {
            "source": key,
            "retrieved_at": "2026-07-23",
            "max_age_days": max_age,
            "stale": False,
        }
        for key, max_age in {
            "property_facts": 45,
            "ownership": 45,
            "constraints": 180,
            "project_activity": 14,
            "land_use_activity": 45,
            "owner_portfolio": 45,
            "tax_lien_sale_history": 45,
            "current_violations": 7,
            "floodplain_screen": 45,
            "environmental_review": 45,
            "mandatory_inclusionary_housing": 45,
            "transit_access": 45,
        }.items()
    }
    return {
        "generated_at": "2026-07-23T00:00:00Z",
        "age_days": 1.0,
        "stale": False,
        "data_sources": data_sources,
        "boroughs": [
            {"slug": slug, "display_name": slug.title(), "count": 1000}
            for slug in ("manhattan", "brooklyn", "queens", "bronx", "staten_island")
        ],
        "quality_gate": {
            "passed": True,
            "failures": [],
            "citywide_acquisition_eligible_count": 5000,
            "citywide_rank_sequence_valid": True,
            "land_use_reconciliation": {
                "schema": (
                    "citylens-parcel-intel/land-use-reconciliation@v1"
                ),
                "source_schema": "citylens-parcel-intel/zap-activity@v1",
                "source_generated_at": "2026-07-23T00:00:00Z",
                "source_sha256": "a" * 64,
                "declared_blocked_bbl_count": 3108,
                "source_blocked_bbl_count": 3108,
                "private_current_project_count": 801,
                "non_parcel_applicable_project_count": 1,
                "non_parcel_applicable_project_ids": ["2022Y0395"],
                "blocking_project_count": 800,
                "joined_blocking_project_count": 796,
                "unjoined_blocking_project_count": 4,
                "unjoined_blocking_project_ids": [
                    "2021K0396",
                    "2022R0129",
                    "2026R0327",
                    "P2013X0306",
                ],
                "minimum_project_bbl_crosswalk_coverage": 0.99,
                "project_bbl_crosswalk_coverage": 0.995,
                "project_detail_source": (
                    "https://zap-api-production.herokuapp.com/projects/"
                    "{project_id}"
                ),
                "project_detail_retrieved_at": (
                    "2026-07-24T08:52:20.198447+00:00"
                ),
                "project_detail_supplemental_relation_count": 1,
                "project_detail_fetch_failure_count": 0,
                "project_detail_fetch_failure_ids": [],
                "current_tax_lot_reconciliation_candidate_count": 27,
                "current_tax_lot_reconciled_relation_count": 15,
                "current_tax_lot_reconciled_project_count": 12,
                "current_tax_lot_reconciled_project_ids": [
                    "P1",
                    "P2",
                    "P3",
                    "P4",
                    "P5",
                    "P6",
                    "P7",
                    "P8",
                    "P9",
                    "P10",
                    "P11",
                    "P12",
                ],
                "current_tax_lot_unmatched_user_input_count": 10,
                "current_tax_lot_universe_count": 858_602,
                "current_tax_lot_index_sha256": "b" * 64,
                "candidate_blocked_bbl_count": 442,
                "published_leakage_count": 0,
                "passed": True,
                "failures": [],
            },
            "ranking_tie_audit": {
                "schema": "citylens-parcel-intel/ranking-tie-audit@v1",
                "primary_field": "score_calibrated",
                "tiebreaker_field": "score_raw",
                "tiebreaker_scope": "equal_calibrated_probability_only",
                "tiebreaker_is_public": False,
                "deterministic_fallback": ["model_rank", "bbl"],
                "boroughs": {
                    slug: {
                        "row_count": 1000,
                        "tiebreaker_count": 1000,
                        "tiebreaker_coverage": 1.0,
                    }
                    for slug in (
                        "manhattan",
                        "brooklyn",
                        "queens",
                        "bronx",
                        "staten_island",
                    )
                },
                "citywide": {
                    "row_count": 5000,
                    "tiebreaker_count": 5000,
                    "tiebreaker_coverage": 1.0,
                },
                "passed": True,
                "failures": [],
            },
            "boroughs": {
                slug: _quality_row()
                for slug in (
                    "manhattan",
                    "brooklyn",
                    "queens",
                    "bronx",
                    "staten_island",
                )
            },
        },
        "model_metadata": {
            "label_definition": "dob_nb_job_filing",
            "evaluation_mode": "rolling_origin",
            "training_origins": [2018, 2020, 2022],
            "calibration_origin": 2024,
            "inference_feature_snapshot": "current",
            "precision_at_100": 0.34,
            "precision_at_1000": 0.104,
            "spatial_cv_base_rate": 0.0012439591,
            "prospective_2026_validated": False,
            "ranking_policy": {
                "primary_field": "score_calibrated",
                "tiebreaker_field": "score_raw",
                "tiebreaker_scope": "equal_calibrated_probability_only",
                "tiebreaker_is_public": False,
                "deterministic_fallback": ["model_rank", "bbl"],
            },
        },
        "generation_diff": {
            "schema": "citylens-parcel-intel/generation-diff@v1",
            "status": "compared",
            "candidate": {"row_count": 5000},
            "inference_feature_drift": {
                "schema": (
                    "citylens-parcel-intel/inference-feature-drift@v1"
                ),
                "status": "compared",
                "candidate": {
                    "row_count": 5000,
                    "column_count": 142,
                    "feature_spec_sha256": "a" * 64,
                },
                "gate": {
                    "passed": True,
                    "failures": [],
                    "warnings": [],
                },
            },
            "gate": {
                "passed": True,
                "thresholds_passed": True,
                "override_applied": False,
                "override_reason": None,
                "failures": [],
            },
        },
        "inference_replay": {
            "schema": "citylens-parcel-intel/inference-replay@v1",
            "row_count": 5000,
            "passed": True,
            "status": "matched",
            "mismatch_count": 0,
            "maximum_absolute_error": 0.0,
        },
    }


def _public_row(*, bbl: str, borough: str, rank: int, citywide_rank: int) -> dict:
    return {
        "bbl": bbl,
        "borough": borough,
        "acquisition_rank": rank,
        "citywide_rank": citywide_rank,
        "acquisition_eligible": True,
        "acquisition_status": "eligible",
        "opportunity_category": "ground_up_candidate",
    }


def test_index_validator_enforces_freshness_quality_and_model_governance() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    assert validate_index(_index(), max_age_days=35, now=now) == []

    bad = deepcopy(_index())
    bad["generated_at"] = "2026-05-01T00:00:00Z"
    bad["quality_gate"]["boroughs"]["queens"]["project_leakage_count"] = 1
    bad["model_metadata"]["prospective_2026_validated"] = True
    failures = validate_index(bad, max_age_days=35, now=now)
    assert any("days old" in failure for failure in failures)
    assert "index: queens project_leakage_count is not zero" in failures
    assert any("prospective 2026 validation flag" in failure for failure in failures)


def test_index_validator_rejects_land_use_source_reconciliation_leakage() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    bad["quality_gate"]["land_use_reconciliation"][
        "published_leakage_count"
    ] = 1
    bad["quality_gate"]["land_use_reconciliation"]["passed"] = False
    bad["quality_gate"]["land_use_reconciliation"]["failures"] = [
        "published_blocked_bbl_leakage"
    ]
    bad["quality_gate"]["boroughs"]["brooklyn"][
        "authoritative_zap_bbl_leakage_count"
    ] = 1

    failures = validate_index(bad, max_age_days=35, now=now)

    assert (
        "index: authoritative ZAP-blocked BBL leaked into published leads"
        in failures
    )
    assert (
        "index: brooklyn authoritative_zap_bbl_leakage_count is not zero"
        in failures
    )


def test_index_validator_rejects_incomplete_land_use_project_crosswalk() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    reconciliation = bad["quality_gate"]["land_use_reconciliation"]
    reconciliation["joined_blocking_project_count"] = 792
    reconciliation["unjoined_blocking_project_count"] = 9
    reconciliation["project_bbl_crosswalk_coverage"] = 0.9887640449438202

    failures = validate_index(bad, max_age_days=35, now=now)

    assert (
        "index: land-use reconciliation unresolved project IDs are invalid"
        in failures
    )
    assert "index: land-use project-to-BBL coverage is below 99%" in failures


def test_index_validator_rejects_land_use_scope_or_detail_failures() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    reconciliation = bad["quality_gate"]["land_use_reconciliation"]
    reconciliation["non_parcel_applicable_project_count"] = 2
    reconciliation["project_detail_fetch_failure_count"] = 1
    reconciliation["project_detail_fetch_failure_ids"] = ["2021K0396"]

    failures = validate_index(bad, max_age_days=35, now=now)

    assert (
        "index: land-use reconciliation non-parcel project IDs are invalid"
        in failures
    )
    assert (
        "index: land-use reconciliation project scope counts disagree"
        in failures
    )
    assert "index: land-use project-detail refresh has failures" in failures
    assert (
        "index: land-use project-detail failure IDs are not empty"
        in failures
    )


def test_index_validator_rejects_weak_current_tax_lot_reconciliation() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    reconciliation = bad["quality_gate"]["land_use_reconciliation"]
    reconciliation["current_tax_lot_reconciled_relation_count"] = 28
    reconciliation["current_tax_lot_reconciled_project_count"] = 13
    reconciliation["current_tax_lot_reconciled_project_ids"] = ["P1"]
    reconciliation["current_tax_lot_unmatched_user_input_count"] = -1
    reconciliation["current_tax_lot_universe_count"] = 799_999
    reconciliation["current_tax_lot_index_sha256"] = "not-a-digest"

    failures = validate_index(bad, max_age_days=35, now=now)

    assert (
        "index: current-tax-lot reconciled relation count is invalid"
        in failures
    )
    assert (
        "index: current-tax-lot reconciled project IDs are invalid"
        in failures
    )
    assert "index: current-tax-lot unmatched input count is invalid" in failures
    assert "index: current PLUTO tax-lot universe is invalid" in failures
    assert "index: current PLUTO tax-lot digest is invalid" in failures


def test_index_validator_rejects_incomplete_ranking_tiebreak_coverage() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    audit = bad["quality_gate"]["ranking_tie_audit"]
    audit["boroughs"]["queens"]["tiebreaker_count"] = 999
    audit["boroughs"]["queens"]["tiebreaker_coverage"] = 0.999
    audit["citywide"]["tiebreaker_count"] = 4999
    audit["citywide"]["tiebreaker_coverage"] = 0.9998
    bad["model_metadata"]["ranking_policy"]["tiebreaker_is_public"] = True

    failures = validate_index(bad, max_age_days=35, now=now)

    assert "index: queens ranking tie-break coverage is incomplete" in failures
    assert (
        "index: citywide ranking tie-break coverage is incomplete"
        in failures
    )
    assert "index: model ranking policy is invalid" in failures


def test_workflow_methodology_validator_requires_maturity_aware_contract() -> None:
    methodology = {
        "schema_version": (
            "citylens/parcel-workflow-analytics-methodology@v2"
        ),
        "analytics_schema_version": "citylens/parcel-workflow-analytics@v3",
        "model_accuracy_claim": False,
        "minimum_rate_denominator": 10,
        "confidence_level": 0.95,
        "uncertainty_semantics": "Two-sided 95% Wilson score intervals.",
        "horizons": [
            {"milestone": milestone, "horizon_days": days}
            for milestone, days in (
                ("owner_contacted", 30),
                ("qualified", 90),
                ("offer_submitted", 180),
                ("under_contract", 270),
                ("closed", 365),
            )
        ],
    }
    assert validate_workflow_methodology(methodology) == []

    bad = deepcopy(methodology)
    bad["analytics_schema_version"] = (
        "citylens/parcel-workflow-analytics@v1"
    )
    bad["model_accuracy_claim"] = True
    bad["horizons"][0]["horizon_days"] = 5
    failures = validate_workflow_methodology(bad)
    assert any("analytics v3" in failure for failure in failures)
    assert any("must not claim model accuracy" in failure for failure in failures)
    assert any("fixed horizons" in failure for failure in failures)


def test_source_sla_validator_recomputes_age_and_warns_before_breach() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    index = _index()
    index["data_sources"]["current_violations"]["retrieved_at"] = "2026-07-19"

    failures, warnings, report = evaluate_source_slas(index, now=now)

    assert failures == []
    assert warnings == [
        "index: source SLA current_violations has 2.0 days remaining"
    ]
    assert report["passed"] is True
    assert report["warning_count"] == 1
    assert report["sources"]["current_violations"]["status"] == "warning"


def test_source_sla_validator_rejects_missing_stale_and_future_sources() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    index = _index()
    del index["data_sources"]["ownership"]
    index["data_sources"]["project_activity"]["retrieved_at"] = "2026-06-01"
    index["data_sources"]["property_facts"]["retrieved_at"] = "2026-07-25"

    failures, _, report = evaluate_source_slas(index, now=now)

    assert "index: source SLA ownership is missing" in failures
    assert any(
        failure.startswith("index: source SLA project_activity is stale")
        for failure in failures
    )
    assert (
        "index: source SLA property_facts retrieved_at is in the future"
        in failures
    )
    assert report["passed"] is False
    assert report["breach_count"] == 3


def test_index_validator_requires_reviewed_generation_diff_override() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    bad["generation_diff"]["gate"].update(
        {
            "thresholds_passed": False,
            "override_applied": False,
            "override_reason": None,
            "failures": ["score_psi_exceeded"],
        }
    )

    failures = validate_index(bad, max_age_days=35, now=now)
    assert (
        "index: failed drift thresholds lack a reviewed override reason"
        in failures
    )

    bad["generation_diff"]["gate"].update(
        {
            "override_applied": True,
            "override_reason": "Reviewed annual model cutover PI-42.",
        }
    )
    assert validate_index(bad, max_age_days=35, now=now) == []


def test_index_validator_requires_full_feature_drift_and_score_replay() -> None:
    now = datetime(2026, 7, 24, tzinfo=timezone.utc)
    bad = deepcopy(_index())
    bad["generation_diff"]["inference_feature_drift"]["candidate"][
        "column_count"
    ] = 141
    bad["inference_replay"].update(
        {
            "passed": False,
            "status": "mismatched",
            "mismatch_count": 1,
            "maximum_absolute_error": 0.01,
        }
    )

    failures = validate_index(bad, max_age_days=35, now=now)

    assert "index: inference feature column count is not 142" in failures
    assert (
        "index: inference score replay did not match all 5,000 rows"
        in failures
    )
    assert (
        "index: inference score replay has non-zero maximum error"
        in failures
    )


def test_map_validator_enforces_caps_ranks_and_public_redaction() -> None:
    prefix = {
        "manhattan": "1",
        "bronx": "2",
        "brooklyn": "3",
        "queens": "4",
        "staten_island": "5",
    }
    rows = []
    citywide_rank = 1
    for borough in ("manhattan", "brooklyn", "queens", "bronx", "staten_island"):
        for rank in range(1, 26):
            rows.append(
                _public_row(
                    bbl=f"{prefix[borough]}{citywide_rank:09d}",
                    borough=borough,
                    rank=rank,
                    citywide_rank=citywide_rank,
                )
            )
            citywide_rank += 1
    payload = {"generated_at": "2026-07-23T00:00:00Z", "rows": rows}
    assert (
        validate_map(payload, expected_generated_at="2026-07-23T00:00:00Z")
        == []
    )

    bad = deepcopy(payload)
    bad["rows"][0]["owner_name"] = "PRIVATE OWNER LLC"
    bad["rows"][1]["recent_change"] = True
    bad["rows"][2]["citywide_rank"] = bad["rows"][3]["citywide_rank"]
    failures = validate_map(
        bad, expected_generated_at="2026-07-23T00:00:00Z"
    )
    assert any("owner_name was exposed" in failure for failure in failures)
    assert any("recent_change was exposed" in failure for failure in failures)
    assert any("duplicate citywide rank" in failure for failure in failures)


def test_public_decision_audit_validator_enforces_roles_metrics_and_privacy() -> None:
    payload = _public_row(
        bbl="3020960069",
        borough="brooklyn",
        rank=1,
        citywide_rank=1,
    )
    payload["decision_audit"] = {
        "schema_version": "citylens/parcel-decision-audit@v1",
        "overall_status": "screened",
        "overall_label": "Eligible lead after current gates",
        "readiness": {
            "status": "limited_preview",
            "label": "Sign in to complete the decision screen",
            "recommended_action": (
                "Review ownership provenance and current diligence overlays "
                "before acting."
            ),
            "blockers": [],
            "review_items": [
                "Protected ownership and diligence evidence is withheld in this preview."
            ],
            "cleared_items": [
                "Current project and acquisition eligibility gates passed.",
                "Current PLUTO property facts matched this tax lot.",
            ],
            "disclaimer": (
                "Decision readiness is not a purchase recommendation or "
                "seller-intent score."
            ),
        },
        "validation": {
            "target": "dob_nb_job_filing",
            "evaluation_scope": "2024 PLUTO to 2025 DOB NB filings",
            "precision_at_100": 0.34,
            "precision_at_1000": 0.104,
            "base_rate": 0.0012439591,
            "prospective_validated": False,
            "disclaimer": (
                "Historical performance is not seller intent or transaction "
                "probability."
            ),
        },
        "checks": [
            {
                "key": "historical_model",
                "layer": "model_signal",
                "status": "informational",
                "summary": "Historical screening order.",
                "source": "Accepted model bundle",
                "as_of": "2025",
                "affects_model_rank": True,
                "affects_acquisition_eligibility": False,
            },
            {
                "key": "acquisition_eligibility",
                "layer": "eligibility_gate",
                "status": "verified",
                "summary": "Passed current gates.",
                "source": "CityLens policy",
                "as_of": "2026-07-24",
                "affects_model_rank": False,
                "affects_acquisition_eligibility": True,
            },
            {
                "key": "current_project_clearance",
                "layer": "eligibility_gate",
                "status": "verified",
                "summary": "No current project exclusion matched.",
                "source": "NYC DOB and ZAP",
                "as_of": "2026-07-24",
                "affects_model_rank": False,
                "affects_acquisition_eligibility": True,
            },
            {
                "key": "property_facts",
                "layer": "source_freshness",
                "status": "verified",
                "summary": "Current property facts matched.",
                "source": "NYC PLUTO",
                "as_of": "2026-07-24",
                "affects_model_rank": False,
                "affects_acquisition_eligibility": True,
            },
            {
                "key": "ownership",
                "layer": "source_freshness",
                "status": "unavailable",
                "summary": "Sign in to review ownership.",
                "source": "NYC ACRIS / NYC PLUTO",
                "as_of": None,
                "affects_model_rank": False,
                "affects_acquisition_eligibility": True,
            },
            {
                "key": "current_diligence",
                "layer": "current_diligence",
                "status": "unavailable",
                "summary": "Sign in to review current diligence overlays.",
                "source": "Current official sources",
                "as_of": None,
                "affects_model_rank": False,
                "affects_acquisition_eligibility": False,
            },
            {
                "key": "transit_access",
                "layer": "current_diligence",
                "status": "unavailable",
                "summary": "Sign in to review subway/SIR accessibility.",
                "source": "MTA Subway Stations",
                "as_of": None,
                "affects_model_rank": False,
                "affects_acquisition_eligibility": False,
            },
        ],
        "limitations": [
            "The target is not owner willingness to sell.",
            "Current sources can lag official updates.",
        ],
    }
    model_metadata = _index()["model_metadata"]

    assert (
        validate_public_decision_audit(
            payload,
            model_metadata=model_metadata,
        )
        == []
    )

    bad = deepcopy(payload)
    bad["decision_audit"]["validation"]["precision_at_100"] = 0.99
    bad["decision_audit"]["readiness"]["review_items"] = [
        "Review the private tax-lien evidence."
    ]
    bad["decision_audit"]["checks"][4]["summary"] = "PRIVATE OWNER LLC"
    bad["decision_audit"]["checks"][5]["affects_model_rank"] = True
    failures = validate_public_decision_audit(
        bad,
        model_metadata=model_metadata,
    )
    assert any("precision_at_100 does not match" in failure for failure in failures)
    assert any("anonymous readiness exposed tax-lien" in failure for failure in failures)
    assert any("anonymous ownership evidence" in failure for failure in failures)
    assert any("diligence-only role is ambiguous" in failure for failure in failures)


def test_sweep_validator_rejects_wrong_borough_and_private_provenance() -> None:
    payload = {
        "borough": "queens",
        "generated_at": "2026-07-23T00:00:00Z",
        "quality_gate": {"passed": True},
        "rows": [
            _public_row(
                bbl="4000000001",
                borough="QN",
                rank=1,
                citywide_rank=1,
            )
        ],
    }
    assert (
        validate_sweep(
            payload,
            slug="queens",
            expected_generated_at="2026-07-23T00:00:00Z",
        )
        == []
    )

    bad = deepcopy(payload)
    bad["rows"][0]["owner_name_source"] = "acris"
    failures = validate_sweep(
        bad,
        slug="queens",
        expected_generated_at="2026-07-23T00:00:00Z",
    )
    assert any("owner_name_source was exposed" in failure for failure in failures)

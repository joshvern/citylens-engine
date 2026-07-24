from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from scripts.verify_production import (
    evaluate_source_slas,
    validate_index,
    validate_map,
    validate_sweep,
)


def _quality_row() -> dict:
    return {
        "passed": True,
        "failures": [],
        "row_count": 1000,
        "project_leakage_count": 0,
        "land_use_project_leakage_count": 0,
        "duplicate_bbl_count": 0,
        "invalid_owner_leakage_count": 0,
        "non_private_owner_leakage_count": 0,
        "negative_unused_floor_area_count": 0,
        "invalid_owner_portfolio_count": 0,
        "owner_coverage": 1.0,
        "geometry_coverage": 1.0,
        "floodplain_coverage": 1.0,
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
            "prospective_2026_validated": False,
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

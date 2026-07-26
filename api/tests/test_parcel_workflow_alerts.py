from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_workflow
from app.services.parcel_workflow_alerts import build_workflow_alerts


def _workflow_item(**overrides):
    item = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "watching": True,
        "archived_at": None,
        "snapshot": {
            "feed_generated_at": "2026-07-20T00:00:00Z",
            "owner_name": "OLD OWNER LLC",
            "last_sale_year": 2019,
            "zoning_district_1": "R6",
            "opportunity_category": "ground_up_candidate",
            "priority_tier": "highest",
            "citywide_rank": 50,
            "tax_lien_sale_year": 2022,
            "critical_violation_count": 0,
            "floodplain_1pct": False,
            "environmental_review_required": False,
            "environmental_designation_number": None,
            "environmental_designation_kind": None,
            "mandatory_inclusionary_housing": False,
            "nearest_transit_complex_id": "A",
            "nearest_transit_station_name": "Old Station",
            "nearest_transit_station_distance_m": 740,
            "transit_access_tier": "walkable",
            "transit_data_as_of": "2026-07-20",
            "recent_change": False,
            "owner_portfolio_lot_count": 2,
        },
    }
    item.update(overrides)
    return item


def _current_row(**overrides):
    row = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "owner_name": "NEW OWNER LLC",
        "last_sale_year": 2025,
        "zoning_district_1": "R7A",
        "opportunity_category": "vacant_site",
        "priority_tier": "medium",
        "citywide_rank": 375,
        "tax_lien_sale_year": 2023,
        "critical_violation_count": 2,
        "floodplain_1pct": True,
        "environmental_review_required": True,
        "environmental_designation_number": "R-14",
        "environmental_designation_kind": "restrictive_declaration",
        "mandatory_inclusionary_housing": True,
        "nearest_transit_complex_id": "B",
        "nearest_transit_station_name": "New Station",
        "nearest_transit_station_distance_m": 310,
        "transit_access_tier": "very_close",
        "transit_data_as_of": "2026-07-24",
        "recent_change": True,
        "owner_portfolio_lot_count": 3,
    }
    row.update(overrides)
    return row


def _screening_row(**overrides):
    row = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "model_rank": 72,
        "acquisition_rank": None,
        "acquisition_eligible": False,
        "acquisition_status": "active_project",
        "acquisition_exclusion_reasons": [
            "approved_land_use_project",
        ],
        "published": False,
        "latest_project_filing_year": 2023,
        "latest_project_status": "Completed — approved",
        "latest_project_type": "land_use_entitlement",
        "latest_project_job_number": "2023K0205",
        "latest_project_url": (
            "https://zap.planning.nyc.gov/projects/2023K0205"
        ),
        "property_facts_as_of": "2026-07-19",
        "ownership_as_of": "2026-07-15",
        "project_activity_as_of": "2026-07-19",
        "land_use_activity_as_of": "2026-07-24",
    }
    row.update(overrides)
    return row


def _evidence_review(**overrides):
    review = {
        "check_key": "property_facts",
        "label": "Current property facts",
        "check_status": "verified",
        "source": "NYC PLUTO",
        "source_as_of": "2026-07-20",
        "feed_generated_at": "2026-07-20T00:00:00Z",
        "reviewed_at": datetime(2026, 7, 21, tzinfo=timezone.utc),
    }
    review.update(overrides)
    return review


def test_alerts_surface_decision_relevant_changes() -> None:
    result = build_workflow_alerts(
        [_workflow_item()],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["schema_version"] == "citylens/parcel-workflow-alerts@v3"
    assert result["watched_count"] == 1
    assert result["changed_lead_count"] == 1
    codes = {alert["code"] for alert in result["alerts"]}
    assert codes == {
        "owner_changed",
        "newer_sale_record",
        "zoning_changed",
        "opportunity_changed",
        "priority_tier_changed",
        "material_rank_move",
        "tax_lien_history_changed",
        "critical_violations_changed",
        "flood_overlay_changed",
        "environmental_review_changed",
        "mih_overlay_changed",
        "transit_access_changed",
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    }
    assert result["severity_counts"]["high"] == 6
    assert result["alerts"][0]["severity"] == "high"


def test_reviewed_evidence_is_current_only_on_an_exact_version_match() -> None:
    item = _workflow_item(
        watching=False,
        evidence_reviews={"property_facts": _evidence_review()},
    )
    current_check = {
        "key": "property_facts",
        "label": "Current property facts",
        "status": "verified",
        "source": "NYC PLUTO",
        "as_of": "2026-07-20",
    }

    result = build_workflow_alerts(
        [item],
        [_current_row()],
        feed_generated_at="2026-07-20T00:00:00Z",
        current_evidence_checks={
            "3020960069": {"property_facts": current_check}
        },
    )

    assert result["reviewed_lead_count"] == 1
    assert result["stale_review_count"] == 0
    assert result["alert_count"] == 0


def test_reviewed_evidence_status_change_is_a_source_bound_alert() -> None:
    item = _workflow_item(
        watching=False,
        evidence_reviews={"property_facts": _evidence_review()},
    )

    result = build_workflow_alerts(
        [item],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
        current_evidence_checks={
            "3020960069": {
                "property_facts": {
                    "key": "property_facts",
                    "label": "Current property facts",
                    "status": "unavailable",
                    "source": "NYC PLUTO",
                    "as_of": "2026-07-24",
                }
            }
        },
    )

    assert result["changed_lead_count"] == 1
    assert result["stale_review_count"] == 1
    assert result["severity_counts"]["high"] == 1
    alert = result["alerts"][0]
    assert alert["code"] == "reviewed_evidence_changed"
    assert alert["field"] == "evidence_reviews"
    assert alert["parcel_available"] is True
    assert alert["evidence_changes"] == [{
        "check_key": "property_facts",
        "label": "Current property facts",
        "reviewed_at": datetime(2026, 7, 21, tzinfo=timezone.utc),
        "reviewed_status": "verified",
        "reviewed_source": "NYC PLUTO",
        "reviewed_source_as_of": "2026-07-20",
        "reviewed_feed_generated_at": "2026-07-20T00:00:00Z",
        "current_status": "unavailable",
        "current_source": "NYC PLUTO",
        "current_source_as_of": "2026-07-24",
        "current_feed_generated_at": "2026-07-24T00:00:00Z",
        "change_reasons": [
            "status",
            "source_as_of",
            "feed_generation",
        ],
    }]
    assert alert["source_evidence"][0]["source"] == "NYC PLUTO"
    assert "does not clear" in alert["recommended_action"]


def test_generation_only_review_change_is_low_severity() -> None:
    item = _workflow_item(
        watching=False,
        evidence_reviews={"property_facts": _evidence_review()},
    )

    result = build_workflow_alerts(
        [item],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
        current_evidence_checks={
            "3020960069": {
                "property_facts": {
                    "key": "property_facts",
                    "label": "Current property facts",
                    "status": "verified",
                    "source": "NYC PLUTO",
                    "as_of": "2026-07-20",
                }
            }
        },
    )

    assert result["stale_review_count"] == 1
    assert result["alerts"][0]["severity"] == "low"
    assert result["alerts"][0]["evidence_changes"][0]["change_reasons"] == [
        "feed_generation"
    ]


def test_multiple_stale_reviews_are_grouped_per_parcel() -> None:
    item = _workflow_item(
        watching=False,
        evidence_reviews={
            "property_facts": _evidence_review(),
            "ownership": _evidence_review(
                check_key="ownership",
                label="Ownership provenance",
                source="NYC ACRIS / NYC PLUTO",
            ),
        },
    )
    result = build_workflow_alerts(
        [item],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
        current_evidence_checks={
            "3020960069": {
                "property_facts": {
                    "key": "property_facts",
                    "label": "Current property facts",
                    "status": "verified",
                    "source": "NYC PLUTO",
                    "as_of": "2026-07-24",
                },
                "ownership": {
                    "key": "ownership",
                    "label": "Ownership provenance",
                    "status": "verified",
                    "source": "NYC ACRIS / NYC PLUTO",
                    "as_of": "2026-07-24",
                },
            }
        },
    )

    assert result["alert_count"] == 1
    assert result["stale_review_count"] == 2
    assert result["alerts"][0]["title"] == (
        "2 reviewed evidence versions need attention"
    )
    assert {
        change["check_key"]
        for change in result["alerts"][0]["evidence_changes"]
    } == {"property_facts", "ownership"}


def test_terminal_workflow_can_inspect_but_not_record_a_new_review() -> None:
    item = _workflow_item(
        stage="pass",
        evidence_reviews={"property_facts": _evidence_review()},
    )
    result = build_workflow_alerts(
        [item],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
        current_evidence_checks={
            "3020960069": {
                "property_facts": {
                    "key": "property_facts",
                    "label": "Current property facts",
                    "status": "verified",
                    "source": "NYC PLUTO",
                    "as_of": "2026-07-24",
                }
            }
        },
    )

    alert = next(
        item
        for item in result["alerts"]
        if item["code"] == "reviewed_evidence_changed"
    )
    assert alert["parcel_available"] is True
    assert alert["review_recordable"] is False
    assert "Reopen the terminal workflow" in alert["recommended_action"]


def test_reviewed_evidence_without_current_parcel_stays_unresolved() -> None:
    item = _workflow_item(
        watching=False,
        evidence_reviews={"property_facts": _evidence_review()},
    )

    result = build_workflow_alerts(
        [item],
        [],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["stale_review_count"] == 1
    alert = result["alerts"][0]
    assert alert["severity"] == "high"
    assert alert["parcel_available"] is False
    assert alert["after"] == {"property_facts": None}
    assert alert["evidence_changes"][0]["change_reasons"] == [
        "current_evidence_unavailable"
    ]
    assert "cannot be matched" in alert["detail"]


def test_removed_lead_is_urgent_but_does_not_invent_a_reason() -> None:
    result = build_workflow_alerts(
        [_workflow_item()],
        [],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["removed_from_feed_count"] == 1
    assert result["severity_counts"]["urgent"] == 1
    alert = result["alerts"][0]
    assert alert["code"] == "removed_from_current_feed"
    assert "does not assert why" in alert["detail"]
    assert "completed" not in alert["detail"].casefold()
    assert alert["current_disposition"] == "not_evaluated"
    assert alert["parcel_available"] is False
    assert result["resolved_exit_count"] == 0
    assert result["unresolved_exit_count"] == 1


def test_screening_ledger_explains_zap_exit_with_official_evidence() -> None:
    result = build_workflow_alerts(
        [_workflow_item()],
        [],
        screening_rows={"3020960069": _screening_row()},
        data_sources={
            "land_use_activity": {
                "source": "NYC ZAP project activity",
                "retrieved_at": "2026-07-24",
            }
        },
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["resolved_exit_count"] == 1
    assert result["unresolved_exit_count"] == 0
    assert result["screened_out_count"] == 1
    alert = result["alerts"][0]
    assert alert["code"] == "screened_out_of_current_feed"
    assert alert["current_disposition"] == "screened_out"
    assert alert["reason_codes"] == ["approved_land_use_project"]
    assert "2023K0205" in alert["detail"]
    assert alert["recommended_action"]
    assert alert["parcel_available"] is False
    assert alert["source_evidence"] == [
        {
            "source": "NYC ZAP project activity",
            "as_of": "2026-07-24",
            "url": "https://zap.planning.nyc.gov/projects/2023K0205",
            "supports": "approved_land_use_project",
        }
    ]
    serialized = str(alert)
    assert "OLD OWNER LLC" not in serialized
    assert "score_calibrated" not in serialized


def test_screening_ledger_distinguishes_eligible_below_cutoff() -> None:
    screening = _screening_row(
        acquisition_rank=1264,
        acquisition_eligible=True,
        acquisition_status="eligible",
        acquisition_exclusion_reasons=[],
        latest_project_filing_year=None,
        latest_project_status=None,
        latest_project_type=None,
        latest_project_job_number=None,
        latest_project_url=None,
    )
    result = build_workflow_alerts(
        [_workflow_item()],
        [],
        screening_rows={"3020960069": screening},
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["eligible_below_cutoff_count"] == 1
    assert result["screened_out_count"] == 0
    alert = result["alerts"][0]
    assert alert["code"] == "eligible_below_published_cutoff"
    assert alert["severity"] == "medium"
    assert alert["current_disposition"] == "eligible_below_cutoff"
    assert "1,264" in alert["detail"]
    assert "disqualification" in alert["recommended_action"]


def test_alerts_ignore_unwatched_archived_and_unknown_baseline_values() -> None:
    no_baseline = _workflow_item(
        bbl="4012340056",
        borough="queens",
        snapshot={"feed_generated_at": "2026-07-20T00:00:00Z"},
    )
    result = build_workflow_alerts(
        [
            no_baseline,
            _workflow_item(bbl="3000000001", watching=False),
            _workflow_item(
                bbl="3000000002",
                archived_at=datetime.now(timezone.utc),
            ),
        ],
        [
            _current_row(
                bbl="4012340056",
                borough="queens",
                owner_name="CURRENT OWNER LLC",
                tax_lien_sale_year=2025,
                critical_violation_count=10,
            )
        ],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["watched_count"] == 1
    assert result["alert_count"] == 0
    assert result["warnings"] == []


def test_transit_alert_ignores_distance_noise_when_complex_and_tier_match() -> None:
    item = _workflow_item(
        snapshot={
            "feed_generated_at": "2026-07-20T00:00:00Z",
            "nearest_transit_complex_id": "A",
            "nearest_transit_station_name": "Same Station",
            "nearest_transit_station_distance_m": 510,
            "transit_access_tier": "walkable",
        }
    )
    current = _current_row(
        owner_name=None,
        last_sale_year=None,
        zoning_district_1=None,
        opportunity_category=None,
        priority_tier=None,
        citywide_rank=None,
        tax_lien_sale_year=None,
        critical_violation_count=None,
        floodplain_1pct=None,
        environmental_review_required=None,
        mandatory_inclusionary_housing=None,
        recent_change=None,
        owner_portfolio_lot_count=None,
        nearest_transit_complex_id="A",
        nearest_transit_station_name="Same Station",
        nearest_transit_station_distance_m=525,
        transit_access_tier="walkable",
    )

    result = build_workflow_alerts(
        [item],
        [current],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["alerts"] == []


class _FakeStore:
    def list_parcel_workflow(
        self, *, app_user_id: str, include_archived: bool = False
    ) -> list[dict]:
        assert app_user_id == "alerts-user"
        return [_workflow_item()]


class _FakeRow:
    def model_dump(self) -> dict:
        return _current_row(owner_name="OLD OWNER LLC")


class _FakeRegistry:
    def citywide_map(self, _gcs):
        return (
            [_FakeRow()],
            {
                "generated_at": "2026-07-24T00:00:00+00:00",
                "data_sources": {},
            },
        )

    def screening_ledger(self, _gcs, *, manifest):
        assert manifest["generated_at"] == "2026-07-24T00:00:00+00:00"
        return {}, manifest


class _FakeScreeningRow:
    def model_dump(self) -> dict:
        return _screening_row()


class _FakeExitRegistry:
    def citywide_map(self, _gcs):
        return (
            [],
            {
                "generated_at": "2026-07-24T00:00:00+00:00",
                "data_sources": {
                    "land_use_activity": {
                        "source": "NYC ZAP project activity",
                        "retrieved_at": "2026-07-24",
                    }
                },
            },
        )

    def screening_ledger(self, _gcs, *, manifest):
        return {"3020960069": _FakeScreeningRow()}, manifest


class _FakeReviewedStore:
    def list_parcel_workflow(
        self, *, app_user_id: str, include_archived: bool = False
    ) -> list[dict]:
        assert app_user_id == "alerts-user"
        return [
            _workflow_item(
                watching=False,
                evidence_reviews={"property_facts": _evidence_review()},
            )
        ]


class _FakeReviewedRow(_FakeRow):
    bbl = "3020960069"


class _FakeReviewedRegistry(_FakeRegistry):
    def citywide_map(self, _gcs):
        rows, manifest = super().citywide_map(_gcs)
        return [_FakeReviewedRow()], manifest


def test_workflow_alerts_endpoint_is_authenticated_and_typed(
    auth_override,
) -> None:
    auth_override(app_user_id="alerts-user")
    app.dependency_overrides[parcel_workflow.get_store] = lambda: _FakeStore()
    app.dependency_overrides[parcel_workflow.get_gcs] = lambda: object()
    app.dependency_overrides[parcel_workflow.get_registry] = (
        lambda: _FakeRegistry()
    )
    client = TestClient(app)

    response = client.get("/v1/parcel-intel/workflow/alerts")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["schema_version"] == "citylens/parcel-workflow-alerts@v3"
    assert payload["feed_generated_at"] == "2026-07-24T00:00:00Z"
    assert payload["watched_count"] == 1
    assert payload["changed_lead_count"] == 1
    assert {alert["code"] for alert in payload["alerts"]} == {
        "newer_sale_record",
        "zoning_changed",
        "opportunity_changed",
        "priority_tier_changed",
        "material_rank_move",
        "tax_lien_history_changed",
        "critical_violations_changed",
        "flood_overlay_changed",
        "environmental_review_changed",
        "mih_overlay_changed",
        "transit_access_changed",
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    }


def test_workflow_alerts_endpoint_returns_typed_source_backed_exit(
    auth_override,
) -> None:
    auth_override(app_user_id="alerts-user")
    app.dependency_overrides[parcel_workflow.get_store] = lambda: _FakeStore()
    app.dependency_overrides[parcel_workflow.get_gcs] = lambda: object()
    app.dependency_overrides[parcel_workflow.get_registry] = (
        lambda: _FakeExitRegistry()
    )
    client = TestClient(app)

    response = client.get("/v1/parcel-intel/workflow/alerts")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["schema_version"] == "citylens/parcel-workflow-alerts@v3"
    assert payload["resolved_exit_count"] == 1
    assert payload["unresolved_exit_count"] == 0
    assert payload["screened_out_count"] == 1
    alert = payload["alerts"][0]
    assert alert["code"] == "screened_out_of_current_feed"
    assert alert["parcel_available"] is False
    assert alert["source_evidence"][0]["url"].endswith("/2023K0205")


def test_workflow_alerts_endpoint_builds_current_review_versions(
    auth_override,
    monkeypatch,
) -> None:
    auth_override(app_user_id="alerts-user")
    app.dependency_overrides[parcel_workflow.get_store] = (
        lambda: _FakeReviewedStore()
    )
    app.dependency_overrides[parcel_workflow.get_gcs] = lambda: object()
    app.dependency_overrides[parcel_workflow.get_registry] = (
        lambda: _FakeReviewedRegistry()
    )
    current_check = SimpleNamespace(
        key="property_facts",
        model_dump=lambda: {
            "key": "property_facts",
            "label": "Current property facts",
            "status": "verified",
            "source": "NYC PLUTO",
            "as_of": "2026-07-24",
        },
    )
    monkeypatch.setattr(
        parcel_workflow,
        "build_parcel_decision_audit",
        lambda row, manifest, premium_access: SimpleNamespace(
            checks=[current_check]
        ),
    )
    client = TestClient(app)

    response = client.get("/v1/parcel-intel/workflow/alerts")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["schema_version"] == "citylens/parcel-workflow-alerts@v3"
    assert payload["watched_count"] == 0
    assert payload["reviewed_lead_count"] == 1
    assert payload["stale_review_count"] == 1
    assert payload["alerts"][0]["code"] == "reviewed_evidence_changed"
    assert payload["alerts"][0]["evidence_changes"][0]["change_reasons"] == [
        "source_as_of",
        "feed_generation",
    ]

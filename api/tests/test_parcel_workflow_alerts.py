from __future__ import annotations

from datetime import datetime, timezone

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
            "e_designation_number": None,
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
        "e_designation_number": "E-442",
        "recent_change": True,
        "owner_portfolio_lot_count": 3,
    }
    row.update(overrides)
    return row


def test_alerts_surface_decision_relevant_changes() -> None:
    result = build_workflow_alerts(
        [_workflow_item()],
        [_current_row()],
        feed_generated_at="2026-07-24T00:00:00Z",
    )

    assert result["schema_version"] == "citylens/parcel-workflow-alerts@v1"
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
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    }
    assert result["severity_counts"]["high"] == 5
    assert result["alerts"][0]["severity"] == "high"


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
            {"generated_at": "2026-07-24T00:00:00+00:00"},
        )


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
    assert payload["schema_version"] == "citylens/parcel-workflow-alerts@v1"
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
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    }

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_workflow


class FakeWorkflowStore:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}
        self.events: dict[str, list[dict]] = {}
        self.searches: dict[str, dict] = {}

    def list_parcel_workflow(
        self, *, app_user_id: str, include_archived: bool = False
    ) -> list[dict]:
        rows = list(self.items.values())
        return rows if include_archived else [
            row for row in rows if row.get("archived_at") is None
        ]

    def upsert_parcel_workflow(self, *, app_user_id: str, bbl: str, payload: dict) -> dict:
        now = datetime.now(timezone.utc)
        doc = {
            **self.items.get(bbl, {}),
            **payload,
            "bbl": bbl,
            "saved_at": self.items.get(bbl, {}).get("saved_at", now),
            "updated_at": now,
        }
        self.items[bbl] = doc
        events = self.events.setdefault(bbl, [])
        events.insert(
            0,
            {
                "event_id": f"event-{len(events) + 1}",
                "schema_version": "citylens/parcel-workflow-event@v1",
                "bbl": bbl,
                "event_type": "created" if len(events) == 0 else "updated",
                "occurred_at": now,
                "from_stage": None,
                "to_stage": payload.get("stage"),
                "from_outcome": None,
                "to_outcome": payload.get("outcome"),
                "from_decision_reason": None,
                "to_decision_reason": payload.get("decision_reason"),
                "changed_fields": sorted(payload),
            },
        )
        doc["event_count"] = len(events)
        return doc

    def delete_parcel_workflow(self, *, app_user_id: str, bbl: str) -> bool:
        if bbl not in self.items or self.items[bbl].get("archived_at") is not None:
            return False
        self.items[bbl]["archived_at"] = datetime.now(timezone.utc)
        return True

    def list_parcel_workflow_events(
        self, *, app_user_id: str, bbl: str
    ) -> list[dict]:
        return self.events.get(bbl, [])

    def list_parcel_saved_searches(self, *, app_user_id: str) -> list[dict]:
        return list(self.searches.values())

    def upsert_parcel_saved_search(
        self, *, app_user_id: str, search_id: str, payload: dict
    ) -> dict:
        now = datetime.now(timezone.utc)
        doc = {
            **self.searches.get(search_id, {}),
            **payload,
            "search_id": search_id,
            "created_at": self.searches.get(search_id, {}).get("created_at", now),
            "updated_at": now,
        }
        self.searches[search_id] = doc
        return doc

    def delete_parcel_saved_search(self, *, app_user_id: str, search_id: str) -> bool:
        return self.searches.pop(search_id, None) is not None


def test_workflow_crud(auth_override) -> None:
    auth_override(app_user_id="workflow-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    created = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "stage": "reviewing",
            "notes": "Call owner",
            "tags": ["assemblage", "assemblage", " corner "],
            "watching": True,
            "decision_reason": "pursuing",
            "outcome": "owner_contacted",
            "snapshot": {"property_facts_as_of": "2026-07-01"},
        },
    )
    assert created.status_code == 200, created.text
    assert created.json()["stage"] == "reviewing"
    assert created.json()["tags"] == ["assemblage", "corner"]
    assert created.json()["decision_reason"] == "pursuing"
    assert created.json()["outcome"] == "owner_contacted"

    listed = client.get("/v1/parcel-intel/workflow")
    assert listed.status_code == 200
    assert [item["bbl"] for item in listed.json()] == ["3020960069"]

    removed = client.delete("/v1/parcel-intel/workflow/3020960069")
    assert removed.status_code == 204
    assert client.get("/v1/parcel-intel/workflow").json() == []


def test_workflow_events_and_prospective_analytics(auth_override) -> None:
    auth_override(app_user_id="workflow-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    body = {
        "borough": "brooklyn",
        "stage": "underwriting",
        "outcome": "qualified",
        "snapshot": {
            "feed_generated_at": "2026-07-23T23:38:01Z",
            "citywide_rank": 82,
            "acquisition_rank": 21,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
        },
    }
    assert client.put("/v1/parcel-intel/workflow/3020960069", json=body).status_code == 200

    events = client.get("/v1/parcel-intel/workflow/3020960069/events")
    assert events.status_code == 200
    assert events.json()[0]["event_type"] == "created"
    assert "notes" not in events.json()[0]

    analytics = client.get("/v1/parcel-intel/workflow/analytics")
    assert analytics.status_code == 200, analytics.text
    payload = analytics.json()
    assert payload["schema_version"] == "citylens/parcel-workflow-analytics@v1"
    assert payload["measurement_status"] == "collecting"
    assert payload["total_records"] == 1
    assert payload["funnel"]["contacted"] == 1
    assert payload["funnel"]["qualified"] == 1
    assert payload["funnel"]["contacted_per_saved"]["denominator"] == 1
    assert payload["rank_snapshot_records"] == 1
    assert any(
        cohort["dimension"] == "rank_band"
        and cohort["value"] == "1-100"
        and cohort["total"] == 1
        for cohort in payload["cohorts"]
    )


def test_saved_search_crud(auth_override) -> None:
    auth_override(app_user_id="search-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    created = client.put(
        "/v1/parcel-intel/saved-searches/brooklyn-vacant",
        json={
            "name": "Brooklyn vacant sites",
            "borough": "brooklyn",
            "filters": {"landUseFilter": "vacant"},
            "alert_frequency": "weekly",
        },
    )
    assert created.status_code == 200, created.text
    assert created.json()["alert_frequency"] == "weekly"
    searches = client.get("/v1/parcel-intel/saved-searches").json()
    assert searches[0]["name"] == "Brooklyn vacant sites"
    removed = client.delete("/v1/parcel-intel/saved-searches/brooklyn-vacant")
    assert removed.status_code == 204


def test_workflow_rejects_bad_bbl(auth_override) -> None:
    auth_override()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: FakeWorkflowStore()
    client = TestClient(app)
    response = client.put(
        "/v1/parcel-intel/workflow/not-a-bbl",
        json={"borough": "brooklyn"},
    )
    assert response.status_code == 422

    wrong_borough = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={"borough": "queens"},
    )
    assert wrong_borough.status_code == 422

    bad_prefix = client.put(
        "/v1/parcel-intel/workflow/0020960069",
        json={"borough": "brooklyn"},
    )
    assert bad_prefix.status_code == 422


def test_workflow_snapshot_and_saved_filters_are_typed(auth_override) -> None:
    auth_override(app_user_id="typed-workflow-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    workflow = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "snapshot": {
                "zoning_district_1": "R7A",
                "allowed_far": 4.0,
                "unbounded_payload": "dropped",
            },
        },
    )
    assert workflow.status_code == 200, workflow.text
    assert workflow.json()["snapshot"] == {
        "feed_generated_at": None,
        "property_facts_as_of": None,
        "citywide_rank": None,
        "acquisition_rank": None,
        "priority_tier": None,
        "opportunity_category": None,
        "score_calibrated": None,
        "zoning_district_1": "R7A",
        "land_use": None,
        "year_built": None,
        "allowed_far": 4.0,
        "unused_floor_area_sqft": None,
        "owner_name": None,
        "owner_entity_type": None,
        "owner_portfolio_lot_count": None,
        "last_sale_year": None,
        "latest_nb_filing_year": None,
        "latest_nb_status": None,
        "redev_status": None,
        "observed_imagery_year": None,
        "tax_lien_sale_year": None,
        "critical_violation_count": None,
        "floodplain_1pct": None,
        "recent_change": None,
    }

    bad_search = client.put(
        "/v1/parcel-intel/saved-searches/bad-filter",
        json={
            "name": "Invalid",
            "borough": "brooklyn",
            "filters": {"landUseFilter": "not-a-real-filter"},
        },
    )
    assert bad_search.status_code == 422

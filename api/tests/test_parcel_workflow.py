from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import parcel_workflow
from app.services.firestore_store import _workflow_effective_payload
from app.services.parcel_workflow_actions import workflow_reminder_fingerprint


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

    def get_parcel_workflow(
        self, *, app_user_id: str, bbl: str
    ) -> dict | None:
        return self.items.get(bbl)

    def upsert_parcel_workflow(self, *, app_user_id: str, bbl: str, payload: dict) -> dict:
        now = datetime.now(timezone.utc)
        existing = self.items.get(bbl, {})
        effective_payload = dict(payload)
        if isinstance(existing.get("snapshot"), dict):
            effective_payload["snapshot"] = existing["snapshot"]
        doc = {
            **existing,
            **effective_payload,
            "bbl": bbl,
            "saved_at": existing.get("saved_at", now),
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

    def set_parcel_workflow_reminder_snooze(
        self, *, app_user_id: str, bbl: str, days: int
    ) -> dict | None:
        item = self.items.get(bbl)
        if (
            item is None
            or item.get("archived_at") is not None
            or item.get("stage") == "pass"
            or item.get("outcome") in {"closed", "rejected", "lost"}
        ):
            return None
        until = (
            datetime.now(timezone.utc) + timedelta(days=days)
            if days > 0
            else None
        )
        item["reminder_snoozed_until"] = until
        item["reminder_fingerprint"] = (
            workflow_reminder_fingerprint(item) if days > 0 else None
        )
        return item

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


class _FakeParcel:
    def model_dump(self) -> dict:
        return {
            "address": "100 E 21 STREET",
            "property_facts_as_of": "2026-07-24",
            "citywide_rank": 82,
            "acquisition_rank": 21,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
            "score_calibrated": 0.42,
            "zoning_district_1": "R5",
            "land_use": "01",
            "year_built": 1930,
            "allowed_far": 2.0,
            "unused_floor_area_sqft": 5_000,
            "owner_name": "CANONICAL OWNER LLC",
            "owner_entity_type": "llc",
            "owner_portfolio_lot_count": 2,
            "last_sale_year": 2025,
            "latest_nb_filing_year": None,
            "latest_nb_status": None,
            "redev_status": "still_vacant",
            "observed_imagery_year": 2024,
            "tax_lien_sale_year": None,
            "critical_violation_count": 0,
            "floodplain_1pct": False,
            "environmental_review_required": False,
            "environmental_designation_number": None,
            "environmental_designation_kind": None,
            "nearest_transit_complex_id": "628",
            "nearest_transit_station_name": "Church Av",
            "nearest_transit_station_distance_m": 420,
            "transit_access_tier": "walkable",
            "transit_data_as_of": "2026-07-24",
            "recent_change": False,
        }


class _FakeWorkflowRegistry:
    def parcel(self, _gcs: object, _bbl: str) -> tuple[_FakeParcel, dict]:
        return _FakeParcel(), {"generated_at": "2026-07-24T02:43:29Z"}


@pytest.fixture(autouse=True)
def _workflow_feed_override():
    app.dependency_overrides[parcel_workflow.get_gcs] = lambda: object()
    app.dependency_overrides[parcel_workflow.get_registry] = (
        lambda: _FakeWorkflowRegistry()
    )
    yield
    app.dependency_overrides.pop(parcel_workflow.get_gcs, None)
    app.dependency_overrides.pop(parcel_workflow.get_registry, None)


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


def test_store_payload_preserves_existing_exposure_snapshot() -> None:
    original = {"citywide_rank": 82, "score_calibrated": 0.42}
    effective = _workflow_effective_payload(
        existing={"snapshot": original, "stage": "new"},
        incoming={
            "snapshot": {"citywide_rank": 999_999, "score_calibrated": 1.0},
            "stage": "reviewing",
        },
        record_exists=True,
    )
    assert effective == {"snapshot": original, "stage": "reviewing"}
    assert effective["snapshot"] is original


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
    assert payload["schema_version"] == "citylens/parcel-workflow-analytics@v2"
    assert payload["measurement_status"] == "collecting"
    assert payload["total_records"] == 1
    assert payload["valid_saved_at_records"] == 1
    assert payload["maturity_windows"][0]["eligible_records"] == 0
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


def test_workflow_action_queue_and_input_invariants(auth_override) -> None:
    auth_override(app_user_id="workflow-actions-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    due_date = (date.today() - timedelta(days=1)).isoformat()

    missing_action = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "stage": "reviewing",
            "next_action_due_date": due_date,
        },
    )
    assert missing_action.status_code == 422
    assert "next_action is required" in missing_action.text

    created = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "stage": "reviewing",
            "next_action": "Call owner",
            "next_action_due_date": due_date,
        },
    )
    assert created.status_code == 200, created.text
    assert created.json()["next_action"] == "Call owner"
    assert created.json()["next_action_due_date"] == due_date
    assert created.json()["snapshot"]["address"] == "100 E 21 STREET"

    actions = client.get("/v1/parcel-intel/workflow/actions")
    assert actions.status_code == 200, actions.text
    payload = actions.json()
    assert payload["schema_version"] == "citylens/parcel-workflow-actions@v1"
    assert payload["open_records"] == 1
    assert payload["overdue_count"] == 1
    assert payload["items"][0]["bbl"] == "3020960069"
    assert payload["items"][0]["action_state"] == "overdue"

    snoozed = client.post(
        "/v1/parcel-intel/workflow/3020960069/reminder",
        json={"days": 1},
    )
    assert snoozed.status_code == 200, snoozed.text
    assert snoozed.json()["is_snoozed"] is True
    snoozed_actions = client.get("/v1/parcel-intel/workflow/actions").json()
    assert snoozed_actions["attention_count"] == 0
    assert snoozed_actions["snoozed_count"] == 1
    assert snoozed_actions["items"][0]["is_snoozed"] is True

    resumed = client.post(
        "/v1/parcel-intel/workflow/3020960069/reminder",
        json={"days": 0},
    )
    assert resumed.status_code == 200
    assert resumed.json()["is_snoozed"] is False

    closed = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "stage": "pursue",
            "outcome": "closed",
            "next_action": "Stale reminder",
            "next_action_due_date": due_date,
        },
    )
    assert closed.status_code == 200, closed.text
    assert closed.json()["next_action"] is None
    assert closed.json()["next_action_due_date"] is None
    assert client.get("/v1/parcel-intel/workflow/actions").json()["open_records"] == 0
    terminal_snooze = client.post(
        "/v1/parcel-intel/workflow/3020960069/reminder",
        json={"days": 1},
    )
    assert terminal_snooze.status_code == 409


def test_workflow_analytics_methodology_is_public_and_data_free() -> None:
    client = TestClient(app)
    response = client.get("/v1/parcel-intel/workflow/analytics/methodology")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["schema_version"] == (
        "citylens/parcel-workflow-analytics-methodology@v1"
    )
    assert payload["analytics_schema_version"] == (
        "citylens/parcel-workflow-analytics@v2"
    )
    assert payload["model_accuracy_claim"] is False
    assert [
        (window["milestone"], window["horizon_days"])
        for window in payload["horizons"]
    ] == [
        ("owner_contacted", 30),
        ("qualified", 90),
        ("offer_submitted", 180),
        ("under_contract", 270),
        ("closed", 365),
    ]


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


def test_workflow_snapshot_is_server_owned_immutable_and_typed(
    auth_override,
) -> None:
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
    snapshot = workflow.json()["snapshot"]
    assert snapshot["feed_generated_at"] == "2026-07-24T02:43:29Z"
    assert snapshot["citywide_rank"] == 82
    assert snapshot["zoning_district_1"] == "R5"
    assert snapshot["allowed_far"] == 2.0
    assert snapshot["nearest_transit_complex_id"] == "628"
    assert snapshot["nearest_transit_station_distance_m"] == 420
    assert snapshot["transit_access_tier"] == "walkable"
    assert "unbounded_payload" not in snapshot

    updated = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={
            "borough": "brooklyn",
            "stage": "contacted",
            "snapshot": {
                "citywide_rank": 999_999,
                "zoning_district_1": "M1-5",
            },
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["snapshot"] == snapshot

    bad_search = client.put(
        "/v1/parcel-intel/saved-searches/bad-filter",
        json={
            "name": "Invalid",
            "borough": "brooklyn",
            "filters": {"landUseFilter": "not-a-real-filter"},
        },
    )
    assert bad_search.status_code == 422

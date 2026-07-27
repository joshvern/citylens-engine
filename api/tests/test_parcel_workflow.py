from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import ParcelIntelRow
from app.routes import parcel_workflow
from app.services.firestore_store import (
    PRODUCT_EVENT_DAILY_LIMIT,
    StaleSavedSearchSnapshot,
    _product_usage_day_payload,
    _workflow_effective_payload,
)
from app.services.parcel_workflow_actions import workflow_reminder_fingerprint


class FakeWorkflowStore:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}
        self.events: dict[str, list[dict]] = {}
        self.product_events: list[dict] = []
        self.searches: dict[str, dict] = {}
        self.evidence_issues: dict[str, dict] = {}

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

    def advance_parcel_workflow(
        self, *, app_user_id: str, bbl: str, payload: dict
    ) -> tuple[dict, str]:
        existing = self.items.get(bbl)
        if existing is not None and existing.get("archived_at") is None:
            return existing, "existing"
        mutation_status = "restored" if existing is not None else "created"
        doc = self.upsert_parcel_workflow(
            app_user_id=app_user_id,
            bbl=bbl,
            payload=payload,
        )
        doc["archived_at"] = None
        return doc, mutation_status

    def set_parcel_workflow_evidence_review(
        self,
        *,
        app_user_id: str,
        bbl: str,
        check_key: str,
        review: dict | None,
    ) -> tuple[dict | None, str]:
        del app_user_id
        item = self.items.get(bbl)
        if item is None:
            return None, "missing"
        if (
            item.get("archived_at") is not None
            or item.get("stage") == "pass"
            or item.get("outcome") in {"closed", "rejected", "lost"}
        ):
            return item, "inactive"
        reviews = dict(item.get("evidence_reviews") or {})
        current = reviews.get(check_key)
        if review is None:
            if current is None:
                return item, "unchanged"
            reviews.pop(check_key)
            mutation_status = "removed"
        else:
            current_identity = (
                {key: value for key, value in current.items() if key != "reviewed_at"}
                if isinstance(current, dict)
                else None
            )
            if current_identity == review:
                return item, "unchanged"
            reviews[check_key] = {
                **review,
                "reviewed_at": datetime.now(timezone.utc),
            }
            mutation_status = "reviewed"
        item["evidence_reviews"] = reviews
        item["updated_at"] = datetime.now(timezone.utc)
        events = self.events.setdefault(bbl, [])
        events.insert(
            0,
            {
                "event_id": f"event-{len(events) + 1}",
                "schema_version": "citylens/parcel-workflow-event@v1",
                "bbl": bbl,
                "event_type": "updated",
                "occurred_at": datetime.now(timezone.utc),
                "from_stage": item.get("stage"),
                "to_stage": item.get("stage"),
                "from_outcome": item.get("outcome"),
                "to_outcome": item.get("outcome"),
                "from_decision_reason": item.get("decision_reason"),
                "to_decision_reason": item.get("decision_reason"),
                "changed_fields": [f"evidence_reviews.{check_key}"],
            },
        )
        item["event_count"] = len(events)
        return item, mutation_status

    def submit_parcel_workflow_evidence_issue(
        self,
        *,
        app_user_id: str,
        bbl: str,
        issue: dict,
    ) -> tuple[dict | None, dict | None, str]:
        item = self.items.get(bbl)
        if item is None:
            return None, None, "missing"
        if item.get("archived_at") is not None:
            return item, None, "inactive"
        issues = dict(item.get("evidence_issues") or {})
        current = issues.get(issue["check_key"])
        identity_keys = (
            "check_key",
            "label",
            "issue_type",
            "reason_code",
            "note",
            "check_status",
            "source",
            "source_as_of",
            "feed_generated_at",
        )
        if isinstance(current, dict) and current.get("status") == "submitted":
            if all(current.get(key) == issue.get(key) for key in identity_keys):
                return item, current, "unchanged"
            return item, current, "conflict"
        now = datetime.now(timezone.utc)
        issue_id = f"pei_{len(self.evidence_issues) + 1:032x}"
        public_issue = {
            **issue,
            "issue_id": issue_id,
            "status": "submitted",
            "submitted_at": now,
            "updated_at": now,
            "resolved_at": None,
            "resolution_note": None,
        }
        issues[issue["check_key"]] = public_issue
        item["evidence_issues"] = issues
        item["updated_at"] = now
        self.evidence_issues[issue_id] = {
            **public_issue,
            "bbl": bbl,
            "borough": item["borough"],
            "submitted_by_user_id": app_user_id,
            "resolved_by_user_id": None,
            "expires_at": now + timedelta(days=730),
        }
        return item, public_issue, "submitted"

    def withdraw_parcel_workflow_evidence_issue(
        self,
        *,
        app_user_id: str,
        bbl: str,
        check_key: str,
    ) -> tuple[dict | None, str]:
        del app_user_id
        item = self.items.get(bbl)
        if item is None:
            return None, "missing"
        if item.get("archived_at") is not None:
            return item, "inactive"
        issues = dict(item.get("evidence_issues") or {})
        current = issues.get(check_key)
        if not isinstance(current, dict):
            return item, "missing_issue"
        if current.get("status") != "submitted":
            return item, "unchanged"
        now = datetime.now(timezone.utc)
        withdrawn = {**current, "status": "withdrawn", "updated_at": now}
        issues[check_key] = withdrawn
        item["evidence_issues"] = issues
        item["updated_at"] = now
        self.evidence_issues[current["issue_id"]].update(
            {"status": "withdrawn", "updated_at": now}
        )
        return item, "withdrawn"

    def list_parcel_evidence_issues(
        self, *, status: str | None = None, limit: int = 100
    ) -> list[dict]:
        items = list(self.evidence_issues.values())
        if status is not None:
            items = [item for item in items if item["status"] == status]
        return items[:limit]

    def resolve_parcel_evidence_issue(
        self,
        *,
        issue_id: str,
        status: str,
        resolution_note: str,
        admin_user_id: str,
    ) -> dict | None:
        issue = self.evidence_issues.get(issue_id)
        if issue is None:
            return None
        if issue["status"] != "submitted":
            return issue
        now = datetime.now(timezone.utc)
        issue.update(
            {
                "status": status,
                "resolution_note": resolution_note,
                "resolved_at": now,
                "resolved_by_user_id": admin_user_id,
                "updated_at": now,
            }
        )
        current = (
            self.items[issue["bbl"]]
            .get("evidence_issues", {})
            .get(issue["check_key"])
        )
        if isinstance(current, dict) and current["issue_id"] == issue_id:
            current.update(
                {
                    "status": status,
                    "resolution_note": resolution_note,
                    "resolved_at": now,
                    "updated_at": now,
                }
            )
        return issue

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

    def record_parcel_product_event(
        self,
        *,
        app_user_id: str,
        event: str,
        source: str,
    ) -> bool:
        self.product_events.append(
            {
                "app_user_id": app_user_id,
                "event": event,
                "source": source,
            }
        )
        return True

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


class _FakeWorkflowRegistry:
    def parcel(self, _gcs: object, _bbl: str) -> tuple[ParcelIntelRow, dict]:
        return ParcelIntelRow.model_validate(
            {
            "bbl": "3020960069",
            "address": "100 E 21 STREET",
            "property_facts_as_of": "2026-07-24",
            "property_facts_current": True,
            "citywide_rank": 82,
            "acquisition_rank": 21,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
            "acquisition_eligible": True,
            "acquisition_status": "eligible",
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
        ), {"generated_at": "2026-07-24T02:43:29Z"}


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

    fetched = client.get("/v1/parcel-intel/workflow/3020960069")
    assert fetched.status_code == 200
    assert fetched.headers["cache-control"] == "private, no-store"
    assert fetched.json()["bbl"] == "3020960069"
    assert client.get("/v1/parcel-intel/workflow/4020960069").json() is None

    listed = client.get("/v1/parcel-intel/workflow")
    assert listed.status_code == 200
    assert [item["bbl"] for item in listed.json()] == ["3020960069"]

    removed = client.delete("/v1/parcel-intel/workflow/3020960069")
    assert removed.status_code == 204
    assert client.get("/v1/parcel-intel/workflow").json() == []
    assert client.get("/v1/parcel-intel/workflow/3020960069").json() is None


def test_comparison_handoff_creates_restores_and_preserves_active_work(
    auth_override,
) -> None:
    auth_override(app_user_id="comparison-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    created = client.post(
        "/v1/parcel-intel/workflow/3020960069/advance",
        json={
            "borough": "brooklyn",
            "next_action": "  Verify current title and ownership records.  ",
            "next_action_due_date": "2026-08-01",
        },
    )
    assert created.status_code == 200, created.text
    assert created.headers["cache-control"] == "private, no-store"
    assert created.json()["status"] == "created"
    assert created.json()["item"]["stage"] == "reviewing"
    assert created.json()["item"]["decision_reason"] == "pursuing"
    assert created.json()["item"]["next_action"] == (
        "Verify current title and ownership records."
    )
    assert created.json()["item"]["watching"] is True

    existing = client.post(
        "/v1/parcel-intel/workflow/3020960069/advance",
        json={
            "borough": "brooklyn",
            "next_action": "This must not replace active work.",
        },
    )
    assert existing.status_code == 200, existing.text
    assert existing.json()["status"] == "existing"
    assert existing.json()["item"]["next_action"] == (
        "Verify current title and ownership records."
    )

    store.items["3020960069"]["archived_at"] = datetime.now(timezone.utc)
    restored = client.post(
        "/v1/parcel-intel/workflow/3020960069/advance",
        json={
            "borough": "brooklyn",
            "next_action": "Recheck the parcel after comparison.",
        },
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["status"] == "restored"
    assert restored.json()["item"]["next_action"] == (
        "Recheck the parcel after comparison."
    )


def test_comparison_handoff_rejects_mismatched_or_unbounded_input(
    auth_override,
) -> None:
    auth_override(app_user_id="comparison-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    mismatch = client.post(
        "/v1/parcel-intel/workflow/3020960069/advance",
        json={"borough": "queens", "next_action": "Verify title."},
    )
    assert mismatch.status_code == 422

    identifying_extra = client.post(
        "/v1/parcel-intel/workflow/3020960069/advance",
        json={
            "borough": "brooklyn",
            "next_action": "Verify title.",
            "notes": "Do not accept arbitrary private workflow text here.",
        },
    )
    assert identifying_extra.status_code == 422


def test_evidence_review_is_source_bound_idempotent_and_reversible(
    auth_override,
) -> None:
    auth_override(app_user_id="evidence-review-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    created = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={"borough": "brooklyn", "stage": "reviewing"},
    )
    assert created.status_code == 200, created.text

    review_body = {
        "expected_check_status": "verified",
        "expected_source": "NYC PLUTO",
        "expected_source_as_of": "2026-07-24",
        "expected_feed_generated_at": "2026-07-24T02:43:29Z",
    }
    reviewed = client.put(
        "/v1/parcel-intel/workflow/3020960069/evidence-reviews/property_facts",
        json=review_body,
    )
    assert reviewed.status_code == 200, reviewed.text
    assert reviewed.headers["cache-control"] == "private, no-store"
    marker = reviewed.json()["evidence_reviews"]["property_facts"]
    assert marker == {
        "check_key": "property_facts",
        "label": "Current property facts",
        "check_status": "verified",
        "source": "NYC PLUTO",
        "source_as_of": "2026-07-24",
        "feed_generated_at": "2026-07-24T02:43:29Z",
        "reviewed_at": marker["reviewed_at"],
    }
    event_count = store.items["3020960069"]["event_count"]
    stored_marker = dict(
        store.items["3020960069"]["evidence_reviews"]["property_facts"]
    )

    retried = client.put(
        "/v1/parcel-intel/workflow/3020960069/evidence-reviews/property_facts",
        json=review_body,
    )
    assert retried.status_code == 200, retried.text
    assert retried.json()["evidence_reviews"]["property_facts"] == marker
    assert store.items["3020960069"]["event_count"] == event_count

    stale = client.put(
        "/v1/parcel-intel/workflow/3020960069/evidence-reviews/property_facts",
        json={**review_body, "expected_feed_generated_at": "2026-07-23T00:00:00Z"},
    )
    assert stale.status_code == 409
    assert "Evidence changed" in stale.text
    assert (
        store.items["3020960069"]["evidence_reviews"]["property_facts"]
        == stored_marker
    )

    removed = client.delete(
        "/v1/parcel-intel/workflow/3020960069/evidence-reviews/property_facts"
    )
    assert removed.status_code == 200, removed.text
    assert removed.headers["cache-control"] == "private, no-store"
    assert removed.json()["evidence_reviews"] == {}


def test_evidence_review_requires_an_open_workflow_and_strict_contract(
    auth_override,
) -> None:
    auth_override(app_user_id="evidence-review-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    review_url = (
        "/v1/parcel-intel/workflow/3020960069/"
        "evidence-reviews/property_facts"
    )
    body = {
        "expected_check_status": "verified",
        "expected_source": "NYC PLUTO",
        "expected_source_as_of": "2026-07-24",
        "expected_feed_generated_at": "2026-07-24T02:43:29Z",
    }

    assert client.put(review_url, json=body).status_code == 409
    assert client.put(
        review_url,
        json={**body, "parcel_address": "private value"},
    ).status_code == 422
    assert client.put(
        review_url.replace("property_facts", "historical_model"),
        json=body,
    ).status_code == 422

    assert client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={"borough": "brooklyn", "stage": "pass"},
    ).status_code == 200
    terminal = client.put(review_url, json=body)
    assert terminal.status_code == 409
    assert "open, active" in terminal.text


def test_evidence_issue_is_source_bound_private_and_reversible(
    auth_override,
) -> None:
    auth_override(app_user_id="evidence-issue-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    created = client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={"borough": "brooklyn", "stage": "reviewing"},
    )
    assert created.status_code == 200, created.text
    original_snapshot = dict(created.json()["snapshot"])
    issue_url = (
        "/v1/parcel-intel/workflow/3020960069/"
        "evidence-issues/property_facts"
    )
    body = {
        "issue_type": "correction",
        "reason_code": "incorrect_value",
        "note": (
            "The official lot area appears inconsistent with the recorded "
            "survey; please verify the source match."
        ),
        "expected_check_status": "verified",
        "expected_source": "NYC PLUTO",
        "expected_source_as_of": "2026-07-24",
        "expected_feed_generated_at": "2026-07-24T02:43:29Z",
    }

    submitted = client.post(issue_url, json=body)
    assert submitted.status_code == 200, submitted.text
    assert submitted.headers["cache-control"] == "private, no-store"
    issue = submitted.json()["evidence_issues"]["property_facts"]
    assert issue["issue_id"].startswith("pei_")
    assert issue["status"] == "submitted"
    assert issue["source"] == "NYC PLUTO"
    assert issue["source_as_of"] == "2026-07-24"
    assert submitted.json()["snapshot"] == original_snapshot
    assert submitted.json()["evidence_reviews"] == {}

    retried = client.post(issue_url, json=body)
    assert retried.status_code == 200, retried.text
    assert (
        retried.json()["evidence_issues"]["property_facts"]["issue_id"]
        == issue["issue_id"]
    )

    conflicting = client.post(
        issue_url,
        json={
            **body,
            "note": (
                "A different material concern should not overwrite the "
                "already submitted governance request."
            ),
        },
    )
    assert conflicting.status_code == 409
    assert "already open" in conflicting.text

    stale = client.post(
        issue_url,
        json={**body, "expected_source_as_of": "2026-07-23"},
    )
    assert stale.status_code == 409
    assert "Evidence changed" in stale.text

    withdrawn = client.delete(issue_url)
    assert withdrawn.status_code == 200, withdrawn.text
    withdrawn_issue = withdrawn.json()["evidence_issues"]["property_facts"]
    assert withdrawn_issue["status"] == "withdrawn"
    assert withdrawn_issue["source"] == "NYC PLUTO"
    assert withdrawn_issue["note"] == issue["note"]
    assert withdrawn.json()["snapshot"] == original_snapshot


def test_evidence_issue_admin_triage_is_authorized_and_mirrored(
    auth_override,
) -> None:
    auth_override(app_user_id="evidence-issue-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    assert client.put(
        "/v1/parcel-intel/workflow/3020960069",
        json={"borough": "brooklyn", "stage": "pass"},
    ).status_code == 200
    submitted = client.post(
        (
            "/v1/parcel-intel/workflow/3020960069/"
            "evidence-issues/ownership"
        ),
        json={
            "issue_type": "suppression_review",
            "reason_code": "privacy_or_safety",
            "note": (
                "Please review whether this ownership display creates a "
                "specific privacy or safety concern for the team."
            ),
            "expected_check_status": "verified",
            "expected_source": "NYC ACRIS / NYC PLUTO",
            "expected_source_as_of": None,
            "expected_feed_generated_at": "2026-07-24T02:43:29Z",
        },
    )
    assert submitted.status_code == 200, submitted.text
    issue_id = submitted.json()["evidence_issues"]["ownership"]["issue_id"]

    forbidden = client.get(
        "/v1/parcel-intel/evidence-issues?status=submitted"
    )
    assert forbidden.status_code == 403

    auth_override(
        app_user_id="evidence-admin",
        plan_type="admin",
        is_admin=True,
    )
    listed = client.get(
        "/v1/parcel-intel/evidence-issues?status=submitted"
    )
    assert listed.status_code == 200, listed.text
    assert listed.headers["cache-control"] == "private, no-store"
    assert listed.headers["vary"] == "Authorization, X-API-Key"
    assert [item["issue_id"] for item in listed.json()["items"]] == [
        issue_id
    ]

    resolved = client.patch(
        f"/v1/parcel-intel/evidence-issues/{issue_id}",
        json={
            "status": "resolved",
            "resolution_note": (
                "Reviewed against the current source; the product display "
                "will be handled through the governed source update process."
            ),
        },
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["status"] == "resolved"
    assert resolved.json()["resolved_by_user_id"] == "evidence-admin"
    mirrored = store.items["3020960069"]["evidence_issues"]["ownership"]
    assert mirrored["status"] == "resolved"
    assert mirrored["resolution_note"].startswith("Reviewed against")

    repeated = client.patch(
        f"/v1/parcel-intel/evidence-issues/{issue_id}",
        json={
            "status": "resolved",
            "resolution_note": (
                "Reviewed against the current source; the product display "
                "will be handled through the governed source update process."
            ),
        },
    )
    assert repeated.status_code == 200, repeated.text

    conflicting = client.patch(
        f"/v1/parcel-intel/evidence-issues/{issue_id}",
        json={
            "status": "dismissed",
            "resolution_note": (
                "A materially different terminal decision must not replace "
                "the recorded governance outcome."
            ),
        },
    )
    assert conflicting.status_code == 409

    no_longer_open = client.get(
        "/v1/parcel-intel/evidence-issues?status=submitted"
    )
    assert no_longer_open.json()["items"] == []


def test_evidence_issue_rejects_unsaved_or_unbounded_requests(
    auth_override,
) -> None:
    auth_override(app_user_id="evidence-issue-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    issue_url = (
        "/v1/parcel-intel/workflow/3020960069/"
        "evidence-issues/property_facts"
    )
    body = {
        "issue_type": "correction",
        "reason_code": "incorrect_value",
        "note": "This is a sufficiently specific bounded correction request.",
        "expected_check_status": "verified",
        "expected_source": "NYC PLUTO",
        "expected_source_as_of": "2026-07-24",
        "expected_feed_generated_at": "2026-07-24T02:43:29Z",
    }
    assert client.post(issue_url, json=body).status_code == 409
    assert client.post(
        issue_url,
        json={**body, "parcel_owner": "private identifying value"},
    ).status_code == 422
    assert client.post(
        issue_url,
        json={**body, "note": "too short"},
    ).status_code == 422


def test_unauthenticated_workflow_errors_are_never_cacheable() -> None:
    client = TestClient(app)
    response = client.put(
        "/v1/parcel-intel/workflow/3020960069/"
        "evidence-reviews/property_facts",
        json={
            "expected_check_status": "verified",
            "expected_source": "NYC PLUTO",
        },
    )
    assert response.status_code == 401
    assert response.headers["cache-control"] == "private, no-store"

    issue_response = client.post(
        "/v1/parcel-intel/workflow/3020960069/"
        "evidence-issues/property_facts",
        json={
            "issue_type": "correction",
            "reason_code": "incorrect_value",
            "note": (
                "This is a bounded correction request for the cited "
                "official source version."
            ),
            "expected_check_status": "verified",
            "expected_source": "NYC PLUTO",
        },
    )
    assert issue_response.status_code == 401
    assert issue_response.headers["cache-control"] == "private, no-store"


def test_product_event_contract_is_value_minimized(auth_override) -> None:
    auth_override(app_user_id="user-adoption")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    response = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "parcel_opened",
            "source": "ranking",
        },
    )
    assert response.status_code == 204
    assert response.headers["cache-control"] == "private, no-store"
    assert store.product_events == [
        {
            "app_user_id": "user-adoption",
            "event": "parcel_opened",
            "source": "ranking",
        }
    ]
    dossier_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "official_dossier_opened",
            "source": "official_dossier",
        },
    )
    assert dossier_opened.status_code == 204
    assert dossier_opened.headers["cache-control"] == "private, no-store"
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "official_dossier_opened",
        "source": "official_dossier",
    }
    screening_lookup = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "screening_lookup_completed",
            "source": "screening_lookup",
        },
    )
    assert screening_lookup.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "screening_lookup_completed",
        "source": "screening_lookup",
    }
    applied = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_view_applied",
            "source": "saved_views",
        },
    )
    assert applied.status_code == 204
    assert applied.headers["cache-control"] == "private, no-store"
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "saved_view_applied",
        "source": "saved_views",
    }
    saved_view_comparison_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_view_comparison_opened",
            "source": "saved_views",
        },
    )
    assert saved_view_comparison_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "saved_view_comparison_opened",
        "source": "saved_views",
    }
    thesis_changes_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_thesis_changes_opened",
            "source": "saved_views",
        },
    )
    assert thesis_changes_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "saved_thesis_changes_opened",
        "source": "saved_views",
    }
    audit_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "decision_audit_opened",
            "source": "decision_posture",
        },
    )
    assert audit_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "decision_audit_opened",
        "source": "decision_posture",
    }
    underwriting_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "underwriting_opened",
            "source": "underwrite_tab",
        },
    )
    assert underwriting_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "underwriting_opened",
        "source": "underwrite_tab",
    }
    underwriting_changed = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "underwriting_assumptions_changed",
            "source": "base_assumptions",
        },
    )
    assert underwriting_changed.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "underwriting_assumptions_changed",
        "source": "base_assumptions",
    }
    screen_audit_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "screen_audit_opened",
            "source": "screen_summary",
        },
    )
    assert screen_audit_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "screen_audit_opened",
        "source": "screen_summary",
    }
    screen_criterion_relaxed = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "screen_criterion_relaxed",
            "source": "screen_audit",
        },
    )
    assert screen_criterion_relaxed.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "screen_criterion_relaxed",
        "source": "screen_audit",
    }
    comparison_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "comparison_opened",
            "source": "comparison",
        },
    )
    assert comparison_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "comparison_opened",
        "source": "comparison",
    }
    peer_comparison_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "comparison_opened",
            "source": "decision_peers",
        },
    )
    assert peer_comparison_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "comparison_opened",
        "source": "decision_peers",
    }
    compared_parcel_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "parcel_opened",
            "source": "comparison",
        },
    )
    assert compared_parcel_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "parcel_opened",
        "source": "comparison",
    }
    peer_parcel_opened = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "parcel_opened",
            "source": "decision_peers",
        },
    )
    assert peer_parcel_opened.status_code == 204
    assert store.product_events[-1] == {
        "app_user_id": "user-adoption",
        "event": "parcel_opened",
        "source": "decision_peers",
    }
    mismatched = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_view_applied",
            "source": "map",
        },
    )
    assert mismatched.status_code == 422
    mismatched_dossier = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "official_dossier_opened",
            "source": "map",
        },
    )
    assert mismatched_dossier.status_code == 422
    mismatched_saved_view_comparison = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_view_comparison_opened",
            "source": "comparison",
        },
    )
    assert mismatched_saved_view_comparison.status_code == 422
    mismatched_thesis_changes = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "saved_thesis_changes_opened",
            "source": "comparison",
        },
    )
    assert mismatched_thesis_changes.status_code == 422
    mismatched_audit_source = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "decision_audit_opened",
            "source": "ranking",
        },
    )
    assert mismatched_audit_source.status_code == 422
    mismatched_underwriting_source = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "underwriting_assumptions_changed",
            "source": "underwrite_tab",
        },
    )
    assert mismatched_underwriting_source.status_code == 422
    mismatched_screen_audit_source = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "screen_audit_opened",
            "source": "screen_audit",
        },
    )
    assert mismatched_screen_audit_source.status_code == 422
    mismatched_comparison_source = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "comparison_opened",
            "source": "map",
        },
    )
    assert mismatched_comparison_source.status_code == 422
    identifying = client.post(
        "/v1/parcel-intel/product-events",
        json={
            "schema_version": "citylens/parcel-product-event@v1",
            "event": "parcel_opened",
            "source": "map",
            "bbl": "3020960069",
        },
    )
    assert identifying.status_code == 422


def test_workflow_outcome_export_is_private_maturity_safe_and_downloadable(
    auth_override,
) -> None:
    auth_override(app_user_id="workflow-export-user")
    store = FakeWorkflowStore()
    saved_at = datetime.now(timezone.utc) - timedelta(days=400)
    store.items["3020960069"] = {
        "bbl": "3020960069",
        "borough": "brooklyn",
        "stage": "pursue",
        "notes": "Private negotiation notes",
        "tags": ["private"],
        "assignee": "Named teammate",
        "watching": True,
        "decision_reason": "pursuing",
        "outcome": "offer_submitted",
        "next_action": "Call owner",
        "next_action_due_date": None,
        "snapshot": {
            "address": "Private address",
            "owner_name": "PRIVATE OWNER LLC",
            "feed_generated_at": "2026-01-01T00:00:00Z",
            "property_facts_as_of": "2026-01-01",
            "citywide_rank": 99,
            "acquisition_rank": 20,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
            "score_calibrated": 0.22,
        },
        "saved_at": saved_at,
        "updated_at": datetime.now(timezone.utc),
        "archived_at": None,
        "event_count": 3,
        "first_contacted_at": saved_at + timedelta(days=4),
        "first_qualified_at": saved_at + timedelta(days=60),
        "first_offer_submitted_at": saved_at + timedelta(days=120),
        "evidence_reviews": {
            "ownership": {
                "source": "PRIVATE REVIEW SOURCE",
                "reviewed_at": datetime.now(timezone.utc),
            }
        },
        "evidence_issues": {
            "ownership": {
                "issue_id": "pei_0123456789abcdef0123456789abcdef",
                "note": "PRIVATE EVIDENCE CORRECTION NOTE",
                "status": "submitted",
            }
        },
    }
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    response = client.get("/v1/parcel-intel/workflow/outcomes/export")

    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["content-disposition"] == (
        'attachment; filename="citylens-outcome-evidence.json"'
    )
    payload = response.json()
    assert payload["schema_version"] == (
        "citylens/parcel-workflow-outcome-export@v1"
    )
    assert payload["exported_record_count"] == 1
    assert payload["rows"][0]["labels"][0]["value"] is True
    serialized = response.text
    assert "Private negotiation notes" not in serialized
    assert "Named teammate" not in serialized
    assert "Private address" not in serialized
    assert "PRIVATE OWNER LLC" not in serialized
    assert "PRIVATE REVIEW SOURCE" not in serialized
    assert "PRIVATE EVIDENCE CORRECTION NOTE" not in serialized
    assert "evidence_reviews" not in serialized
    assert "evidence_issues" not in serialized

def test_product_usage_day_is_aggregate_only_and_bounded() -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    payload = _product_usage_day_payload(
        existing={},
        event="parcel_opened",
        source="map",
        occurred_at=now,
    )
    assert payload is not None
    assert payload["events"] == {"parcel_opened": 1}
    assert payload["sources"] == {"parcel_opened:map": 1}
    assert payload["total_events"] == 1
    assert payload["expires_at"] == now + timedelta(days=90)
    assert not {
        "bbl",
        "address",
        "owner",
        "notes",
        "tags",
        "assignee",
        "url",
    }.intersection(payload)

    assert (
        _product_usage_day_payload(
            existing={"total_events": PRODUCT_EVENT_DAILY_LIMIT},
            event="parcel_opened",
            source="map",
            occurred_at=now,
        )
        is None
    )


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
    assert payload["schema_version"] == "citylens/parcel-workflow-analytics@v3"
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
        "citylens/parcel-workflow-analytics-methodology@v2"
    )
    assert payload["analytics_schema_version"] == (
        "citylens/parcel-workflow-analytics@v3"
    )
    assert payload["confidence_level"] == 0.95
    assert "Wilson" in payload["uncertainty_semantics"]
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
            "name": "  Citywide vacant sites  ",
            "borough": "all",
            "filters": {
                "query": "  llc  ",
                "priority": "high_or_better",
                "opportunity": "vacant_site",
                "overlay": "opportunity",
            },
            "alert_frequency": "off",
        },
    )
    assert created.status_code == 200, created.text
    assert created.headers["cache-control"] == "private, no-store"
    assert created.json()["schema_version"] == "citylens/parcel-saved-view@v2"
    assert created.json()["name"] == "Citywide vacant sites"
    assert created.json()["filters"]["query"] == "llc"
    assert created.json()["filters"]["site_type"] == "vacant_site"
    assert created.json()["filters"]["signals"] == []
    assert created.json()["alert_frequency"] == "off"
    listed = client.get("/v1/parcel-intel/saved-searches")
    assert listed.headers["cache-control"] == "private, no-store"
    assert listed.json()[0]["name"] == "Citywide vacant sites"
    removed = client.delete("/v1/parcel-intel/saved-searches/brooklyn-vacant")
    assert removed.status_code == 204
    assert removed.headers["cache-control"] == "private, no-store"


def test_saved_search_persists_generation_bound_private_snapshot(
    auth_override,
) -> None:
    auth_override(app_user_id="monitor-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)
    matched_bbls = ["1000010001", "3000010002", "5000010003"]

    created = client.put(
        "/v1/parcel-intel/saved-searches/citywide-thesis",
        json={
            "name": "Citywide acquisition thesis",
            "borough": "all",
            "filters": {
                "query": "",
                "priority": "high_or_better",
                "opportunity": "all",
                "site_type": "uncommitted",
                "signals": ["long_held"],
                "overlay": "priority",
            },
            "alert_frequency": "off",
            "snapshot": {
                "schema_version": (
                    "citylens/parcel-saved-view-snapshot@v1"
                ),
                "feed_generation": (
                    "20260727T030301358307Z-a32b245a82db"
                ),
                "feed_generated_at": "2026-07-27T03:03:01.358307Z",
                "match_count": len(matched_bbls),
                "matched_bbls": matched_bbls,
            },
        },
    )

    assert created.status_code == 200, created.text
    body = created.json()
    assert body["schema_version"] == "citylens/parcel-saved-view@v3"
    assert body["snapshot"]["match_count"] == 3
    assert body["snapshot"]["matched_bbls"] == matched_bbls
    assert "owner_name" not in body["snapshot"]
    assert "address" not in body["snapshot"]
    assert "score" not in body["snapshot"]
    listed = client.get("/v1/parcel-intel/saved-searches")
    assert listed.status_code == 200
    assert listed.json()[0]["snapshot"] == body["snapshot"]


def test_saved_search_rejects_a_stale_baseline_without_disclosing_generations(
    auth_override,
) -> None:
    auth_override(app_user_id="monitor-user")

    class StaleBaselineStore(FakeWorkflowStore):
        def upsert_parcel_saved_search(
            self,
            *,
            app_user_id: str,
            search_id: str,
            payload: dict,
        ) -> dict:
            raise StaleSavedSearchSnapshot(
                existing_generation=(
                    "20260728T030301358307Z-b32b245a82db"
                ),
                incoming_generation=(
                    "20260727T030301358307Z-a32b245a82db"
                ),
            )

    app.dependency_overrides[parcel_workflow.get_store] = (
        lambda: StaleBaselineStore()
    )
    client = TestClient(app)
    response = client.put(
        "/v1/parcel-intel/saved-searches/citywide-thesis",
        json={
            "name": "Citywide acquisition thesis",
            "borough": "all",
            "alert_frequency": "off",
            "snapshot": {
                "schema_version": (
                    "citylens/parcel-saved-view-snapshot@v1"
                ),
                "feed_generation": (
                    "20260727T030301358307Z-a32b245a82db"
                ),
                "feed_generated_at": "2026-07-27T03:03:01.358307Z",
                "match_count": 1,
                "matched_bbls": ["3000010001"],
            },
        },
    )

    assert response.status_code == 409
    assert response.headers["cache-control"] == "private, no-store"
    assert response.json() == {
        "detail": (
            "This saved thesis has a newer baseline. Refresh the inventory "
            "before updating it."
        )
    }
    assert "202607" not in response.text
    assert "3000010001" not in response.text


@pytest.mark.parametrize(
    "payload",
    [
        {
            "name": "Imaginary weekly alert",
            "borough": "all",
            "alert_frequency": "weekly",
        },
        {
            "name": "Invalid owner focus",
            "borough": "all",
            "filters": {
                "opportunity": "vacant_site",
                "owner_portfolio_id": "owner-1",
            },
        },
        {
            "name": "Conflicting legacy and compound filters",
            "borough": "all",
            "filters": {
                "opportunity": "vacant_site",
                "site_type": "ground_up_candidate",
            },
        },
        {
            "name": "Invalid opportunity",
            "borough": "all",
            "filters": {"opportunity": "seller_intent"},
        },
        {
            "name": "Invalid numeric site criterion",
            "borough": "all",
            "filters": {
                "opportunity": "all",
                "min_lot_area_sqft": 0,
            },
        },
        {
            "name": "Unsorted snapshot membership",
            "borough": "all",
            "snapshot": {
                "schema_version": (
                    "citylens/parcel-saved-view-snapshot@v1"
                ),
                "feed_generation": (
                    "20260727T030301358307Z-a32b245a82db"
                ),
                "feed_generated_at": "2026-07-27T03:03:01.358307Z",
                "match_count": 2,
                "matched_bbls": ["3000010002", "1000010001"],
            },
        },
        {
            "name": "Mismatched snapshot count",
            "borough": "all",
            "snapshot": {
                "schema_version": (
                    "citylens/parcel-saved-view-snapshot@v1"
                ),
                "feed_generation": (
                    "20260727T030301358307Z-a32b245a82db"
                ),
                "feed_generated_at": "2026-07-27T03:03:01.358307Z",
                "match_count": 2,
                "matched_bbls": ["1000010001"],
            },
        },
    ],
)
def test_saved_search_rejects_unrestorable_or_unimplemented_state(
    auth_override, payload: dict
) -> None:
    auth_override(app_user_id="search-user")
    app.dependency_overrides[parcel_workflow.get_store] = lambda: FakeWorkflowStore()
    client = TestClient(app)
    response = client.put(
        "/v1/parcel-intel/saved-searches/invalid-view",
        json=payload,
    )
    assert response.status_code == 422


def test_saved_search_preserves_compound_site_and_signal_filters(
    auth_override,
) -> None:
    auth_override(app_user_id="compound-search-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    created = client.put(
        "/v1/parcel-intel/saved-searches/queens-long-held-transit",
        json={
            "name": "Queens long-held transit sites",
            "borough": "queens",
            "filters": {
                "query": "",
                "priority": "high_or_better",
                "opportunity": "all",
                "site_type": "uncommitted",
                "signals": ["long_held", "transit_800m", "long_held"],
                "min_lot_area_sqft": 5_000,
                "min_unused_floor_area_sqft": 10_000,
                "overlay": "priority",
            },
            "alert_frequency": "off",
        },
    )

    assert created.status_code == 200, created.text
    filters = created.json()["filters"]
    assert filters["opportunity"] == "all"
    assert filters["site_type"] == "uncommitted"
    assert filters["signals"] == ["long_held", "transit_800m"]
    assert filters["min_lot_area_sqft"] == 5_000
    assert filters["min_unused_floor_area_sqft"] == 10_000

    listed = client.get("/v1/parcel-intel/saved-searches")
    assert listed.status_code == 200
    assert listed.json()[0]["filters"] == filters


def test_saved_search_normalizes_legacy_portfolio_focus(auth_override) -> None:
    auth_override(app_user_id="legacy-portfolio-user")
    store = FakeWorkflowStore()
    app.dependency_overrides[parcel_workflow.get_store] = lambda: store
    client = TestClient(app)

    created = client.put(
        "/v1/parcel-intel/saved-searches/legacy-portfolio",
        json={
            "name": "Legacy owner portfolio",
            "borough": "all",
            "filters": {
                "opportunity": "portfolio",
                "owner_portfolio_id": "owner-portfolio-1",
            },
            "alert_frequency": "off",
        },
    )

    assert created.status_code == 200, created.text
    assert created.json()["filters"]["site_type"] == "all"
    assert created.json()["filters"]["signals"] == ["portfolio"]
    assert (
        created.json()["filters"]["owner_portfolio_id"]
        == "owner-portfolio-1"
    )


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
            "filters": {"opportunity": "not-a-real-filter"},
        },
    )
    assert bad_search.status_code == 422

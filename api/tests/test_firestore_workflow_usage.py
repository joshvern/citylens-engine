from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from google.api_core import exceptions as gexc

from app.services import firestore_store, retry
from app.services.firestore_store import (
    FirestoreStore,
    StaleSavedSearchSnapshot,
    parcel_lead_review_id,
)


class _Snapshot:
    def __init__(self, value: dict[str, Any] | None) -> None:
        self.exists = value is not None
        self._value = value

    def to_dict(self) -> dict[str, Any] | None:
        return deepcopy(self._value)


class _Document:
    def __init__(self, client: "_Client", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def get(self, *, transaction=None) -> _Snapshot:
        del transaction
        return _Snapshot(self.client.documents.get(self.path))

    def collection(self, name: str) -> "_Collection":
        return _Collection(self.client, (*self.path, name))


class _Collection:
    def __init__(self, client: "_Client", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def document(self, identifier: str) -> _Document:
        return _Document(self.client, (*self.path, identifier))

    def where(self, *, filter) -> "_Query":
        return _Query(self.client, self.path, filter=filter)


class _Query:
    def __init__(
        self,
        client: "_Client",
        path: tuple[str, ...],
        *,
        filter,
        limit: int | None = None,
    ) -> None:
        self.client = client
        self.path = path
        self.filter = filter
        self._limit = limit

    def limit(self, value: int) -> "_Query":
        return _Query(
            self.client,
            self.path,
            filter=self.filter,
            limit=value,
        )

    def stream(self):
        matches = [
            value
            for path, value in self.client.documents.items()
            if path[:-1] == self.path
            and self.filter.op_string == "=="
            and value.get(self.filter.field_path) == self.filter.value
        ]
        if self._limit is not None:
            matches = matches[: self._limit]
        return [_Snapshot(value) for value in matches]


class _Transaction:
    def __init__(self, client: "_Client") -> None:
        self.client = client

    def set(
        self,
        reference: _Document,
        value: dict[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        if merge:
            existing = self.client.documents.get(reference.path, {})
            self.client.documents[reference.path] = {
                **deepcopy(existing),
                **deepcopy(value),
            }
        else:
            self.client.documents[reference.path] = deepcopy(value)

    def delete(self, reference: _Document) -> None:
        self.client.documents.pop(reference.path, None)


class _Client:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, ...], dict[str, Any]] = {}

    def collection(self, name: str) -> _Collection:
        return _Collection(self, (name,))

    def transaction(self) -> _Transaction:
        return _Transaction(self)


def test_lead_review_is_generation_bound_auditable_and_workflow_independent(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(
        project_id="test",
        client=client,  # type: ignore[arg-type]
    )
    generation = "20260730T092749819158Z-daf06394d35b"
    bbl = "3058920038"
    review_id = parcel_lead_review_id(
        feed_generation=generation,
        bbl=bbl,
    )

    created, status = store.upsert_parcel_lead_review(
        app_user_id="private-user",
        bbl=bbl,
        feed_generation=generation,
        verdict="pass",
        reason_codes=["active_or_completed_project"],
        snapshot={
            "citywide_rank": 42,
            "acquisition_rank": 39,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
        },
    )

    assert status == "created"
    assert created["revision"] == 1
    assert created["feed_generation"] == generation
    assert created["citywide_rank"] == 42
    assert store.get_parcel_lead_review(
        app_user_id="private-user",
        bbl=bbl,
        feed_generation=generation,
    ) == created
    stale_generation = "20260729T092749819158Z-daf06394d35b"
    stale_review_id = parcel_lead_review_id(
        feed_generation=stale_generation,
        bbl=bbl,
    )
    client.documents[
        (
            "users",
            "private-user",
            "parcel_lead_reviews",
            stale_review_id,
        )
    ] = {
        **created,
        "review_id": stale_review_id,
        "feed_generation": stale_generation,
    }
    assert store.list_parcel_lead_reviews(
        app_user_id="private-user",
        feed_generation=generation,
    ) == [created]
    review_path = (
        "users",
        "private-user",
        "parcel_lead_reviews",
        review_id,
    )
    assert client.documents[review_path]["bbl"] == bbl
    assert not {
        "address",
        "owner_name",
        "notes",
        "assignee",
        "user_id",
    }.intersection(client.documents[review_path])
    assert not any("parcel_workflow" in path for path in client.documents)

    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-30",
    )
    assert client.documents[usage_path]["events"] == {
        "lead_review_created": 1
    }
    assert client.documents[usage_path]["sources"] == {
        "lead_review_created:lead_review": 1
    }

    repeated, repeated_status = store.upsert_parcel_lead_review(
        app_user_id="private-user",
        bbl=bbl,
        feed_generation=generation,
        verdict="pass",
        reason_codes=["active_or_completed_project"],
        snapshot={"citywide_rank": 999},
    )
    assert repeated_status == "unchanged"
    assert repeated["revision"] == 1
    assert repeated["citywide_rank"] == 42
    assert client.documents[usage_path]["events"] == {
        "lead_review_created": 1
    }

    updated, updated_status = store.upsert_parcel_lead_review(
        app_user_id="private-user",
        bbl=bbl,
        feed_generation=generation,
        verdict="watch",
        reason_codes=["missing_facts"],
        snapshot={
            "citywide_rank": 42,
            "acquisition_rank": 39,
            "priority_tier": "highest",
            "opportunity_category": "ground_up_candidate",
        },
    )
    assert updated_status == "updated"
    assert updated["revision"] == 2
    assert client.documents[usage_path]["events"] == {
        "lead_review_created": 1,
        "lead_review_updated": 1,
    }
    events = [
        value
        for path, value in client.documents.items()
        if path[:4] == review_path and "lead_review_events" in path
    ]
    assert len(events) == 2
    assert {event["to_verdict"] for event in events} == {
        "pass",
        "watch",
    }


def test_workflow_lifecycle_usage_is_transactional_and_idempotent(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    payload = {
        "borough": "brooklyn",
        "stage": "new",
        "outcome": "unknown",
        "snapshot": {"citywide_rank": 12},
    }

    created = store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload=payload,
    )
    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-24",
    )
    usage = client.documents[usage_path]
    assert created["event_count"] == 1
    assert usage["events"] == {"workflow_created": 1}
    assert usage["sources"] == {"workflow_created:parcel": 1}
    assert not {"bbl", "address", "owner", "notes", "tags"}.intersection(usage)

    # A transport retry of the same effective mutation updates no lifecycle
    # event and therefore cannot inflate the activation counter.
    retried = store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload=payload,
    )
    assert retried["event_count"] == 1
    assert client.documents[usage_path]["events"] == {"workflow_created": 1}

    updated = store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={**payload, "stage": "reviewing"},
    )
    assert updated["event_count"] == 2
    assert client.documents[usage_path]["events"] == {
        "workflow_created": 1,
        "workflow_updated": 1,
    }

    assert store.delete_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
    )
    assert client.documents[usage_path]["events"] == {
        "workflow_archived": 1,
        "workflow_created": 1,
        "workflow_updated": 1,
    }

    # Deleting an already archived record is a no-op and remains idempotent.
    assert not store.delete_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
    )
    assert client.documents[usage_path]["events"]["workflow_archived"] == 1


def test_underwriting_workflow_source_is_transactional_and_value_minimized(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]

    store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={
            "borough": "brooklyn",
            "stage": "reviewing",
            "snapshot": {"citywide_rank": 12},
        },
        entry_source="underwriting",
    )

    usage = client.documents[
        (
            "users",
            "private-user",
            "product_usage_days",
            "2026-07-24",
        )
    ]
    assert usage["events"] == {"workflow_created": 1}
    assert usage["sources"] == {"workflow_created:underwriting": 1}
    assert not {
        "bbl",
        "address",
        "owner",
        "notes",
        "tags",
        "entry_source",
    }.intersection(usage)


def test_comparison_advance_is_atomic_and_attributed_without_identifiers(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    payload = {
        "borough": "brooklyn",
        "stage": "reviewing",
        "decision_reason": "pursuing",
        "next_action": "Verify title.",
        "outcome": "unknown",
        "snapshot": {"citywide_rank": 12},
    }

    created, status = store.advance_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload=payload,
    )
    assert status == "created"
    assert created["next_action"] == "Verify title."
    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-24",
    )
    usage = client.documents[usage_path]
    assert usage["events"] == {"workflow_created": 1}
    assert usage["sources"] == {"workflow_created:comparison": 1}
    assert not {"bbl", "address", "next_action", "snapshot"}.intersection(usage)

    existing, status = store.advance_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={**payload, "next_action": "Do not overwrite."},
    )
    assert status == "existing"
    assert existing["next_action"] == "Verify title."
    assert client.documents[usage_path]["events"] == {"workflow_created": 1}

    assert store.delete_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
    )
    restored, status = store.advance_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={**payload, "next_action": "Review again."},
    )
    assert status == "restored"
    assert restored["next_action"] == "Review again."
    assert client.documents[usage_path]["events"]["workflow_created"] == 2
    assert (
        client.documents[usage_path]["sources"]["workflow_created:comparison"]
        == 2
    )


def test_evidence_review_usage_is_private_source_bound_and_idempotent(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={
            "borough": "brooklyn",
            "stage": "reviewing",
            "outcome": "unknown",
            "snapshot": {"citywide_rank": 12},
        },
    )
    review = {
        "check_key": "property_facts",
        "label": "Current property facts",
        "check_status": "verified",
        "source": "NYC PLUTO",
        "source_as_of": "2026-07-24",
        "feed_generated_at": "2026-07-24T02:43:29Z",
    }

    reviewed, mutation_status = store.set_parcel_workflow_evidence_review(
        app_user_id="private-user",
        bbl="3020960069",
        check_key="property_facts",
        review=review,
    )
    assert mutation_status == "reviewed"
    assert reviewed is not None
    marker = reviewed["evidence_reviews"]["property_facts"]
    assert marker["reviewed_at"] == now
    assert marker["source"] == "NYC PLUTO"
    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-24",
    )
    usage = client.documents[usage_path]
    assert usage["events"] == {
        "workflow_created": 1,
        "workflow_evidence_reviewed": 1,
    }
    assert usage["sources"] == {
        "workflow_created:parcel": 1,
        "workflow_evidence_reviewed:workflow": 1,
    }
    assert not {
        "bbl",
        "check_key",
        "label",
        "source_as_of",
        "feed_generated_at",
    }.intersection(usage)

    retried, mutation_status = store.set_parcel_workflow_evidence_review(
        app_user_id="private-user",
        bbl="3020960069",
        check_key="property_facts",
        review=review,
    )
    assert mutation_status == "unchanged"
    assert retried is not None
    assert retried["evidence_reviews"]["property_facts"] == marker
    assert (
        client.documents[usage_path]["events"]["workflow_evidence_reviewed"]
        == 1
    )

    removed, mutation_status = store.set_parcel_workflow_evidence_review(
        app_user_id="private-user",
        bbl="3020960069",
        check_key="property_facts",
        review=None,
    )
    assert mutation_status == "removed"
    assert removed is not None
    assert removed["evidence_reviews"] == {}
    assert (
        client.documents[usage_path]["events"]["workflow_evidence_reviewed"]
        == 1
    )

    store.delete_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
    )
    inactive, mutation_status = store.set_parcel_workflow_evidence_review(
        app_user_id="private-user",
        bbl="3020960069",
        check_key="property_facts",
        review=review,
    )
    assert mutation_status == "inactive"
    assert inactive is not None


def test_evidence_issue_governance_preserves_source_and_private_usage(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    created = store.upsert_parcel_workflow(
        app_user_id="private-user",
        bbl="3020960069",
        payload={
            "borough": "brooklyn",
            "stage": "reviewing",
            "outcome": "unknown",
            "snapshot": {
                "citywide_rank": 12,
                "owner_name": "OFFICIAL OWNER LLC",
            },
        },
    )
    source_snapshot = deepcopy(created["snapshot"])
    issue_payload = {
        "check_key": "property_facts",
        "label": "Current property facts",
        "issue_type": "correction",
        "reason_code": "incorrect_value",
        "note": "The mapped lot area conflicts with a current signed survey.",
        "check_status": "verified",
        "source": "NYC PLUTO",
        "source_as_of": "2026-07-24",
        "feed_generated_at": "2026-07-24T02:43:29Z",
    }

    workflow, issue, mutation_status = (
        store.submit_parcel_workflow_evidence_issue(
            app_user_id="private-user",
            bbl="3020960069",
            issue=issue_payload,
        )
    )
    assert mutation_status == "submitted"
    assert workflow is not None
    assert issue is not None
    assert workflow["snapshot"] == source_snapshot
    assert workflow["evidence_issues"]["property_facts"] == issue
    issue_id = issue["issue_id"]
    governance_path = ("parcel_evidence_issues", issue_id)
    governance = client.documents[governance_path]
    assert governance["submitted_by_user_id"] == "private-user"
    assert governance["expires_at"] == now + timedelta(days=730)
    assert governance["source"] == "NYC PLUTO"

    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-24",
    )
    usage = client.documents[usage_path]
    assert usage["events"] == {
        "workflow_created": 1,
        "workflow_evidence_issue_submitted": 1,
    }
    assert not {
        "bbl",
        "check_key",
        "note",
        "source",
        "reason_code",
    }.intersection(usage)

    retried, retried_issue, mutation_status = (
        store.submit_parcel_workflow_evidence_issue(
            app_user_id="private-user",
            bbl="3020960069",
            issue=issue_payload,
        )
    )
    assert mutation_status == "unchanged"
    assert retried_issue == issue
    assert retried is not None
    assert (
        client.documents[usage_path]["events"][
            "workflow_evidence_issue_submitted"
        ]
        == 1
    )

    resolved = store.resolve_parcel_evidence_issue(
        issue_id=issue_id,
        status="resolved",
        resolution_note="Verified and queued for the governed source update.",
        admin_user_id="private-admin",
    )
    assert resolved is not None
    assert resolved["status"] == "resolved"
    assert resolved["resolved_by_user_id"] == "private-admin"
    workflow_path = (
        "users",
        "private-user",
        "parcel_workflow",
        "3020960069",
    )
    mirrored = client.documents[workflow_path]["evidence_issues"][
        "property_facts"
    ]
    assert mirrored["status"] == "resolved"
    assert client.documents[workflow_path]["snapshot"] == source_snapshot


def test_saved_view_lifecycle_usage_is_transactional_private_and_idempotent(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    payload = {
        "schema_version": "citylens/parcel-saved-view@v2",
        "name": "Private acquisitions",
        "borough": "all",
        "query": "confidential owner",
        "filters": {
            "priority": "highest",
            "opportunity": "ground_up_candidate",
            "owner_portfolio": "multi_lot",
            "overlay": "priority",
        },
        "alert_frequency": "off",
    }

    created = store.upsert_parcel_saved_search(
        app_user_id="private-user",
        search_id="private-search-id",
        payload=payload,
    )
    usage_path = (
        "users",
        "private-user",
        "product_usage_days",
        "2026-07-24",
    )
    usage = client.documents[usage_path]
    assert created["name"] == "Private acquisitions"
    assert usage["events"] == {"saved_view_created": 1}
    assert usage["sources"] == {"saved_view_created:saved_views": 1}
    assert not {
        "search_id",
        "name",
        "query",
        "filters",
        "owner",
        "owner_id",
    }.intersection(usage)

    created_at = created["created_at"]
    updated_at = created["updated_at"]
    retried = store.upsert_parcel_saved_search(
        app_user_id="private-user",
        search_id="private-search-id",
        payload=payload,
    )
    assert retried["created_at"] == created_at
    assert retried["updated_at"] == updated_at
    assert client.documents[usage_path]["events"] == {"saved_view_created": 1}

    updated = store.upsert_parcel_saved_search(
        app_user_id="private-user",
        search_id="private-search-id",
        payload={**payload, "name": "Updated private acquisitions"},
    )
    assert updated["name"] == "Updated private acquisitions"
    assert client.documents[usage_path]["events"] == {
        "saved_view_created": 1,
        "saved_view_updated": 1,
    }

    assert store.delete_parcel_saved_search(
        app_user_id="private-user",
        search_id="private-search-id",
    )
    assert client.documents[usage_path]["events"] == {
        "saved_view_created": 1,
        "saved_view_deleted": 1,
        "saved_view_updated": 1,
    }

    assert not store.delete_parcel_saved_search(
        app_user_id="private-user",
        search_id="private-search-id",
    )
    assert client.documents[usage_path]["events"]["saved_view_deleted"] == 1


def test_saved_view_upsert_restarts_an_expired_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def transactional(function):
        def wrapped(transaction):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise gexc.InvalidArgument(
                    "The referenced transaction has expired or is no longer valid."
                )
            return function(transaction)

        return wrapped

    monkeypatch.setattr(firestore_store.firestore, "transactional", transactional)
    monkeypatch.setattr(retry.time, "sleep", lambda _seconds: None)
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]

    saved = store.upsert_parcel_saved_search(
        app_user_id="private-user",
        search_id="transaction-recovery",
        payload={
            "schema_version": "citylens/parcel-saved-view@v2",
            "name": "Recovered saved screen",
            "borough": "all",
            "filters": {"priority": "highest", "overlay": "priority"},
            "alert_frequency": "off",
        },
    )

    assert attempts == 2
    assert saved["name"] == "Recovered saved screen"
    assert client.documents[
        (
            "users",
            "private-user",
            "parcel_saved_searches",
            "transaction-recovery",
        )
    ]["search_id"] == "transaction-recovery"


def test_saved_thesis_baselines_are_monotonic_private_and_transactional(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 27, 4, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    first_generation = "20260727T030301358307Z-a32b245a82db"
    next_generation = "20260728T030301358307Z-b32b245a82db"
    base_payload = {
        "schema_version": "citylens/parcel-saved-view@v3",
        "name": "Private monitored thesis",
        "borough": "all",
        "filters": {
            "priority": "high_or_better",
            "opportunity": "all",
            "site_type": "uncommitted",
            "signals": ["long_held"],
            "overlay": "priority",
        },
        "alert_frequency": "off",
        "snapshot": {
            "schema_version": "citylens/parcel-saved-view-snapshot@v1",
            "feed_generation": first_generation,
            "feed_generated_at": now,
            "match_count": 1,
            "matched_bbls": ["3000010001"],
        },
    }

    created = store.upsert_parcel_saved_search(
        app_user_id="private-monitor-user",
        search_id="private-monitor-id",
        payload=base_payload,
    )
    usage_path = (
        "users",
        "private-monitor-user",
        "product_usage_days",
        "2026-07-27",
    )
    usage = client.documents[usage_path]
    assert created["snapshot"]["feed_generation"] == first_generation
    assert usage["events"] == {
        "saved_thesis_baseline_created": 1,
        "saved_view_created": 1,
    }
    assert usage["sources"] == {
        "saved_thesis_baseline_created:saved_views": 1,
        "saved_view_created:saved_views": 1,
    }
    assert not {
        "search_id",
        "snapshot",
        "feed_generation",
        "matched_bbls",
        "match_count",
        "filters",
        "name",
    }.intersection(usage)

    retried = store.upsert_parcel_saved_search(
        app_user_id="private-monitor-user",
        search_id="private-monitor-id",
        payload=base_payload,
    )
    assert retried["updated_at"] == created["updated_at"]
    assert client.documents[usage_path]["total_events"] == 2

    renamed = store.upsert_parcel_saved_search(
        app_user_id="private-monitor-user",
        search_id="private-monitor-id",
        payload={**base_payload, "name": "Renamed monitored thesis"},
    )
    assert renamed["name"] == "Renamed monitored thesis"
    assert client.documents[usage_path]["events"] == {
        "saved_thesis_baseline_created": 1,
        "saved_view_created": 1,
        "saved_view_updated": 1,
    }

    advanced_payload = {
        **base_payload,
        "name": "Renamed monitored thesis",
        "snapshot": {
            **base_payload["snapshot"],
            "feed_generation": next_generation,
            "feed_generated_at": now + timedelta(days=1),
            "match_count": 2,
            "matched_bbls": ["3000010001", "4000010002"],
        },
    }
    advanced = store.upsert_parcel_saved_search(
        app_user_id="private-monitor-user",
        search_id="private-monitor-id",
        payload=advanced_payload,
    )
    assert advanced["snapshot"]["feed_generation"] == next_generation
    assert client.documents[usage_path]["events"] == {
        "saved_thesis_baseline_advanced": 1,
        "saved_thesis_baseline_created": 1,
        "saved_view_created": 1,
        "saved_view_updated": 2,
    }

    preserved = store.upsert_parcel_saved_search(
        app_user_id="private-monitor-user",
        search_id="private-monitor-id",
        payload={
            **{
                key: value
                for key, value in base_payload.items()
                if key != "snapshot"
            },
            "schema_version": "citylens/parcel-saved-view@v2",
            "name": "Metadata-only edit",
        },
    )
    assert preserved["schema_version"] == "citylens/parcel-saved-view@v3"
    assert preserved["snapshot"]["feed_generation"] == next_generation

    with pytest.raises(StaleSavedSearchSnapshot):
        store.upsert_parcel_saved_search(
            app_user_id="private-monitor-user",
            search_id="private-monitor-id",
            payload=base_payload,
        )
    saved_path = (
        "users",
        "private-monitor-user",
        "parcel_saved_searches",
        "private-monitor-id",
    )
    assert (
        client.documents[saved_path]["snapshot"]["feed_generation"]
        == next_generation
    )


def test_pilot_request_storage_is_idempotent_and_expires(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 24, 20, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: now)
    monkeypatch.setattr(
        firestore_store.firestore,
        "transactional",
        lambda function: function,
    )
    client = _Client()
    store = FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]
    request_id = "pr_0123456789abcdef0123456789abcdef"
    payload = {
        "schema_version": "citylens/pilot-request@v1",
        "plan": "acquisitions",
        "name": "Jordan Lee",
        "work_email": "jordan@example.com",
        "company": "Example Development",
        "role": "Acquisitions director",
        "team_size": "2-5",
        "target_boroughs": ["brooklyn"],
        "workflow_summary": "We need a shared development-site workflow.",
        "consent": True,
    }

    created = store.create_pilot_request(
        request_id=request_id,
        payload=payload,
    )
    assert created["status"] == "new"
    assert created["expires_at"] == now + timedelta(days=365)

    retried = store.create_pilot_request(
        request_id=request_id,
        payload={**payload, "company": "Must not overwrite"},
    )
    assert retried == created
    path = ("pilot_requests", request_id)
    assert client.documents[path]["company"] == "Example Development"

    contacted = store.update_pilot_request_status(
        request_id=request_id,
        status="contacted",
        admin_user_id="private-admin-id",
    )
    assert contacted is not None
    assert contacted["status"] == "contacted"
    assert contacted["status_updated_by"] == "private-admin-id"
    assert contacted["expires_at"] == now + timedelta(days=365)

    unchanged = store.update_pilot_request_status(
        request_id=request_id,
        status="contacted",
        admin_user_id="another-admin",
    )
    assert unchanged == contacted
    assert unchanged["status_updated_by"] == "private-admin-id"

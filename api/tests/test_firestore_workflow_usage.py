from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services import firestore_store
from app.services.firestore_store import FirestoreStore


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
    assert usage["sources"] == {"workflow_created:workflow": 1}
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

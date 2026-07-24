from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
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

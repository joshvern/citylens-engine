"""Auth-path Firestore write throttling.

Every JWT-authenticated request used to write `last_login_at` on the user doc
plus a patch on the identity doc, and every API-key request wrote
`last_used_at` — pure write amplification at request rate. The store now skips
those refresh writes when the stored activity timestamp is within
AUTH_ACTIVITY_WRITE_INTERVAL (15 min), while material changes (new email,
admin promotion, provider change) always write.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from app.services import firestore_store
from app.services.firestore_store import (
    AUTH_ACTIVITY_WRITE_INTERVAL,
    FirestoreStore,
    _hash_api_key,
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

    def set(self, value: dict[str, Any], *, merge: bool = False) -> None:
        self.client.writes.append((self.path, deepcopy(value)))
        if merge:
            existing = self.client.documents.get(self.path, {})
            self.client.documents[self.path] = {
                **deepcopy(existing),
                **deepcopy(value),
            }
        else:
            self.client.documents[self.path] = deepcopy(value)

    def collection(self, name: str) -> "_Collection":
        return _Collection(self.client, (*self.path, name))


class _Collection:
    def __init__(self, client: "_Client", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def document(self, identifier: str) -> _Document:
        return _Document(self.client, (*self.path, identifier))


class _Client:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, ...], dict[str, Any]] = {}
        self.writes: list[tuple[tuple[str, ...], dict[str, Any]]] = []

    def collection(self, name: str) -> _Collection:
        return _Collection(self, (name,))


_NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def store(monkeypatch) -> FirestoreStore:
    monkeypatch.setattr(firestore_store, "utcnow", lambda: _NOW)
    client = _Client()
    return FirestoreStore(project_id="test", client=client)  # type: ignore[arg-type]


def _login(store: FirestoreStore, **overrides) -> dict[str, Any]:
    kwargs: dict[str, Any] = dict(
        provider="neon",
        subject="sub-1",
        email="user@example.com",
        email_verified=True,
        is_admin_override=False,
    )
    kwargs.update(overrides)
    return store.get_or_create_user_by_identity(**kwargs)


def test_first_login_writes_then_immediate_repeat_does_not(store) -> None:
    client = store.client

    _login(store)
    first_writes = len(client.writes)
    assert first_writes == 2  # user doc + identity doc

    doc = _login(store)
    assert len(client.writes) == first_writes  # no refresh writes
    assert doc["user_id"]
    assert doc["last_login_at"] == _NOW


def test_stale_login_timestamp_writes_again(store, monkeypatch) -> None:
    client = store.client
    _login(store)
    baseline = len(client.writes)

    later = _NOW + AUTH_ACTIVITY_WRITE_INTERVAL + timedelta(seconds=1)
    monkeypatch.setattr(firestore_store, "utcnow", lambda: later)

    doc = _login(store)
    assert len(client.writes) == baseline + 2  # user + identity refreshed
    assert doc["last_login_at"] == later


def test_material_change_bypasses_throttle(store) -> None:
    client = store.client
    _login(store)
    baseline = len(client.writes)

    doc = _login(store, email="renamed@example.com")

    # Fresh timestamp, but the email changed — both docs must be rewritten.
    assert len(client.writes) == baseline + 2
    assert doc["email"] == "renamed@example.com"


def test_admin_promotion_bypasses_throttle(store) -> None:
    client = store.client
    _login(store)
    baseline = len(client.writes)

    doc = _login(store, is_admin_override=True)

    assert doc["is_admin"] is True
    assert doc["plan_type"] == "admin"
    assert len(client.writes) > baseline


def _seed_api_key(client: _Client, *, last_used_at: datetime | None) -> str:
    plaintext = "clk_live_testkey"
    plaintext_hash = _hash_api_key(plaintext)
    index_doc: dict[str, Any] = {
        "app_user_id": "user-1",
        "key_id": "key-1",
        "revoked_at": None,
    }
    if last_used_at is not None:
        index_doc["last_used_at"] = last_used_at
    client.documents[("api_keys_by_hash", plaintext_hash)] = index_doc
    client.documents[("users", "user-1", "api_keys", "key-1")] = {
        "key_id": "key-1",
        "last_used_at": last_used_at,
    }
    return plaintext


def test_api_key_first_use_writes_last_used_then_repeat_does_not(store) -> None:
    client = store.client
    plaintext = _seed_api_key(client, last_used_at=None)

    assert store.get_user_id_for_api_key(plaintext) == "user-1"
    # Key record + index mirror both refreshed.
    assert len(client.writes) == 2
    key_path = ("users", "user-1", "api_keys", "key-1")
    assert client.documents[key_path]["last_used_at"] == _NOW

    assert store.get_user_id_for_api_key(plaintext) == "user-1"
    assert len(client.writes) == 2  # throttled: no additional writes


def test_api_key_stale_last_used_writes_again(store, monkeypatch) -> None:
    client = store.client
    stale = _NOW - AUTH_ACTIVITY_WRITE_INTERVAL - timedelta(seconds=1)
    plaintext = _seed_api_key(client, last_used_at=stale)

    assert store.get_user_id_for_api_key(plaintext) == "user-1"

    assert len(client.writes) == 2
    key_path = ("users", "user-1", "api_keys", "key-1")
    assert client.documents[key_path]["last_used_at"] == _NOW


def test_api_key_revoked_still_rejected_without_writes(store) -> None:
    client = store.client
    plaintext = _seed_api_key(client, last_used_at=None)
    plaintext_hash = _hash_api_key(plaintext)
    client.documents[("api_keys_by_hash", plaintext_hash)]["revoked_at"] = _NOW

    assert store.get_user_id_for_api_key(plaintext) is None
    assert client.writes == []

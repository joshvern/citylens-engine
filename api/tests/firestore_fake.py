"""Shared in-memory Firestore client fake for driving REAL FirestoreStore
transactional methods in tests.

Mirrors the surface the production store exercises — document get/set (with
merge), single-filter ``where`` queries with ``order_by``/``limit``, and
client transactions — and, critically, emulates the real client's
READ-AFTER-WRITE guard: once a transaction has buffered a write, any further
``get(transaction=...)`` raises the real
:class:`google.cloud.firestore_v1._helpers.ReadAfterWriteError`, exactly
like the production client. Every test that runs a transactional store
method through this fake therefore also verifies the method's
reads-before-writes ordering for free.

Callers must monkeypatch ``firestore.transactional`` (on the store module
under test) to the identity decorator — the real decorator requires a live
gRPC transaction. The store's own ``_op`` wrappers still create one fake
transaction per attempt via ``client.transaction()``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

try:  # The real client's exception, so store code sees the genuine type.
    from google.cloud.firestore_v1._helpers import ReadAfterWriteError
except ImportError:  # pragma: no cover - only if the library reorganizes

    class ReadAfterWriteError(Exception):  # type: ignore[no-redef]
        """Raised when a read is attempted after a write."""


class FakeSnapshot:
    def __init__(self, value: dict[str, Any] | None) -> None:
        self.exists = value is not None
        self._value = value

    def to_dict(self) -> dict[str, Any] | None:
        return deepcopy(self._value)


class FakeDocument:
    def __init__(self, client: "FakeFirestoreClient", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def get(self, *, transaction: "FakeTransaction | None" = None) -> FakeSnapshot:
        if transaction is not None and transaction.write_count > 0:
            # Mirrors google.cloud.firestore_v1._helpers.get_transaction_id:
            # transactional reads are forbidden once writes are buffered.
            raise ReadAfterWriteError(
                "Attempted read after write in a transaction."
            )
        return FakeSnapshot(self.client.documents.get(self.path))

    def set(self, value: dict[str, Any], *, merge: bool = False) -> None:
        if merge:
            existing = self.client.documents.get(self.path, {})
            self.client.documents[self.path] = {
                **deepcopy(existing),
                **deepcopy(value),
            }
        else:
            self.client.documents[self.path] = deepcopy(value)

    def collection(self, name: str) -> "FakeCollection":
        return FakeCollection(self.client, (*self.path, name))


class FakeCollection:
    def __init__(self, client: "FakeFirestoreClient", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def document(self, identifier: str) -> FakeDocument:
        return FakeDocument(self.client, (*self.path, identifier))

    def where(self, *, filter) -> "FakeQuery":
        return FakeQuery(self.client, self.path, filter=filter)


class FakeQuery:
    def __init__(
        self,
        client: "FakeFirestoreClient",
        path: tuple[str, ...],
        *,
        filter,
        limit: int | None = None,
        order_by_field: str | None = None,
    ) -> None:
        self.client = client
        self.path = path
        self.filter = filter
        self._limit = limit
        self._order_by_field = order_by_field

    def limit(self, value: int) -> "FakeQuery":
        return FakeQuery(
            self.client,
            self.path,
            filter=self.filter,
            limit=value,
            order_by_field=self._order_by_field,
        )

    def order_by(self, field: str) -> "FakeQuery":
        # Ascending only, like the production reconciler scan. Honoring the
        # ordering is what lets the over-cap starvation test prove the query
        # — not just an in-memory sort — examines oldest-first.
        return FakeQuery(
            self.client,
            self.path,
            filter=self.filter,
            limit=self._limit,
            order_by_field=field,
        )

    def _matches(self, value: dict[str, Any]) -> bool:
        field = value.get(self.filter.field_path)
        if self.filter.op_string == "==":
            return field == self.filter.value
        if self.filter.op_string == "in":
            return field in self.filter.value
        raise NotImplementedError(self.filter.op_string)

    def stream(self):
        matches = [
            value
            for path, value in self.client.documents.items()
            if path[:-1] == self.path and self._matches(value)
        ]
        if self._order_by_field is not None:
            field = self._order_by_field
            matches.sort(key=lambda value: value.get(field))
        if self._limit is not None:
            matches = matches[: self._limit]
        return [FakeSnapshot(value) for value in matches]


class FakeTransaction:
    def __init__(self, client: "FakeFirestoreClient") -> None:
        self.client = client
        self.write_count = 0

    def set(
        self,
        reference: FakeDocument,
        value: dict[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        self.write_count += 1
        reference.set(value, merge=merge)


class FakeFirestoreClient:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, ...], dict[str, Any]] = {}

    def collection(self, name: str) -> FakeCollection:
        return FakeCollection(self, (name,))

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

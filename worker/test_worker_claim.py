"""Worker startup claim guard.

A queued Cloud Run job execution can be delayed long enough for the API's
stuck-run reconciler to mark the run failed AND refund its quota slot. The
old unconditional ``status="running"`` startup write would resurrect that
terminal run, letting a refunded run finish as succeeded — a free run. The
claim is now a Firestore transaction: terminal runs ("failed", "succeeded")
are refused and the worker exits 0 (a non-zero exit could retrigger the
job); "queued" — and defensively "running" — runs are claimed by writing
running+updated_at inside the transaction.

The store-level tests drive the REAL worker FirestoreStore against an
in-memory client fake (mirroring api/tests/test_run_reconciler.py); the
main()-level tests stub the store to prove the process-exit contract.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest

import worker as worker_module
from services import firestore_store as worker_store_module
from services.firestore_store import FirestoreStore

try:  # The real client's read-after-write exception type.
    from google.cloud.firestore_v1._helpers import ReadAfterWriteError
except ImportError:  # pragma: no cover - only if the library reorganizes

    class ReadAfterWriteError(Exception):  # type: ignore[no-redef]
        """Raised when a read is attempted after a write."""


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

    def get(self, *, transaction: "_Transaction | None" = None) -> _Snapshot:
        if transaction is not None and transaction.write_count > 0:
            # Same guard as the production client (and the api/tests shared
            # fake): transactional reads are forbidden once writes are
            # buffered, so the claim tests below also verify the
            # transaction's reads-before-writes ordering.
            raise ReadAfterWriteError(
                "Attempted read after write in a transaction."
            )
        return _Snapshot(self.client.documents.get(self.path))

    def set(self, value: dict[str, Any], *, merge: bool = False) -> None:
        if merge:
            existing = self.client.documents.get(self.path, {})
            self.client.documents[self.path] = {
                **deepcopy(existing),
                **deepcopy(value),
            }
        else:
            self.client.documents[self.path] = deepcopy(value)


class _Collection:
    def __init__(self, client: "_Client", path: tuple[str, ...]) -> None:
        self.client = client
        self.path = path

    def document(self, identifier: str) -> _Document:
        return _Document(self.client, (*self.path, identifier))


class _Transaction:
    def __init__(self, client: "_Client") -> None:
        self.client = client
        self.write_count = 0

    def set(
        self,
        reference: _Document,
        value: dict[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        self.write_count += 1
        reference.set(value, merge=merge)


class _Client:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, ...], dict[str, Any]] = {}

    def collection(self, name: str) -> _Collection:
        return _Collection(self, (name,))

    def transaction(self) -> _Transaction:
        return _Transaction(self)


@pytest.fixture
def fake_client(monkeypatch) -> _Client:
    monkeypatch.setattr(
        worker_store_module.firestore, "transactional", lambda function: function
    )
    return _Client()


@pytest.fixture
def real_store(fake_client) -> FirestoreStore:
    return FirestoreStore(project_id="test", client=fake_client)  # type: ignore[arg-type]


def _seed_run(client: _Client, *, run_id: str, status: str, **overrides: Any) -> None:
    stamp = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = {
        "run_id": run_id,
        "user_id": "u1",
        "status": status,
        "stage": status,
        "progress": 0,
        "request": {"address": "100 E 21st St Brooklyn, NY"},
        "error": None,
        "execution_id": "exec-1",
        "created_at": stamp,
        "updated_at": stamp,
    }
    doc.update(overrides)
    client.documents[("runs", run_id)] = doc


# ---- Store-level claim semantics ----------------------------------------


def test_fake_transaction_enforces_read_after_write(fake_client) -> None:
    """Meta-test: the guard the claim tests rely on is actually armed — a
    transactional get() after the first buffered write raises, exactly like
    the production client."""

    txn = fake_client.transaction()
    ref = fake_client.collection("runs").document("r1")
    ref.get(transaction=txn)  # reads before writes are fine
    txn.set(ref, {"status": "running"})
    with pytest.raises(ReadAfterWriteError):
        ref.get(transaction=txn)


def test_claim_queued_run_writes_running_in_transaction(
    fake_client, real_store
) -> None:
    _seed_run(fake_client, run_id="r-q", status="queued", stage="queued")

    claimed, run_doc = real_store.claim_run_for_processing("r-q")

    assert claimed is True
    assert run_doc is not None
    assert run_doc["request"] == {"address": "100 E 21st St Brooklyn, NY"}
    stored = fake_client.documents[("runs", "r-q")]
    assert stored["status"] == "running"
    assert stored["stage"] == "starting"
    assert stored["progress"] == 1
    assert stored["error"] is None
    # The claim bumps updated_at so the reconciler's staleness clock resets.
    assert stored["updated_at"] > datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_claim_running_run_is_claimed_defensively(fake_client, real_store) -> None:
    # Cloud Run job retries are disabled, but a duplicate execution must
    # not bypass the guard: "running" is claimable, terminal is not.
    _seed_run(fake_client, run_id="r-r", status="running", stage="segmentation")

    claimed, run_doc = real_store.claim_run_for_processing("r-r")

    assert claimed is True
    assert run_doc is not None
    assert fake_client.documents[("runs", "r-r")]["status"] == "running"


@pytest.mark.parametrize("status", ["failed", "succeeded"])
def test_claim_refuses_terminal_run_without_writing(
    fake_client, real_store, status: str
) -> None:
    _seed_run(
        fake_client,
        run_id="r-t",
        status=status,
        stage=status,
        progress=100,
        quota_refunded=True,
    )
    before = deepcopy(fake_client.documents[("runs", "r-t")])

    claimed, run_doc = real_store.claim_run_for_processing("r-t")

    assert claimed is False
    assert run_doc is not None
    assert run_doc["status"] == status
    # Untouched: the terminal run is not resurrected, so the
    # succeeded-but-refunded state can never come into existence.
    assert fake_client.documents[("runs", "r-t")] == before


def test_claim_missing_run_returns_none(real_store) -> None:
    claimed, run_doc = real_store.claim_run_for_processing("r-missing")
    assert claimed is False
    assert run_doc is None


# ---- main()-level exit contract ------------------------------------------


class _StubSettings:
    project_id = "test-project"
    runs_collection = "runs"
    bucket = "test-bucket"
    work_root = "/tmp/runs"


class _StubStore:
    def __init__(self, run_doc: dict[str, Any] | None) -> None:
        self.run_doc = run_doc
        self.claim_calls: list[str] = []
        self.update_calls: list[tuple[str, dict[str, Any]]] = []

    def claim_run_for_processing(self, run_id: str):
        self.claim_calls.append(run_id)
        if self.run_doc is None:
            return False, None
        if str(self.run_doc.get("status") or "") in ("failed", "succeeded"):
            return False, deepcopy(self.run_doc)
        self.run_doc = {
            **self.run_doc,
            "status": "running",
            "stage": "starting",
            "progress": 1,
            "error": None,
        }
        return True, deepcopy(self.run_doc)

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        self.update_calls.append((run_id, dict(patch)))
        self.run_doc = {**(self.run_doc or {}), **patch}


def _run_main(monkeypatch, store: _StubStore, *, pipeline_calls: list[dict]):
    monkeypatch.setenv("CITYLENS_RUN_ID", "r-main")
    monkeypatch.setattr(worker_module, "get_settings", lambda: _StubSettings())
    monkeypatch.setattr(worker_module, "FirestoreStore", lambda **_kw: store)
    monkeypatch.setattr(worker_module, "GcsArtifacts", lambda **_kw: object())
    monkeypatch.setattr(
        worker_module,
        "configure_json_logging",
        lambda **_kw: None,
    )

    def _fake_pipeline(**kwargs):
        pipeline_calls.append(kwargs)

    monkeypatch.setattr(worker_module, "run_pipeline", _fake_pipeline)
    return worker_module.main()


@pytest.mark.parametrize("status", ["failed", "succeeded"])
def test_main_exits_zero_without_processing_terminal_run(
    monkeypatch, status: str
) -> None:
    """A failed+refunded (or already succeeded) run must not be resurrected
    by a delayed execution: main() logs and exits 0 — exit 0 on purpose, so
    the job cannot retrigger — without touching the pipeline or the doc."""

    store = _StubStore(
        {
            "run_id": "r-main",
            "status": status,
            "stage": status,
            "quota_refunded": True,
            "request": {"address": "x"},
        }
    )
    pipeline_calls: list[dict] = []

    exit_code = _run_main(monkeypatch, store, pipeline_calls=pipeline_calls)

    assert exit_code == 0
    assert store.claim_calls == ["r-main"]
    assert pipeline_calls == []
    assert store.update_calls == []
    assert store.run_doc["status"] == status
    assert store.run_doc["quota_refunded"] is True


def test_main_claims_queued_run_and_processes(monkeypatch) -> None:
    store = _StubStore(
        {
            "run_id": "r-main",
            "status": "queued",
            "stage": "queued",
            "request": {"address": "100 E 21st St Brooklyn, NY"},
        }
    )
    pipeline_calls: list[dict] = []

    exit_code = _run_main(monkeypatch, store, pipeline_calls=pipeline_calls)

    assert exit_code == 0
    assert store.claim_calls == ["r-main"]
    assert len(pipeline_calls) == 1
    assert pipeline_calls[0]["run_id"] == "r-main"
    assert pipeline_calls[0]["request_dict"] == {
        "address": "100 E 21st St Brooklyn, NY"
    }
    assert store.run_doc["status"] == "running"


def test_main_missing_run_still_raises(monkeypatch) -> None:
    store = _StubStore(None)
    pipeline_calls: list[dict] = []

    with pytest.raises(RuntimeError, match="Run not found"):
        _run_main(monkeypatch, store, pipeline_calls=pipeline_calls)

    assert pipeline_calls == []

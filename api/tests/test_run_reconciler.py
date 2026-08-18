"""Stuck-run reconciler (POST /v1/admin/runs/reconcile).

A worker killed by the Cloud Run task timeout (SIGKILL) or OOM never
reaches its exception handler, leaving the run "running" forever — which
permanently bricks a free account (max_concurrent_runs=1 counts queued +
running). These tests drive the REAL FirestoreStore logic (the status-in
query, mark_failed, and the idempotent refund transaction) against an
in-memory Firestore client fake, mirroring test_firestore_workflow_usage.py
so the fake exercises the same single-filter query shape the production
store uses (no composite-index-only compound queries).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

# The shared in-memory fake enforces the real client's read-after-write
# guard inside transactions, so every reconcile/refund transaction driven
# through it also proves its reads-before-writes ordering.
from firestore_fake import FakeFirestoreClient as _Client

from app.main import app
from app.routes import admin_runs
from app.services import auth as auth_module
from app.services import firestore_store
from app.services.firestore_store import FirestoreStore

ADMIN_KEY = "test-admin-reconcile-key-123"
ADMIN_KEY_HASH = hashlib.sha256(ADMIN_KEY.encode("utf-8")).hexdigest()


def _month_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _seed_run(
    client: _Client,
    *,
    run_id: str,
    user_id: str,
    status: str,
    age_minutes: int,
    stage: str = "segmentation",
    **overrides: Any,
) -> datetime:
    now = datetime.now(timezone.utc)
    stamp = now - timedelta(minutes=age_minutes)
    doc = {
        "run_id": run_id,
        "user_id": user_id,
        "status": status,
        "stage": stage,
        "progress": 40,
        "request": {"address": "100 E 21st St Brooklyn, NY"},
        "error": None,
        "execution_id": "exec-1",
        "created_at": stamp,
        "updated_at": stamp,
    }
    doc.update(overrides)
    client.documents[("runs", run_id)] = doc
    return stamp


def _seed_usage(client: _Client, *, user_id: str, when: datetime, used: int) -> tuple[str, ...]:
    mk = _month_key(when.astimezone(timezone.utc))
    path = ("usage_months", f"{user_id}_{mk}")
    client.documents[path] = {
        "app_user_id": user_id,
        "month_key": mk,
        "runs_used": used,
        "updated_at": when,
    }
    return path


@pytest.fixture
def fake_client(monkeypatch) -> _Client:
    monkeypatch.setattr(
        firestore_store.firestore, "transactional", lambda function: function
    )
    return _Client()


@pytest.fixture
def real_store(fake_client) -> FirestoreStore:
    return FirestoreStore(project_id="test", client=fake_client)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides = {}


def _install(store: FirestoreStore) -> TestClient:
    app.dependency_overrides[admin_runs.get_store] = lambda: store
    return TestClient(app)


def test_reconcile_requires_admin(fake_client, real_store, auth_override) -> None:
    _seed_run(fake_client, run_id="r-stale", user_id="u1", status="running", age_minutes=90)
    client = _install(real_store)

    unauthenticated = client.post("/v1/admin/runs/reconcile")
    assert unauthenticated.status_code == 401
    # Admin responses (including early errors) never enter shared caches.
    assert unauthenticated.headers["cache-control"] == "private, no-store"

    auth_override(app_user_id="regular-user", is_admin=False)
    forbidden = client.post("/v1/admin/runs/reconcile")
    assert forbidden.status_code == 403

    # Nothing was reconciled along the way.
    assert fake_client.documents[("runs", "r-stale")]["status"] == "running"


def test_stale_running_run_fails_and_refunds_exactly_once(
    fake_client, real_store, auth_override
) -> None:
    stamp = _seed_run(
        fake_client, run_id="r-stuck", user_id="u1", status="running", age_minutes=90
    )
    usage_path = _seed_usage(fake_client, user_id="u1", when=stamp, used=3)
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200, resp.text
    assert resp.headers["cache-control"] == "private, no-store"
    body = resp.json()
    assert body == {
        "examined": 1,
        "reconciled": 1,
        "refunded": 1,
        "skipped_now_active": 0,
        "run_ids": ["r-stuck"],
    }

    run = fake_client.documents[("runs", "r-stuck")]
    assert run["status"] == "failed"
    assert run["stage"] == "failed"
    assert run["progress"] == 100
    assert run["error"]["code"] == "WORKER_TIMEOUT"
    assert "exceeded its processing window" in run["error"]["message"]
    # The error payload preserves the stage where the worker stalled.
    assert run["error"]["stage"] == "segmentation"
    assert run["quota_refunded"] is True
    assert fake_client.documents[usage_path]["runs_used"] == 2

    # Re-run: the run is no longer active, so nothing is examined again and
    # the refund cannot double-apply.
    again = client.post("/v1/admin/runs/reconcile")
    assert again.status_code == 200
    assert again.json() == {
        "examined": 0,
        "reconciled": 0,
        "refunded": 0,
        "skipped_now_active": 0,
        "run_ids": [],
    }
    assert fake_client.documents[usage_path]["runs_used"] == 2
    # And the shared refund path itself reports "already refunded".
    assert real_store.refund_run_quota_if_failed("r-stuck") is False


def test_fresh_runs_and_terminal_runs_are_untouched(
    fake_client, real_store, auth_override
) -> None:
    _seed_run(fake_client, run_id="r-fresh-run", user_id="u1", status="running", age_minutes=5)
    _seed_run(fake_client, run_id="r-fresh-queue", user_id="u1", status="queued", age_minutes=1)
    _seed_run(fake_client, run_id="r-done", user_id="u1", status="succeeded", age_minutes=500)
    _seed_run(fake_client, run_id="r-failed", user_id="u1", status="failed", age_minutes=500)
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Only the two active runs are candidates; neither is stale.
    assert body == {
        "examined": 2,
        "reconciled": 0,
        "refunded": 0,
        "skipped_now_active": 0,
        "run_ids": [],
    }
    assert fake_client.documents[("runs", "r-fresh-run")]["status"] == "running"
    assert fake_client.documents[("runs", "r-fresh-queue")]["status"] == "queued"
    assert fake_client.documents[("runs", "r-done")]["status"] == "succeeded"


def test_stale_queued_run_is_reconciled_too(
    fake_client, real_store, auth_override
) -> None:
    # Trigger succeeded but the worker never started (or died before its
    # first status write): the run sits "queued" forever.
    stamp = _seed_run(
        fake_client,
        run_id="r-ghost",
        user_id="u2",
        status="queued",
        age_minutes=120,
        stage="queued",
        progress=0,
    )
    usage_path = _seed_usage(fake_client, user_id="u2", when=stamp, used=1)
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200, resp.text
    assert resp.json()["reconciled"] == 1
    assert resp.json()["refunded"] == 1
    run = fake_client.documents[("runs", "r-ghost")]
    assert run["status"] == "failed"
    assert run["error"]["code"] == "WORKER_TIMEOUT"
    assert run["error"]["stage"] == "queued"
    assert fake_client.documents[usage_path]["runs_used"] == 0


def test_already_refunded_run_reconciles_without_second_refund(
    fake_client, real_store, auth_override
) -> None:
    stamp = _seed_run(
        fake_client,
        run_id="r-pre-refunded",
        user_id="u3",
        status="running",
        age_minutes=90,
        quota_refunded=True,
    )
    usage_path = _seed_usage(fake_client, user_id="u3", when=stamp, used=1)
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200
    body = resp.json()
    assert body["reconciled"] == 1
    assert body["refunded"] == 0
    assert fake_client.documents[("runs", "r-pre-refunded")]["status"] == "failed"
    assert fake_client.documents[usage_path]["runs_used"] == 1


def test_staleness_threshold_and_batch_size_come_from_env(
    fake_client, real_store, auth_override, monkeypatch
) -> None:
    # 40-minute-old runs: stale at the default 35, fresh at 120.
    _seed_run(fake_client, run_id="r-a", user_id="u1", status="running", age_minutes=40)
    _seed_run(fake_client, run_id="r-b", user_id="u1", status="running", age_minutes=300)
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    monkeypatch.setenv("CITYLENS_RUN_STALE_MINUTES", "120")
    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200
    # Only the 300-minute-old run crosses the raised threshold.
    assert resp.json()["run_ids"] == ["r-b"]
    assert fake_client.documents[("runs", "r-a")]["status"] == "running"

    # Batch bound: with two stale runs and batch=1, the oldest goes first
    # and the next pass drains the remainder — safe under a scheduler loop.
    _seed_run(fake_client, run_id="r-c", user_id="u1", status="running", age_minutes=200)
    monkeypatch.setenv("CITYLENS_RUN_STALE_MINUTES", "35")
    monkeypatch.setenv("CITYLENS_RUN_RECONCILE_BATCH_SIZE", "1")
    first = client.post("/v1/admin/runs/reconcile")
    assert first.status_code == 200
    assert first.json()["examined"] == 2
    assert first.json()["run_ids"] == ["r-c"]  # oldest remaining first
    second = client.post("/v1/admin/runs/reconcile")
    assert second.json()["run_ids"] == ["r-a"]
    third = client.post("/v1/admin/runs/reconcile")
    assert third.json() == {
        "examined": 0,
        "reconciled": 0,
        "refunded": 0,
        "skipped_now_active": 0,
        "run_ids": [],
    }


def test_run_that_progressed_between_scan_and_transaction_is_skipped(
    fake_client, real_store, auth_override, monkeypatch
) -> None:
    """The scan is only a snapshot: a queued execution delayed past the
    staleness window can start (or finish) AFTER the scan and BEFORE the
    failure write. The fail-and-refund transaction re-reads the run and
    must skip it — never overwrite a legitimately live/succeeded run, and
    never refund a run that is no longer failed."""

    _seed_run(
        fake_client, run_id="r-finished", user_id="u1", status="running", age_minutes=90
    )
    _seed_run(
        fake_client, run_id="r-claimed", user_id="u1", status="queued", age_minutes=90
    )
    usage_path = _seed_usage(
        fake_client, user_id="u1", when=datetime.now(timezone.utc), used=3
    )
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)

    original_scan = real_store.list_stale_active_runs

    def scan_then_progress(**kwargs):
        result = original_scan(**kwargs)
        now = datetime.now(timezone.utc)
        # A delayed worker finished one run and claimed the other after the
        # scan snapshot was taken but before the reconcile transactions.
        finished = fake_client.documents[("runs", "r-finished")]
        finished["status"] = "succeeded"
        finished["updated_at"] = now
        claimed = fake_client.documents[("runs", "r-claimed")]
        claimed["status"] = "running"
        claimed["updated_at"] = now
        return result

    monkeypatch.setattr(real_store, "list_stale_active_runs", scan_then_progress)

    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "examined": 2,
        "reconciled": 0,
        "refunded": 0,
        "skipped_now_active": 2,
        "run_ids": [],
    }
    # Neither run was overwritten and no refund happened: the
    # succeeded-but-refunded state is impossible.
    finished = fake_client.documents[("runs", "r-finished")]
    assert finished["status"] == "succeeded"
    assert "quota_refunded" not in finished
    assert fake_client.documents[("runs", "r-claimed")]["status"] == "running"
    assert fake_client.documents[usage_path]["runs_used"] == 3


def test_over_cap_scan_examines_oldest_first(
    fake_client, real_store, auth_override, monkeypatch
) -> None:
    """When the active population exceeds the scan cap, the ordered query
    (status IN + ORDER BY updated_at ASC — composite index in production)
    must surface the OLDEST runs; an unordered first-N read could repeatedly
    select the same fresh subset and starve stale runs forever."""

    monkeypatch.setattr(FirestoreStore, "RECONCILE_SCAN_CAP", 3)
    # Insertion order is deliberately freshest-first so an unordered scan
    # would fill its cap with fresh runs and never see the stale ones.
    _seed_run(fake_client, run_id="r-f1", user_id="u1", status="running", age_minutes=1)
    _seed_run(fake_client, run_id="r-f2", user_id="u1", status="running", age_minutes=2)
    _seed_run(fake_client, run_id="r-f3", user_id="u1", status="queued", age_minutes=3)
    _seed_run(
        fake_client, run_id="r-old2", user_id="u1", status="running", age_minutes=400
    )
    _seed_run(
        fake_client, run_id="r-old1", user_id="u1", status="queued", age_minutes=500
    )

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=35)
    stale, examined = real_store.list_stale_active_runs(cutoff=cutoff, limit=50)
    assert examined == 3  # capped scan
    # The capped window contains the oldest runs, oldest first — the fresh
    # majority cannot starve them.
    assert [run["run_id"] for run in stale] == ["r-old1", "r-old2"]

    # End-to-end: the route reconciles exactly those oldest runs.
    auth_override(app_user_id="admin-user", is_admin=True)
    client = _install(real_store)
    resp = client.post("/v1/admin/runs/reconcile")
    assert resp.status_code == 200, resp.text
    assert resp.json()["run_ids"] == ["r-old1", "r-old2"]
    assert fake_client.documents[("runs", "r-old1")]["status"] == "failed"
    assert fake_client.documents[("runs", "r-old2")]["status"] == "failed"
    assert fake_client.documents[("runs", "r-f1")]["status"] == "running"


def test_hash_only_admin_api_key_can_reconcile(
    fake_client, real_store, monkeypatch
) -> None:
    """The Cloud Scheduler path: X-API-Key checked against
    CITYLENS_ADMIN_API_KEY_HASHES via the REAL require_auth dependency."""

    monkeypatch.setenv("CITYLENS_ALLOW_ADMIN_API_KEYS", "true")
    monkeypatch.setenv("CITYLENS_ADMIN_API_KEY_HASHES", ADMIN_KEY_HASH)
    monkeypatch.setattr(auth_module, "_store_factory", lambda settings: real_store)
    stamp = _seed_run(
        fake_client, run_id="r-sched", user_id="u9", status="running", age_minutes=90
    )
    usage_path = _seed_usage(fake_client, user_id="u9", when=stamp, used=1)
    client = _install(real_store)

    wrong_key = client.post(
        "/v1/admin/runs/reconcile", headers={"X-API-Key": "not-the-admin-key"}
    )
    assert wrong_key.status_code == 401

    resp = client.post(
        "/v1/admin/runs/reconcile", headers={"X-API-Key": ADMIN_KEY}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["run_ids"] == ["r-sched"]
    assert fake_client.documents[("runs", "r-sched")]["status"] == "failed"
    assert fake_client.documents[usage_path]["runs_used"] == 0

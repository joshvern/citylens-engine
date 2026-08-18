"""Idempotency-Key on POST /v1/runs.

A retried or double-clicked POST must not burn a second monthly quota
slot or launch a second Cloud Run job. The header is OPTIONAL — keyless
requests keep today's behavior exactly.

The keyed create path is TRANSACTIONAL: duplicate re-check, monthly-quota
increment (with the limit check), and run creation are one atomic step in
the store, so a same-key race loser gets the winner's run back without
ever touching the counter — no reserve-then-release window, no reliance
on a best-effort decrement. The route-level fakes here mirror those
semantics and the race tests assert them without masking (the loser's
decrement path is tracked and must stay untouched).

The final section drives the REAL ``FirestoreStore.create_run_idempotent``
transaction against the shared in-memory Firestore client fake (same
pattern as test_run_reconciler.py) — including the fake's read-after-write
guard, which fails any regression that reorders the transaction's reads
after its writes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from firestore_fake import FakeFirestoreClient, ReadAfterWriteError

from app.main import app
from app.routes import runs as runs_routes
from app.routes.runs import _idempotent_run_id, _request_fingerprint
from app.services import firestore_store as firestore_store_module
from app.services.core_adapter import CitylensRequest
from app.services.firestore_store import FirestoreStore, MonthlyQuotaExceeded
from app.services.run_options import DEFAULT_AOI_RADIUS_M


def _canonical_request(address: str = "1 Main St") -> dict:
    """The validated payload exactly as the route computes it."""

    return CitylensRequest.model_validate(
        {
            "address": address,
            "aoi_radius_m": DEFAULT_AOI_RADIUS_M,
            "imagery_year": 2024,
            "baseline_year": 2017,
            "segmentation_backend": "sam2",
            "outputs": ["previews", "change", "mesh"],
            "notes": None,
        }
    ).model_dump(mode="json")


class _NoOpGcs:
    def signed_url(self, **_kwargs):
        return None


class FakeStore:
    def __init__(self, *, concurrent: int = 0) -> None:
        self.concurrent = concurrent
        self.runs: dict[str, dict] = {}
        self.usage: dict[tuple[str, str], int] = {}
        self.reserve_calls = 0
        self.decrement_calls = 0

    def count_user_concurrent_runs(self, *, user_id: str):
        return self.concurrent

    def try_increment_monthly_usage(self, *, app_user_id, month_key, limit):
        used = self.usage.get((app_user_id, month_key), 0)
        if limit is not None and used >= limit:
            raise MonthlyQuotaExceeded(
                runs_used=used, monthly_run_limit=int(limit), month_key=month_key
            )
        self.reserve_calls += 1
        new_used = used + 1
        self.usage[(app_user_id, month_key)] = new_used
        return new_used

    def decrement_monthly_usage(self, *, app_user_id, month_key):
        self.decrement_calls += 1
        used = self.usage.get((app_user_id, month_key), 0)
        new_used = max(0, used - 1)
        self.usage[(app_user_id, month_key)] = new_used
        return new_used

    def _doc(
        self,
        *,
        run_id: str,
        user_id: str,
        request_dict: dict,
        request_fingerprint: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "run_id": run_id,
            "user_id": user_id,
            "status": "queued",
            "stage": "queued",
            "progress": 0,
            "request": request_dict,
            "request_fingerprint": request_fingerprint,
            "error": None,
            "execution_id": None,
            "created_at": now,
            "updated_at": now,
        }

    def create_run(self, *, user_id: str, request_dict: dict):
        run_id = uuid.uuid4().hex
        doc = self._doc(run_id=run_id, user_id=user_id, request_dict=request_dict)
        self.runs[run_id] = doc
        return doc

    def create_run_idempotent(
        self,
        *,
        run_id: str,
        user_id: str,
        request_dict: dict,
        request_fingerprint: str,
        month_key: str,
        monthly_limit,
    ):
        """Mirror of the real transactional semantics: an existing doc wins
        with NO usage increment; otherwise the limit check + increment +
        create happen as one atomic step."""

        existing = self.runs.get(run_id)
        if existing is not None:
            return existing, False
        used = self.usage.get((user_id, month_key), 0)
        if monthly_limit is not None and used >= monthly_limit:
            raise MonthlyQuotaExceeded(
                runs_used=used,
                monthly_run_limit=int(monthly_limit),
                month_key=month_key,
            )
        self.reserve_calls += 1
        self.usage[(user_id, month_key)] = used + 1
        doc = self._doc(
            run_id=run_id,
            user_id=user_id,
            request_dict=request_dict,
            request_fingerprint=request_fingerprint,
        )
        self.runs[run_id] = doc
        return doc, True

    def get_run(self, run_id: str):
        return self.runs.get(run_id)

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        run = self.runs.get(run_id)
        if run is not None:
            run["execution_id"] = execution_id

    def mark_failed(self, run_id: str, error) -> None:
        return None

    def list_artifacts(self, run_id: str):
        return []


class LateWinnerStore(FakeStore):
    """Simulates the winner's run landing between the loser's optimistic
    pre-check and its later reads: the FIRST get_run misses, every later
    read sees the winner."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.get_run_calls = 0

    def get_run(self, run_id: str):
        self.get_run_calls += 1
        if self.get_run_calls == 1:
            return None
        return super().get_run(run_id)


class FakeTrigger:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def run(self, *, run_id: str) -> str:
        self.calls.append(run_id)
        return f"exec-{len(self.calls)}"


def _install(store: FakeStore) -> tuple[TestClient, FakeTrigger]:
    trigger = FakeTrigger()
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: trigger
    app.dependency_overrides[runs_routes.get_gcs_lazy] = lambda: (
        lambda: _NoOpGcs()
    )
    return TestClient(app), trigger


KEY = "retry-safe-key-0001"


def _seed_winner(store: FakeStore, *, user_id: str, address: str = "1 Main St") -> str:
    """Plant the concurrent winner's deterministic run doc, fingerprint and
    all, exactly as the winning request would have created it."""

    run_id = _idempotent_run_id(user_id=user_id, idempotency_key=KEY)
    request_dict = _canonical_request(address)
    store.runs[run_id] = store._doc(
        run_id=run_id,
        user_id=user_id,
        request_dict=request_dict,
        request_fingerprint=_request_fingerprint(request_dict),
    )
    return run_id


def test_same_key_twice_creates_one_run_one_reservation_one_trigger(
    auth_override,
) -> None:
    auth_override(app_user_id="u-idem", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    first = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert first.status_code == 200, first.text
    run_id = first.json()["run_id"]
    assert run_id == _idempotent_run_id(user_id="u-idem", idempotency_key=KEY)

    replay = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["run_id"] == run_id

    assert len(store.runs) == 1
    assert store.reserve_calls == 1
    assert sum(store.usage.values()) == 1
    assert trigger.calls == [run_id]


def test_replay_returns_existing_run_current_state(auth_override) -> None:
    auth_override(app_user_id="u-replay", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    created = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    run_id = created.json()["run_id"]
    # The worker finished in the meantime.
    store.runs[run_id]["status"] = "succeeded"
    store.runs[run_id]["stage"] = "complete"
    store.runs[run_id]["progress"] = 100

    replay = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "succeeded"
    assert trigger.calls == [run_id]


def test_replay_of_succeeded_run_includes_artifacts(auth_override) -> None:
    """A replay must be a faithful representation of the run — the same
    artifact presentation as GET /v1/runs/{run_id}, not artifacts=[]."""

    auth_override(app_user_id="u-replay-art", plan_type="free")
    store = FakeStore()
    client, _trigger = _install(store)

    created = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    run_id = created.json()["run_id"]
    store.runs[run_id]["status"] = "succeeded"
    store.runs[run_id]["stage"] = "complete"
    store.runs[run_id]["progress"] = 100
    # Worker-written compact artifact map (bucket matches conftest env).
    store.runs[run_id]["artifacts"] = {
        "mesh.ply": f"gs://test-bucket/runs/{run_id}/mesh.ply",
        "change.geojson": f"gs://test-bucket/runs/{run_id}/change.geojson",
    }

    replay = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert replay.status_code == 200, replay.text
    artifacts = replay.json()["artifacts"]
    assert sorted(a["name"] for a in artifacts) == ["change.geojson", "mesh.ply"]
    by_name = {a["name"]: a for a in artifacts}
    assert by_name["mesh.ply"]["gcs_object"] == f"runs/{run_id}/mesh.ply"
    assert by_name["mesh.ply"]["gcs_uri"] == (
        f"gs://test-bucket/runs/{run_id}/mesh.ply"
    )


def test_same_key_different_payload_is_409_key_reused(auth_override) -> None:
    auth_override(app_user_id="u-fp", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    first = client.post(
        "/v1/runs",
        json={"address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert first.status_code == 200, first.text

    conflict = client.post(
        "/v1/runs",
        json={"address": "2 Other Ave"},
        headers={"Idempotency-Key": KEY},
    )
    assert conflict.status_code == 409, conflict.text
    detail = conflict.json()["detail"]
    assert detail["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert "different" in detail["message"]

    # The conflicting request consumed nothing.
    assert len(store.runs) == 1
    assert sum(store.usage.values()) == 1
    assert len(trigger.calls) == 1


def test_fingerprint_ignores_irrelevant_ordering(auth_override) -> None:
    """Same semantic request, different JSON field order and different
    ``outputs`` order → same fingerprint → replay, not 409."""

    auth_override(app_user_id="u-fp-order", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    first = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "outputs": ["mesh", "change", "previews"]},
        headers={"Idempotency-Key": KEY},
    )
    assert first.status_code == 200, first.text
    run_id = first.json()["run_id"]

    replay = client.post(
        "/v1/runs",
        json={"outputs": ["previews", "mesh", "change"], "address": "1 Main St"},
        headers={"Idempotency-Key": KEY},
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["run_id"] == run_id
    assert len(store.runs) == 1
    assert trigger.calls == [run_id]


def test_different_users_same_key_get_two_runs(auth_override) -> None:
    store = FakeStore()
    client, trigger = _install(store)

    auth_override(app_user_id="u-alpha", plan_type="free")
    first = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert first.status_code == 200, first.text

    auth_override(app_user_id="u-beta", plan_type="free")
    second = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert second.status_code == 200, second.text

    assert first.json()["run_id"] != second.json()["run_id"]
    assert len(store.runs) == 2
    assert store.reserve_calls == 2
    assert len(trigger.calls) == 2


def test_malformed_key_is_422_and_reserves_nothing(auth_override) -> None:
    auth_override(app_user_id="u-bad-key", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    for bad in ("too-short", "has spaces here-but-long-enough", "bad!chars#here-1234"):
        resp = client.post(
            "/v1/runs",
            json={"address": "1 Main St"},
            headers={"Idempotency-Key": bad},
        )
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"]["code"] == "INVALID_IDEMPOTENCY_KEY"

    assert store.runs == {}
    assert store.reserve_calls == 0
    assert trigger.calls == []


def test_no_key_behavior_unchanged_two_posts_two_runs(auth_override) -> None:
    auth_override(app_user_id="u-no-key", plan_type="free")
    store = FakeStore()
    client, trigger = _install(store)

    first = client.post("/v1/runs", json={"address": "1 Main St"})
    second = client.post("/v1/runs", json={"address": "1 Main St"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["run_id"] != second.json()["run_id"]
    assert len(store.runs) == 2
    assert store.reserve_calls == 2
    assert len(trigger.calls) == 2


def test_lost_race_in_create_transaction_returns_winner_without_quota(
    auth_override,
) -> None:
    """The winner appears AFTER the loser's pre-check but before its create
    transaction. The transaction finds the winner's doc and the loser gets
    it back having consumed nothing — no increment, no best-effort
    decrement, no second job."""

    auth_override(app_user_id="u-race", plan_type="free")
    store = LateWinnerStore()
    run_id = _seed_winner(store, user_id="u-race")
    client, trigger = _install(store)

    resp = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["run_id"] == run_id
    assert len(store.runs) == 1
    # Net-zero extra quota WITHOUT relying on a release: the loser never
    # incremented, so nothing ever needed decrementing.
    assert store.reserve_calls == 0
    assert store.decrement_calls == 0
    assert sum(store.usage.values()) == 0
    assert trigger.calls == []


def test_concurrency_429_loser_with_key_receives_winners_run(
    auth_override,
) -> None:
    """Free plan allows 1 concurrent run, and it is the WINNER's queued run
    that fills the slot. The loser's concurrency pre-check 429s — but the
    winner's run beats a 429: the route re-reads the deterministic doc and
    replays it."""

    auth_override(app_user_id="u-race-429", plan_type="free")
    store = LateWinnerStore(concurrent=1)
    run_id = _seed_winner(store, user_id="u-race-429")
    client, trigger = _install(store)

    resp = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["run_id"] == run_id
    assert len(store.runs) == 1
    assert store.reserve_calls == 0
    assert store.decrement_calls == 0
    assert sum(store.usage.values()) == 0
    assert trigger.calls == []


def test_concurrency_429_without_winner_is_still_429(auth_override) -> None:
    auth_override(app_user_id="u-429-real", plan_type="free")
    store = FakeStore(concurrent=1)  # slot filled by an unrelated run
    client, trigger = _install(store)

    resp = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert resp.status_code == 429, resp.text
    assert resp.json()["detail"]["code"] == "CONCURRENT_LIMIT_EXCEEDED"
    assert store.runs == {}
    assert trigger.calls == []


def test_monthly_quota_429_loser_with_key_receives_winners_run(
    auth_override,
) -> None:
    """The winner consumed the final monthly slot concurrently: the loser's
    create transaction raises the quota error, but the follow-up read finds
    the winner's doc and replays it instead of returning 429."""

    class QuotaRaceStore(LateWinnerStore):
        def create_run_idempotent(self, **kwargs):
            raise MonthlyQuotaExceeded(
                runs_used=5, monthly_run_limit=5, month_key=kwargs["month_key"]
            )

    auth_override(app_user_id="u-race-quota", plan_type="free")
    store = QuotaRaceStore()
    run_id = _seed_winner(store, user_id="u-race-quota")
    client, trigger = _install(store)

    resp = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["run_id"] == run_id
    assert store.decrement_calls == 0
    assert trigger.calls == []


def test_monthly_quota_429_without_winner_is_still_429(auth_override) -> None:
    auth_override(app_user_id="u-quota-real", plan_type="free")
    store = FakeStore()
    now = datetime.now(timezone.utc)
    mk = f"{now.year:04d}-{now.month:02d}"
    store.usage[("u-quota-real", mk)] = 5  # free monthly limit (conftest env)
    client, trigger = _install(store)

    resp = client.post(
        "/v1/runs", json={"address": "1 Main St"}, headers={"Idempotency-Key": KEY}
    )
    assert resp.status_code == 429, resp.text
    detail = resp.json()["detail"]
    assert detail["code"] == "MONTHLY_QUOTA_EXCEEDED"
    assert detail["monthly_run_limit"] == 5
    assert detail["runs_used"] == 5
    assert store.runs == {}
    assert store.usage[("u-quota-real", mk)] == 5
    assert trigger.calls == []


# ---- REAL FirestoreStore.create_run_idempotent --------------------------
#
# Everything above exercises route behavior through route-level fakes. This
# section drives the REAL transactional store method against the shared
# in-memory Firestore client fake (mirroring test_run_reconciler.py). The
# fake's transactions enforce the production client's read-after-write rule,
# so these tests also verify the transaction reads the run doc and the
# usage doc BEFORE buffering any write.


@pytest.fixture
def fake_client(monkeypatch) -> FakeFirestoreClient:
    monkeypatch.setattr(
        firestore_store_module.firestore,
        "transactional",
        lambda function: function,
    )
    return FakeFirestoreClient()


@pytest.fixture
def real_store(fake_client) -> FirestoreStore:
    return FirestoreStore(project_id="test", client=fake_client)  # type: ignore[arg-type]


_MK = "2026-08"
_FP = "a" * 64


def test_fake_transaction_enforces_read_after_write() -> None:
    """Meta-test: the guard the real-store tests rely on is actually armed —
    a transactional get() after the transaction's first buffered write must
    raise, exactly like the production client."""

    client = FakeFirestoreClient()
    txn = client.transaction()
    ref = client.collection("runs").document("r1")
    ref.get(transaction=txn)  # reads before writes are fine
    txn.set(ref, {"status": "queued"})
    with pytest.raises(ReadAfterWriteError):
        ref.get(transaction=txn)


def test_real_store_fresh_key_creates_run_and_increments_atomically(
    fake_client, real_store
) -> None:
    doc, created = real_store.create_run_idempotent(
        run_id="run-fresh",
        user_id="u1",
        request_dict={"address": "1 Main St"},
        request_fingerprint=_FP,
        month_key=_MK,
        monthly_limit=5,
    )

    assert created is True
    assert doc["status"] == "queued"
    # The fingerprint is persisted on the doc at create.
    assert doc["request_fingerprint"] == _FP
    stored = fake_client.documents[("runs", "run-fresh")]
    assert stored["request_fingerprint"] == _FP
    assert stored["request"] == {"address": "1 Main St"}
    usage = fake_client.documents[("usage_months", f"u1_{_MK}")]
    assert usage["runs_used"] == 1
    assert usage["app_user_id"] == "u1"
    assert usage["month_key"] == _MK
    assert "created_at" in usage  # first write stamps created_at

    # A second, different key increments the existing counter (no reset).
    _doc2, created2 = real_store.create_run_idempotent(
        run_id="run-fresh-2",
        user_id="u1",
        request_dict={"address": "1 Main St"},
        request_fingerprint=_FP,
        month_key=_MK,
        monthly_limit=5,
    )
    assert created2 is True
    assert fake_client.documents[("usage_months", f"u1_{_MK}")]["runs_used"] == 2


def test_real_store_duplicate_key_returns_existing_without_increment(
    fake_client, real_store
) -> None:
    """The duplicate check runs BEFORE the usage increment: the loser gets
    the winner's doc back and the counter is untouched — a regression that
    increments before checking existence fails on the usage assertion."""

    winner, created = real_store.create_run_idempotent(
        run_id="run-dup",
        user_id="u2",
        request_dict={"address": "1 Main St"},
        request_fingerprint=_FP,
        month_key=_MK,
        monthly_limit=5,
    )
    assert created is True
    usage_path = ("usage_months", f"u2_{_MK}")
    assert fake_client.documents[usage_path]["runs_used"] == 1
    usage_before = dict(fake_client.documents[usage_path])

    replay, created_again = real_store.create_run_idempotent(
        run_id="run-dup",
        user_id="u2",
        request_dict={"address": "SOMETHING ELSE ENTIRELY"},
        request_fingerprint="b" * 64,
        month_key=_MK,
        monthly_limit=5,
    )

    assert created_again is False
    # The existing doc is returned untouched — original request and
    # fingerprint, not the replayer's.
    assert replay["request"] == {"address": "1 Main St"}
    assert replay["request_fingerprint"] == _FP
    assert replay["created_at"] == winner["created_at"]
    assert fake_client.documents[usage_path] == usage_before
    stored = fake_client.documents[("runs", "run-dup")]
    assert stored["request_fingerprint"] == _FP


def test_real_store_limit_reached_raises_without_doc_or_increment(
    fake_client, real_store
) -> None:
    fake_client.documents[("usage_months", f"u3_{_MK}")] = {
        "app_user_id": "u3",
        "month_key": _MK,
        "runs_used": 5,
    }

    with pytest.raises(MonthlyQuotaExceeded) as excinfo:
        real_store.create_run_idempotent(
            run_id="run-over",
            user_id="u3",
            request_dict={"address": "1 Main St"},
            request_fingerprint=_FP,
            month_key=_MK,
            monthly_limit=5,
        )

    assert excinfo.value.runs_used == 5
    assert excinfo.value.monthly_run_limit == 5
    assert excinfo.value.month_key == _MK
    # Atomic abort: no run doc, no increment.
    assert ("runs", "run-over") not in fake_client.documents
    assert fake_client.documents[("usage_months", f"u3_{_MK}")]["runs_used"] == 5


def test_real_store_unlimited_plan_skips_limit_but_still_counts(
    fake_client, real_store
) -> None:
    fake_client.documents[("usage_months", f"u4_{_MK}")] = {
        "app_user_id": "u4",
        "month_key": _MK,
        "runs_used": 999,
    }

    _doc, created = real_store.create_run_idempotent(
        run_id="run-unlimited",
        user_id="u4",
        request_dict={"address": "1 Main St"},
        request_fingerprint=_FP,
        month_key=_MK,
        monthly_limit=None,
    )

    assert created is True
    assert fake_client.documents[("usage_months", f"u4_{_MK}")]["runs_used"] == 1000

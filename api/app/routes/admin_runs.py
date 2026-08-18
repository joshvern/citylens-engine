"""Admin-only stuck-run reconciler, designed for Cloud Scheduler.

The worker's failure handling only catches Python exceptions
(worker/worker.py): SIGKILL on the Cloud Run task timeout or an OOM kill
leaves a run in status "running" forever. Because the free plan counts
``status in ("queued", "running")`` against ``max_concurrent_runs=1`` and
the quota refund only fires for ``status == "failed"``, one stuck run
permanently bricks a free account. This endpoint marks abandoned runs
failed (code ``WORKER_TIMEOUT``) and applies the idempotent quota refund
in the same transaction so the quota slot comes back.

Authentication copies the admin_users.py pattern exactly: ``require_auth``
resolves the credential, then ``_require_admin`` rejects non-admins with
403. In production the scheduler authenticates with the hash-only admin
API key (``X-API-Key`` checked against ``CITYLENS_ADMIN_API_KEY_HASHES``).

Idempotent and safe to run every few minutes: a reconciled run leaves the
active-status query, the refund is guarded by the ``quota_refunded`` flag,
and each pass is bounded by ``CITYLENS_RUN_RECONCILE_BATCH_SIZE``.

Race-safe: the scan is only a snapshot, so each candidate is re-checked and
failed+refunded inside ONE Firestore transaction
(``fail_and_refund_if_stale_active``) — a run that a delayed worker claimed
(or finished) between the scan and the transaction is skipped and surfaced
as ``skipped_now_active`` instead of being overwritten.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Response

from ..models.schemas import RunReconcileResponse
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore, utcnow
from ..services.settings import Settings, get_settings

log = logging.getLogger(__name__)

router = APIRouter(tags=["admin-runs"])


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
        api_keys_index_collection=settings.api_keys_index_collection,
    )


def _require_admin(auth: AuthContext) -> None:
    if not auth.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")


def _private(response: Response) -> None:
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Authorization, X-API-Key"


@router.post("/admin/runs/reconcile", response_model=RunReconcileResponse)
def reconcile_stuck_runs(
    response: Response,
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    _require_admin(auth)
    _private(response)

    stale_minutes = int(settings.run_stale_minutes)
    cutoff = utcnow() - timedelta(minutes=stale_minutes)
    stale_runs, examined = store.list_stale_active_runs(
        cutoff=cutoff,
        limit=settings.run_reconcile_batch_size,
    )

    reconciled = 0
    refunded = 0
    skipped_now_active = 0
    run_ids: list[str] = []
    for run in stale_runs:
        run_id = str(run.get("run_id") or "")
        if not run_id:
            continue
        error = {
            "code": "WORKER_TIMEOUT",
            "message": (
                "The run made no progress for more than "
                f"{stale_minutes} minutes and exceeded its processing "
                "window, so it was marked failed. The monthly quota slot "
                "for this run has been returned — please try again."
            ),
            # Preserve the stage where the worker stalled; more useful in
            # the run record than a generic "failed".
            "stage": str(run.get("stage") or run.get("status") or "running"),
            "traceback_summary": [],
        }
        try:
            # Conditional + atomic: re-checks stale-active status and folds
            # the idempotent quota refund into the SAME transaction, so a
            # run a delayed worker just claimed cannot be overwritten and a
            # refund can never land without the failure (or vice versa).
            did_reconcile, did_refund = store.fail_and_refund_if_stale_active(
                run_id=run_id,
                cutoff=cutoff,
                error=error,
            )
        except Exception:
            # Self-heals on the next reconciler pass.
            log.exception(
                "reconciler transaction failed",
                extra={"run_id": run_id},
            )
            continue
        if not did_reconcile:
            # The run progressed or finished between the scan snapshot and
            # the transaction — a legitimately live run. Leave it alone.
            skipped_now_active += 1
            continue
        reconciled += 1
        if did_refund:
            refunded += 1
        run_ids.append(run_id)
        log.warning(
            "reconciled stuck run",
            extra={
                "run_id": run_id,
                "user_id": run.get("user_id"),
                "previous_status": run.get("status"),
                "stale_minutes": stale_minutes,
            },
        )

    return {
        "examined": examined,
        "reconciled": reconciled,
        "refunded": refunded,
        "skipped_now_active": skipped_now_active,
        "run_ids": run_ids,
    }


__all__ = ["get_store", "reconcile_stuck_runs", "router"]

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, Header, HTTPException

from ..models.schemas import RunListItem, RunListResponse, RunResponse
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.core_adapter import CitylensRequest
from ..services.firestore_store import FirestoreStore, MonthlyQuotaExceeded
from ..services.gcs_artifacts import GcsArtifacts
from ..services.job_trigger import CloudRunJobTrigger
from ..services.plans import get_policy, month_key
from ..services.quotas import (
    enforce_concurrent_quota,
    monthly_quota_exceeded_http,
    release_monthly_run,
    reserve_monthly_run,
)
from ..services.rate_limit import create_run_rate_limit
from ..services.run_errors import normalize_run_record
from ..services.run_options import DEFAULT_AOI_RADIUS_M, PublicRunRequest
from ..services.run_presenter import build_run_response
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["runs"])
logger = logging.getLogger(__name__)

# Same accepted alphabet/length as pilot_requests.py's Idempotency-Key.
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9_-]{16,128}")


def _idempotent_run_id(*, user_id: str, idempotency_key: str) -> str:
    """Deterministic run id scoped to the user.

    Hashing ``user_id`` together with the client key guarantees two users
    sending the same Idempotency-Key can never collide into each other's
    run. 32 hex chars matches the uuid4().hex ids minted for keyless runs.
    """

    digest = hashlib.sha256(
        f"{user_id}:{idempotency_key}".encode("utf-8")
    ).hexdigest()
    return digest[:32]


def _request_fingerprint(request_dict: dict[str, Any]) -> str:
    """Canonical fingerprint of the VALIDATED run request.

    Computed AFTER server-side validation/injection (the ``CitylensRequest``
    dump), so two semantically identical submissions fingerprint the same
    regardless of client-side field order (canonical JSON sorts keys) or
    ``outputs`` list order (set-semantic, sorted here). Persisted on the run
    doc so a replay can detect the same Idempotency-Key being reused with
    different parameters.
    """

    canonical = dict(request_dict)
    outputs = canonical.get("outputs")
    if isinstance(outputs, list):
        canonical["outputs"] = sorted(str(o) for o in outputs)
    raw = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _idempotent_replay(
    existing: dict[str, Any],
    *,
    fingerprint: str,
    auth: AuthContext,
    settings: Settings,
    store: FirestoreStore,
    gcs_factory: Callable[[], GcsArtifacts],
) -> RunResponse:
    """Return the existing run for a replayed Idempotency-Key.

    Enforces ownership, rejects key reuse with different parameters (409),
    and builds the response through the same artifact path as
    ``GET /v1/runs/{run_id}`` so a replay of a finished run is a faithful
    representation (artifacts included) rather than ``artifacts=[]``.
    """

    if existing.get("user_id") != auth.app_user_id:
        # Unreachable by construction (the id hashes the user id); kept as
        # a hard stop so a collision can never leak another user's run.
        raise HTTPException(status_code=409, detail="Idempotency conflict")
    stored = existing.get("request_fingerprint")
    # Legacy docs created before fingerprints were stored have no basis for
    # comparison and replay leniently; any stored fingerprint must match.
    if isinstance(stored, str) and stored and stored != fingerprint:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "IDEMPOTENCY_KEY_REUSED",
                "message": (
                    "This Idempotency-Key was already used with different "
                    "request parameters. Retry the original request "
                    "unchanged to retrieve its run, or send a new key for "
                    "a new request."
                ),
            },
        )
    artifacts = store.list_artifacts(str(existing.get("run_id") or ""))
    return build_run_response(
        run=existing,
        artifacts=artifacts,
        settings=settings,
        gcs=gcs_factory(),
    )


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
    )


def get_job_trigger(settings: Settings = Depends(get_settings)) -> CloudRunJobTrigger:
    return CloudRunJobTrigger(
        project_id=settings.project_id, region=settings.region, job_name=settings.job_name
    )


def get_gcs(settings: Settings = Depends(get_settings)) -> GcsArtifacts:
    return GcsArtifacts(bucket=settings.bucket)


def get_gcs_lazy(
    settings: Settings = Depends(get_settings),
) -> Callable[[], GcsArtifacts]:
    """Deferred GCS handle for POST /v1/runs.

    Only the idempotent-replay branch needs GCS (artifact presentation);
    constructing the storage client eagerly would tax every create request,
    so the route receives a factory and calls it only when replaying.
    """

    return lambda: GcsArtifacts(bucket=settings.bucket)


@router.post("/runs", response_model=RunResponse)
def create_run(
    request: PublicRunRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
    trigger: CloudRunJobTrigger = Depends(get_job_trigger),
    gcs_factory: Callable[[], GcsArtifacts] = Depends(get_gcs_lazy),
    _rate_limit: None = Depends(create_run_rate_limit),
) -> RunResponse:
    canonical = CitylensRequest.model_validate(
        {
            "address": request.address,
            "aoi_radius_m": DEFAULT_AOI_RADIUS_M,
            "imagery_year": request.imagery_year,
            "baseline_year": request.baseline_year,
            "segmentation_backend": request.segmentation_backend,
            "outputs": list(request.outputs),
            "notes": request.notes,
        }
    )
    request_dict = canonical.model_dump(mode="json")

    # OPTIONAL Idempotency-Key: when present, a retried/double-clicked POST
    # returns the already-created run instead of burning a second monthly
    # quota slot and launching a second Cloud Run job. The optimistic read
    # below is a fast path only — correctness against a concurrent same-key
    # request comes from the create transaction, which re-checks the
    # deterministic doc, the quota, and the create atomically.
    run_id: str | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        if not _IDEMPOTENCY_KEY.fullmatch(idempotency_key):
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "INVALID_IDEMPOTENCY_KEY",
                    "message": (
                        "Idempotency-Key must contain 16-128 letters, "
                        "digits, underscores, or hyphens."
                    ),
                },
            )
        run_id = _idempotent_run_id(
            user_id=auth.app_user_id, idempotency_key=idempotency_key
        )
        fingerprint = _request_fingerprint(request_dict)
        existing = store.get_run(run_id)
        if existing is not None:
            return _idempotent_replay(
                existing,
                fingerprint=fingerprint,
                auth=auth,
                settings=settings,
                store=store,
                gcs_factory=gcs_factory,
            )

    try:
        enforce_concurrent_quota(
            store=store, app_user_id=auth.app_user_id, plan_type=auth.plan_type
        )
    except HTTPException as exc:
        if run_id is not None and fingerprint is not None and exc.status_code == 429:
            # The winner of a same-key race may itself be what fills the
            # concurrency slot. The winner's run beats a 429: re-read the
            # deterministic doc and replay it when it exists.
            existing = store.get_run(run_id)
            if existing is not None:
                return _idempotent_replay(
                    existing,
                    fingerprint=fingerprint,
                    auth=auth,
                    settings=settings,
                    store=store,
                    gcs_factory=gcs_factory,
                )
        raise

    if run_id is not None and fingerprint is not None:
        # Keyed path: duplicate re-check, monthly-quota increment (with the
        # limit check), and run creation happen in ONE Firestore transaction
        # — a same-key loser gets the winner's doc back without ever
        # touching the counter, so there is no reserve-then-release window.
        mk = month_key(datetime.now(timezone.utc))
        policy = get_policy(auth.plan_type)
        try:
            run_doc, created = store.create_run_idempotent(
                run_id=run_id,
                user_id=auth.app_user_id,
                request_dict=request_dict,
                request_fingerprint=fingerprint,
                month_key=mk,
                monthly_limit=policy["monthly_run_limit"],
            )
        except MonthlyQuotaExceeded as exc:
            # Same rule as the concurrency pre-check: the winner's run
            # beats a 429.
            existing = store.get_run(run_id)
            if existing is not None:
                return _idempotent_replay(
                    existing,
                    fingerprint=fingerprint,
                    auth=auth,
                    settings=settings,
                    store=store,
                    gcs_factory=gcs_factory,
                )
            raise monthly_quota_exceeded_http(
                exc, plan_type=auth.plan_type
            ) from exc
        if not created:
            # Lost the same-key race inside the transaction: the winner
            # owns the quota reservation and the job trigger; this
            # request consumed nothing.
            return _idempotent_replay(
                run_doc,
                fingerprint=fingerprint,
                auth=auth,
                settings=settings,
                store=store,
                gcs_factory=gcs_factory,
            )
    else:
        mk = reserve_monthly_run(
            store=store, app_user_id=auth.app_user_id, plan_type=auth.plan_type
        )
        try:
            run_doc = store.create_run(
                user_id=auth.app_user_id, request_dict=request_dict
            )
        except Exception:
            release_monthly_run(
                store=store, app_user_id=auth.app_user_id, month_key=mk
            )
            raise

    try:
        execution_id = trigger.run(run_id=run_doc["run_id"])
        if execution_id:
            store.set_execution_id(run_doc["run_id"], execution_id)
            run_doc["execution_id"] = execution_id
    except Exception as e:
        release_monthly_run(store=store, app_user_id=auth.app_user_id, month_key=mk)
        error = {
            "code": "TRIGGER_FAILED",
            "message": str(e),
            "stage": "queued",
            "traceback_summary": [],
        }
        store.mark_failed(run_doc["run_id"], error)
        logger.exception(
            "failed to trigger worker job",
            extra={"run_id": run_doc["run_id"], "user_id": auth.app_user_id},
        )
        raise HTTPException(status_code=500, detail=f"Failed to trigger worker job: {e}")

    return RunResponse(
        **normalize_run_record(run_doc),
        artifacts=[],
    )


@router.get("/runs", response_model=RunListResponse)
def list_runs(
    limit: int = 20,
    cursor: str | None = None,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> RunListResponse:
    limit = max(1, min(int(limit), 100))

    try:
        runs, next_cursor = store.list_runs(user_id=auth.app_user_id, limit=limit, cursor=cursor)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Reactive quota refund: any failed run that hasn't been refunded yet
    # gets its monthly counter slot back. Idempotent and only writes to runs
    # that need it, so list-page reads stay cheap when nothing's failed.
    for run in runs:
        if str(run.get("status") or "") == "failed" and not run.get("quota_refunded"):
            try:
                store.refund_run_quota_if_failed(str(run.get("run_id") or ""))
            except Exception:
                logger.exception(
                    "quota refund failed",
                    extra={"run_id": run.get("run_id"), "user_id": auth.app_user_id},
                )

    items = [RunListItem(**normalize_run_record(run)) for run in runs]
    return RunListResponse(items=items, next_cursor=next_cursor)


@router.get("/runs/{run_id}", response_model=RunResponse)
def get_run(
    run_id: str,
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
    gcs: GcsArtifacts = Depends(get_gcs),
) -> RunResponse:
    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.get("user_id") != auth.app_user_id:
        raise HTTPException(status_code=404, detail="Run not found")

    if str(run.get("status") or "") == "failed" and not run.get("quota_refunded"):
        try:
            if store.refund_run_quota_if_failed(run_id):
                run["quota_refunded"] = True
        except Exception:
            logger.exception(
                "quota refund failed",
                extra={"run_id": run_id, "user_id": auth.app_user_id},
            )

    artifacts = store.list_artifacts(run_id)
    return build_run_response(run=run, artifacts=artifacts, settings=settings, gcs=gcs)

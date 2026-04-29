from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from ..models.schemas import RunListItem, RunListResponse, RunResponse
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.core_adapter import CitylensRequest
from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.job_trigger import CloudRunJobTrigger
from ..services.quotas import (
    enforce_concurrent_quota,
    release_monthly_run,
    reserve_monthly_run,
)
from ..services.run_errors import normalize_run_record
from ..services.run_options import DEFAULT_AOI_RADIUS_M, PublicRunRequest
from ..services.run_presenter import build_run_response
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["runs"])
logger = logging.getLogger(__name__)


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


@router.post("/runs", response_model=RunResponse)
def create_run(
    request: PublicRunRequest,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
    trigger: CloudRunJobTrigger = Depends(get_job_trigger),
) -> RunResponse:
    enforce_concurrent_quota(
        store=store, app_user_id=auth.app_user_id, plan_type=auth.plan_type
    )
    month_key = reserve_monthly_run(
        store=store, app_user_id=auth.app_user_id, plan_type=auth.plan_type
    )

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

    try:
        run_doc = store.create_run(user_id=auth.app_user_id, request_dict=request_dict)
    except Exception:
        release_monthly_run(
            store=store, app_user_id=auth.app_user_id, month_key=month_key
        )
        raise

    try:
        execution_id = trigger.run(run_id=run_doc["run_id"])
        if execution_id:
            store.set_execution_id(run_doc["run_id"], execution_id)
            run_doc["execution_id"] = execution_id
    except Exception as e:
        release_monthly_run(
            store=store, app_user_id=auth.app_user_id, month_key=month_key
        )
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
        **run_doc,
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
        runs, next_cursor = store.list_runs(
            user_id=auth.app_user_id, limit=limit, cursor=cursor
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

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

    artifacts = store.list_artifacts(run_id)
    return build_run_response(run=run, artifacts=artifacts, settings=settings, gcs=gcs)

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from ..models.schemas import RunResponse
from ..services.auth import require_user_id
from ..services.core_adapter import CitylensRequest
from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.job_trigger import CloudRunJobTrigger
from ..services.quotas import enforce_quotas
from ..services.run_presenter import build_run_response
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["runs"])


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
    )


def get_job_trigger(settings: Settings = Depends(get_settings)) -> CloudRunJobTrigger:
    return CloudRunJobTrigger(project_id=settings.project_id, region=settings.region, job_name=settings.job_name)


def get_gcs(settings: Settings = Depends(get_settings)) -> GcsArtifacts:
    return GcsArtifacts(bucket=settings.bucket)


def _infer_type(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".png"):
        return "image/png"
    if lower.endswith(".geojson"):
        return "application/geo+json"
    if lower.endswith(".ply"):
        return "model/ply"
    if lower.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


@router.post("/runs", response_model=RunResponse)
def create_run(
    request: CitylensRequest,
    user_id: str = Depends(require_user_id),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
    trigger: CloudRunJobTrigger = Depends(get_job_trigger),
) -> RunResponse:
    # quota enforcement
    enforce_quotas(store=store, user_id=user_id)

    # ensure user exists
    store.get_or_create_user(user_id)

    request_dict = request.model_dump()
    run_doc = store.create_run(user_id=user_id, request_dict=request_dict)

    try:
        execution_id = trigger.run(run_id=run_doc["run_id"])
        if execution_id:
            store.set_execution_id(run_doc["run_id"], execution_id)
            run_doc["execution_id"] = execution_id
    except Exception as e:
        store.mark_failed(run_doc["run_id"], str(e))
        raise HTTPException(status_code=500, detail=f"Failed to trigger worker job: {e}")

    return RunResponse(
        **run_doc,
        artifacts=[],
    )


@router.get("/runs/{run_id}", response_model=RunResponse)
def get_run(
    run_id: str,
    user_id: str = Depends(require_user_id),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
    gcs: GcsArtifacts = Depends(get_gcs),
) -> RunResponse:
    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Run not found")

    artifacts = store.list_artifacts(run_id)
    return build_run_response(run=run, artifacts=artifacts, settings=settings, gcs=gcs)

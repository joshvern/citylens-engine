from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Response, status

from ..models.schemas import (
    ParcelSavedSearch,
    ParcelSavedSearchUpdate,
    ParcelWorkflowAnalytics,
    ParcelWorkflowEvent,
    ParcelWorkflowItem,
    ParcelWorkflowUpdate,
)
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.parcel_workflow_analytics import build_workflow_analytics
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["parcel-workflow"])

_BOROUGH_BY_BBL_PREFIX = {
    "1": "manhattan",
    "2": "bronx",
    "3": "brooklyn",
    "4": "queens",
    "5": "staten_island",
}


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
        api_keys_index_collection=settings.api_keys_index_collection,
    )


@router.get("/parcel-intel/workflow", response_model=list[ParcelWorkflowItem])
def list_workflow(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> list[dict]:
    return store.list_parcel_workflow(app_user_id=auth.app_user_id)


@router.get(
    "/parcel-intel/workflow/analytics", response_model=ParcelWorkflowAnalytics
)
def workflow_analytics(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    items = store.list_parcel_workflow(
        app_user_id=auth.app_user_id, include_archived=True
    )
    return build_workflow_analytics(items)


@router.get(
    "/parcel-intel/workflow/{bbl}/events",
    response_model=list[ParcelWorkflowEvent],
)
def list_workflow_events(
    bbl: str,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> list[dict]:
    if not re.fullmatch(r"[1-5][0-9]{9}", bbl):
        raise HTTPException(
            status_code=422, detail="BBL must be 10 digits with borough prefix 1-5"
        )
    return store.list_parcel_workflow_events(
        app_user_id=auth.app_user_id, bbl=bbl
    )


@router.put("/parcel-intel/workflow/{bbl}", response_model=ParcelWorkflowItem)
def upsert_workflow(
    bbl: str,
    body: ParcelWorkflowUpdate,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    if not re.fullmatch(r"[1-5][0-9]{9}", bbl):
        raise HTTPException(
            status_code=422, detail="BBL must be 10 digits with borough prefix 1-5"
        )
    if body.borough != _BOROUGH_BY_BBL_PREFIX[bbl[0]]:
        raise HTTPException(status_code=422, detail="BBL does not match borough")
    payload = body.model_dump()
    payload["tags"] = sorted({tag.strip()[:40] for tag in body.tags if tag.strip()})
    return store.upsert_parcel_workflow(
        app_user_id=auth.app_user_id, bbl=bbl, payload=payload
    )


@router.delete("/parcel-intel/workflow/{bbl}", status_code=status.HTTP_204_NO_CONTENT)
def delete_workflow(
    bbl: str,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> Response:
    if not store.delete_parcel_workflow(app_user_id=auth.app_user_id, bbl=bbl):
        raise HTTPException(status_code=404, detail="Not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/parcel-intel/saved-searches", response_model=list[ParcelSavedSearch])
def list_saved_searches(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> list[dict]:
    return store.list_parcel_saved_searches(app_user_id=auth.app_user_id)


@router.put("/parcel-intel/saved-searches/{search_id}", response_model=ParcelSavedSearch)
def upsert_saved_search(
    search_id: str,
    body: ParcelSavedSearchUpdate,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,63}", search_id):
        raise HTTPException(status_code=422, detail="Invalid search id")
    return store.upsert_parcel_saved_search(
        app_user_id=auth.app_user_id,
        search_id=search_id,
        payload=body.model_dump(),
    )


@router.delete(
    "/parcel-intel/saved-searches/{search_id}", status_code=status.HTTP_204_NO_CONTENT
)
def delete_saved_search(
    search_id: str,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> Response:
    if not store.delete_parcel_saved_search(
        app_user_id=auth.app_user_id, search_id=search_id
    ):
        raise HTTPException(status_code=404, detail="Not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

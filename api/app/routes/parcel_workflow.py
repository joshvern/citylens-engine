from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Response, status

from ..models.schemas import (
    ParcelIntelRow,
    ParcelSavedSearch,
    ParcelSavedSearchUpdate,
    ParcelWorkflowActions,
    ParcelWorkflowAlerts,
    ParcelWorkflowAnalytics,
    ParcelWorkflowAnalyticsMethodology,
    ParcelWorkflowEvent,
    ParcelWorkflowItem,
    ParcelWorkflowReminderSnoozeRequest,
    ParcelWorkflowReminderSnoozeResponse,
    ParcelWorkflowSnapshot,
    ParcelWorkflowUpdate,
)
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.parcel_workflow_actions import (
    build_workflow_actions,
    normalize_workflow_action_payload,
)
from ..services.parcel_workflow_alerts import build_workflow_alerts
from ..services.parcel_workflow_analytics import (
    build_workflow_analytics,
    workflow_analytics_methodology,
)
from ..services.settings import Settings, get_settings
from .parcel_intel import (
    ParcelIntelRegistry,
    get_gcs,
    get_registry,
)

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
    "/parcel-intel/workflow/analytics/methodology",
    response_model=ParcelWorkflowAnalyticsMethodology,
)
def workflow_analytics_methodology_contract() -> dict:
    return workflow_analytics_methodology()


@router.get(
    "/parcel-intel/workflow/actions", response_model=ParcelWorkflowActions
)
def workflow_actions(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    items = store.list_parcel_workflow(app_user_id=auth.app_user_id)
    return build_workflow_actions(items)


def _canonical_workflow_snapshot(
    *,
    row: ParcelIntelRow,
    manifest: dict | None,
) -> dict:
    row_values = row.model_dump()
    generated_at = (manifest or {}).get("generated_at")
    row_values["feed_generated_at"] = (
        generated_at if isinstance(generated_at, str) else None
    )
    return ParcelWorkflowSnapshot(**row_values).model_dump()


@router.get(
    "/parcel-intel/workflow/alerts", response_model=ParcelWorkflowAlerts
)
def workflow_alerts(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> dict:
    items = store.list_parcel_workflow(app_user_id=auth.app_user_id)
    rows, manifest = registry.citywide_map(gcs)
    generated_at = (manifest or {}).get("generated_at")
    return build_workflow_alerts(
        items,
        [row.model_dump() for row in rows],
        feed_generated_at=(
            generated_at if isinstance(generated_at, str) else None
        ),
    )


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
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> dict:
    if not re.fullmatch(r"[1-5][0-9]{9}", bbl):
        raise HTTPException(
            status_code=422, detail="BBL must be 10 digits with borough prefix 1-5"
        )
    if body.borough != _BOROUGH_BY_BBL_PREFIX[bbl[0]]:
        raise HTTPException(status_code=422, detail="BBL does not match borough")
    payload = body.model_dump()
    payload["tags"] = sorted({tag.strip()[:40] for tag in body.tags if tag.strip()})
    try:
        payload = normalize_workflow_action_payload(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    existing = store.get_parcel_workflow(
        app_user_id=auth.app_user_id, bbl=bbl
    )
    if existing is not None and isinstance(existing.get("snapshot"), dict):
        payload["snapshot"] = existing["snapshot"]
    else:
        row, manifest = registry.parcel(gcs, bbl)
        payload["snapshot"] = _canonical_workflow_snapshot(
            row=row, manifest=manifest
        )
    return store.upsert_parcel_workflow(
        app_user_id=auth.app_user_id, bbl=bbl, payload=payload
    )


@router.post(
    "/parcel-intel/workflow/{bbl}/reminder",
    response_model=ParcelWorkflowReminderSnoozeResponse,
)
def snooze_workflow_reminder(
    bbl: str,
    body: ParcelWorkflowReminderSnoozeRequest,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    if not re.fullmatch(r"[1-5][0-9]{9}", bbl):
        raise HTTPException(
            status_code=422, detail="BBL must be 10 digits with borough prefix 1-5"
        )
    existing = store.get_parcel_workflow(
        app_user_id=auth.app_user_id, bbl=bbl
    )
    if existing is None:
        raise HTTPException(status_code=404, detail="Workflow record not found")
    updated = store.set_parcel_workflow_reminder_snooze(
        app_user_id=auth.app_user_id,
        bbl=bbl,
        days=body.days,
    )
    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Only open, active workflow records can be snoozed",
        )
    snoozed_until = updated.get("reminder_snoozed_until")
    return {
        "bbl": bbl,
        "reminder_snoozed_until": snoozed_until,
        "is_snoozed": snoozed_until is not None,
    }


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

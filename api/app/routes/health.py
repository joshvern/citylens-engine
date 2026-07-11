from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response

from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.settings import Settings, get_settings
from .parcel_intel import ParcelIntelRegistry, get_gcs, get_registry

log = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    # Keep-warm ping: must stay dependency-free and instant.
    return {"ok": True, "version": "0.1.0"}


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
        api_keys_index_collection=settings.api_keys_index_collection,
    )


@router.get("/health/ready")
def health_ready(
    response: Response,
    store: FirestoreStore = Depends(get_store),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> dict:
    """Deep readiness probe.

    - Firestore unreachable → 503 (the API cannot serve authed traffic).
    - Parcel-intel data missing/invalid/stale → still 200, reported via
      flags (the run pipeline works without it; it's degraded, not down).
    """
    firestore_ok = True
    try:
        store.ping()
    except Exception:
        firestore_ok = False
        log.warning("health/ready: Firestore ping failed", exc_info=True)

    parcel_intel: dict[str, Any] = {
        "present": False,
        "age_days": None,
        "stale": False,
    }
    try:
        idx = registry.index(gcs)
        parcel_intel = {
            "present": True,
            "age_days": idx.age_days,
            "stale": idx.stale,
        }
    except HTTPException as exc:
        # 503 from the registry = not published / invalid manifest.
        # Degraded, not fatal — reported via present=False.
        log.warning("health/ready: parcel-intel degraded: %s", exc.detail)
    except Exception:
        log.warning("health/ready: parcel-intel check failed", exc_info=True)

    if not firestore_ok:
        response.status_code = 503

    return {
        "ok": firestore_ok,
        "firestore": firestore_ok,
        "parcel_intel": parcel_intel,
    }

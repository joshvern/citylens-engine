"""Admin-only user lookup + plan assignment.

Authentication copies the pilot-requests admin pattern exactly:
``require_auth`` resolves the credential, then ``_require_admin`` rejects
non-admins with 403. In production the operator authenticates with a
hash-only admin API key — ``X-API-Key`` whose SHA-256 is compared in
constant time against ``CITYLENS_ADMIN_API_KEY_HASHES`` inside
``services/auth.py::_check_admin_api_key`` — or an allowlisted admin JWT.

Plan assignment is the only write: it persists ``plan_type`` on the user
document (with ``plan_changed_at`` + an append-only ``plan_history``
audit trail) and is validated against the plan registry in
``services/plans.py``. It never touches ``is_admin``.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from ..models.schemas import (
    AdminUserList,
    AdminUserPlanUpdate,
    AdminUserRecord,
)
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.plans import ASSIGNABLE_PLAN_TYPES
from ..services.settings import Settings, get_settings

log = logging.getLogger(__name__)

router = APIRouter(tags=["admin-users"])


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


@router.get("/admin/users", response_model=AdminUserList)
def lookup_users_by_email(
    response: Response,
    email: str = Query(..., min_length=3, max_length=320),
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    """Exact-match email lookup so the owner can find a user id."""

    _require_admin(auth)
    _private(response)
    return {"items": store.find_users_by_email(email)}


@router.get("/admin/users/{user_id}", response_model=AdminUserRecord)
def get_admin_user(
    user_id: str,
    response: Response,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    _require_admin(auth)
    record = store.get_user(user_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Not found")
    _private(response)
    return record


@router.patch(
    "/admin/users/{user_id}/plan",
    response_model=AdminUserRecord,
)
def assign_user_plan(
    user_id: str,
    body: AdminUserPlanUpdate,
    response: Response,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    _require_admin(auth)
    if body.plan_type not in ASSIGNABLE_PLAN_TYPES:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "UNKNOWN_PLAN_TYPE",
                "message": (
                    "plan_type must be one of "
                    f"{', '.join(ASSIGNABLE_PLAN_TYPES)}."
                ),
                "allowed_plan_types": list(ASSIGNABLE_PLAN_TYPES),
            },
        )
    record = store.set_user_plan(
        app_user_id=user_id,
        plan_type=body.plan_type,
        changed_by=auth.app_user_id,
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Not found")
    log.info(
        "admin plan assignment",
        extra={
            "target_user_id": user_id,
            "plan_type": body.plan_type,
            "changed_by": auth.app_user_id,
        },
    )
    _private(response)
    return record


__all__ = [
    "assign_user_plan",
    "get_admin_user",
    "get_store",
    "lookup_users_by_email",
    "router",
]

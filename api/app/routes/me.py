from __future__ import annotations

from fastapi import APIRouter, Depends

from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.quotas import get_quota_state
from ..services.settings import Settings, get_settings


router = APIRouter(tags=["me"])


def _store(settings: Settings) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
    )


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return _store(settings)


@router.get("/me")
def me(
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    quota = get_quota_state(
        store=store, app_user_id=auth.app_user_id, plan_type=auth.plan_type
    )
    return {
        "user": {
            "id": auth.app_user_id,
            "email": auth.email,
            "plan_type": auth.plan_type,
            "is_admin": auth.is_admin,
        },
        "quota": quota,
    }

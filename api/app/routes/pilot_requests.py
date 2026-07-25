from __future__ import annotations

import hashlib
import re
import secrets

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Response,
    status,
)

from ..models.schemas import (
    PilotRequestAdminList,
    PilotRequestAdminRecord,
    PilotRequestCreate,
    PilotRequestReceipt,
    PilotRequestStatus,
    PilotRequestStatusUpdate,
)
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore, utcnow
from ..services.rate_limit import pilot_request_rate_limit
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["pilot-requests"])

_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9_-]{16,128}")


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
        api_keys_index_collection=settings.api_keys_index_collection,
        pilot_requests_collection=settings.pilot_requests_collection,
    )


def _request_id(idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"pr_{digest[:32]}"


def _require_admin(auth: AuthContext) -> None:
    if not auth.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")


@router.post(
    "/pilot-requests",
    response_model=PilotRequestReceipt,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_pilot_request(
    body: PilotRequestCreate,
    response: Response,
    _rate_limit: None = Depends(pilot_request_rate_limit),
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    response.headers["Cache-Control"] = "no-store"
    if not idempotency_key or not _IDEMPOTENCY_KEY.fullmatch(
        idempotency_key
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_IDEMPOTENCY_KEY",
                "message": (
                    "Idempotency-Key must contain 16-128 letters, digits, "
                    "underscores, or hyphens."
                ),
            },
        )

    # A filled hidden website field is treated as automated traffic. Return an
    # indistinguishable receipt without storing contact data.
    if body.website:
        return {
            "schema_version": "citylens/pilot-request-receipt@v1",
            "request_id": f"pr_{secrets.token_hex(16)}",
            "status": "received",
            "created_at": utcnow(),
        }

    request_id = _request_id(idempotency_key)
    record = store.create_pilot_request(
        request_id=request_id,
        payload=body.model_dump(),
    )
    return {
        "schema_version": "citylens/pilot-request-receipt@v1",
        "request_id": request_id,
        "status": "received",
        "created_at": record["created_at"],
    }


@router.get(
    "/pilot-requests",
    response_model=PilotRequestAdminList,
)
def list_pilot_requests(
    response: Response,
    request_status: PilotRequestStatus | None = Query(
        default=None,
        alias="status",
    ),
    limit: int = 100,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    _require_admin(auth)
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=422, detail="limit must be 1-200")
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Authorization, X-API-Key"
    return {
        "items": store.list_pilot_requests(
            status=request_status,
            limit=limit,
        )
    }


@router.patch(
    "/pilot-requests/{request_id}",
    response_model=PilotRequestAdminRecord,
)
def update_pilot_request_status(
    request_id: str,
    body: PilotRequestStatusUpdate,
    response: Response,
    auth: AuthContext = Depends(require_auth),
    store: FirestoreStore = Depends(get_store),
) -> dict:
    _require_admin(auth)
    if not re.fullmatch(r"pr_[a-f0-9]{32}", request_id):
        raise HTTPException(status_code=404, detail="Not found")
    record = store.update_pilot_request_status(
        request_id=request_id,
        status=body.status,
        admin_user_id=auth.app_user_id,
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Not found")
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Authorization, X-API-Key"
    return record


__all__ = [
    "create_pilot_request",
    "get_store",
    "list_pilot_requests",
    "router",
    "update_pilot_request_status",
]

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field

from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["api-keys"])


def _store(settings: Settings) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
        api_keys_index_collection=settings.api_keys_index_collection,
    )


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return _store(settings)


def _ensure_enabled(settings: Settings) -> None:
    if not settings.allow_user_api_keys:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "USER_API_KEYS_DISABLED",
                "message": "User API keys are disabled in this environment.",
            },
        )


class CreateApiKeyRequest(BaseModel):
    label: str = Field(..., min_length=1, max_length=128)


def _serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Strip secret material and stringify timestamps for JSON output."""
    return {
        "key_id": record.get("key_id"),
        "label": record.get("label"),
        "key_prefix": record.get("key_prefix"),
        "created_at": _isoformat(record.get("created_at")),
        "last_used_at": _isoformat(record.get("last_used_at")),
        "revoked_at": _isoformat(record.get("revoked_at")),
    }


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            return None
    return str(value) if value else None


@router.post("/api-keys", status_code=status.HTTP_201_CREATED)
def create_api_key(
    body: CreateApiKeyRequest,
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
) -> dict[str, Any]:
    _ensure_enabled(settings)
    _, plaintext, record = store.create_api_key(
        app_user_id=auth.app_user_id, label=body.label
    )
    response = _serialize_record(record)
    # Plaintext is shown ONCE on the create response and never again.
    response["plaintext_key"] = plaintext
    return response


@router.get("/api-keys")
def list_api_keys(
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
) -> dict[str, Any]:
    _ensure_enabled(settings)
    items = store.list_api_keys(app_user_id=auth.app_user_id)
    return {"items": [_serialize_record(item) for item in items]}


@router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_api_key(
    key_id: str,
    auth: AuthContext = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
) -> Response:
    _ensure_enabled(settings)
    revoked = store.revoke_api_key(app_user_id=auth.app_user_id, key_id=key_id)
    if not revoked:
        # Could be: not the user's key, already revoked, or never existed.
        # Don't leak the difference.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

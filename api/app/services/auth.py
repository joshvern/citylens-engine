from __future__ import annotations

import hashlib
from typing import Optional

from fastapi import Depends, Header, HTTPException

from .settings import Settings, get_settings


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def require_user_id(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    settings: Settings = Depends(get_settings),
) -> str:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key")

    if settings.api_keys:
        if x_api_key not in settings.api_keys:
            raise HTTPException(status_code=401, detail="Invalid API key")
    else:
        # If no allowlist is configured, fail closed.
        raise HTTPException(status_code=500, detail="CITYLENS_API_KEYS not configured")

    return sha256_hex(x_api_key)

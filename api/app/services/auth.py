from __future__ import annotations

import hashlib
import hmac
import logging
from functools import lru_cache
from typing import Optional

from fastapi import Depends, Header, HTTPException

from .auth_context import AuthContext
from .firestore_store import FirestoreStore
from .oidc_verifier import (
    AuthVerificationError,
    MockVerifier,
    OIDCVerifier,
)
from .settings import Settings, get_settings

logger = logging.getLogger(__name__)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _store_factory(settings: Settings) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
        auth_identities_collection=settings.auth_identities_collection,
        usage_months_collection=settings.usage_months_collection,
    )


@lru_cache(maxsize=4)
def _build_verifier_cached(provider: str, jwks_url: str, issuer: str | None, audience: str | None):
    if provider == "mock":
        return MockVerifier()
    return OIDCVerifier(jwks_url=jwks_url, issuer=issuer, audience=audience)


def _get_verifier(settings: Settings):
    provider = settings.auth_provider or "neon"
    if provider == "mock":
        if not settings.allow_mock_auth:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Mock auth provider requires CITYLENS_ALLOW_MOCK_AUTH=true. "
                    "Refusing to authenticate in this configuration."
                ),
            )
        return _build_verifier_cached("mock", "", None, None)
    if not settings.auth_jwks_url:
        raise HTTPException(
            status_code=503,
            detail="Auth provider not configured: set CITYLENS_AUTH_JWKS_URL.",
        )
    return _build_verifier_cached(
        provider,
        settings.auth_jwks_url,
        settings.auth_issuer,
        settings.auth_audience,
    )


def _admin_for_oidc(claims: dict, settings: Settings) -> bool:
    sub = str(claims.get("sub") or "")
    email = claims.get("email")
    email_verified = bool(claims.get("email_verified", False))
    if sub and sub in set(settings.admin_auth_subs):
        return True
    if isinstance(email, str) and email_verified and email in set(settings.admin_emails):
        return True
    return False


def _check_admin_api_key(provided: str, settings: Settings) -> bool:
    if not settings.allow_admin_api_keys:
        return False
    candidate_hash = sha256_hex(provided)
    for key in settings.admin_api_keys:
        if hmac.compare_digest(key, provided):
            return True
    for stored_hash in settings.admin_api_key_hashes:
        if hmac.compare_digest(stored_hash.lower(), candidate_hash.lower()):
            return True
    return False


def require_auth(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    settings: Settings = Depends(get_settings),
) -> AuthContext:
    bearer_token: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip() or None

    if bearer_token:
        verifier = _get_verifier(settings)
        try:
            claims = verifier.verify(bearer_token)
        except AuthVerificationError as exc:
            logger.info("auth: token verification failed: %s", exc)
            raise HTTPException(status_code=401, detail="Invalid auth token") from exc

        provider = settings.auth_provider or "neon"
        subject = str(claims.get("sub") or "")
        if not subject:
            raise HTTPException(status_code=401, detail="Auth token missing subject")
        email_raw = claims.get("email")
        email = str(email_raw) if isinstance(email_raw, str) and email_raw else None
        # Accept both `email_verified` (OIDC standard, snake_case) and
        # `emailVerified` (Better Auth / Neon Auth's spelling, camelCase).
        # Whichever is present and truthy wins.
        email_verified = bool(
            claims.get("email_verified") or claims.get("emailVerified") or False
        )
        is_admin_override = _admin_for_oidc(claims, settings)

        store = _store_factory(settings)
        user_doc = store.get_or_create_user_by_identity(
            provider=provider,
            subject=subject,
            email=email,
            email_verified=email_verified,
            is_admin_override=is_admin_override,
        )
        plan_type = str(user_doc.get("plan_type") or ("admin" if is_admin_override else "free"))
        is_admin = bool(user_doc.get("is_admin") or is_admin_override)
        return AuthContext(
            app_user_id=str(user_doc["user_id"]),
            auth_provider=provider,
            auth_subject=subject,
            email=email or user_doc.get("email"),
            email_verified=email_verified,
            is_admin=is_admin,
            plan_type=plan_type,
        )

    if x_api_key and settings.allow_admin_api_keys and _check_admin_api_key(x_api_key, settings):
        api_key_hash = sha256_hex(x_api_key)
        store = _store_factory(settings)
        user_doc = store.get_admin_user_for_api_key(api_key_hash)
        return AuthContext(
            app_user_id=str(user_doc["user_id"]),
            auth_provider="admin_api_key",
            auth_subject=api_key_hash,
            email=user_doc.get("email"),
            email_verified=False,
            is_admin=True,
            plan_type="admin",
        )

    if authorization or x_api_key:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    raise HTTPException(status_code=401, detail="Authentication required")

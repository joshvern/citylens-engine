from __future__ import annotations

import base64
import json
import time
from typing import Any, Optional

import httpx
import jwt
from jwt import PyJWKClient


class AuthVerificationError(Exception):
    pass


class OIDCVerifier:
    def __init__(
        self,
        *,
        jwks_url: str,
        issuer: Optional[str],
        audience: Optional[str],
        cache_ttl_seconds: int = 600,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if not jwks_url:
            raise RuntimeError("OIDCVerifier requires a JWKS URL")
        self._jwks_url = jwks_url
        self._issuer = issuer
        self._audience = audience
        self._cache_ttl = cache_ttl_seconds
        self._jwks_client = PyJWKClient(jwks_url)
        self._http = http_client or httpx.Client(timeout=5.0)
        self._jwks_cache: Optional[dict[str, Any]] = None
        self._jwks_fetched_at: float = 0.0

    def verify(self, token: str) -> dict[str, Any]:
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token).key
        except Exception as exc:
            raise AuthVerificationError(f"Could not resolve JWKS signing key: {exc}") from exc

        decode_kwargs: dict[str, Any] = {
            # EdDSA (Ed25519) is what Neon Auth / Better Auth's JWT plugin
            # signs with by default. RS256/ES256 are kept for OIDC issuers
            # that use them.
            "algorithms": ["EdDSA", "RS256", "RS512", "ES256", "ES512"],
            "options": {"require": ["exp", "iat"]},
        }
        if self._audience:
            decode_kwargs["audience"] = self._audience
        else:
            decode_kwargs.setdefault("options", {})["verify_aud"] = False
        if self._issuer:
            decode_kwargs["issuer"] = self._issuer

        try:
            claims = jwt.decode(token, signing_key, **decode_kwargs)
        except jwt.PyJWTError as exc:
            raise AuthVerificationError(f"JWT verification failed: {exc}") from exc

        return claims


class MockVerifier:
    """Dev/test verifier. Accepts tokens shaped `mock.<base64url(json)>`.

    Production must NOT enable this. The auth selection logic enforces that
    `CITYLENS_AUTH_PROVIDER=mock` requires `CITYLENS_ALLOW_MOCK_AUTH=true`.
    """

    def verify(self, token: str) -> dict[str, Any]:
        if not token.startswith("mock."):
            raise AuthVerificationError("Mock verifier requires 'mock.<payload>' tokens")
        body = token[len("mock.") :]
        padded = body.encode("ascii")
        padded += b"=" * (-len(padded) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded).decode("utf-8")
            claims = json.loads(decoded)
        except Exception as exc:
            raise AuthVerificationError(f"Mock token payload invalid: {exc}") from exc
        if not isinstance(claims, dict):
            raise AuthVerificationError("Mock token payload must be a JSON object")
        # Synthesize iat/exp so downstream logic can rely on standard claims.
        claims.setdefault("iat", int(time.time()))
        claims.setdefault("exp", int(time.time()) + 3600)
        if "sub" not in claims or not claims["sub"]:
            raise AuthVerificationError("Mock token must include 'sub'")
        return claims

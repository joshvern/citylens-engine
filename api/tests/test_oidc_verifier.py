"""End-to-end tests for OIDCVerifier against an in-memory EdDSA JWKS.

Pins the algorithm allowlist so a future shrink can't silently break Neon
Auth (which signs JWTs with EdDSA / Ed25519 via Better Auth's JWT plugin).
"""

from __future__ import annotations

import base64
import json
import time
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from app.services.oidc_verifier import AuthVerificationError, OIDCVerifier


def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _make_eddsa_keypair_and_jwks() -> tuple[bytes, dict, str]:
    sk = Ed25519PrivateKey.generate()
    pk_raw = sk.public_key().public_bytes(
        encoding=Encoding.Raw, format=PublicFormat.Raw
    )
    sk_pem = sk.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    kid = "test-kid"
    jwks = {
        "keys": [
            {
                "kty": "OKP",
                "crv": "Ed25519",
                "alg": "EdDSA",
                "kid": kid,
                "x": _b64u(pk_raw),
            }
        ]
    }
    return sk_pem, jwks, kid


def _sign(private_key_pem: bytes, kid: str, payload: dict) -> str:
    return jwt.encode(payload, private_key_pem, algorithm="EdDSA", headers={"kid": kid})


def test_oidc_verifier_accepts_eddsa(monkeypatch) -> None:
    sk_pem, jwks, kid = _make_eddsa_keypair_and_jwks()
    now = int(time.time())
    token = _sign(sk_pem, kid, {"sub": "user-1", "iat": now, "exp": now + 600})

    verifier = OIDCVerifier(
        jwks_url="https://example.test/jwks",
        issuer=None,
        audience=None,
    )
    # Patch the JWKS client to return our in-memory key without HTTP.
    class _FakeKey:
        def __init__(self, k):
            self.key = k

    fake = jwt.PyJWK(jwks["keys"][0], algorithm="EdDSA").key

    with patch.object(verifier._jwks_client, "get_signing_key_from_jwt", return_value=_FakeKey(fake)):
        claims = verifier.verify(token)

    assert claims["sub"] == "user-1"


def test_oidc_verifier_rejects_expired() -> None:
    sk_pem, jwks, kid = _make_eddsa_keypair_and_jwks()
    now = int(time.time())
    token = _sign(sk_pem, kid, {"sub": "user-1", "iat": now - 7200, "exp": now - 3600})

    verifier = OIDCVerifier(
        jwks_url="https://example.test/jwks",
        issuer=None,
        audience=None,
    )

    class _FakeKey:
        def __init__(self, k):
            self.key = k

    fake = jwt.PyJWK(jwks["keys"][0], algorithm="EdDSA").key

    with patch.object(verifier._jwks_client, "get_signing_key_from_jwt", return_value=_FakeKey(fake)):
        with pytest.raises(AuthVerificationError):
            verifier.verify(token)


def test_oidc_verifier_validates_issuer_when_configured() -> None:
    sk_pem, jwks, kid = _make_eddsa_keypair_and_jwks()
    now = int(time.time())
    bad = _sign(sk_pem, kid, {"sub": "u", "iat": now, "exp": now + 600, "iss": "https://wrong"})

    verifier = OIDCVerifier(
        jwks_url="https://example.test/jwks",
        issuer="https://expected.test",
        audience=None,
    )

    class _FakeKey:
        def __init__(self, k):
            self.key = k

    fake = jwt.PyJWK(jwks["keys"][0], algorithm="EdDSA").key

    with patch.object(verifier._jwks_client, "get_signing_key_from_jwt", return_value=_FakeKey(fake)):
        with pytest.raises(AuthVerificationError):
            verifier.verify(bad)


def test_oidc_verifier_algorithm_allowlist_includes_eddsa() -> None:
    """Pin the algorithm list. Removing EdDSA breaks Neon Auth integration."""
    import inspect
    src = inspect.getsource(OIDCVerifier.verify)
    assert '"EdDSA"' in src, "OIDCVerifier must accept EdDSA (Neon Auth signs JWTs with Ed25519)"

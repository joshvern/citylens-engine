"""Regression: require_auth must read `emailVerified` (Better Auth / Neon
Auth's camelCase spelling) AND `email_verified` (OIDC standard snake_case).
Either being truthy means verified.
"""

from __future__ import annotations

import pytest

from app.services import auth as auth_module
from app.services.settings import get_settings


@pytest.fixture
def fake_verifier(monkeypatch):
    """Bypass real OIDC verification; whatever claims dict is passed gets
    returned by verifier.verify()."""
    captured = {"claims": None}

    class _StubVerifier:
        def verify(self, token):
            return captured["claims"]

    def _set_claims(c):
        captured["claims"] = c

    monkeypatch.setattr(auth_module, "_get_verifier", lambda settings: _StubVerifier())
    return _set_claims


@pytest.fixture
def fake_store(monkeypatch):
    """Capture what get_or_create_user_by_identity is called with."""
    captured = {"call": None}

    class _Store:
        def get_or_create_user_by_identity(self, **kwargs):
            captured["call"] = kwargs
            return {
                "user_id": kwargs.get("subject", "uid"),
                "email": kwargs.get("email"),
                "email_verified": kwargs.get("email_verified"),
                "plan_type": "admin" if kwargs.get("is_admin_override") else "free",
                "is_admin": bool(kwargs.get("is_admin_override")),
            }

    monkeypatch.setattr(auth_module, "_store_factory", lambda settings: _Store())
    return captured


def _call_require_auth(token: str = "fake.jwt.token"):
    settings = get_settings()
    return auth_module.require_auth(
        authorization=f"Bearer {token}",
        x_api_key=None,
        settings=settings,
    )


def test_email_verified_snake_case(fake_verifier, fake_store):
    fake_verifier({
        "sub": "u1",
        "email": "x@example.com",
        "email_verified": True,
        "iat": 1, "exp": 9999999999,
    })
    ctx = _call_require_auth()
    assert ctx.email_verified is True
    assert fake_store["call"]["email_verified"] is True


def test_email_verified_camel_case_better_auth(fake_verifier, fake_store):
    fake_verifier({
        "sub": "u1",
        "email": "x@example.com",
        "emailVerified": True,
        "iat": 1, "exp": 9999999999,
    })
    ctx = _call_require_auth()
    assert ctx.email_verified is True, "engine must accept Better Auth's emailVerified"
    assert fake_store["call"]["email_verified"] is True


def test_email_verified_neither_field(fake_verifier, fake_store):
    fake_verifier({
        "sub": "u1",
        "email": "x@example.com",
        "iat": 1, "exp": 9999999999,
    })
    ctx = _call_require_auth()
    assert ctx.email_verified is False


def test_email_verified_explicit_false_either_form(fake_verifier, fake_store):
    fake_verifier({
        "sub": "u1",
        "email": "x@example.com",
        "email_verified": False,
        "emailVerified": False,
        "iat": 1, "exp": 9999999999,
    })
    ctx = _call_require_auth()
    assert ctx.email_verified is False

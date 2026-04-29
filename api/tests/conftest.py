from __future__ import annotations

import sys
from pathlib import Path

import pytest

from app.services.auth_context import AuthContext


def pytest_configure() -> None:
    api_root = Path(__file__).resolve().parents[1]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))


@pytest.fixture(autouse=True)
def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    # Tests don't rely on the deprecated user allowlist anymore, but the auth
    # module still references it for the legacy admin-key path.
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")
    monkeypatch.setenv(
        "CITYLENS_CORS_ORIGINS",
        "https://citylens.dev,https://www.citylens.dev,http://localhost:3000,http://localhost:3001",
    )
    monkeypatch.setenv("CITYLENS_AUTH_PROVIDER", "mock")
    monkeypatch.setenv("CITYLENS_ALLOW_MOCK_AUTH", "true")
    monkeypatch.setenv("CITYLENS_AUTH_REQUIRED", "true")
    monkeypatch.setenv("CITYLENS_FREE_MONTHLY_RUNS", "5")


@pytest.fixture
def auth_override():
    """Helper to install an `app.dependency_overrides[require_auth]` returning a
    synthetic AuthContext."""
    from app.main import app
    from app.services.auth import require_auth

    def _set(*, app_user_id: str = "user-test-1", plan_type: str = "free", is_admin: bool = False):
        ctx = AuthContext(
            app_user_id=app_user_id,
            auth_provider="mock",
            auth_subject=f"sub-{app_user_id}",
            email=f"{app_user_id}@example.com",
            email_verified=True,
            is_admin=is_admin,
            plan_type=plan_type,
        )
        app.dependency_overrides[require_auth] = lambda: ctx
        return ctx

    yield _set

    app.dependency_overrides = {}

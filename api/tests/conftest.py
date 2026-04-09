from __future__ import annotations

import sys
from pathlib import Path

import pytest


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
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")
    monkeypatch.setenv(
        "CITYLENS_CORS_ORIGINS",
        "https://citylens.dev,https://www.citylens.dev,http://localhost:3000,http://localhost:3001",
    )

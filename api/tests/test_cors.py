from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_cors_preflight_uses_env_origins(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")
    monkeypatch.setenv("CITYLENS_CORS_ORIGINS", "https://example.com,http://localhost:3000")

    client = TestClient(app)
    resp = client.options(
        "/v1/runs",
        headers={
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-API-Key",
        },
    )
    assert resp.status_code == 204
    assert resp.headers["access-control-allow-origin"] == "https://example.com"

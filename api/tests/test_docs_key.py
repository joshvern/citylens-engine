from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient

from app.main import app


def test_docs_404_when_key_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("CITYLENS_DOCS_ACCESS_KEY_SHA256", raising=False)
    client = TestClient(app)
    assert client.get("/docs").status_code == 404
    assert client.get("/redoc").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_docs_401_without_key_when_configured(monkeypatch) -> None:
    secret = "topsecret"
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    monkeypatch.setenv("CITYLENS_DOCS_ACCESS_KEY_SHA256", digest)
    client = TestClient(app)
    assert client.get("/docs").status_code == 401


def test_docs_serves_with_correct_key(monkeypatch) -> None:
    secret = "topsecret"
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    monkeypatch.setenv("CITYLENS_DOCS_ACCESS_KEY_SHA256", digest)
    client = TestClient(app)
    resp = client.get("/openapi.json", headers={"X-Docs-Key": secret})
    assert resp.status_code == 200
    body = resp.json()
    assert "openapi" in body or "paths" in body


def test_docs_key_does_not_authenticate_runs(monkeypatch) -> None:
    secret = "topsecret"
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    monkeypatch.setenv("CITYLENS_DOCS_ACCESS_KEY_SHA256", digest)
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        headers={"X-Docs-Key": secret},
        json={"address": "1 Main St"},
    )
    assert resp.status_code == 401

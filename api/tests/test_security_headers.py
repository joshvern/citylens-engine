from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

EXPECTED_HEADERS = {
    "permissions-policy": (
        "browsing-topics=(), camera=(), geolocation=(), microphone=(), "
        "payment=()"
    ),
    "referrer-policy": "no-referrer",
    "strict-transport-security": "max-age=63072000",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "x-xss-protection": "0",
}


def _assert_security_headers(response) -> None:
    for name, value in EXPECTED_HEADERS.items():
        assert response.headers[name] == value
    policy = response.headers["content-security-policy"]
    assert "base-uri 'none'" in policy
    assert "object-src 'none'" in policy
    assert "frame-ancestors 'none'" in policy


def test_security_headers_cover_success_and_error_responses() -> None:
    client = TestClient(app)

    _assert_security_headers(client.get("/v1/health"))
    _assert_security_headers(client.get("/not-found"))
    _assert_security_headers(client.get("/docs"))


def test_security_headers_cover_cors_preflight(monkeypatch) -> None:
    monkeypatch.setenv(
        "CITYLENS_CORS_ORIGINS",
        "https://www.citylens.dev",
    )
    client = TestClient(app)

    response = client.options(
        "/v1/parcel-intel/index",
        headers={
            "Origin": "https://www.citylens.dev",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 204
    _assert_security_headers(response)

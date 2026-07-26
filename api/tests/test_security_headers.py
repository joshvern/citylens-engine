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


def test_private_parcel_routes_are_no_store_before_authentication() -> None:
    client = TestClient(app)

    responses = [
        client.get("/v1/parcel-intel/workflow"),
        client.get("/v1/parcel-intel/saved-searches"),
        client.post(
            "/v1/parcel-intel/product-events",
            json={
                "schema_version": "citylens/parcel-product-event@v1",
                "event": "parcel_opened",
                "source": "ranking",
            },
        ),
        client.get(
            "/v1/parcel-intel/evidence-issues?status=submitted&limit=1"
        ),
    ]

    assert {response.status_code for response in responses} == {401}
    for response in responses:
        assert response.headers["cache-control"] == "private, no-store"
        _assert_security_headers(response)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import pilot_requests

_NOW = datetime(2026, 7, 24, 20, 0, tzinfo=timezone.utc)
_REQUEST_ID = "pr_0123456789abcdef0123456789abcdef"


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "citylens/pilot-request@v1",
        "plan": "acquisitions",
        "name": "  Jordan   Lee ",
        "work_email": "  JORDAN@EXAMPLE.COM ",
        "company": "  Example   Development ",
        "role": " Acquisitions   director ",
        "team_size": "2-5",
        "target_boroughs": ["brooklyn", "queens", "brooklyn"],
        "workflow_summary": (
            " We screen small development sites and need a shared review "
            "workflow before owner outreach. "
        ),
        "consent": True,
        "website": "",
    }
    payload.update(overrides)
    return payload


class FakePilotStore:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.create_calls: list[dict[str, Any]] = []

    def create_pilot_request(
        self,
        *,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.create_calls.append(
            {"request_id": request_id, "payload": payload}
        )
        existing = self.records.get(request_id)
        if existing is not None:
            return existing
        record = {
            **payload,
            "request_id": request_id,
            "status": "new",
            "created_at": _NOW,
            "updated_at": _NOW,
            "expires_at": _NOW + timedelta(days=365),
        }
        self.records[request_id] = record
        return record

    def list_pilot_requests(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = list(self.records.values())
        if status is not None:
            rows = [row for row in rows if row["status"] == status]
        return rows[:limit]

    def update_pilot_request_status(
        self,
        *,
        request_id: str,
        status: str,
        admin_user_id: str,
    ) -> dict[str, Any] | None:
        record = self.records.get(request_id)
        if record is None:
            return None
        record.update(
            {
                "status": status,
                "updated_at": _NOW,
                "status_updated_at": _NOW,
                "status_updated_by": admin_user_id,
            }
        )
        return record


def test_public_pilot_request_is_normalized_private_and_idempotent() -> None:
    store = FakePilotStore()
    app.dependency_overrides[pilot_requests.get_store] = lambda: store
    client = TestClient(app)
    headers = {"Idempotency-Key": "browser-request-123456789"}

    created = client.post(
        "/v1/pilot-requests",
        headers=headers,
        json=_payload(),
    )
    assert created.status_code == 202, created.text
    assert created.headers["cache-control"] == "no-store"
    assert created.json() == {
        "schema_version": "citylens/pilot-request-receipt@v1",
        "request_id": created.json()["request_id"],
        "status": "received",
        "created_at": "2026-07-24T20:00:00Z",
    }
    assert created.json()["request_id"].startswith("pr_")

    stored = store.create_calls[0]["payload"]
    assert stored["name"] == "Jordan Lee"
    assert stored["work_email"] == "jordan@example.com"
    assert stored["company"] == "Example Development"
    assert stored["role"] == "Acquisitions director"
    assert stored["target_boroughs"] == ["brooklyn", "queens"]
    assert stored["consent"] is True
    assert "website" not in stored
    assert not {
        "ip",
        "client_ip",
        "user_agent",
        "referrer",
    }.intersection(stored)

    retried = client.post(
        "/v1/pilot-requests",
        headers=headers,
        json=_payload(company="Different company on a transport retry"),
    )
    assert retried.status_code == 202
    assert retried.json()["request_id"] == created.json()["request_id"]
    assert store.records[created.json()["request_id"]]["company"] == (
        "Example Development"
    )


def test_honeypot_submission_returns_generic_receipt_without_storage() -> None:
    store = FakePilotStore()
    app.dependency_overrides[pilot_requests.get_store] = lambda: store
    client = TestClient(app)

    response = client.post(
        "/v1/pilot-requests",
        headers={"Idempotency-Key": "automated-request-123456"},
        json=_payload(website="https://spam.invalid"),
    )

    assert response.status_code == 202
    assert response.json()["status"] == "received"
    assert response.json()["request_id"].startswith("pr_")
    assert store.create_calls == []
    assert store.records == {}


@pytest.mark.parametrize(
    ("headers", "payload", "expected_status"),
    [
        ({}, _payload(), 400),
        ({"Idempotency-Key": "too-short"}, _payload(), 400),
        (
            {"Idempotency-Key": "valid-request-key-12345"},
            _payload(work_email="not-an-email"),
            422,
        ),
        (
            {"Idempotency-Key": "valid-request-key-12345"},
            _payload(consent=False),
            422,
        ),
        (
            {"Idempotency-Key": "valid-request-key-12345"},
            _payload(secret_owner="must not be accepted"),
            422,
        ),
        (
            {"Idempotency-Key": "valid-request-key-12345"},
            _payload(target_boroughs=[]),
            422,
        ),
        (
            {"Idempotency-Key": "valid-request-key-12345"},
            _payload(workflow_summary=" " * 20),
            422,
        ),
    ],
)
def test_public_pilot_request_fails_closed(
    headers: dict[str, str],
    payload: dict[str, Any],
    expected_status: int,
) -> None:
    store = FakePilotStore()
    app.dependency_overrides[pilot_requests.get_store] = lambda: store
    response = TestClient(app).post(
        "/v1/pilot-requests",
        headers=headers,
        json=payload,
    )
    assert response.status_code == expected_status
    assert response.headers["cache-control"] == "no-store"
    assert store.records == {}


def test_public_pilot_request_is_rate_limited() -> None:
    store = FakePilotStore()
    app.dependency_overrides[pilot_requests.get_store] = lambda: store
    client = TestClient(app)

    for index in range(3):
        response = client.post(
            "/v1/pilot-requests",
            headers={"Idempotency-Key": f"rate-limit-request-{index:08d}"},
            json=_payload(work_email=f"pilot-{index}@example.com"),
        )
        assert response.status_code == 202

    limited = client.post(
        "/v1/pilot-requests",
        headers={"Idempotency-Key": "rate-limit-request-99999999"},
        json=_payload(work_email="pilot-4@example.com"),
    )
    assert limited.status_code == 429
    assert limited.json()["detail"] == "Rate limit exceeded"


def test_admin_queue_is_private_and_status_controlled(auth_override) -> None:
    store = FakePilotStore()
    store.records[_REQUEST_ID] = {
        **_payload(website=""),
        "request_id": _REQUEST_ID,
        "status": "new",
        "created_at": _NOW,
        "updated_at": _NOW,
        "expires_at": _NOW + timedelta(days=365),
    }
    store.records[_REQUEST_ID].pop("website")
    app.dependency_overrides[pilot_requests.get_store] = lambda: store
    client = TestClient(app)

    unauthenticated = client.get("/v1/pilot-requests")
    assert unauthenticated.status_code == 401

    auth_override(app_user_id="regular-user", is_admin=False)
    forbidden = client.get("/v1/pilot-requests")
    assert forbidden.status_code == 403

    auth_override(app_user_id="admin-user", is_admin=True)
    listed = client.get("/v1/pilot-requests?status=new&limit=25")
    assert listed.status_code == 200, listed.text
    assert listed.headers["cache-control"] == "private, no-store"
    assert listed.headers["vary"] == "Authorization, X-API-Key"
    assert listed.json()["items"][0]["request_id"] == _REQUEST_ID
    assert "status_updated_by" not in listed.json()["items"][0]

    updated = client.patch(
        f"/v1/pilot-requests/{_REQUEST_ID}",
        json={
            "schema_version": "citylens/pilot-request-status@v1",
            "status": "contacted",
        },
    )
    assert updated.status_code == 200
    assert updated.headers["cache-control"] == "private, no-store"
    assert updated.json()["status"] == "contacted"
    assert "status_updated_by" not in updated.json()

    missing = client.patch(
        "/v1/pilot-requests/pr_ffffffffffffffffffffffffffffffff",
        json={
            "schema_version": "citylens/pilot-request-status@v1",
            "status": "contacted",
        },
    )
    assert missing.status_code == 404

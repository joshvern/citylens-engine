from __future__ import annotations

import json
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")


class FakeStore:
    def get_run(self, run_id: str):
        if run_id != "r1":
            return None
        now = datetime.utcnow()
        return {
            "run_id": "r1",
            "user_id": "u1",
            "status": "succeeded",
            "stage": "done",
            "progress": 100,
            "request": {"address": "x"},
            "error": None,
            "execution_id": "exec-1",
            "created_at": now,
            "updated_at": now,
        }

    def list_artifacts(self, run_id: str):
        if run_id != "r1":
            return []
        return [
            {
                "name": "preview.png",
                "gcs_uri": "gs://b/runs/r1/preview.png",
                "gcs_object": "runs/r1/preview.png",
                "sha256": "deadbeef",
                "size_bytes": 123,
                "created_at": datetime.utcnow(),
            }
        ]


class FakeGcs:
    def signed_url(self, *, object_name: str, ttl_seconds: int) -> str:
        return f"https://signed.example/{object_name}?ttl={ttl_seconds}"


def test_demo_featured_no_api_key(monkeypatch, tmp_path) -> None:
    _set_required_env(monkeypatch)
    allowlist = tmp_path / "demo_runs.json"
    allowlist.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": "r1",
                        "category": "Examples",
                        "label": "Example",
                        "address": "100 E 21st St Brooklyn, NY 11226",
                        "imagery_year": 2024,
                        "baseline_year": 2017,
                        "segmentation_backend": "sam2",
                        "outputs": ["preview.png"],
                    }
                ]
            }
        )
    )
    monkeypatch.setenv("CITYLENS_DEMO_ALLOWLIST_PATH", str(allowlist))

    client = TestClient(app)
    resp = client.get("/v1/demo/featured")
    assert resp.status_code == 200
    body = resp.json()
    assert "Examples" in body
    assert body["Examples"][0]["run_id"] == "r1"


def test_demo_run_allowlisted_signed_urls(monkeypatch, tmp_path) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("CITYLENS_SIGN_URLS", "true")
    monkeypatch.setenv("CITYLENS_SIGN_URL_TTL_SECONDS", "123")

    allowlist = tmp_path / "demo_runs.json"
    allowlist.write_text(json.dumps({"runs": [{"run_id": "r1"}]}))
    monkeypatch.setenv("CITYLENS_DEMO_ALLOWLIST_PATH", str(allowlist))

    app.dependency_overrides[runs_routes.get_store] = lambda: FakeStore()
    app.dependency_overrides[runs_routes.get_gcs] = lambda: FakeGcs()

    client = TestClient(app)

    resp = client.get("/v1/demo/runs/r1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "r1"
    assert body["artifacts"][0]["signed_url"].startswith("https://signed.example/")

    resp2 = client.get("/v1/demo/runs/not-allowlisted")
    assert resp2.status_code == 404

    app.dependency_overrides = {}

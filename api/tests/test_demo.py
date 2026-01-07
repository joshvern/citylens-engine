from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.routes import demo as demo_routes
from app.services.demo_registry import DemoRegistry


class FakeStore:
    def __init__(self, *, run: dict, artifacts: list[dict]) -> None:
        self._run = run
        self._artifacts = artifacts

    def get_run(self, run_id: str):
        if run_id != self._run.get("run_id"):
            return None
        return self._run

    def list_artifacts(self, run_id: str):
        if run_id != self._run.get("run_id"):
            return []
        return self._artifacts


class FakeGcs:
    def signed_url(self, *, object_name: str, ttl_seconds: int) -> str:
        return f"https://signed.invalid/{object_name}?ttl={ttl_seconds}"


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("CITYLENS_REGION", "us-central1")
    monkeypatch.setenv("CITYLENS_BUCKET", "test-bucket")
    monkeypatch.setenv("CITYLENS_JOB_NAME", "test-job")
    monkeypatch.setenv("CITYLENS_API_KEYS", "dev-key-1")


def test_demo_featured_no_api_key_required(monkeypatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)

    demo_file = tmp_path / "demo_runs.json"
    demo_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "category": "Featured",
                        "run_id": "demo-1",
                        "label": "A",
                        "address": "1 Market St",
                        "imagery_year": 2024,
                        "baseline_year": 2017,
                        "segmentation_backend": "sam2",
                        "outputs": ["previews", "change", "mesh"],
                    }
                ]
            }
        )
    )

    app.dependency_overrides[demo_routes.get_demo_registry] = lambda: DemoRegistry(json_path=str(demo_file))

    client = TestClient(app)
    resp = client.get("/v1/demo/featured")
    assert resp.status_code == 200

    body = resp.json()
    assert "Featured" in body
    assert body["Featured"][0]["run_id"] == "demo-1"

    app.dependency_overrides = {}


def test_demo_run_allowlist_enforced(monkeypatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("CITYLENS_SIGN_URLS", "1")
    monkeypatch.setenv("CITYLENS_SIGN_URL_TTL_SECONDS", "300")

    demo_file = tmp_path / "demo_runs.json"
    demo_file.write_text(json.dumps({"runs": [{"run_id": "demo-allow", "label": "A", "address": "x", "imagery_year": 2024, "baseline_year": 2017, "segmentation_backend": "sam2", "outputs": []}]}))

    run_doc = {
        "run_id": "demo-allow",
        "user_id": "any",
        "status": "succeeded",
        "stage": "done",
        "progress": 100,
        "request": {"address": "x"},
        "error": None,
        "execution_id": None,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    artifacts = [
        {
            "name": "preview.png",
            "gcs_uri": "gs://b/runs/demo-allow/preview.png",
            "gcs_object": "runs/demo-allow/preview.png",
            "sha256": "x",
            "size_bytes": 1,
            "created_at": datetime.utcnow(),
        }
    ]

    app.dependency_overrides[demo_routes.get_demo_registry] = lambda: DemoRegistry(json_path=str(demo_file))
    app.dependency_overrides[demo_routes.get_store] = lambda: FakeStore(run=run_doc, artifacts=artifacts)
    app.dependency_overrides[demo_routes.get_gcs] = lambda: FakeGcs()

    client = TestClient(app)

    # allowlisted
    ok = client.get("/v1/demo/runs/demo-allow")
    assert ok.status_code == 200
    out = ok.json()
    assert out["run_id"] == "demo-allow"
    assert isinstance(out.get("artifacts"), list)
    assert out["artifacts"][0]["signed_url"].startswith("https://signed.invalid/")

    # not allowlisted, even if it might exist elsewhere
    miss = client.get("/v1/demo/runs/not-allowlisted")
    assert miss.status_code == 404

    app.dependency_overrides = {}

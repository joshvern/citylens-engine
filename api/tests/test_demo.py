from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import demo as demo_routes
from app.services.demo_bundle import EXPECTED_DEMO_ARTIFACTS
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


@pytest.fixture(autouse=True)
def _reset_demo_registry_cache():
    old = demo_routes._DEMO_REGISTRY
    demo_routes._DEMO_REGISTRY = None
    try:
        yield
    finally:
        demo_routes._DEMO_REGISTRY = None
        app.dependency_overrides = {}
        demo_routes._DEMO_REGISTRY = old


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

    app.dependency_overrides[demo_routes.get_demo_registry] = lambda: DemoRegistry(
        json_path=str(demo_file)
    )

    client = TestClient(app)
    resp = client.get("/v1/demo/featured")
    assert resp.status_code == 200

    body = resp.json()
    assert "Featured" in body
    assert body["Featured"][0]["run_id"] == "demo-1"


def test_demo_run_allowlist_enforced(monkeypatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("CITYLENS_SIGN_URLS", "1")
    monkeypatch.setenv("CITYLENS_SIGN_URL_TTL_SECONDS", "300")

    demo_file = tmp_path / "demo_runs.json"
    demo_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": "demo-allow",
                        "label": "A",
                        "address": "x",
                        "imagery_year": 2024,
                        "baseline_year": 2017,
                        "segmentation_backend": "sam2",
                        "outputs": [],
                    }
                ]
            }
        )
    )

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

    app.dependency_overrides[demo_routes.get_demo_registry] = lambda: DemoRegistry(
        json_path=str(demo_file)
    )
    app.dependency_overrides[demo_routes.get_store] = lambda: FakeStore(
        run=run_doc, artifacts=artifacts
    )
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


def test_demo_run_serves_static_bundle(monkeypatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)

    demo_file = tmp_path / "demo_runs.json"
    demo_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "category": "Featured",
                        "run_id": "demo-static",
                        "label": "Static demo",
                        "address": "100 E 21st St Brooklyn, NY 11226",
                        "imagery_year": 2024,
                        "baseline_year": 2017,
                        "segmentation_backend": "sam2",
                        "outputs": ["previews", "change", "mesh"],
                    }
                ]
            }
        )
    )

    artifacts_root = tmp_path / "demo_artifacts" / "demo-static"
    artifacts_root.mkdir(parents=True)
    (artifacts_root / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n")
    (artifacts_root / "change.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"kind": "added", "crs": "EPSG:4326"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-73.9580, 40.6600],
                                    [-73.9576, 40.6600],
                                    [-73.9576, 40.6603],
                                    [-73.9580, 40.6603],
                                    [-73.9580, 40.6600],
                                ]
                            ],
                        },
                    }
                ],
            }
        )
    )
    (artifacts_root / "mesh.ply").write_text(
        (
            "ply\n"
            "format ascii 1.0\n"
            "element vertex 3\n"
            "property float x\n"
            "property float y\n"
            "property float z\n"
            "element face 1\n"
            "property list uchar int vertex_indices\n"
            "end_header\n"
            "0 0 0\n"
            "1 0 0\n"
            "0 1 0\n"
            "3 0 1 2\n"
        )
    )
    (artifacts_root / "run_summary.json").write_text(
        json.dumps(
            {
                "qa": {
                    "reference_case_id": "100 E 21st St Brooklyn, NY 11226",
                    "baseline_footprints_used": True,
                    "lidar_used": False,
                    "mask_iou": 0.91,
                    "change_polygon_f1": 0.87,
                    "mesh_footprint_iou": 0.86,
                    "parity_status": "demo_bundle",
                },
                "performance": {
                    "total_runtime_seconds": 12.3,
                    "stage_timings_seconds": {"fetch": 1.0, "segment": 8.2, "change": 1.7},
                },
            }
        )
    )

    monkeypatch.setenv("CITYLENS_DEMO_ARTIFACTS_PATH", str(tmp_path / "demo_artifacts"))

    app.dependency_overrides[demo_routes.get_demo_registry] = lambda: DemoRegistry(
        json_path=str(demo_file)
    )
    app.dependency_overrides[demo_routes.get_store] = lambda: None
    app.dependency_overrides[demo_routes.get_gcs] = lambda: None

    client = TestClient(app)
    resp = client.get("/v1/demo/runs/demo-static")
    assert resp.status_code == 200

    body = resp.json()
    assert body["run_id"] == "demo-static"
    assert body["status"] == "succeeded"
    artifact_names = {artifact["name"] for artifact in body["artifacts"]}
    assert artifact_names == set(EXPECTED_DEMO_ARTIFACTS)
    preview_artifact = next(
        artifact
        for artifact in body["artifacts"]
        if artifact["name"] == "preview.png"
    )
    assert preview_artifact["signed_url"] == "http://testserver/v1/demo/artifacts/demo-static/preview.png"

    artifact_resp = client.get("/v1/demo/artifacts/demo-static/run_summary.json")
    assert artifact_resp.status_code == 200
    assert artifact_resp.headers["content-type"].startswith("application/json")
    assert artifact_resp.json()["qa"]["parity_status"] == "demo_bundle"


def test_demo_bundle_validation_fails_fast_on_startup(monkeypatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)

    demo_file = tmp_path / "demo_runs.json"
    demo_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "category": "Featured",
                        "run_id": "demo-bad",
                        "label": "Broken bundle",
                        "address": "100 E 21st St Brooklyn, NY 11226",
                        "imagery_year": 2024,
                        "baseline_year": 2017,
                        "segmentation_backend": "sam2",
                        "outputs": ["previews", "change", "mesh"],
                    }
                ]
            }
        )
    )

    artifacts_root = tmp_path / "demo_artifacts" / "demo-bad"
    artifacts_root.mkdir(parents=True)
    (artifacts_root / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n")

    monkeypatch.setenv("CITYLENS_DEMO_RUNS_PATH", str(demo_file))
    monkeypatch.setenv("CITYLENS_DEMO_ARTIFACTS_PATH", str(tmp_path / "demo_artifacts"))

    with pytest.raises(RuntimeError, match="Invalid bundled demo artifacts"):
        with TestClient(app):
            pass


def test_demo_routes_allow_vercel_preview_cors(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    client = TestClient(app)

    origin = "https://citylens-web-git-demo-josh.vercel.app"
    resp = client.options(
        "/v1/demo/featured",
        headers={
            "origin": origin,
            "access-control-request-method": "GET",
        },
    )

    assert resp.status_code == 204
    assert resp.headers["access-control-allow-origin"] == origin


def test_live_routes_keep_strict_cors(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    client = TestClient(app)

    resp = client.options(
        "/v1/runs",
        headers={
            "origin": "https://citylens-web-git-demo-josh.vercel.app",
            "access-control-request-method": "GET",
        },
    )

    assert resp.status_code == 403

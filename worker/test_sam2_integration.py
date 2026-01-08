#!/usr/bin/env python3
"""
Smoke test: ensure citylens-core with SAM2 can be invoked from engine worker context.
This mimics what the worker does: load request, invoke run_citylens, upload artifacts.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from services.core_adapter import CitylensRequest, run_citylens


def test_sam2_request_structure() -> None:
    """Verify a SAM2 request can be created and passed to run_citylens."""
    req_dict = {
        "address": "1 Market St",
        "imagery_year": 2024,
        "baseline_year": 2017,
        "segmentation_backend": "sam2",
        "sam2_cfg": "configs/sam2.1/sam2.1_hiera_s.yaml",
        "sam2_checkpoint": "weights/sam2.1_hiera_small.pt",
        "outputs": ["previews", "change", "mesh"],
    }
    req = CitylensRequest.model_validate(req_dict)
    assert req.segmentation_backend == "sam2"
    assert req.outputs == ["previews", "change", "mesh"]
    print("✓ CitylensRequest with SAM2 validates")


def test_core_handles_missing_assets() -> None:
    """Verify core fails gracefully when SAM2 assets are missing."""
    req_dict = {
        "address": "test",
        "segmentation_backend": "sam2",
        "outputs": ["preview"],  # Request preview only
    }
    req = CitylensRequest.model_validate(req_dict)

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)
        # Create minimal input images so preflight passes input check
        (work_dir / "orthophoto.png").write_bytes(b"x")
        (work_dir / "baseline.png").write_bytes(b"x")

        # Call run_citylens; should fail due to missing SAM2 assets
        result = run_citylens(req, work_dir)

        # Verify failure contract: only summary returned
        assert "summary" in result, f"Expected 'summary' key in failure result, got {result.keys()}"
        assert len(result) == 1, f"Expected only summary on failure, got {list(result.keys())}"

        summary_path = Path(result["summary"])
        assert summary_path.exists(), f"Summary file not found: {summary_path}"

        summary = json.loads(summary_path.read_text())
        assert summary.get("ok") is False, "Summary should indicate failure"
        assert summary.get("error_code") == "missing_dependency", f"Got error_code={summary.get('error_code')}"

        print("✓ Core returns summary-only on missing SAM2 assets")


def test_worker_artifact_flow() -> None:
    """Simulate the worker's artifact flow."""
    # Mock: simulate artifacts map as the worker would create it
    artifacts_map = {
        "summary": Path("/tmp/run_summary.json"),
        "preview": Path("/tmp/preview.png"),
        "change": Path("/tmp/change.geojson"),
    }

    # Worker extracts names and validates
    expected_names = {"preview.png", "change.geojson", "mesh.ply", "run_summary.json"}
    for _, local_path in artifacts_map.items():
        name = Path(local_path).name
        assert name in expected_names, f"Unexpected artifact: {name}"

    # Worker builds upload map
    uploaded_by_name = {}
    for _, local_path in artifacts_map.items():
        local_path = Path(local_path)
        name = local_path.name
        # Simulate upload
        gcs_uri = f"gs://test-bucket/runs/test-run/{name}"
        uploaded_by_name[name] = {"gcs_uri": gcs_uri}

    # Worker stores compact map
    run_update = {"artifacts": {k: v.get("gcs_uri") for k, v in uploaded_by_name.items()}}
    assert "artifacts" in run_update
    assert len(run_update["artifacts"]) == 3

    print("✓ Worker artifact flow validated")


if __name__ == "__main__":
    test_sam2_request_structure()
    test_core_handles_missing_assets()
    test_worker_artifact_flow()
    print("\n✓ All integration tests passed")

from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts import precompute_demo_runs as precompute


def test_validate_completed_run_rejects_missing_required_artifacts() -> None:
    with pytest.raises(RuntimeError, match="missing required artifacts"):
        precompute._validate_completed_run(
            {
                "run_id": "run-123",
                "artifacts": {
                    "preview.png": {"name": "preview.png", "signed_url": "https://example.test/preview.png"}
                },
            },
            run_id="run-123",
        )


def test_main_writes_only_valid_precomputed_demo_runs(
    monkeypatch, tmp_path: Path
) -> None:
    addresses_path = tmp_path / "demo_addresses.json"
    out_path = tmp_path / "demo_runs.json"
    addresses_path.write_text(
        json.dumps(
            [
                {
                    "category": "Featured",
                    "label": "Brooklyn reference",
                    "address": "100 E 21st St Brooklyn, NY 11226",
                    "imagery_year": 2024,
                    "baseline_year": 2017,
                    "segmentation_backend": "sam2",
                    "outputs": ["previews", "change", "mesh"],
                }
            ]
        ),
        encoding="utf-8",
    )

    calls: list[tuple[str, str]] = []

    def fake_http_json(method: str, url: str, *, api_key: str, body=None, timeout_s: float = 30.0):
        calls.append((method, url))
        if method == "POST" and url.endswith("/v1/runs"):
            return {"run_id": "demo-run-1"}
        if method == "GET" and url.endswith("/v1/runs/demo-run-1"):
            return {
                "run_id": "demo-run-1",
                "status": "succeeded",
                "stage": "complete",
                "progress": 100,
                "artifacts": [
                    {"name": "preview.png", "signed_url": "https://example.test/preview.png"},
                    {"name": "change.geojson", "signed_url": "https://example.test/change.geojson"},
                    {"name": "mesh.ply", "signed_url": "https://example.test/mesh.ply"},
                    {"name": "run_summary.json", "signed_url": "https://example.test/run_summary.json"},
                ],
            }
        raise AssertionError(f"Unexpected API call: {method} {url}")

    def fake_probe_url(url: str, *, timeout_s: float = 30.0) -> None:
        assert url in {
            "https://example.test/preview.png",
            "https://example.test/change.geojson",
            "https://example.test/mesh.ply",
        }

    def fake_fetch_json_url(url: str, *, timeout_s: float = 30.0):
        assert url == "https://example.test/run_summary.json"
        return {
            "qa": {"reference_case_id": "100 E 21st St Brooklyn, NY 11226"},
            "performance": {"total_runtime_seconds": 1.23, "stage_timings_seconds": {"fetch": 0.4}},
        }

    monkeypatch.setattr(precompute, "_http_json", fake_http_json)
    monkeypatch.setattr(precompute, "_probe_url", fake_probe_url)
    monkeypatch.setattr(precompute, "_fetch_json_url", fake_fetch_json_url)

    exit_code = precompute.main(
        [
            "--api-base",
            "https://api.example.test",
            "--admin-api-key",
            "test-admin-key",
            "--addresses",
            str(addresses_path),
            "--out",
            str(out_path),
            "--poll-interval-seconds",
            "0",
            "--timeout-seconds",
            "5",
        ]
    )

    assert exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload == {
        "runs": [
            {
                "category": "Featured",
                "run_id": "demo-run-1",
                "label": "Brooklyn reference",
                "address": "100 E 21st St Brooklyn, NY 11226",
                "imagery_year": 2024,
                "baseline_year": 2017,
                "segmentation_backend": "sam2",
                "outputs": ["previews", "change", "mesh"],
            }
        ]
    }
    assert ("POST", "https://api.example.test/v1/runs") in calls
    assert ("GET", "https://api.example.test/v1/runs/demo-run-1") in calls


def test_main_fails_when_completed_run_is_missing_required_artifacts(
    monkeypatch, tmp_path: Path
) -> None:
    addresses_path = tmp_path / "demo_addresses.json"
    out_path = tmp_path / "demo_runs.json"
    addresses_path.write_text(
        json.dumps(
            [
                {
                    "category": "Featured",
                    "label": "Brooklyn reference",
                    "address": "100 E 21st St Brooklyn, NY 11226",
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_http_json(method: str, url: str, *, api_key: str, body=None, timeout_s: float = 30.0):
        if method == "POST" and url.endswith("/v1/runs"):
            return {"run_id": "demo-run-bad"}
        if method == "GET" and url.endswith("/v1/runs/demo-run-bad"):
            return {
                "run_id": "demo-run-bad",
                "status": "succeeded",
                "stage": "complete",
                "progress": 100,
                "artifacts": [{"name": "preview.png", "signed_url": "https://example.test/preview.png"}],
            }
        raise AssertionError(f"Unexpected API call: {method} {url}")

    monkeypatch.setattr(precompute, "_http_json", fake_http_json)

    with pytest.raises(RuntimeError, match="missing required artifacts"):
        precompute.main(
            [
                "--api-base",
                "https://api.example.test",
                "--admin-api-key",
                "test-admin-key",
                "--addresses",
                str(addresses_path),
                "--out",
                str(out_path),
                "--poll-interval-seconds",
                "0",
                "--timeout-seconds",
                "5",
            ]
        )

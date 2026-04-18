from __future__ import annotations

import json
from pathlib import Path

from services import pipeline_runner


class FakeStore:
    def __init__(self) -> None:
        self.updates: list[tuple[str, dict]] = []

    def update_run(self, run_id: str, patch: dict) -> None:
        self.updates.append((run_id, dict(patch)))

    def write_artifact(self, *, run_id: str, artifact_id: str, doc: dict) -> None:
        return None


class FakeGcs:
    def __init__(self) -> None:
        self.bucket_name = "test-bucket"
        self.client = object()

    def upload(self, *, local_path: Path, object_name: str):
        return f"gs://test-bucket/{object_name}", int(local_path.stat().st_size), "sha256"


def test_pipeline_marks_failed_summary_as_structured_error(monkeypatch, tmp_path: Path) -> None:
    def fake_inputs(**kwargs):
        work_dir = Path(kwargs["work_dir"])
        (work_dir / "orthophoto.tif").write_bytes(b"x")
        (work_dir / "orthophoto.png").write_bytes(b"x")
        (work_dir / "baseline.tif").write_bytes(b"x")
        (work_dir / "baseline.png").write_bytes(b"x")
        (work_dir / "input_manifest.json").write_text("{}")
        return {
            "input_manifest_path": str(work_dir / "input_manifest.json"),
            "orthophoto_path": str(work_dir / "orthophoto.tif"),
            "baseline_path": str(work_dir / "baseline.tif"),
        }

    def fake_run_citylens(req, work_dir, progress_cb=None):
        assert str(req.orthophoto_path).endswith("orthophoto.tif")
        assert str(req.baseline_path).endswith("baseline.tif")
        work_dir = Path(work_dir)
        (work_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n")
        (work_dir / "change.geojson").write_text('{"type":"FeatureCollection","features":[]}')
        (work_dir / "mesh.ply").write_text("ply\nformat ascii 1.0\nend_header\n")
        (work_dir / "run_summary.json").write_text(
            json.dumps(
                {
                    "ok": False,
                    "error_code": "PIPELINE_FAILED",
                    "error_message": "core failed",
                }
            )
        )
        return {
            "preview": work_dir / "preview.png",
            "change": work_dir / "change.geojson",
            "mesh": work_dir / "mesh.ply",
            "summary": work_dir / "run_summary.json",
        }

    monkeypatch.setattr(pipeline_runner, "ensure_work_dir_inputs", fake_inputs)
    monkeypatch.setattr(pipeline_runner, "run_citylens", fake_run_citylens)

    store = FakeStore()
    gcs = FakeGcs()
    settings = type("S", (), {"work_root": str(tmp_path)})()

    pipeline_runner.run(
        run_id="run-1",
        request_dict={"address": "1 Main St", "segmentation_backend": "sam2"},
        work_root=tmp_path,
        store=store,
        gcs=gcs,
        settings=settings,
    )

    assert store.updates[-1][1]["status"] == "failed"
    error = store.updates[-1][1]["error"]
    assert error["code"] == "PIPELINE_FAILED"
    assert error["message"] == "core failed"
    assert error["stage"] == "done"


def _ok_inputs_factory(tmp_path: Path):
    def fake_inputs(**kwargs):
        work_dir = Path(kwargs["work_dir"])
        (work_dir / "orthophoto.tif").write_bytes(b"x")
        (work_dir / "orthophoto.png").write_bytes(b"x")
        (work_dir / "baseline.tif").write_bytes(b"x")
        (work_dir / "baseline.png").write_bytes(b"x")
        (work_dir / "input_manifest.json").write_text("{}")
        return {
            "input_manifest_path": str(work_dir / "input_manifest.json"),
            "orthophoto_path": str(work_dir / "orthophoto.tif"),
            "baseline_path": str(work_dir / "baseline.tif"),
        }

    return fake_inputs


def test_tripwire_fires_on_placeholder_sized_mesh(monkeypatch, tmp_path: Path) -> None:
    def fake_run_citylens(req, work_dir, progress_cb=None):
        work_dir = Path(work_dir)
        # Above threshold for preview + change, below threshold for mesh.
        (work_dir / "preview.png").write_bytes(b"\x89PNG" + b"\x00" * 20_000)
        (work_dir / "change.geojson").write_text(
            '{"type":"FeatureCollection","features":['
            + ",".join(
                [
                    '{"type":"Feature","properties":{"kind":"added"},"geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}}'
                ]
                * 2
            )
            + "]}"
        )
        # This is the scaffold-era placeholder: ~154 bytes, just a PLY header.
        (work_dir / "mesh.ply").write_text(
            "ply\nformat ascii 1.0\nelement vertex 0\n"
            "property float x\nproperty float y\nproperty float z\n"
            "element face 0\nproperty list uchar int vertex_indices\nend_header\n"
        )
        (work_dir / "run_summary.json").write_text('{"ok": true}')
        return {
            "preview": work_dir / "preview.png",
            "change": work_dir / "change.geojson",
            "mesh": work_dir / "mesh.ply",
            "summary": work_dir / "run_summary.json",
        }

    monkeypatch.setattr(pipeline_runner, "ensure_work_dir_inputs", _ok_inputs_factory(tmp_path))
    monkeypatch.setattr(pipeline_runner, "run_citylens", fake_run_citylens)

    store = FakeStore()
    gcs = FakeGcs()
    settings = type("S", (), {"work_root": str(tmp_path)})()

    pipeline_runner.run(
        run_id="run-trip",
        request_dict={"address": "1 Main St", "segmentation_backend": "sam2"},
        work_root=tmp_path,
        store=store,
        gcs=gcs,
        settings=settings,
    )

    final = store.updates[-1][1]
    assert final["status"] == "failed"
    assert final["error"]["code"] == "PLACEHOLDER_ARTIFACT_DETECTED"
    assert "mesh.ply" in final["error"]["message"]


def test_tripwire_passes_on_real_sized_artifacts(monkeypatch, tmp_path: Path) -> None:
    def fake_run_citylens(req, work_dir, progress_cb=None):
        work_dir = Path(work_dir)
        (work_dir / "preview.png").write_bytes(b"\x89PNG" + b"\x00" * 50_000)
        (work_dir / "change.geojson").write_text(
            '{"type":"FeatureCollection","features":['
            + ",".join(
                [
                    '{"type":"Feature","properties":{"kind":"added"},"geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}}'
                ]
                * 2
            )
            + "]}"
        )
        (work_dir / "mesh.ply").write_text("ply\n" + "x" * 20_000)
        (work_dir / "run_summary.json").write_text('{"ok": true}')
        return {
            "preview": work_dir / "preview.png",
            "change": work_dir / "change.geojson",
            "mesh": work_dir / "mesh.ply",
            "summary": work_dir / "run_summary.json",
        }

    monkeypatch.setattr(pipeline_runner, "ensure_work_dir_inputs", _ok_inputs_factory(tmp_path))
    monkeypatch.setattr(pipeline_runner, "run_citylens", fake_run_citylens)

    store = FakeStore()
    gcs = FakeGcs()
    settings = type("S", (), {"work_root": str(tmp_path)})()

    pipeline_runner.run(
        run_id="run-ok",
        request_dict={"address": "1 Main St", "segmentation_backend": "sam2"},
        work_root=tmp_path,
        store=store,
        gcs=gcs,
        settings=settings,
    )

    final = store.updates[-1][1]
    assert final["status"] == "succeeded"
    assert final["error"] is None

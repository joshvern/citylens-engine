from __future__ import annotations

import json
from pathlib import Path

from scripts import parity_reference_case as parity

from services.nysgis import AddressAssets, LidarTile


def _write_geojson(path: Path, features: list[dict]) -> None:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))


def _write_ascii_ply(path: Path, points: list[tuple[float, float, float]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write("ply\n")
        f.write("format ascii 1.0\n")
        f.write(f"element vertex {len(points)}\n")
        f.write("property float x\nproperty float y\nproperty float z\n")
        f.write("element face 0\n")
        f.write("property list uchar int vertex_indices\n")
        f.write("end_header\n")
        for x, y, z in points:
            f.write(f"{x} {y} {z}\n")


def test_build_parity_report_computes_metrics_from_local_artifacts(
    monkeypatch, tmp_path: Path
) -> None:
    run_dir = tmp_path / "modular"
    run_dir.mkdir()
    reference_root = tmp_path / "Urban3D-DeepRecon"
    data_dir = reference_root / "data"
    data_dir.mkdir(parents=True)

    def fake_run_modular_case(address: str, work_dir: Path, *, gcs_client):
        work_dir.mkdir(parents=True, exist_ok=True)
        _write_geojson(
            work_dir / "change.geojson",
            [
                {
                    "type": "Feature",
                    "properties": {"kind": "added"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"kind": "removed"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[3, 3], [5, 3], [5, 5], [3, 5], [3, 3]]],
                    },
                },
            ],
        )
        _write_ascii_ply(work_dir / "mesh.ply", [(0, 0, 0), (2, 0, 0), (2, 2, 1), (0, 2, 1)])
        (work_dir / "input_manifest.json").write_text(json.dumps({"ok": True}))
        return (
            {"orthophoto_path": str(work_dir / "orthophoto.tif")},
            {
                "performance": {
                    "total_runtime_seconds": 1.5,
                    "stage_timings_seconds": {"segment": 0.2},
                }
            },
        )

    _write_geojson(
        data_dir / "123456_sam2_prompt_deadbeef_segmentation_changes_added.geojson",
        [
            {
                "type": "Feature",
                "properties": {"kind": "added"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
                },
            }
        ],
    )
    _write_geojson(
        data_dir / "123456_sam2_prompt_deadbeef_segmentation_changes_removed.geojson",
        [
            {
                "type": "Feature",
                "properties": {"kind": "removed"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[3, 3], [5, 3], [5, 5], [3, 5], [3, 3]]],
                },
            }
        ],
    )
    _write_ascii_ply(
        reference_root / "output_mesh.ply", [(0, 0, 0), (2, 0, 0), (2, 2, 1), (0, 2, 1)]
    )

    monkeypatch.setattr(parity, "_run_modular_case", fake_run_modular_case)
    fake_assets = AddressAssets(
        normalized_address="100 E 21st St Brooklyn, NY 11226",
        x=1.0,
        y=2.0,
        lidar_tile=LidarTile(
            tile_id="123456", filename="123456.las", direct_url="https://example.test/123456.las"
        ),
        ortho_zip_url="https://example.test/123456.zip",
    )
    monkeypatch.setattr(parity.NYSGISAPI, "get_assets_for_address", lambda self, addr: fake_assets)

    report = parity.build_parity_report(
        address=parity.FIXED_REFERENCE_ADDRESS,
        work_dir=run_dir,
        reference_root=reference_root,
        gcs_client=parity.NullGcsClient(),
    )

    assert report["reference_case_id"] == "100_e_21st_st_brooklyn_ny_11226"
    assert report["metrics"]["mask_iou"] == 1.0
    assert report["metrics"]["change_polygon_f1"] == 1.0
    assert report["metrics"]["mesh_footprint_iou"] == 1.0
    assert report["status"] == "complete"

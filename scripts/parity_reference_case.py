#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shapely.geometry import MultiPoint, shape
from shapely.ops import unary_union

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKER_ROOT = PROJECT_ROOT / "worker"
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from services.imagery_inputs import ensure_work_dir_inputs  # noqa: E402
from services.nysgis import NYSGISAPI  # noqa: E402
from services.pipeline_runner import run as run_pipeline  # noqa: E402
from services.settings import Settings  # noqa: E402

FIXED_REFERENCE_ADDRESS = "100 E 21st St Brooklyn, NY 11226"


@dataclass(frozen=True)
class NullBlob:
    def exists(self) -> bool:
        return False

    def download_to_filename(self, filename: str) -> None:
        raise AssertionError("unexpected download")

    def upload_from_filename(self, filename: str) -> None:
        return None


class NullBucket:
    def blob(self, object_name: str) -> NullBlob:
        return NullBlob()


class NullGcsClient:
    def bucket(self, bucket: str) -> NullBucket:
        return NullBucket()


class NullStore:
    def update_run(self, run_id: str, patch: dict) -> None:
        return None

    def write_artifact(self, *, run_id: str, artifact_id: str, doc: dict) -> None:
        return None


class NullGcsArtifacts:
    bucket_name = "null"
    client = NullGcsClient()

    def upload(self, *, local_path: Path, object_name: str):
        return f"gs://null/{object_name}", int(local_path.stat().st_size), "sha256"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_address(text: str) -> str:
    return " ".join((text or "").strip().split())


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (text or "").lower())
    return slug.strip("_") or "unknown"


def _feature_kind(feature: dict[str, Any], default_kind: str | None = None) -> str:
    props = feature.get("properties") or {}
    kind = props.get("kind")
    if isinstance(kind, str) and kind.strip():
        return kind.strip().lower()
    return (default_kind or "unknown").lower()


def _load_geojson_features(
    path: Path, *, default_kind: str | None = None
) -> list[tuple[str, Any]]:
    if not path.exists():
        return []
    data = _load_json(path)
    features: list[tuple[str, Any]] = []
    for feature in data.get("features") or []:
        geom = feature.get("geometry")
        if not geom:
            continue
        try:
            features.append((_feature_kind(feature, default_kind), shape(geom)))
        except Exception:
            continue
    return features


def _union_geometry(features: list[tuple[str, Any]], *, kind: str | None = None):
    geoms = [
        geom for feature_kind, geom in features if kind is None or feature_kind == kind
    ]
    if not geoms:
        return None
    try:
        return unary_union(geoms)
    except Exception:
        return None


def _iou(a, b) -> float | None:
    if a is None or b is None or a.is_empty or b.is_empty:
        return None
    inter = a.intersection(b).area
    union = a.union(b).area
    if union <= 0:
        return None
    return float(inter / union)


def _feature_f1(
    mod_features: list[tuple[str, Any]],
    ref_features: list[tuple[str, Any]],
    *,
    iou_threshold: float = 0.5,
) -> float | None:
    if not mod_features or not ref_features:
        return None

    candidates: list[tuple[float, int, int]] = []
    for mi, (_, mg) in enumerate(mod_features):
        for ri, (_, rg) in enumerate(ref_features):
            if mg.is_empty or rg.is_empty:
                continue
            if mod_features[mi][0] != ref_features[ri][0]:
                continue
            iou = _iou(mg, rg)
            if iou is None:
                continue
            candidates.append((iou, mi, ri))

    matched_mod: set[int] = set()
    matched_ref: set[int] = set()
    matches = 0
    for iou, mi, ri in sorted(candidates, key=lambda item: item[0], reverse=True):
        if iou < iou_threshold or mi in matched_mod or ri in matched_ref:
            continue
        matched_mod.add(mi)
        matched_ref.add(ri)
        matches += 1

    precision = matches / len(mod_features)
    recall = matches / len(ref_features)
    if precision + recall == 0:
        return None
    return float(2.0 * precision * recall / (precision + recall))


def _load_ascii_ply_vertices(path: Path) -> list[tuple[float, float, float]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        if f.readline().strip() != "ply":
            return []

        vertex_count = 0
        for line in f:
            line = line.strip()
            if line.startswith("element vertex "):
                vertex_count = int(line.rsplit(" ", 1)[-1])
            elif line == "end_header":
                break

        vertices: list[tuple[float, float, float]] = []
        for _ in range(vertex_count):
            line = f.readline()
            if not line:
                break
            parts = line.split()
            if len(parts) < 3:
                continue
            try:
                vertices.append((float(parts[0]), float(parts[1]), float(parts[2])))
            except ValueError:
                continue
        return vertices


def _footprint_from_ply(path: Path):
    vertices = _load_ascii_ply_vertices(path)
    if not vertices:
        return None
    points = MultiPoint([(x, y) for x, y, _ in vertices])
    hull = points.convex_hull
    return hull if not hull.is_empty else None


def _find_reference_artifacts(
    reference_root: Path, tile_id: str
) -> dict[str, Path | None]:
    data_dir = reference_root / "data"
    change_added = sorted(
        data_dir.glob(f"{tile_id}_*_segmentation_changes_added.geojson")
    )
    change_removed = sorted(
        data_dir.glob(f"{tile_id}_*_segmentation_changes_removed.geojson")
    )
    buildings = sorted(data_dir.glob(f"{tile_id}_*_segmentation_buildings.geojson"))

    mesh_candidates = [
        reference_root / "textured_mesh.ply",
        reference_root / "output_mesh.ply",
    ]
    mesh = next((p for p in mesh_candidates if p.exists()), None)

    return {
        "change_added": change_added[0] if change_added else None,
        "change_removed": change_removed[0] if change_removed else None,
        "buildings": buildings[0] if buildings else None,
        "mesh": mesh,
    }


def _combine_change_features(
    path_added: Path | None, path_removed: Path | None
) -> list[tuple[str, Any]]:
    features: list[tuple[str, Any]] = []
    if path_added and path_added.exists():
        features.extend(_load_geojson_features(path_added, default_kind="added"))
    if path_removed and path_removed.exists():
        features.extend(_load_geojson_features(path_removed, default_kind="removed"))
    return features


def _run_modular_case(
    address: str, run_dir: Path, *, gcs_client: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        manifest = ensure_work_dir_inputs(
            request=type(
                "Req",
                (),
                {"address": address, "orthophoto_path": None, "baseline_path": None},
            )(),
            work_dir=run_dir,
            gcs_client=gcs_client,
            bucket="null",
        )
    except Exception as exc:
        return {}, {
            "ok": False,
            "error_code": "INPUT_PREP_FAILED",
            "error_message": f"{type(exc).__name__}: {exc}",
            "performance": {"total_runtime_seconds": 0.0, "stage_timings_seconds": {}},
        }

    request = {
        "address": address,
        "segmentation_backend": "sam2",
        "outputs": ["previews", "change", "mesh"],
        "orthophoto_path": manifest["orthophoto_path"],
        "baseline_path": manifest["baseline_path"],
    }

    store = NullStore()
    gcs = NullGcsArtifacts()
    settings = Settings(
        project_id="null",
        region="null",
        bucket="null",
        work_root=str(run_dir.parent),
        reference_data_dir="/tmp/reference-data",
    )
    run_id = run_dir.name
    t0 = time.time()
    summary: dict[str, Any] = {}
    try:
        run_pipeline(
            run_id=run_id,
            request_dict=request,
            work_root=run_dir.parent,
            store=store,
            gcs=gcs,
            settings=settings,
        )
    except Exception as exc:
        # The harness must still emit a report if the modular execution cannot complete.
        summary_path = run_dir / "run_summary.json"
        if summary_path.exists():
            try:
                summary = _load_json(summary_path)
            except Exception:
                summary = {}
        else:
            summary = {}
        summary.setdefault("ok", False)
        summary.setdefault("error_code", "PIPELINE_FAILED")
        summary.setdefault("error_message", f"{type(exc).__name__}: {exc}")
    duration = max(0.0, time.time() - t0)
    summary_path = run_dir / "run_summary.json"
    if summary_path.exists():
        summary = _load_json(summary_path)
    summary.setdefault("performance", {})
    summary["performance"]["total_runtime_seconds"] = duration
    return manifest, summary


def build_parity_report(
    *,
    address: str,
    work_dir: Path,
    reference_root: Path,
    gcs_client: Any,
) -> dict[str, Any]:
    normalized_address = _normalize_address(address)
    resolver = NYSGISAPI()
    assets = resolver.get_assets_for_address(normalized_address)

    run_dir = work_dir / f"modular-{_slug(normalized_address)}"
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest, summary = _run_modular_case(
        normalized_address, run_dir, gcs_client=gcs_client
    )

    modular_change = run_dir / "change.geojson"
    modular_mesh = run_dir / "mesh.ply"

    reference = _find_reference_artifacts(reference_root, assets.lidar_tile.tile_id)
    reference_change_features = _combine_change_features(
        reference["change_added"], reference["change_removed"]
    )
    reference_change_union = _union_geometry(reference_change_features)
    modular_change_features = _load_geojson_features(modular_change)
    modular_change_union = _union_geometry(modular_change_features)

    reference_mesh = (
        _footprint_from_ply(reference["mesh"]) if reference["mesh"] else None
    )
    modular_mesh_footprint = _footprint_from_ply(modular_mesh)

    metrics = {
        "mask_iou": _iou(modular_change_union, reference_change_union)
        if reference_change_union is not None
        else None,
        "change_polygon_f1": _feature_f1(
            modular_change_features, reference_change_features
        )
        if reference_change_features
        else None,
        "mesh_footprint_iou": _iou(modular_mesh_footprint, reference_mesh)
        if reference_mesh is not None
        else None,
    }

    available = any(value is not None for value in metrics.values())
    status = (
        "complete"
        if all(value is not None for value in metrics.values())
        else "partial"
        if available
        else "reference_missing"
    )

    return {
        "address": normalized_address,
        "reference_case_id": _slug(normalized_address),
        "tile_id": assets.lidar_tile.tile_id,
        "work_dir": str(work_dir),
        "reference_root": str(reference_root),
        "reference_artifacts": {
            "change_added": str(reference["change_added"])
            if reference["change_added"]
            else None,
            "change_removed": str(reference["change_removed"])
            if reference["change_removed"]
            else None,
            "buildings": str(reference["buildings"])
            if reference["buildings"]
            else None,
            "mesh": str(reference["mesh"]) if reference["mesh"] else None,
        },
        "modular_artifacts": {
            "change": str(modular_change) if modular_change.exists() else None,
            "mesh": str(modular_mesh) if modular_mesh.exists() else None,
            "summary": str(run_dir / "run_summary.json"),
            "manifest": str(run_dir / "input_manifest.json"),
        },
        "metrics": metrics,
        "performance": {
            "total_runtime_seconds": float(
                summary.get("performance", {}).get("total_runtime_seconds") or 0.0
            ),
            "stage_timings_seconds": summary.get("performance", {}).get(
                "stage_timings_seconds"
            )
            or {},
        },
        "status": status,
        "reference_available": available,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Run the fixed CityLens parity case and write parity_report.json"
    )
    parser.add_argument(
        "--address",
        default=FIXED_REFERENCE_ADDRESS,
        help="Reference case address (default: the fixed Brooklyn parity case)",
    )
    parser.add_argument(
        "--work-dir",
        default=str(PROJECT_ROOT / ".parity-work"),
        help="Directory for modular pipeline artifacts",
    )
    parser.add_argument(
        "--reference-root",
        default=str(PROJECT_ROOT.parent / "Urban3D-DeepRecon"),
        help="Path to the read-only Urban3D-DeepRecon reference repo",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "parity_report.json"),
        help="Path to write parity_report.json",
    )

    args = parser.parse_args(argv)
    work_dir = Path(args.work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    report = build_parity_report(
        address=args.address,
        work_dir=work_dir,
        reference_root=Path(args.reference_root),
        gcs_client=NullGcsClient(),
    )
    _write_json(Path(args.output), report)
    print(Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

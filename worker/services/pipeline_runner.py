from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .core_adapter import CitylensRequest, run_citylens
from .firestore_store import FirestoreStore
from .gcs_artifacts import GcsArtifacts


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _infer_type(filename: str) -> str:
    lower = filename.lower()
    if lower.endswith(".png"):
        return "image/png"
    if lower.endswith(".geojson"):
        return "application/geo+json"
    if lower.endswith(".ply"):
        return "model/ply"
    if lower.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


def run(*, run_id: str, request_dict: dict[str, Any], work_root: Path, store: FirestoreStore, gcs: GcsArtifacts) -> None:
    work_dir = (work_root / run_id).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    def progress_cb(pct: int, stage: str | None = None) -> None:
        patch: dict[str, Any] = {"progress": int(pct)}
        if stage:
            patch["stage"] = str(stage)
        patch["status"] = "running"
        store.update_run(run_id, patch)

    req = CitylensRequest.model_validate(request_dict)

    artifacts_map = run_citylens(req, work_dir, progress_cb=progress_cb)

    # Upload artifacts: use the *core-produced filenames* (Path.name)
    expected_names = {"preview.png", "change.geojson", "mesh.ply", "run_summary.json"}

    uploaded_by_name: dict[str, dict[str, Any]] = {}

    for _, local_path in artifacts_map.items():
        local_path = Path(local_path)
        name = local_path.name
        if name not in expected_names:
            # Fail loudly if core contract changes.
            raise RuntimeError(f"Unexpected artifact filename from citylens-core: {name}")

        object_name = f"runs/{run_id}/{name}"
        gcs_uri, size_bytes, sha256 = gcs.upload(local_path=local_path, object_name=object_name)

        doc = {
            "name": name,
            "type": _infer_type(name),
            "gcs_uri": gcs_uri,
            "gcs_object": object_name,
            "sha256": sha256,
            "size_bytes": int(size_bytes),
            "created_at": _utcnow(),
        }

        store.write_artifact(run_id=run_id, artifact_id=name, doc=doc)
        uploaded_by_name[name] = doc

    # Convenience: also stash a compact map on the run document itself.
    # This makes it easy for the API/UI to show artifacts without extra reads.
    if uploaded_by_name:
        store.update_run(run_id, {"artifacts": {k: v.get("gcs_uri") for k, v in uploaded_by_name.items()}})

    # Determine success/failure from core run_summary.json (core may not raise).
    ok = True
    summary_path = work_dir / "run_summary.json"
    if summary_path.exists():
        try:
            import json

            summary = json.loads(summary_path.read_text())
            if isinstance(summary, dict) and summary.get("ok") is False:
                ok = False
        except Exception:
            ok = False

    if not ok:
        store.update_run(run_id, {"status": "failed", "stage": "done", "progress": 100, "error": "pipeline failed"})
        return

    store.update_run(run_id, {"status": "succeeded", "stage": "done", "progress": 100, "error": None})

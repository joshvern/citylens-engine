from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .core_adapter import CitylensRequest, run_citylens
from .firestore_store import FirestoreStore
from .gcs_artifacts import GcsArtifacts
from .imagery_inputs import ensure_work_dir_inputs
from .run_errors import build_error_payload
from .settings import Settings

logger = logging.getLogger(__name__)


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


def run(
    *,
    run_id: str,
    request_dict: dict[str, Any],
    work_root: Path,
    store: FirestoreStore,
    gcs: GcsArtifacts,
    settings: Settings,
) -> None:
    work_dir = (work_root / run_id).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    def progress_cb(pct: int, stage: str | None = None) -> None:
        patch: dict[str, Any] = {"progress": int(pct)}
        if stage:
            patch["stage"] = str(stage)
        patch["status"] = "running"
        store.update_run(run_id, patch)

    req = CitylensRequest.model_validate(request_dict)

    logger.info(
        "preparing work_dir inputs",
        extra={
            "run_id": run_id,
            "stage": "fetch_inputs",
            "work_root": str(settings.work_root),
        },
    )
    progress_cb(2, "fetch_inputs")
    manifest = ensure_work_dir_inputs(
        request=req,
        work_dir=work_dir,
        gcs_client=gcs.client,
        bucket=gcs.bucket_name,
    )

    req = req.model_copy(
        update={
            "orthophoto_path": manifest.get("orthophoto_path"),
            "baseline_path": manifest.get("baseline_path"),
        }
    )

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

        logger.info(
            "artifact_uploaded",
            extra={
                "run_id": run_id,
                "stage": "upload",
                "name": name,
                "size_bytes": int(size_bytes),
                "sha256": sha256,
                "gcs_uri": gcs_uri,
            },
        )

    # Convenience: also stash a compact map on the run document itself.
    # This makes it easy for the API/UI to show artifacts without extra reads.
    if uploaded_by_name:
        store.update_run(
            run_id, {"artifacts": {k: v.get("gcs_uri") for k, v in uploaded_by_name.items()}}
        )

    # Determine success/failure from core run_summary.json (core may not raise).
    ok = True
    summary_path = work_dir / "run_summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text())
            if isinstance(summary, dict) and summary.get("ok") is False:
                ok = False
        except Exception:
            ok = False

    if not ok:
        error = build_error_payload(
            RuntimeError("citylens-core reported ok=false"),
            code="PIPELINE_FAILED",
            stage="done",
        )
        try:
            if summary_path.exists():
                summary = json.loads(summary_path.read_text())
                if isinstance(summary, dict):
                    code = str(summary.get("error_code") or "PIPELINE_FAILED")
                    message = str(summary.get("error_message") or "citylens-core reported ok=false")
                    error["code"] = code
                    error["message"] = message
                    error["traceback_summary"].append(f"summary: {summary_path.name}")
        except Exception:
            pass

        store.update_run(
            run_id,
            {"status": "failed", "stage": "done", "progress": 100, "error": error},
        )
        return

    # Tripwire: on the success path, verify every required artifact is above a
    # minimum byte size. Catches regressions where core claims ok=true but
    # silently emits placeholder-sized bytes (e.g., 154-byte empty mesh.ply).
    # These thresholds sit well above the old placeholder sizes and well
    # below any real output (real preview.png >> 100KB, real mesh.ply >> 1MB).
    _MIN_ARTIFACT_SIZES = {
        "preview.png": 10_000,
        "change.geojson": 200,
        "mesh.ply": 10_000,
    }
    too_small = [
        (n, int(d.get("size_bytes") or 0))
        for n, d in uploaded_by_name.items()
        if n in _MIN_ARTIFACT_SIZES and int(d.get("size_bytes") or 0) < _MIN_ARTIFACT_SIZES[n]
    ]
    if too_small:
        error = build_error_payload(
            RuntimeError(
                f"Artifact(s) smaller than placeholder threshold: {too_small}"
            ),
            code="PLACEHOLDER_ARTIFACT_DETECTED",
            stage="done",
        )
        store.update_run(
            run_id,
            {"status": "failed", "stage": "done", "progress": 100, "error": error},
        )
        return

    store.update_run(
        run_id, {"status": "succeeded", "stage": "done", "progress": 100, "error": None}
    )

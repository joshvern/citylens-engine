from __future__ import annotations

import logging
import os
from pathlib import Path

from services.firestore_store import FirestoreStore
from services.gcs_artifacts import GcsArtifacts
from services.logging import configure_json_logging
from services.pipeline_runner import run as run_pipeline
from services.run_errors import build_error_payload
from services.settings import get_settings

logger = logging.getLogger(__name__)


def main() -> int:
    configure_json_logging(service_name="citylens-engine-worker")

    run_id = os.getenv("CITYLENS_RUN_ID", "").strip()
    if not run_id:
        raise RuntimeError("CITYLENS_RUN_ID is required")

    settings = get_settings()
    store = FirestoreStore(project_id=settings.project_id, runs_collection=settings.runs_collection)
    gcs = GcsArtifacts(bucket=settings.bucket)

    run_doc = store.get_run(run_id)
    if not run_doc:
        raise RuntimeError(f"Run not found: {run_id}")

    store.update_run(
        run_id, {"status": "running", "stage": "starting", "progress": 1, "error": None}
    )

    try:
        request_dict = dict(run_doc.get("request") or {})
        run_pipeline(
            run_id=run_id,
            request_dict=request_dict,
            work_root=Path(settings.work_root),
            store=store,
            gcs=gcs,
            settings=settings,
        )
        return 0
    except Exception as e:
        error = build_error_payload(e)
        store.update_run(
            run_id,
            {"status": "failed", "stage": "failed", "progress": 100, "error": error},
        )
        logger.exception("worker failed", extra={"run_id": run_id, "stage": "failed"})
        raise


if __name__ == "__main__":
    raise SystemExit(main())

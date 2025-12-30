# Architecture

## Components

- **API (Cloud Run)**
  - Auth via `X-API-Key` allowlist (`CITYLENS_API_KEYS`).
  - Creates Firestore run docs and triggers a Cloud Run Job execution.
  - Optionally returns signed URLs for artifacts.

- **Worker (Cloud Run Job)**
  - Reads `CITYLENS_RUN_ID`.
  - Loads run doc, executes `citylens_core.pipeline.run_citylens`.
  - Uploads returned standard artifacts to GCS and writes artifact docs.

## Data

Firestore:
- `users/{user_id}`: user record keyed by sha256(api_key)
- `runs/{run_id}`: run status/progress/request
- `runs/{run_id}/artifacts/{artifact_id}`: artifact metadata + GCS URI

GCS:
- `gs://<CITYLENS_BUCKET>/runs/<run_id>/<artifact_filename>`

## Core contract

- Request schema: `citylens_core.models.CitylensRequest`
- Pipeline entrypoint: `citylens_core.pipeline.run_citylens(request, work_dir, progress_cb)`
- Standard artifact filenames written in `work_dir`:
  - `preview.png`
  - `change.geojson`
  - `mesh.ply`
  - `run_summary.json`

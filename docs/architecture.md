# Architecture

`citylens-engine` is an independently runnable repo inside the shared
`/home/josh/citylens` workspace. It owns the runtime/API layer for the product,
while `citylens-core` owns the canonical pipeline contract and `citylens-web`
owns the browser UI.

## Components

- **API (Cloud Run)**
  - Auth: `Authorization: Bearer <token>` — a Neon Auth (OIDC/JWKS) user JWT, or
    a `clk_live_` user API key resolved against Firestore. An optional admin
    `X-API-Key` surface (hash-only, `CITYLENS_ADMIN_API_KEY_HASHES`) exists for
    internal scripts. See [security.md](security.md).
  - Creates Firestore run docs and triggers a Cloud Run Job execution.
  - Serves the public read endpoints `/v1/demo/*` and `/v1/parcel-intel/index`.
    `/v1/parcel-intel/sweep` is tiered: a public preview (≤25 rows, premium
    fields stripped) + an authenticated full feed (any valid credential; up to
    1000 rows, `Cache-Control: private, no-store`).
  - Health: `/v1/health` is the dependency-free keep-warm ping;
    `/v1/health/ready` additionally probes Firestore (503 if unreachable) and
    reports parcel-intel presence/freshness flags.
  - A `lifespan` handler pre-warms the demo + parcel-intel registries; only
    `CitylensRequest` is imported from `citylens-core` (the heavy pipeline import
    is lazy, kept off the API cold-start path — the worker runs the pipeline).
  - Optionally returns signed URLs for artifacts.

- **Worker (Cloud Run Job)**
  - Reads `CITYLENS_RUN_ID`.
  - Resolves address-driven inputs into `orthophoto.tif`, `baseline.tif`,
    `baseline_footprints.geojson`, and `lidar.las` in the run's `work_dir`.
  - Loads run doc, executes `citylens_core.pipeline.run_citylens`.
  - Uploads returned standard artifacts to GCS and writes artifact docs.

## Data

Firestore:
- `users/{app_user_id}`: user record (`plan_type`, `email`, `is_admin`)
- `auth_identities/{sha256(provider:sub)}` → `app_user_id` (OIDC identity map)
- `usage_months/{app_user_id}_{YYYY-MM}`: monthly run-quota counter (transactional)
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

Workspace/runtime notes:

- Use the repo-root `uv.lock` for dependency resolution across both `api/` and `worker/`.
- Recreate the root `.venv` with Python 3.11 via `uv sync --all-packages --all-extras`.
- Install the sibling `citylens-core` repo into the same `.venv` with
  `uv pip install --python ./.venv/bin/python -e ../citylens-core`, or in CI/Docker
  with `citylens-core[sam2] @ ${CITYLENS_CORE_GIT_URL}`.
- Open this repo directly in VS Code, or use a multi-root workspace that keeps
  `citylens-engine`, `citylens-core`, and `citylens-web` as distinct folders.
- Do not depend on a parent-folder Python environment for engine development.

Fixed parity/reference case:

- `100 E 21st St Brooklyn, NY 11226`
- parity harness: `scripts/parity_reference_case.py`
- reference repo: `../Urban3D-DeepRecon`

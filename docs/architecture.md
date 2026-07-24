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
  - Serves the public read endpoints `/v1/demo/*` and
    `/v1/parcel-intel/index`. Parcel Intelligence progressively loads a
    compact `/v1/parcel-intel/map` projection, fetches a full record from
    `/v1/parcel-intel/parcel/{bbl}` only when selected, and reserves
    `/v1/parcel-intel/sweep` for CSV/export and compatibility. Public inventory
    is capped at 25 rows per borough with premium fields stripped;
    authenticated users can load 1,000 per borough. Large JSON responses are
    gzip-compressed. Historical NYC DOF final lien-sale and current DOB
    Safety/OATH/HPD violation fields are premium diligence context and never
    exposed anonymously. These overlays are post-score and do not modify
    acquisition ranks.
  - Selected parcel detail adds a read-time
    `citylens/parcel-decision-audit@v1` object. The API, rather than the
    browser, assigns each explanation to historical model signal,
    deterministic eligibility, current diligence, or source provenance and
    declares whether it can affect model rank or acquisition eligibility.
    Public responses retain methodology and current-gate explanations but
    replace owner and diligence evidence with sign-in gates. User workflow
    evidence remains a separate authenticated contract.
  - Parcel feed generations are immutable. A stable GCS manifest pointer names
    the active generation and records each object's SHA-256, byte size, and row
    count; readers validate all of these and retain a legacy-flat fallback.
    Generation-keyed caches prevent an in-flight old reader from repopulating a
    new generation's cache.
  - New manifests carry `generation_diff` evidence from the publisher:
    inventory turnover, top-rank retention, rank movement, score PSI, source
    vintages, model identity, cohorts, required-field coverage, full 142-column
    inference drift, and any explicitly reviewed override. Aggregate
    `inference_replay` evidence proves all 5,000 stored scores match the
    reconstructed matrix. The index/sweep contracts expose this
    operational provenance, and the scheduled verifier fails when the report
    is absent, failed, or overridden without a recorded reason.
  - Health: `/v1/health` is the dependency-free keep-warm ping;
    `/v1/health/ready` additionally probes Firestore (503 if unreachable) and
    reports parcel-intel presence/freshness flags.
  - Authenticated parcel workflow alerts compare each watched lead's saved
    baseline with the current generation. The contract is computed on request,
    user scoped, and never exposed anonymously. It reports decision-relevant
    differences without changing model scores or guessing why a parcel left
    the eligible feed.
  - Authenticated parcel workflow actions are also computed on request from
    user-owned records. The server classifies next-action due dates as overdue,
    due today, due soon, scheduled, or unscheduled; flags missing assignees and
    30-day-old records without an outcome; reports workflow-plan, assignee, and
    outcome-review coverage; and excludes terminal records. Reminder snoozes
    are transactional, user scoped, and fingerprinted to the current action,
    due date, assignee, stage, and outcome, so changed commitments cannot stay
    hidden behind stale snoozes. Browser clients do not own these
    classifications or reminder identity.
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

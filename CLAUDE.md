# citylens-engine

CityLens API + worker. FastAPI on Cloud Run (`citylens-api`) plus a Cloud Run Job (`citylens-worker`) that runs the `citylens-core` pipeline. Firestore for metadata; GCS for artifacts. GCP project `citylens-001`. Public host: https://api.citylens.dev.

uv workspace with two members: `api/` and `worker/`. Single root `.venv` and root `uv.lock`.

## Quick commands

```bash
make sync                  # uv sync --all-packages --all-extras + install citylens-core
                           # (prefers ../citylens-core via -e; falls back to pinned git URL)
make dev                   # uvicorn app.main:app on :8080 (cd api && ...)
make test                  # pytest api/tests + pytest worker
make fmt                   # ruff format + ruff check --fix on api/ and worker/

./deploy/deploy_all.sh                   # deploy worker job + API service from .env
./deploy/deploy_all.sh --precompute      # also runs scripts/precompute_demo_runs.py + redeploys
./.venv/bin/python scripts/parity_reference_case.py   # parity harness vs Urban3D-DeepRecon
```

Deploy requires a `.env` at repo root (git-ignored) with `GOOGLE_CLOUD_PROJECT`, `CITYLENS_REGION`, `CITYLENS_BUCKET`, `CITYLENS_JOB_NAME`, `CITYLENS_API_KEYS`.

## Layout

- [api/app/main.py](api/app/main.py) — FastAPI app; CORS, docs gating, route mounting
- [api/app/routes/](api/app/routes/) — `runs`, `demo`, `parcel_intel`, `me`, `run_options`, `health`, `api_keys`
- [api/app/services/](api/app/services/) — `auth`, `oidc_verifier`, `firestore_store`, `gcs_artifacts`, `quotas`, `plans`, `core_adapter`, `job_trigger`
- [api/app/models/schemas.py](api/app/models/schemas.py) — API request/response shapes (NOT pipeline schema; that comes from `citylens_core`). Includes `ParcelIntelRow`, `ParcelIntelIndex`, `TopFeature` for the parcel-intel passthrough.
- [worker/worker.py](worker/worker.py) — Cloud Run Job entry; reads `CITYLENS_RUN_ID` env, runs pipeline, writes Firestore + GCS
- [worker/services/](worker/services/) — `pipeline_runner`, `core_adapter`, `imagery_inputs`, `nysgis`, `firestore_store`, `gcs_artifacts`, `reference_data`
- [deploy/](deploy/) — `deploy_api.sh`, `deploy_worker.sh`, `deploy_all.sh`, `demo_runs.json`, `demo_addresses.json`
- [scripts/](scripts/) — `precompute_demo_runs.py`, `parity_reference_case.py`
- [docs/](docs/) — `architecture.md`, `deploy_gcp.md`, `security.md`

## Conventions

- This repo does NOT define its own pipeline request schema. It imports `CitylensRequest` and `run_citylens` from `citylens-core` ([api/app/services/core_adapter.py](api/app/services/core_adapter.py), [worker/services/core_adapter.py](worker/services/core_adapter.py)).
- `citylens-core` is pinned by git URL via `CITYLENS_CORE_GIT_URL` in deploy scripts and Dockerfiles. Bumping core requires editing every script listed in the workspace [CLAUDE.md](../CLAUDE.md), not just one. Local `make sync` overrides with the sibling checkout.
- Auth: `Authorization: Bearer …`. Tokens prefixed `clk_live_` are routed as user API keys against Firestore in [services/firestore_store.py](api/app/services/firestore_store.py); all other bearers go through JWKS verification ([services/oidc_verifier.py](api/app/services/oidc_verifier.py)). Don't add a separate API-key path.
- Run options are server-locked (`imagery_year=2024`, `baseline_year=2017`, `segmentation_backend=sam2`, `aoi_radius_m=250`). The API narrowly validates client payloads and injects defaults — never trust client overrides. Discoverable at `GET /v1/run-options`.
- Free users: 5 real runs per UTC calendar month (env `CITYLENS_FREE_MONTHLY_RUNS`). Admins (`CITYLENS_ADMIN_AUTH_SUBS` or `CITYLENS_ADMIN_EMAILS`) are unlimited. Quota refunds on failure live in [services/quotas.py](api/app/services/quotas.py); see `tests/test_quota_refund.py`.
- `deploy/demo_runs.json` must reference real precomputed runs whose artifacts already exist in Firestore + GCS. Regenerate via `scripts/precompute_demo_runs.py`, commit, redeploy. Demo artifact URLs are rewritten same-origin at response time — the browser never hits GCS directly for demos.
- `/docs`, `/redoc`, `/openapi.json` are off by default. Set `CITYLENS_DOCS_ACCESS_KEY_SHA256` and call with header `X-Docs-Key`. Docs key cannot create runs.
- CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)): `uv sync --all-packages --all-extras --frozen` + install `citylens-core` from git, then ruff + pytest on api/ and worker/.
- Tests use a fake/in-memory Firestore + GCS; do not call live GCP from pytest. See `api/tests/conftest.py`, `worker/conftest.py`.
- **Parcel intelligence routes** (`/v1/parcel-intel/index`, `/v1/parcel-intel/sweep?borough=…&top=…`) are passthrough — no model lives in this repo. The data is published to `gs://<bucket>/parcel-intel/v1/{<borough>.jsonl, manifest.json}` by `citylens-parcel-intel/scripts/publish_sweep.py`. The engine reads via `GcsArtifacts.download_bytes` and caches parsed JSONL in a process-level `ParcelIntelRegistry` keyed on `manifest.generated_at`. Cache invalidates automatically when the publisher re-uploads. Endpoints are public (rate-limited via `demo_rate_limit`) and edge-cached for 10 minutes (`Cache-Control: public, s-maxage=600, stale-while-revalidate=300`).
- The `top_features` field on `ParcelIntelRow` is per-row SHAP attribution from the LightGBM ensemble, computed at publish time. Engine surfaces it verbatim — `value` is `str | int | float | bool | None` (Pydantic union order matters; do NOT reorder).
- A new schema field on `ParcelIntelRow` (or any change to `TopFeature`) requires re-deploying the API even though the data layer is JSONL — Pydantic strips unknown fields silently, so the field needs to be declared here for round-trip.

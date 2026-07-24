# citylens-engine

The CityLens **API, worker, auth, quotas, and artifact storage**. Powers
the live product at **https://www.citylens.dev**, served via the API at
**https://api.citylens.dev**.

Companion repos:

- [`citylens-web`](https://github.com/joshvern/citylens-web) — Next.js
  product frontend.
- [`citylens-core`](https://github.com/joshvern/citylens-core) — reusable
  Python pipeline library (segmentation, change detection, mesh).

This repo is independently runnable under the shared `/home/josh/citylens`
workspace. For local development, open `citylens-engine/` directly in VS Code, or
use a multi-root workspace that includes this folder so the editor resolves the
repo-local tooling correctly.

It has two separate deployment surfaces:

- **API**: FastAPI service on Cloud Run (login-gated `/v1/runs*`,
  `/v1/me`, plus public `/v1/demo/*` and `/v1/run-options`).
- **Worker**: Python Cloud Run Job that runs the citylens-core pipeline.
- **Metadata**: Firestore (`runs`, `users`, `auth_identities`,
  `usage_months`).
- **Artifacts**: GCS (private bucket; API can optionally return signed URLs).
- **Auth**: OIDC/JWKS verification (signature, `iss`, `aud`, `exp`); Neon
  Auth tokens accepted out of the box. Optional admin API keys for
  internal scripts only.

Critical constraint: this repo **does not define its own pipeline request schema**.
It imports and uses the canonical `CitylensRequest` and pipeline entrypoint from `citylens-core`.

Local development uses the repo-local `.venv` at the engine root. The API service
and worker job share that same root environment for local checks, while deployed
images use Python 3.11-slim.

`Urban3D-DeepRecon` is treated as a read-only reference repo. This repo is the active
runtime/API layer for the productized system.

The repo uses a single workspace lockfile at `uv.lock` for the `api/` and `worker/`
packages. Regenerate it from the repo root with `uv lock`, then sync the root
environment with `uv sync --all-packages --all-extras`.

`citylens-core` lives in the sibling repo and is installed separately into the same
root `.venv`:

- local dev: `uv pip install --python ./.venv/bin/python -e ../citylens-core`
- CI default: `uv pip install --python ./.venv/bin/python "citylens-core[sam2] @ git+https://github.com/joshvern/citylens-core.git@v0.3.25"`
- CI / Docker override: `uv pip install --python ./.venv/bin/python "citylens-core[sam2] @ ${CITYLENS_CORE_GIT_URL}"`

Use `make sync` to perform the workspace sync plus the sibling-core install when the
neighboring repo is available. Without the sibling checkout, it falls back to the
public GitHub repo using `CITYLENS_CORE_REF` (default `v0.3.25`) or an explicit
`CITYLENS_CORE_GIT_URL` override.

Current pinned release tag:

- `citylens-core@v0.3.25`

## Auth & quotas

- Real run endpoints (`POST /v1/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/me`) require `Authorization: Bearer <token>` from Neon Auth (or any compatible OIDC issuer).
- Admin promotion is via env: `CITYLENS_ADMIN_AUTH_SUBS` (sub allowlist) or `CITYLENS_ADMIN_EMAILS` (verified-email allowlist).
- Free users get 5 real runs per UTC calendar month (override with `CITYLENS_FREE_MONTHLY_RUNS`); admins are unlimited.
- Run options are server-locked: `imagery_year=2024`, `baseline_year=2017`, `segmentation_backend=sam2`, `aoi_radius_m=250`, outputs ⊂ `{previews, change, mesh}`. Discover via `GET /v1/run-options`.
- Demo endpoints (`/v1/demo/*`), `/v1/health`, `/v1/health/ready`, and
  `/v1/parcel-intel/index` remain public. Parcel Intelligence progressively
  loads `/v1/parcel-intel/map`, fetches full selected-parcel detail from
  `/v1/parcel-intel/parcel/{bbl}`, and reserves `/v1/parcel-intel/sweep` for
  CSV/export and compatibility. Public inventory is capped at 25 rows per
  borough with premium fields stripped; authenticated users can load 1,000
  rows per borough. Large JSON responses are gzip-compressed.
- Parcel Intelligence accepts the `published_sweep@v5` contract: separate
  historical-model, borough-acquisition, and citywide-acquisition ranks;
  explicit eligibility/exclusion evidence; owner provenance; current-project
  context; historical NYC DOF final lien-sale diligence; current DOB
  Safety/OATH/HPD violation snapshots; adopted-2007 and preliminary-2015
  PLUTO/FEMA 1% annual-chance floodplain tax-lot screens; current PLUTO
  E-designation/restrictive-declaration diligence; exact-name,
  current-PLUTO legal-entity portfolio summaries; and a publisher quality-gate
  summary plus a generation-to-generation drift report exposed by the
  index/sweep endpoints. The drift report covers inventory turnover, top-rank
  retention, retained-lead rank movement, score PSI, source vintages, model
  identity, required-field coverage, and all 142 accepted-model input columns.
  The index also exposes aggregate score-replay evidence proving all 5,000
  published scores came from the profiled matrix. Failed thresholds require a
  recorded reviewed override before publication. Owner, lien-sale, violation,
  portfolio, floodplain, and environmental-designation fields are stripped from anonymous
  map, sweep, and detail responses. The authenticated compact map carries portfolio counts,
  `critical_violation_count`, `floodplain_1pct`, and the boolean
  `environmental_review_required`; detailed agency/map fields, designation
  type/number, and dates load on parcel selection. An E-designation or
  restrictive declaration is an
  air/noise/hazardous-materials diligence requirement, not a contamination
  finding or ranking input. Portfolio matching preserves legal form,
  never groups natural-person names, and does not infer beneficial ownership or
  related LLCs.
- The authenticated acquisition workflow preserves an immutable,
  value-minimized event history and soft-archives removed leads. The
  user-scoped `/v1/parcel-intel/workflow/analytics` endpoint reports
  maturity-qualified 30-day contact, 90-day qualification, 180-day offer,
  270-day contract, and 365-day close rates. A lead enters a denominator only
  after its full observation window; late-recorded milestones do not count as
  on-time outcomes. The public, data-free
  `/v1/parcel-intel/workflow/analytics/methodology` endpoint publishes this
  contract for deployment verification. These are selected, user-saved
  workflow outcomes—not model accuracy, seller intent, or transaction
  probability. See
  [`docs/prospective_outcomes.md`](docs/prospective_outcomes.md). Per-lead
  audit events are available from
  `/v1/parcel-intel/workflow/{bbl}/events`. The authenticated
  `/v1/parcel-intel/workflow/alerts` endpoint compares watched leads' saved
  snapshots with the current atomic feed and reports owner, newer-sale,
  zoning, opportunity, rank/tier, lien, violation, flood, environmental designation,
  imagery, exact-name portfolio, and feed-removal changes. A removed lead is
  deliberately labeled for current-record verification rather than being
  called sold, built, or completed without authoritative evidence.
- Production Parcel Intelligence manifests may use
  `atomic-publication@v1`: immutable `generations/<id>/` borough/map objects
  plus one stable manifest pointer. The API validates the pointer path,
  SHA-256, byte length, and row count before serving a generation and fails
  closed on missing, corrupt, partial, or path-injection metadata. Legacy flat
  `published_sweep@v5` objects remain readable during migration.
- Interactive docs (`/docs`, `/redoc`, `/openapi.json`) are off by default. Set `CITYLENS_DOCS_ACCESS_KEY_SHA256` and call with `X-Docs-Key`. The docs key cannot create runs and cannot bypass quotas.
- `CITYLENS_API_KEYS` is deprecated and ignored by auth. The optional admin `X-API-Key` path (internal scripts only) is hash-only: `CITYLENS_ALLOW_ADMIN_API_KEYS=true` + `CITYLENS_ADMIN_API_KEY_HASHES` (SHA-256 of each key).

See [docs/security.md](docs/security.md) for the full credential model.

## Demo Mode

The API exposes unauthenticated demo endpoints:

- `GET /v1/demo/featured`
- `GET /v1/demo/runs/{run_id}`
- `GET /v1/demo/artifacts/{run_id}/{artifact_name}`

These routes are backed by the allowlist under:

- [deploy/demo_runs.json](deploy/demo_runs.json)

`deploy/demo_runs.json` is not a placeholder bundle. It must contain only real,
successful precomputed runs whose artifacts already exist in Firestore + GCS.

The supported publish flow is:

1. Deploy the worker and API.
2. Run `scripts/precompute_demo_runs.py` against the deployed API with an admin API key.
3. Inspect and commit the generated `deploy/demo_runs.json`.
4. Redeploy the API so `GET /v1/demo/featured` serves the updated allowlist.

When the API returns a demo run, its artifact URLs are rewritten to same-origin API
paths like `/v1/demo/artifacts/<run_id>/<artifact_name>`. The browser never needs
direct GCS URLs for demo mode.

## Production verification

The secret-free production verifier exercises the live API, all five parcel
generation objects, and the web route:

```bash
./.venv/bin/python scripts/verify_production.py \
  --output production-verification.json
```

It fails on stale/missing feeds (including every required source SLA),
quality-gate regressions, missing or failed
generation-diff evidence, input-feature drift, score-replay mismatch,
unreviewed drift overrides, model-provenance drift,
borough or rank gaps, anonymous premium-field exposure, missing gzip,
unavailable Firestore, public workflow access, or a broken Parcel Intelligence
page. [production-smoke.yml](.github/workflows/production-smoke.yml) runs the
same verifier every six hours and on demand, publishes a job summary, and
retains the JSON report for 30 days. A failure is an incident signal; do not
weaken a contract assertion merely to make the scheduled check green.

### VS Code folder expectations

- Open `citylens-engine/` as its own folder when you want engine-specific Python
  tooling, interpreter selection, or test execution.
- If you keep `/home/josh/citylens` open as the parent folder, use a proper
  multi-root workspace so VS Code does not blur repo boundaries between
  `citylens-core`, `citylens-engine`, and `citylens-web`.
- The engine repo should resolve its interpreter from `citylens-engine/.venv`,
  not from any parent-level environment.

## Fixed Reference Case

The acceptance case for modular parity is:

- `100 E 21st St Brooklyn, NY 11226`

Run the parity harness from the repo root:

```bash
./.venv/bin/python scripts/parity_reference_case.py
```

This writes `parity_report.json` and compares the modular outputs against the
`Urban3D-DeepRecon` reference repo.

See [docs/architecture.md](docs/architecture.md) and [docs/deploy_gcp.md](docs/deploy_gcp.md).

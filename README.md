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
- CI default: `uv pip install --python ./.venv/bin/python "citylens-core[sam2] @ git+https://github.com/joshvern/citylens-core.git@v0.3.0"`
- CI / Docker override: `uv pip install --python ./.venv/bin/python "citylens-core[sam2] @ ${CITYLENS_CORE_GIT_URL}"`

Use `make sync` to perform the workspace sync plus the sibling-core install when the
neighboring repo is available. Without the sibling checkout, it falls back to the
public GitHub repo using `CITYLENS_CORE_REF` (default `v0.3.0`) or an explicit
`CITYLENS_CORE_GIT_URL` override.

Current pinned release tag:

- `citylens-core@v0.3.0`

## Auth & quotas

- Real run endpoints (`POST /v1/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/me`) require `Authorization: Bearer <token>` from Neon Auth (or any compatible OIDC issuer).
- Admin promotion is via env: `CITYLENS_ADMIN_AUTH_SUBS` (sub allowlist) or `CITYLENS_ADMIN_EMAILS` (verified-email allowlist).
- Free users get 5 real runs per UTC calendar month (override with `CITYLENS_FREE_MONTHLY_RUNS`); admins are unlimited.
- Run options are server-locked: `imagery_year=2024`, `baseline_year=2017`, `segmentation_backend=sam2`, `aoi_radius_m=250`, outputs ⊂ `{previews, change, mesh}`. Discover via `GET /v1/run-options`.
- Demo endpoints (`/v1/demo/*`) and `/v1/health` remain public.
- Interactive docs (`/docs`, `/redoc`, `/openapi.json`) are off by default. Set `CITYLENS_DOCS_ACCESS_KEY_SHA256` and call with `X-Docs-Key`. The docs key cannot create runs and cannot bypass quotas.
- `CITYLENS_API_KEYS` is deprecated for normal users. Use the optional `CITYLENS_ALLOW_ADMIN_API_KEYS` path only for internal scripts.

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

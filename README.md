# citylens-engine

Cloud Run deployment repo for Citylens.

This repo is independently runnable under the shared `/home/josh/citylens`
workspace. For local development, open `citylens-engine/` directly in VS Code, or
use a multi-root workspace that includes this folder so the editor resolves the
repo-local tooling correctly.

It has two separate deployment surfaces:

- **API**: FastAPI service on Cloud Run
- **Worker**: Python Cloud Run Job
- **Metadata**: Firestore (runs/users/artifacts)
- **Artifacts**: GCS (private bucket; API can optionally return signed URLs)

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

## Demo Mode

The API now ships with built-in unauthenticated demo endpoints:

- `GET /v1/demo/featured`
- `GET /v1/demo/runs/{run_id}`
- `GET /v1/demo/artifacts/{run_id}/{artifact_name}`

By default, these routes are backed by the versioned files under:

- [deploy/demo_runs.json](deploy/demo_runs.json)
- [deploy/demo_artifacts/](deploy/demo_artifacts)

That means demo mode can work immediately after an API deploy without first
precomputing Firestore/GCS runs.

If you want demo runs backed by real pipeline outputs instead, use
`scripts/precompute_demo_runs.py` or `deploy/deploy_all.sh --precompute` to
generate a new allowlist and rebuild the API image.

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

# citylens-engine

Cloud Run deployment repo for Citylens:

- **API**: FastAPI service on Cloud Run
- **Worker**: Python Cloud Run Job
- **Metadata**: Firestore (runs/users/artifacts)
- **Artifacts**: GCS (private bucket; API can optionally return signed URLs)

Critical constraint: this repo **does not define its own pipeline request schema**.
It imports and uses the canonical `CitylensRequest` and pipeline entrypoint from `citylens-core`.

See [docs/architecture.md](docs/architecture.md) and [docs/deploy_gcp.md](docs/deploy_gcp.md).

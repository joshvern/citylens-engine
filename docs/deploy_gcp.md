# Deploy to GCP (Cloud Run + Firestore + GCS)

Placeholders used: `<PROJECT_ID> <REGION> <BUCKET_NAME> <API_SERVICE_NAME> <JOB_NAME> <API_SA> <WORKER_SA>`

This repo uses:

- Firestore (Native mode) for metadata (`runs`, `users`, `runs/{run_id}/artifacts/*`)
- A private GCS bucket for artifacts, stored at `runs/<run_id>/<artifact_filename>`

## ✅ GCP Setup Checklist

### 1) Enable APIs

```bash
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  iamcredentials.googleapis.com
```

### 2) Create Firestore (Native mode)

You can do this in either console:

- GCP Console: Firestore → Create database → **Native mode** → choose a location
- Firebase Console: Build → Firestore Database → Create database → **Production mode** → choose a location

Notes:

- Pick the Firestore location carefully; it’s not trivial to change later.
- For simplest ops/latency, choose the same broad region as your Cloud Run deployment when possible.

### 2b) Create required Firestore indexes (for quotas)

The API enforces per-day and concurrent-run quotas using Firestore queries on the `runs` collection.
Depending on your Firestore configuration, you may be prompted to create composite indexes.

These commands create the two composite indexes typically required:

```bash
gcloud firestore indexes composite create \
  --collection-group=runs \
  --field-config=field-path=user_id,order=ascending \
  --field-config=field-path=created_at,order=descending

gcloud firestore indexes composite create \
  --collection-group=runs \
  --field-config=field-path=user_id,order=ascending \
  --field-config=field-path=status,order=ascending
```

Index build can take a few minutes. If quota enforcement fails with an error like “The query requires an index”, create the index it specifies.

### 3) Create a private GCS bucket

This bucket should be private (no public access). Recommended settings:

- Uniform bucket-level access (UBLA)
- Public access prevention enforced

Create the bucket:

```bash
gsutil mb -p <PROJECT_ID> -l <REGION> -b on gs://<BUCKET_NAME>
```

Enforce public access prevention:

```bash
gcloud storage buckets update gs://<BUCKET_NAME> --public-access-prevention
```

Optional: lifecycle deletion for old runs (example: delete objects older than 30 days):

```bash
cat > lifecycle.json <<'JSON'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 30}
    }
  ]
}
JSON

gsutil lifecycle set lifecycle.json gs://<BUCKET_NAME>
```

### 4) Bucket object layout (important)

The worker uploads artifacts returned by `citylens_core.pipeline.run_citylens` to:

- `gs://<BUCKET_NAME>/runs/<RUN_ID>/preview.png`
- `gs://<BUCKET_NAME>/runs/<RUN_ID>/change.geojson`
- `gs://<BUCKET_NAME>/runs/<RUN_ID>/mesh.ply`
- `gs://<BUCKET_NAME>/runs/<RUN_ID>/run_summary.json`

Those filenames are part of the contract and are used by tests.

### 5) Create service accounts

```bash
gcloud iam service-accounts create <API_SA>
gcloud iam service-accounts create <WORKER_SA>
```

### 6) Minimal IAM roles

Grant roles (recommended to keep these narrow; adjust per your org policies):

API service account needs:

- Firestore read/write: `roles/datastore.user`
- Trigger Cloud Run Job executions: `roles/run.developer`
- If you enable signed URLs (`CITYLENS_SIGN_URLS=1`): allow signing with IAMCredentials
  - Also grant the API service account read access to artifacts so the signed URLs can be used to download objects (needs `storage.objects.get`, e.g. `roles/storage.objectViewer` on the bucket).
  - `roles/iam.serviceAccountTokenCreator` on the API service account

Worker service account needs:

- Firestore read/write: `roles/datastore.user`
- Upload artifacts: `roles/storage.objectAdmin` on the bucket

Example commands:

```bash
PROJECT=<PROJECT_ID>
API_SA_EMAIL=<API_SA>@${PROJECT}.iam.gserviceaccount.com
WORKER_SA_EMAIL=<WORKER_SA>@${PROJECT}.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member=serviceAccount:${API_SA_EMAIL} \
  --role=roles/datastore.user

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member=serviceAccount:${API_SA_EMAIL} \
  --role=roles/run.developer

gcloud iam service-accounts add-iam-policy-binding ${API_SA_EMAIL} \
  --member=serviceAccount:${API_SA_EMAIL} \
  --role=roles/iam.serviceAccountTokenCreator

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member=serviceAccount:${WORKER_SA_EMAIL} \
  --role=roles/datastore.user

gcloud storage buckets add-iam-policy-binding gs://<BUCKET_NAME> \
  --member=serviceAccount:${WORKER_SA_EMAIL} \
  --role=roles/storage.objectAdmin
```

### 7) Build & deploy API to Cloud Run

Important: the API and worker Dockerfiles intentionally require `CITYLENS_CORE_GIT_URL` at build time.
This is because `citylens-core` is the canonical contract and must be installed into the container.

If you use `gcloud run deploy --source .` without providing Docker build args, Cloud Build will fail.
Use the Cloud Build config in `api/cloudbuild.yaml` to build with the required build-arg, then deploy from the built image.

Set the core git URL (must be reachable by Cloud Build). Example:

`CITYLENS_CORE_GIT_URL="git+https://github.com/<ORG>/citylens-core.git@<REF>"`

Build the API image:

```bash
CITYLENS_CORE_GIT_URL="git+https://github.com/<ORG>/citylens-core.git@<REF>"
API_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/cloud-run-source-deploy/citylens-api:latest"

gcloud builds submit . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${API_IMAGE}
```

Deploy the API service from the built image:

```bash
cd api
gcloud run deploy <API_SERVICE_NAME> \
  --image ${API_IMAGE} \
  --region <REGION> \
  --project <PROJECT_ID> \
  --service-account <API_SA>@<PROJECT_ID>.iam.gserviceaccount.com \
  --set-env-vars GOOGLE_CLOUD_PROJECT=<PROJECT_ID>,CITYLENS_REGION=<REGION>,CITYLENS_BUCKET=<BUCKET_NAME>,CITYLENS_JOB_NAME=<JOB_NAME>,CITYLENS_API_KEYS=<COMMA_SEPARATED_KEYS>
```

Note: `--set-env-vars` replaces the entire env-var set for the service/job. If you later want to tweak only one or two values (e.g., enabling `CITYLENS_SIGN_URLS=1`), prefer `gcloud run services update ... --update-env-vars KEY=VALUE` (or re-specify all required env vars).

### 8) Build & create Cloud Run Job for worker

Build the worker image:

```bash
cd ../worker

CITYLENS_CORE_GIT_URL="git+https://github.com/<ORG>/citylens-core.git@<REF>"
WORKER_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/cloud-run-source-deploy/citylens-worker:latest"

gcloud builds submit . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${WORKER_IMAGE}
```

Create/update the Cloud Run Job from the built image:

```bash
gcloud run jobs deploy <JOB_NAME> \
  --image ${WORKER_IMAGE} \
  --region <REGION> \
  --project <PROJECT_ID> \
  --service-account <WORKER_SA>@<PROJECT_ID>.iam.gserviceaccount.com \
  --set-env-vars GOOGLE_CLOUD_PROJECT=<PROJECT_ID>,CITYLENS_REGION=<REGION>,CITYLENS_BUCKET=<BUCKET_NAME>
```

### 9) Configure env vars

API:

- `GOOGLE_CLOUD_PROJECT`, `CITYLENS_REGION`, `CITYLENS_BUCKET`, `CITYLENS_JOB_NAME`
- `CITYLENS_API_KEYS`
- Optional: `CITYLENS_SIGN_URLS=1` and `CITYLENS_SIGN_URL_TTL_SECONDS=300`

Worker:

- `GOOGLE_CLOUD_PROJECT`, `CITYLENS_REGION`, `CITYLENS_BUCKET`

### 10) Local sanity checks (before deploying)

Set the active project:

```bash
gcloud config set project <PROJECT_ID>
```

Log in for Application Default Credentials (ADC) (for local runs only):

```bash
gcloud auth application-default login
```

Firestore “ready check” (avoids heredoc pitfalls):

```bash
python3.10 -c 'from google.cloud import firestore; c=firestore.Client(project="<PROJECT_ID>"); print("ok", c.project)'
```

Example for your project:

```bash
python3.10 -c 'from google.cloud import firestore; c=firestore.Client(project="citylens-001"); print("ok", c.project)'
```

### CORS (browser clients)

If a browser app (like `citylens-web` on Vercel) calls the API with a custom header like `X-API-Key`, the browser will send a **preflight** request first:

- `OPTIONS /v1/...` (the preflight)
- then the real request (e.g. `POST /v1/runs`)

The API enables CORS via Starlette/FastAPI `CORSMiddleware` in [api/app/main.py](../api/app/main.py). Allowed origins are:

- `https://citylens.dev`
- `https://www.citylens.dev`
- `http://localhost:3000`

To allow a new domain, add it to the `allow_origins` list and redeploy the API.

If you enable signed URLs (`CITYLENS_SIGN_URLS=1`), the browser will download artifacts *directly from GCS* (not from the API). In that case you must also configure **bucket CORS** on your artifacts bucket to allow your site origin(s), e.g.:

- `https://citylens.dev`
- `https://www.citylens.dev`
- `http://localhost:3000`

### 11) Test end-to-end

Health:

```bash
curl https://<API_URL>/v1/health
```

Create run (must match `CitylensRequest` JSON):

```bash
curl -X POST https://<API_URL>/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: <YOUR_KEY>' \
  -d '{
    "address": "1 Market St, San Francisco, CA",
    "aoi_radius_m": 250,
    "imagery_year": 2024,
    "baseline_year": 2017,
    "segmentation_backend": "sam2",
    "outputs": ["previews", "change", "mesh"]
  }'
```

Poll run:

```bash
curl -H 'X-API-Key: <YOUR_KEY>' https://<API_URL>/v1/runs/<RUN_ID>
```

Confirm artifacts exist in GCS:

```bash
gsutil ls gs://<BUCKET_NAME>/runs/<RUN_ID>/
```

### 12) Demo endpoints (optional, for citylens-web “Demo mode”)

The API exposes unauthenticated demo endpoints:

- `GET /v1/demo/featured`
- `GET /v1/demo/runs/{run_id}`

These endpoints are backed by an allowlist file baked into the API image: `deploy/demo_runs.json`.
By default in this repo, [deploy/demo_runs.json](../deploy/demo_runs.json) contains an empty list, so `/v1/demo/featured` will return empty until you generate it.

To generate demo runs:

1) Edit [deploy/demo_addresses.json](../deploy/demo_addresses.json) with the addresses/years you want.
2) Deploy using the helper that can precompute and then re-deploy the API to bake the resulting allowlist:

```bash
./deploy/deploy_all.sh --precompute
```

Notes:

- Precompute requires an admin API key (it uses `POST /v1/runs` and waits for completion). By default it uses the first key in `CITYLENS_API_KEYS`, or you can set `CITYLENS_ADMIN_API_KEY` in your `.env`.
- If you want to keep the demo allowlist in git, commit the updated `deploy/demo_runs.json` after precompute.

If demo runs load but artifacts don’t render in the browser, ensure you have signed URLs enabled (`CITYLENS_SIGN_URLS=1`) and bucket CORS configured (see “CORS (browser clients)” above).

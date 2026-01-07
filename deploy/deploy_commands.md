# Deploy commands (source .env)

This is a copy/paste command sheet for deploying **Citylens Engine**:

- Cloud Run **service** (API)
- Cloud Run **job** (worker)
- Firestore (metadata)
- GCS (artifacts)

It assumes you run `source .env` first to define the variables.

## 0) Prereqs

- You already created Firestore (Native mode) in the console (one-time).
- You are authenticated:

```bash
gcloud auth login
gcloud auth application-default login
```

## 1) Source env

Your `.env` is local-only (git-ignored). It must include at least:

- `GOOGLE_CLOUD_PROJECT`
- `CITYLENS_REGION`
- `CITYLENS_BUCKET`
- `CITYLENS_JOB_NAME`
- `CITYLENS_API_KEYS`

Load it and derive a few helper vars:

```bash
set -a
source .env
set +a

export PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
export REGION="${CITYLENS_REGION}"
export BUCKET_NAME="${CITYLENS_BUCKET}"
export JOB_NAME="${CITYLENS_JOB_NAME}"

# Choose names (you can change these)
export API_SERVICE_NAME="citylens-api"
export API_SA_NAME="citylens-api"
export WORKER_SA_NAME="citylens-worker"

export API_SA_EMAIL="${API_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export WORKER_SA_EMAIL="${WORKER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

## 2) Enable required APIs (one-time)

```bash
gcloud config set project "${PROJECT_ID}"

gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  iamcredentials.googleapis.com
```

## 3) Create required Firestore indexes (quotas)

These are required for the quota enforcement queries on the `runs` collection.

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

Index build can take a few minutes.

## 4) Create a private GCS bucket (one-time)

If you already created the bucket, skip to section 5.

```bash
# Create bucket (UBLA on)
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" -b on "gs://${BUCKET_NAME}"

# Enforce public access prevention (flag syntax varies by gcloud version; this matches your working SDK)
gcloud storage buckets update "gs://${BUCKET_NAME}" --public-access-prevention
```

## 5) Artifact Registry repository (one-time)

This repo was already present in your project (`cloud-run-source-deploy`). If you need to create it:

```bash
gcloud artifacts repositories create cloud-run-source-deploy \
  --repository-format=docker \
  --location "${REGION}" \
  --project "${PROJECT_ID}"
```

## 6) Service accounts

Create service accounts (safe to re-run; if they already exist, you’ll get an “already exists” message).

```bash
gcloud iam service-accounts create "${API_SA_NAME}" --project "${PROJECT_ID}"
gcloud iam service-accounts create "${WORKER_SA_NAME}" --project "${PROJECT_ID}"
```

## 7) IAM permissions

API service account needs:

- Firestore read/write: `roles/datastore.user`
- Trigger Cloud Run Job executions: `roles/run.developer`

Worker service account needs:

- Firestore read/write: `roles/datastore.user`
- Upload artifacts to bucket: `roles/storage.objectAdmin` (bucket-level)

Apply:

```bash
# API SA

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${API_SA_EMAIL}" \
  --role roles/datastore.user

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${API_SA_EMAIL}" \
  --role roles/run.developer

# Worker SA

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${WORKER_SA_EMAIL}" \
  --role roles/datastore.user

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member "serviceAccount:${WORKER_SA_EMAIL}" \
  --role roles/storage.objectAdmin
```

If you enable signed URLs (`CITYLENS_SIGN_URLS=1`) in the API, also grant:

```bash
# Signed URLs are downloaded directly from GCS. The API service account must be able to read objects.
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member "serviceAccount:${API_SA_EMAIL}" \
  --role roles/storage.objectViewer

# Allows the API service to sign URLs (only needed when sign_urls is enabled)
gcloud iam service-accounts add-iam-policy-binding "${API_SA_EMAIL}" \
  --member "serviceAccount:${API_SA_EMAIL}" \
  --role roles/iam.serviceAccountTokenCreator

# Browser clients fetch signed URLs directly from GCS, so you must also allow your site origins via bucket CORS.
cat >/tmp/citylens-artifacts-cors.json <<'JSON'
[
  {
    "origin": [
      "https://citylens.dev",
      "https://www.citylens.dev",
      "http://localhost:3000"
    ],
    "method": ["GET", "HEAD"],
    "responseHeader": ["Content-Type", "Content-Length", "ETag"],
    "maxAgeSeconds": 3600
  }
]
JSON

gcloud storage buckets update "gs://${BUCKET_NAME}" --cors-file=/tmp/citylens-artifacts-cors.json
```

## 8) Build images (Cloud Build)

Both API and worker Dockerfiles install `citylens-core` **at build time** via a required build arg.

Set the canonical core git URL (this is what worked for you):

```bash
export CITYLENS_CORE_GIT_URL='git+https://github.com/joshvern/citylens-core.git@v0.2.0'
```

Choose image names:

```bash
export API_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-api:latest"
export WORKER_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-worker:latest"
```

Build API image:

```bash
cd /home/josh/citylens/citylens-engine/api

gcloud builds submit . \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${API_IMAGE}
```

Build worker image:

```bash
cd /home/josh/citylens/citylens-engine/worker

gcloud builds submit . \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${WORKER_IMAGE}
```

## 9) Deploy worker Cloud Run Job

```bash
gcloud run jobs deploy "${JOB_NAME}" \
  --image "${WORKER_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${WORKER_SA_EMAIL}" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME}"
```

## 10) Deploy API Cloud Run service

Note: this deploy uses `CITYLENS_API_KEYS` from your local `.env`. Treat that as a secret.

Note: `--set-env-vars` replaces the entire env-var set for the service/job. If you later want to tweak only one value (e.g., `CITYLENS_SIGN_URLS=1`), use `gcloud run services update ... --update-env-vars KEY=VALUE` (or re-specify all required env vars).

```bash
gcloud run deploy "${API_SERVICE_NAME}" \
  --image "${API_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${API_SA_EMAIL}" \
  --allow-unauthenticated \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_API_KEYS=${CITYLENS_API_KEYS}"
```

If you want to *require* API keys (recommended), leaving `--allow-unauthenticated` is fine: it only controls IAM auth; your app still enforces `X-API-Key`.

## 11) Smoke test

Get the service URL:

```bash
export API_URL="$(gcloud run services describe "${API_SERVICE_NAME}" --region "${REGION}" --project "${PROJECT_ID}" --format='value(status.url)')"
echo "API_URL=${API_URL}"
```

Health:

```bash
curl -sS "${API_URL}/v1/health" && echo
```

Create a run:

```bash
curl -sS -X POST "${API_URL}/v1/runs" \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: ${CITYLENS_API_KEYS}" \
  -d '{"address":"100 E 21st St Brooklyn, NY"}'
```

Watch the job executions:

```bash
gcloud run jobs executions list \
  --job "${JOB_NAME}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}"
```

List artifacts in GCS (replace `<RUN_ID>` with the returned run id):

```bash
RUN_ID="<RUN_ID>"
gsutil ls "gs://${BUCKET_NAME}/runs/${RUN_ID}/" || true
```

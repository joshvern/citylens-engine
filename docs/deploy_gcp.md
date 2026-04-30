# Deploy to GCP (Cloud Run + Firestore + GCS)

Placeholders used: `<PROJECT_ID> <REGION> <BUCKET_NAME> <API_SERVICE_NAME> <JOB_NAME> <API_SA> <WORKER_SA>`

`citylens-engine` has two independent deployment surfaces:

- the API service, deployed as a Cloud Run service
- the worker, deployed as a Cloud Run Job

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

The API and worker Dockerfiles default to the pinned core release tag:

- `git+https://github.com/joshvern/citylens-core.git@v0.3.0`

You can override that build input with `CITYLENS_CORE_GIT_URL` when you need a different core revision.
Use the Cloud Build config in `api/cloudbuild.yaml` to build with the default tag or an explicit override, then deploy from the built image.

Set the core git URL only if you want to override the default tag. Example:

`CITYLENS_CORE_GIT_URL="git+https://github.com/<ORG>/citylens-core.git@<REF>"`

Build the API image:

```bash
CITYLENS_CORE_GIT_URL="${CITYLENS_CORE_GIT_URL:-git+https://github.com/joshvern/citylens-core.git@v0.3.0}"
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

The worker is deployed separately from the API service. Keep the two surfaces
aligned on the same `citylens-core` revision, but build and deploy them as
independent Cloud Run resources.

Build the worker image:

```bash
cd ../worker

CITYLENS_CORE_GIT_URL="${CITYLENS_CORE_GIT_URL:-git+https://github.com/joshvern/citylens-core.git@v0.3.0}"
WORKER_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/cloud-run-source-deploy/citylens-worker:latest"

gcloud builds submit . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${WORKER_IMAGE}
```

Create/update the Cloud Run Job from the built image. The worker runs SAM2 on CPU,
rasterio, laspy, and a Python mesh writer — default Cloud Run Job limits
(512Mi / 1 vCPU / 10min / 3 retries) will OOM or time out before a real NYC
pipeline completes, so the resource flags below are required:

```bash
gcloud run jobs deploy <JOB_NAME> \
  --image ${WORKER_IMAGE} \
  --region <REGION> \
  --project <PROJECT_ID> \
  --service-account <WORKER_SA>@<PROJECT_ID>.iam.gserviceaccount.com \
  --memory 8Gi \
  --cpu 4 \
  --task-timeout 1800s \
  --max-retries 0 \
  --set-env-vars GOOGLE_CLOUD_PROJECT=<PROJECT_ID>,CITYLENS_REGION=<REGION>,CITYLENS_BUCKET=<BUCKET_NAME>,CITYLENS_ASSETS_ROOT=/opt/citylens-assets,CITYLENS_REFERENCE_DATA_DIR=/tmp/reference-data
```

Rationale:

- `--memory 8Gi --cpu 4`: sized for SAM2-small CPU inference plus rasterio and
  laspy. Bump to 16Gi/8vCPU if SAM2 benchmarks come back too slow.
- `--task-timeout 1800s`: first cold-start has to GCS-fetch NYC county footprint
  GDBs (hundreds of MB) plus LiDAR (1-2GB) plus the orthophoto and run the full
  pipeline. Subsequent runs hit the GCS cache and are much faster.
- `--max-retries 0`: the worker's placeholder tripwire
  (`worker/services/pipeline_runner.py`) stops runs that emit suspiciously
  small artifacts. Auto-retries would mask these and are disabled.
- `CITYLENS_REFERENCE_DATA_DIR=/tmp/reference-data`: scratch space for the
  county-footprint GDB expansion. The source of truth is GCS
  (`gs://<BUCKET_NAME>/reference-data/nyc-footprints/<County>.tar.gz`), so
  losing `/tmp` between invocations is fine.

### 9) Configure env vars

API (Cloud Run service):

- Infra: `GOOGLE_CLOUD_PROJECT`, `CITYLENS_REGION`, `CITYLENS_BUCKET`, `CITYLENS_JOB_NAME`
- Auth (Neon Auth or any OIDC issuer):
  - `CITYLENS_AUTH_PROVIDER=neon`
  - `CITYLENS_AUTH_REQUIRED=true`
  - `CITYLENS_AUTH_ISSUER=<issuer claim from your provider>`
  - `CITYLENS_AUTH_AUDIENCE=<audience claim — optional, but if set the engine validates `aud`>`
  - `CITYLENS_AUTH_JWKS_URL=<url that returns the JSON Web Key Set for token verification>`
  - For Neon Auth on Vercel: `CITYLENS_AUTH_JWKS_URL=https://<your-vercel-domain>/api/auth/jwks` and `CITYLENS_AUTH_ISSUER=https://<your-vercel-domain>/api/auth`. The web app's `app/api/auth/[...path]/route.ts` (using `@neondatabase/auth/next/server`) serves both endpoints automatically once `NEON_AUTH_BASE_URL` and `NEON_AUTH_COOKIE_SECRET` are set on Vercel.
  - DO NOT set `CITYLENS_AUTH_PROVIDER=mock` in production; mock auth additionally requires `CITYLENS_ALLOW_MOCK_AUTH=true` and the engine refuses to start otherwise.
- Plan / quota:
  - `CITYLENS_FREE_MONTHLY_RUNS=5`
  - `CITYLENS_ADMIN_AUTH_SUBS=<comma-separated provider subs>` (optional)
  - `CITYLENS_ADMIN_EMAILS=<comma-separated verified emails>` (optional)
- Interactive docs gate (recommended, otherwise `/docs` returns 404):
  - `CITYLENS_DOCS_ACCESS_KEY_SHA256=$(printf '%s' "$DOCS_KEY" | openssl dgst -sha256 -hex | awk '{print $2}')` — store the **hash**, never the raw key
- Optional admin API keys (internal scripts only — leave disabled unless you specifically need them):
  - `CITYLENS_ALLOW_ADMIN_API_KEYS=true`
  - `CITYLENS_ADMIN_API_KEY_HASHES=<comma-separated sha256 hashes>` (preferred over `CITYLENS_ADMIN_API_KEYS`)
- Optional: `CITYLENS_SIGN_URLS=1` and `CITYLENS_SIGN_URL_TTL_SECONDS=300`

Use Secret Manager for any value that resolves to a real secret (`CITYLENS_DOCS_ACCESS_KEY_SHA256`, `CITYLENS_ADMIN_API_KEY_HASHES`). The literal `*_SHA256` is a hash, not a secret, but treat it conservatively. Never set `CITYLENS_API_KEYS` for normal users — that path is deprecated and the auth dependency ignores it.

Example update (idempotent):

```bash
gcloud run services update <API_SERVICE_NAME> \
  --region <REGION> --project <PROJECT_ID> \
  --update-env-vars \
CITYLENS_AUTH_PROVIDER=neon,\
CITYLENS_AUTH_REQUIRED=true,\
CITYLENS_AUTH_ISSUER=https://citylens.dev/api/auth,\
CITYLENS_AUTH_JWKS_URL=https://citylens.dev/api/auth/jwks,\
CITYLENS_FREE_MONTHLY_RUNS=5,\
CITYLENS_ADMIN_EMAILS=you@example.com
```

Worker (Cloud Run Job):

- `GOOGLE_CLOUD_PROJECT`, `CITYLENS_REGION`, `CITYLENS_BUCKET`
- The worker does NOT see auth env vars — it reads `CITYLENS_RUN_ID` from the job execution and writes back to Firestore using its own service account credentials.

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
./.venv/bin/python -c 'from google.cloud import firestore; c=firestore.Client(project="<PROJECT_ID>"); print("ok", c.project)'
```

Example for your project:

```bash
./.venv/bin/python -c 'from google.cloud import firestore; c=firestore.Client(project="citylens-001"); print("ok", c.project)'
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
Unauthenticated demo routes under `/v1/demo/*` also allow secure Vercel preview origins ending in `.vercel.app`, so demo mode works from preview deployments without widening live authenticated CORS.

If you enable signed URLs (`CITYLENS_SIGN_URLS=1`), the browser will download artifacts *directly from GCS* (not from the API). In that case you must also configure **bucket CORS** on your artifacts bucket to allow your site origin(s), e.g.:

- `https://citylens.dev`
- `https://www.citylens.dev`
- `http://localhost:3000`

### 11) Test end-to-end

Health (no auth):

```bash
curl https://<API_URL>/v1/health
```

Run-options (no auth):

```bash
curl https://<API_URL>/v1/run-options
```

Authenticated request — get a JWT from your Neon Auth-enabled web app first:

```bash
# 1) Sign in via the browser at https://<your-vercel-domain>/sign-in
# 2) Open dev-tools and grab a JWT via:
#    fetch('/api/auth/token', {credentials: 'include'}).then(r => r.json())
#    -> { token: "<JWT>" }
TOKEN="<paste the JWT here>"
```

Identity / plan:

```bash
curl -H "Authorization: Bearer $TOKEN" https://<API_URL>/v1/me
# => {"user":{"id","email","plan_type","is_admin"},"quota":{"month_key","monthly_run_limit","runs_used","runs_remaining","unlimited","max_concurrent_runs"}}
```

Create run (the engine forbids extra fields like `aoi_radius_m`/`sam2_*`/non-2024 years — the public payload is intentionally narrow):

```bash
curl -X POST https://<API_URL>/v1/runs \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "address": "1 Market St, San Francisco, CA",
    "outputs": ["previews", "change", "mesh"]
  }'
```

Poll run:

```bash
curl -H "Authorization: Bearer $TOKEN" https://<API_URL>/v1/runs/<RUN_ID>
```

Confirm artifacts exist in GCS:

```bash
gsutil ls gs://<BUCKET_NAME>/runs/<RUN_ID>/
```

### 11b) Worker smoke-test

The worker code path was not modified by the auth/quota refactor; the API still triggers Cloud Run Jobs with `CITYLENS_RUN_ID=<run_id>` and the worker still writes artifacts back to Firestore + GCS. Verify after the first authenticated POST:

```bash
# After POSTing /v1/runs and receiving { run_id: "<RUN_ID>" }:
RUN_ID="<RUN_ID>"

# 1) Cloud Run Job execution exists and is running/succeeded
gcloud run jobs executions list \
  --job=<JOB_NAME> --region=<REGION> --project=<PROJECT_ID> \
  --limit=5

# 2) Firestore run doc reflects status changes
./.venv/bin/python -c "
from google.cloud import firestore
c = firestore.Client(project='<PROJECT_ID>')
print(c.collection('runs').document('${RUN_ID}').get().to_dict())
"

# 3) Artifacts land in GCS at the expected path
gsutil ls gs://<BUCKET_NAME>/runs/${RUN_ID}/

# 4) Monthly usage counter incremented
./.venv/bin/python -c "
from google.cloud import firestore
c = firestore.Client(project='<PROJECT_ID>')
import datetime as dt
mk = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m')
APP_USER_ID = '<your app_user_id from /v1/me>'
print(c.collection('usage_months').document(f'{APP_USER_ID}_{mk}').get().to_dict())
"
```

Failure modes to watch for:
- 401 on `/v1/runs` → the JWT failed JWKS verification. Check `CITYLENS_AUTH_JWKS_URL` is reachable from the Cloud Run service and that the issuer/audience claims line up.
- 429 with `code=MONTHLY_QUOTA_EXCEEDED` after 5 successful runs → expected free-plan behavior; promote yourself by adding your email to `CITYLENS_ADMIN_EMAILS` (must be `email_verified=true`) or by sub via `CITYLENS_ADMIN_AUTH_SUBS`.
- Trigger-failure path: if the Cloud Run Job trigger fails, the API decrements the monthly counter automatically. Check the API logs for the `failed to trigger worker job` log line and the run doc's `error.code=TRIGGER_FAILED`.

### 11c) Docs gate smoke-test

```bash
DOCS_KEY="<the-raw-docs-key-you-hashed-into-CITYLENS_DOCS_ACCESS_KEY_SHA256>"
curl -i https://<API_URL>/openapi.json                         # 401 if key configured, 404 if not
curl -i -H "X-Docs-Key: $DOCS_KEY" https://<API_URL>/openapi.json  # 200
curl -i -X POST -H "X-Docs-Key: $DOCS_KEY" -H 'Content-Type: application/json' \
  -d '{"address":"x"}' https://<API_URL>/v1/runs               # 401 — docs key cannot create runs
```

### 12) Demo endpoints (optional, for citylens-web “Demo mode”)

The API exposes unauthenticated demo endpoints:

- `GET /v1/demo/featured`
- `GET /v1/demo/runs/{run_id}`

These endpoints are backed by an allowlist file baked into the API image: `deploy/demo_runs.json`.
`deploy/demo_runs.json` must contain only real successful runs that already exist in
Firestore and GCS. There is no baked placeholder artifact bundle.

For allowlisted demo runs, the API proxies real artifacts through these unauthenticated routes:

- `GET /v1/demo/runs/{run_id}`
- `GET /v1/demo/artifacts/{run_id}/{artifact_name}`
To generate demo runs:

1) Edit [deploy/demo_addresses.json](../deploy/demo_addresses.json) with the addresses/years you want.
2) Deploy the worker + API so the modular pipeline is live.
3) Run the precompute helper against the deployed API:

```bash
./deploy/deploy_all.sh --precompute
```

Notes:

- Precompute requires an admin API key (it uses `POST /v1/runs` and waits for completion). By default it uses the first key in `CITYLENS_API_KEYS`, or you can set `CITYLENS_ADMIN_API_KEY` in your `.env`.
- `scripts/precompute_demo_runs.py` now rejects incomplete runs. It writes `deploy/demo_runs.json` only after verifying `preview.png`, `change.geojson`, `mesh.ply`, and `run_summary.json`.
- Commit the updated `deploy/demo_runs.json` after precompute if you want the allowlist versioned in git.
- Redeploy the API after committing the new allowlist so `/v1/demo/featured` reflects it.

If demo runs load but artifacts do not render in the browser, verify that the run is
allowlisted and that `GET /v1/demo/runs/{run_id}` returns same-origin artifact paths
like `/v1/demo/artifacts/<run_id>/<artifact_name>`.

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

The API pins the core release in `api/pyproject.toml` and `uv.lock`:

- `git+https://github.com/joshvern/citylens-core.git@v0.3.25`

Update the API manifest and lockfile together when releasing a new core
version. The worker pins the same release through `worker/pyproject.toml` and
the shared lockfile.

Build the API image:

```bash
API_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/cloud-run-source-deploy/citylens-api:latest"

gcloud builds submit . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --config api/cloudbuild.yaml \
  --substitutions _IMAGE=${API_IMAGE}
```

Deploy the API service from the built image:

```bash
cd api
gcloud run deploy <API_SERVICE_NAME> \
  --image ${API_IMAGE} \
  --region <REGION> \
  --project <PROJECT_ID> \
  --service-account <API_SA>@<PROJECT_ID>.iam.gserviceaccount.com \
  --update-env-vars GOOGLE_CLOUD_PROJECT=<PROJECT_ID>,CITYLENS_REGION=<REGION>,CITYLENS_BUCKET=<BUCKET_NAME>,CITYLENS_JOB_NAME=<JOB_NAME>
```

Use `--update-env-vars` for image releases so the service retains separately
managed OIDC, quota, CORS, signing, and optional hash-only admin-key settings.
The retired plaintext `CITYLENS_API_KEYS` and `CITYLENS_ADMIN_API_KEYS`
variables are ignored and should not be configured.

### 8) Build & create Cloud Run Job for worker

The worker is deployed separately from the API service. Keep the two surfaces
aligned on the same `citylens-core` revision, but build and deploy them as
independent Cloud Run resources.

Build the worker image:

```bash
cd ..

WORKER_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/cloud-run-source-deploy/citylens-worker:latest"

gcloud builds submit . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --config worker/cloudbuild.yaml \
  --substitutions _IMAGE=${WORKER_IMAGE}
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
  - `CITYLENS_ADMIN_API_KEY_HASHES=<comma-separated sha256 hashes>` (hash-only; the plaintext `CITYLENS_ADMIN_API_KEYS` env var was removed and is ignored)
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

After deployment, run the same adversarial public-contract check used by the
scheduled production monitor:

```bash
./.venv/bin/python scripts/verify_production.py \
  --api-base https://api.citylens.dev \
  --web-base https://www.citylens.dev \
  --max-age-days 35 \
  --output production-verification.json
```

This intentionally needs no credential. It verifies that anonymous requests
cannot read workflow, owner, portfolio, change, violation, lien, flood, transit, or
model-explanation fields while exercising the API's full generation checksum
and row-schema validation for every borough. It also requires the immutable
`generation_diff` release report, its 142-column inference drift report, and
the 5,000-row score replay to pass; any failed thresholds must have an explicit
reviewed override reason recorded by the publisher. It opens one public parcel
detail and requires the v1 decision audit, exact accepted-model validation
metrics, unambiguous rank/eligibility/diligence roles, and redacted
ownership/diligence evidence. It also requires the publisher's hashed
authoritative ZAP BBL reconciliation to show a non-empty source universe,
100% current private project-to-BBL coverage, valid hashed current-PLUTO and
reviewed official-filed-document evidence, blocked candidates actually
exercised, consistent source counts, and zero published leakage. It also
requires the official MTA station source SLA,
complete per-borough transit enrichment, anonymous transit redaction, and an
audit statement that transit is diligence-only. The repository workflow
`.github/workflows/production-smoke.yml` repeats it every six hours and stores
the machine-readable report. A scheduled failure creates or updates one
deduplicated production incident in the engine repository, and the next
successful scheduled run closes it with a recovery comment. Manual dispatches
remain diagnostic unless an operator explicitly enables the
`manage_incident` input to reconcile the production issue. Investigate
scheduled failures before the feed crosses the API's 45-day stale threshold
or the prospective monitor crosses its eight-day observation-lag limit.

### 10.1) Independent Google Cloud uptime monitoring

The GitHub production smoke is comprehensive, but it is not the only
availability signal. CityLens also manages two public Google Cloud uptime
checks and two alert policies:

- API readiness and prospective-evidence freshness
- Parcel Intelligence web availability
- regional failure from at least two Google probe locations
- TLS certificate expiration within 15 days

The operator needs `monitoring.uptimeCheckConfigs.*` and
`monitoring.alertPolicies.*` permissions; `roles/monitoring.editor` is the
standard project role that contains the required mutations. Ensure the API is
enabled:

```bash
gcloud services enable monitoring.googleapis.com \
  --project "${PROJECT_ID}"
```

Preview the exact idempotent plan:

```bash
./.venv/bin/python scripts/configure_production_monitoring.py \
  --project "${PROJECT_ID}"
```

Apply only after reviewing the plan:

```bash
./.venv/bin/python scripts/configure_production_monitoring.py \
  --project "${PROJECT_ID}" \
  --apply
```

The command fails closed on duplicate display names and immutable target drift.
It updates mutable contract drift, preserves existing notification channels,
and never deletes resources. It checks the API for HTTP 200 plus the exact
`"status":"current"` marker; therefore stale prospective evidence triggers the
same independent failure signal as an unavailable API.

List notification channels before choosing one:

```bash
gcloud beta monitoring channels list \
  --project "${PROJECT_ID}" \
  --format='table(name,displayName,type,enabled)'
```

Create and verify a channel in Cloud Monitoring, then attach its fully
qualified resource name:

```bash
./.venv/bin/python scripts/configure_production_monitoring.py \
  --project "${PROJECT_ID}" \
  --notification-channel \
  "projects/${PROJECT_ID}/notificationChannels/<CHANNEL_ID>" \
  --apply
```

An enabled policy with no channel still opens Cloud Monitoring incidents but
does not deliver a page. Channel ownership and destination verification are an
operator decision; do not guess them in automation.

Verify fresh multi-region observations after at least one five-minute probe
cycle:

```bash
./.venv/bin/python scripts/verify_production_monitoring.py \
  --project "${PROJECT_ID}"
```

The verifier is read-only and fails on missing checks, fewer than three fresh
regions, stale observations, or any latest regional failure.

Rollback is deliberately manual. Delete alert policies before their uptime
checks so no policy is left pointing at a missing metric:

```bash
gcloud monitoring policies list \
  --project "${PROJECT_ID}" \
  --format='table(name,displayName,enabled)'

gcloud monitoring policies delete <POLICY_ID> \
  --project "${PROJECT_ID}"

gcloud monitoring uptime list-configs \
  --project "${PROJECT_ID}" \
  --format='table(name,displayName)'

gcloud monitoring uptime delete <CHECK_ID> \
  --project "${PROJECT_ID}"
```

Review display names and resource IDs before every delete. Re-running the
configuration command with `--apply` recreates missing managed resources, but
does not recreate notification channels.

### 10.2) Production data recovery

Firestore application state is not reconstructable from the public parcel
feed. The production baseline therefore requires:

- database delete protection
- seven-day point-in-time recovery
- one daily backup schedule with 14-day retention
- one Sunday backup schedule with 14-week retention

The GCS artifact bucket must retain public access prevention, uniform
bucket-level access, and at least seven days of soft delete. CityLens artifacts
use immutable generation/object names plus an atomic active pointer, so object
versioning is not added as a redundant unbounded storage layer.

PITR data, backup storage, restore, and clone operations are billable Firestore
features. Check current database size and the Google Cloud billing account
before applying, and keep a budget alert around the project. Backup creation
does not consume application reads/writes or affect live database performance.

Preview and apply the idempotent baseline:

```bash
./.venv/bin/python scripts/configure_production_recovery.py \
  --project "${PROJECT_ID}" \
  --bucket "${CITYLENS_BUCKET}"

./.venv/bin/python scripts/configure_production_recovery.py \
  --project "${PROJECT_ID}" \
  --bucket "${CITYLENS_BUCKET}" \
  --apply
```

The command never deletes a backup, schedule, object, bucket, or database. It
fails closed on duplicate schedules and only raises protections or repairs the
two bounded retention schedules.

Verify configuration and backup freshness:

```bash
./.venv/bin/python scripts/verify_production_recovery.py \
  --project "${PROJECT_ID}" \
  --bucket "${CITYLENS_BUCKET}" \
  --location "${CITYLENS_REGION}"
```

The strict verifier requires a `READY` backup no more than 36 hours old. During
the first 26 hours after initial schedule creation only, add
`--allow-collecting`; this permits a `collecting` result but never accepts
configuration drift, an overdue first backup, a stale backup, or an expired
backup.

The repository's daily
`recovery-verification.yml` workflow runs this verifier independently of an
operator workstation. Configure these repository variables:

```text
CITYLENS_GCP_PROJECT=citylens-001
CITYLENS_GCP_REGION=us-central1
CITYLENS_ARTIFACT_BUCKET=citylens-001-artifacts
GCP_RECOVERY_WORKLOAD_IDENTITY_PROVIDER=<provider resource name>
GCP_RECOVERY_VERIFIER_SERVICE_ACCOUNT=<read-only verifier service account>
```

The workload identity provider must restrict tokens to
`joshvern/citylens-engine` on `refs/heads/master`. Grant its service account a
custom role containing only:

```text
datastore.databases.get
datastore.databases.getMetadata
datastore.backupSchedules.get
datastore.backupSchedules.list
datastore.backups.get
datastore.backups.list
storage.buckets.get
```

The workflow always passes `--allow-collecting`, but that flag remains bounded
inside the verifier: after 26 hours without a first backup it fails. Scheduled
failures open or update one GitHub incident, successful recovery closes it,
and every run retains the JSON report for 90 days. Manual runs are read-only
and do not mutate the incident unless `manage_incident` is selected.

Run a restore drill only into a new named database. Never overwrite or delete
the production `(default)` database:

```bash
BACKUP="$(
  gcloud firestore backups list \
    --project "${PROJECT_ID}" \
    --location "${CITYLENS_REGION}" \
    --filter="state=READY AND database='projects/${PROJECT_ID}/databases/(default)'" \
    --sort-by='~snapshotTime' \
    --limit=1 \
    --format='value(name)'
)"

DRILL_DATABASE="recovery-drill-$(date -u +%Y%m%d)"

gcloud firestore databases restore \
  --project "${PROJECT_ID}" \
  --source-backup "${BACKUP}" \
  --destination-database "${DRILL_DATABASE}"
```

Verify the restored database independently before removing it:

- confirm required collections and representative documents exist
- confirm IAM denies application traffic until explicitly authorized
- reapply and verify TTL field policies before any production cutover
- record the backup ID, snapshot time, operation ID, duration, and reviewer
- delete only the named drill database after the review is complete

Firestore backups include data and index configuration, but not TTL policies
or Firebase Security Rules. A restore is therefore evidence of data
recoverability, not by itself a complete application cutover.

The same verifier requires API and web HSTS, clickjacking protection, MIME
sniffing protection, explicit referrer policy, disabled unused browser
capabilities, and the narrow enforced CSP baseline. It also rejects
`X-Powered-By` on the web. Verify these on the custom domains rather than only
the Cloud Run or Vercel preview origins.

Authenticated outcome evidence is available from
`GET /v1/parcel-intel/workflow/outcomes/export`. It is user-scoped,
`private, no-store`, rate-limited, and returned with a download disposition.
The artifact is deliberately value-minimized and integrity-hashed; never
replace it with a Firestore collection dump or add workflow free text to the
export. Deployment tests must keep pending and uninstrumented labels null.

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

Enable the 90-day retention boundary for aggregate Parcel Intelligence product
usage counters. Firestore TTL is a collection-group policy, so this applies to
every nested `users/{user_id}/product_usage_days/{day}` document:

```bash
gcloud firestore fields ttls update expires_at \
  --collection-group=product_usage_days \
  --database='(default)' \
  --project="<PROJECT_ID>" \
  --enable-ttl
```

Verify the policy:

```bash
gcloud firestore fields ttls list \
  --collection-group=product_usage_days \
  --database='(default)' \
  --project="<PROJECT_ID>" \
  --format='table(name,ttlConfig.state)'
```

Enable and verify the separate 365-day TTL boundary for consented pilot
requests. The API writes the expiry timestamp; Firestore owns physical
deletion:

```bash
gcloud firestore fields ttls update expires_at \
  --collection-group=pilot_requests \
  --database='(default)' \
  --project="<PROJECT_ID>" \
  --enable-ttl

gcloud firestore fields ttls list \
  --collection-group=pilot_requests \
  --database='(default)' \
  --project="<PROJECT_ID>" \
  --format='table(name,ttlConfig.state)'
```

Set `CITYLENS_PILOT_REQUESTS_COLLECTION=pilot_requests` unless a deliberate
environment-specific collection is required. Public submission is
unauthenticated but bounded, consented, idempotent, honeypot-filtered, and
throttled. Queue list/status operations still require an admin identity.

The API writes only aggregate daily event/source counts to this collection.
Parcel opens and saved-view applies are value-minimized client counters.
Workflow lifecycle and saved-view create/update/delete counters are written
transactionally with their canonical mutations, so dropped browser telemetry
cannot erase a real save and unchanged retries cannot inflate it. Do not add
BBLs, addresses, owners, URLs, workflow text, saved-view names/search/filter
state, or event-level rows.
Generate the aggregate operator report with:

```bash
./.venv/bin/python scripts/report_product_adoption.py \
  --project "<PROJECT_ID>" \
  --days 30
```

The v3 report includes aggregate canonical workflow and saved-view inventory,
an activation-evidence gate, and a saved-view-reuse evidence gate. Activation
remains `collecting` until at least 30 workflow records exist across at least
three users. Saved-view reuse remains `collecting` until at least 10
best-effort apply events exist across at least three users. The saved-view
inventory query selects only the schema marker and never reads names, search
text, filters, or owners. These are product-use signals only; they do not
establish model accuracy, seller intent, transaction probability, unique
parcels, or lead quality.

The repository's scheduled
`.github/workflows/adoption-report.yml` runs the same report daily and retains
the aggregate JSON artifact for 90 days. It authenticates with GitHub OIDC,
not a service-account key. Configure these repository variables:

```text
CITYLENS_GCP_PROJECT=<PROJECT_ID>
GCP_ADOPTION_REPORTER_SERVICE_ACCOUNT=citylens-adoption-reporter@<PROJECT_ID>.iam.gserviceaccount.com
GCP_ADOPTION_WORKLOAD_IDENTITY_PROVIDER=projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-pool/providers/citylens-engine-provider
```

The Google service account should have only a custom read role containing
`datastore.entities.get` and `datastore.entities.list`. Restrict the workload
identity provider to `joshvern/citylens-engine` on `refs/heads/master`.
Do not grant this reporting identity deployment, bucket-write, secret-access,
or service-account-key permissions. The workflow warns rather than fails when
the activation gate is still collecting; query/authentication failures remain
real workflow failures.

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

- Precompute requires an admin API key (it uses `POST /v1/runs` and waits for completion). Set `CITYLENS_ADMIN_API_KEY` in your `.env` (or pass `--admin-api-key`); its SHA-256 must be listed in the deployed service's `CITYLENS_ADMIN_API_KEY_HASHES`.
- `scripts/precompute_demo_runs.py` now rejects incomplete runs. It writes `deploy/demo_runs.json` only after verifying `preview.png`, `change.geojson`, `mesh.ply`, and `run_summary.json`.
- Commit the updated `deploy/demo_runs.json` after precompute if you want the allowlist versioned in git.
- Redeploy the API after committing the new allowlist so `/v1/demo/featured` reflects it.

If demo runs load but artifacts do not render in the browser, verify that the run is
allowlisted and that `GET /v1/demo/runs/{run_id}` returns same-origin artifact paths
like `/v1/demo/artifacts/<run_id>/<artifact_name>`.

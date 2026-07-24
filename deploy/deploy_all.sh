#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy/deploy_all.sh [--precompute] [--admin-api-key KEY] [--api-base URL]

Deploys the worker job and API service, then optionally precomputes demo runs and
re-deploys the API so the updated deploy/demo_runs.json is baked into the image.

Defaults:
  - Reads configuration from .env in the repo root
  - Builds the exact API and worker dependency graphs recorded in uv.lock

Options:
  --precompute           Run scripts/precompute_demo_runs.py after first API deploy
  --admin-api-key KEY    API key used for POST /v1/runs (or set CITYLENS_ADMIN_API_KEY in .env; its SHA-256 must be listed in CITYLENS_ADMIN_API_KEY_HASHES)
  --api-base URL         Override API base URL for precompute (otherwise read from Cloud Run)
  -h, --help             Show this help
EOF
}

PRECOMPUTE=0
ADMIN_API_KEY_OVERRIDE=""
API_BASE_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --precompute)
      PRECOMPUTE=1
      shift
      ;;
    --admin-api-key)
      ADMIN_API_KEY_OVERRIDE="${2:-}"
      shift 2
      ;;
    --api-base)
      API_BASE_OVERRIDE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo "Missing ${ROOT_DIR}/.env (this file is git-ignored)." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ROOT_DIR}/.env"
set +a

ADMIN_API_KEY="${ADMIN_API_KEY_OVERRIDE:-${CITYLENS_ADMIN_API_KEY:-}}"

: "${GOOGLE_CLOUD_PROJECT:?Missing GOOGLE_CLOUD_PROJECT}"
: "${CITYLENS_REGION:?Missing CITYLENS_REGION}"
: "${CITYLENS_BUCKET:?Missing CITYLENS_BUCKET}"
: "${CITYLENS_JOB_NAME:?Missing CITYLENS_JOB_NAME}"

PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
REGION="${CITYLENS_REGION}"
BUCKET_NAME="${CITYLENS_BUCKET}"
JOB_NAME="${CITYLENS_JOB_NAME}"

RUNS_COLLECTION="${CITYLENS_RUNS_COLLECTION:-runs}"
USERS_COLLECTION="${CITYLENS_USERS_COLLECTION:-users}"
# For browser clients (citylens-web), signed URLs are typically required to render/download
# private GCS artifacts. Override in .env if desired.
SIGN_URLS="${CITYLENS_SIGN_URLS:-1}"
SIGN_URL_TTL_SECONDS="${CITYLENS_SIGN_URL_TTL_SECONDS:-300}"

API_SERVICE_NAME="${API_SERVICE_NAME:-citylens-api}"
API_SA_NAME="${API_SA_NAME:-citylens-api}"
API_SA_EMAIL="${API_SA_EMAIL:-${API_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
API_IMAGE="${API_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-api:latest}"

WORKER_SA_NAME="${WORKER_SA_NAME:-citylens-worker}"
WORKER_SA_EMAIL="${WORKER_SA_EMAIL:-${WORKER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
WORKER_IMAGE="${WORKER_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-worker:latest}"
WORKER_MEMORY="${WORKER_MEMORY:-8Gi}"
WORKER_CPU="${WORKER_CPU:-4}"
WORKER_TASK_TIMEOUT="${WORKER_TASK_TIMEOUT:-1800s}"
WORKER_MAX_RETRIES="${WORKER_MAX_RETRIES:-0}"

DEPLOYER_ACCOUNT="${DEPLOYER_ACCOUNT:-}"
if [[ -z "${DEPLOYER_ACCOUNT}" ]]; then
  DEPLOYER_ACCOUNT="$(gcloud config get-value account 2>/dev/null || true)"
fi
DEPLOYER_MEMBER="${DEPLOYER_MEMBER:-}"
if [[ -z "${DEPLOYER_MEMBER}" && -n "${DEPLOYER_ACCOUNT}" ]]; then
  DEPLOYER_MEMBER="user:${DEPLOYER_ACCOUNT}"
fi

ensure_service_account() {
  local sa_email="$1"
  local sa_name="$2"
  local display_name="$3"

  if ! gcloud iam service-accounts describe "${sa_email}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Creating service account: ${sa_email}"
    gcloud iam service-accounts create "${sa_name}" \
      --project "${PROJECT_ID}" \
      --display-name "${display_name}"
  fi

  if [[ -n "${DEPLOYER_MEMBER}" ]]; then
    echo "Ensuring deployer can actAs ${sa_email} (${DEPLOYER_MEMBER})"
    gcloud iam service-accounts add-iam-policy-binding "${sa_email}" \
      --project "${PROJECT_ID}" \
      --member "${DEPLOYER_MEMBER}" \
      --role "roles/iam.serviceAccountUser" >/dev/null
  fi
}

get_api_url() {
  gcloud run services describe "${API_SERVICE_NAME}" \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --format='value(status.url)'
}

echo "Project: ${PROJECT_ID}"
echo "Region:  ${REGION}"
echo "API:     ${API_SERVICE_NAME} (${API_IMAGE})"
echo "Worker:  ${JOB_NAME} (${WORKER_IMAGE})"

# --- Deploy worker ---
ensure_service_account "${WORKER_SA_EMAIL}" "${WORKER_SA_NAME}" "CityLens worker"

# Required for reading/writing run state in Firestore.
echo "Ensuring worker SA has Firestore permissions (${WORKER_SA_EMAIL})"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${WORKER_SA_EMAIL}" \
  --role "roles/datastore.user" \
  --quiet >/dev/null

# Required for uploading artifacts into the configured GCS bucket.
echo "Ensuring worker SA can write artifacts to ${BUCKET_NAME} (${WORKER_SA_EMAIL})"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member "serviceAccount:${WORKER_SA_EMAIL}" \
  --role "roles/storage.objectAdmin" \
  --quiet >/dev/null

(
  cd "${ROOT_DIR}"
  gcloud builds submit . \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --config "worker/cloudbuild.yaml" \
    --substitutions _IMAGE=${WORKER_IMAGE}
)

gcloud run jobs deploy "${JOB_NAME}" \
  --image "${WORKER_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${WORKER_SA_EMAIL}" \
  --cpu "${WORKER_CPU}" \
  --memory "${WORKER_MEMORY}" \
  --task-timeout "${WORKER_TASK_TIMEOUT}" \
  --max-retries "${WORKER_MAX_RETRIES}" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_ASSETS_ROOT=/opt/citylens-assets,CITYLENS_REFERENCE_DATA_DIR=/tmp/reference-data"

# --- Deploy API ---
ensure_service_account "${API_SA_EMAIL}" "${API_SA_NAME}" "CityLens API"

(
  cd "${ROOT_DIR}"
  gcloud builds submit . \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --config api/cloudbuild.yaml \
    --substitutions _IMAGE=${API_IMAGE}
)

gcloud run deploy "${API_SERVICE_NAME}" \
  --image "${API_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${API_SA_EMAIL}" \
  --allow-unauthenticated \
  --update-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_RUNS_COLLECTION=${RUNS_COLLECTION},CITYLENS_USERS_COLLECTION=${USERS_COLLECTION},CITYLENS_SIGN_URLS=${SIGN_URLS},CITYLENS_SIGN_URL_TTL_SECONDS=${SIGN_URL_TTL_SECONDS},CITYLENS_ALLOW_ADMIN_API_KEYS=${CITYLENS_ALLOW_ADMIN_API_KEYS:-false},CITYLENS_ADMIN_API_KEY_HASHES=${CITYLENS_ADMIN_API_KEY_HASHES:-},CITYLENS_ALLOW_USER_API_KEYS=${CITYLENS_ALLOW_USER_API_KEYS:-true}"

gcloud run services update-traffic "${API_SERVICE_NAME}" \
  --to-latest \
  --region "${REGION}" \
  --project "${PROJECT_ID}"

API_URL="${API_BASE_OVERRIDE}"
if [[ -z "${API_URL}" ]]; then
  API_URL="$(get_api_url)"
fi

echo "Deployed API: ${API_URL}"

if [[ "${PRECOMPUTE}" -eq 1 ]]; then
  if [[ -z "${ADMIN_API_KEY}" ]]; then
    echo "Missing admin API key. Provide --admin-api-key or set CITYLENS_ADMIN_API_KEY in .env" >&2
    exit 1
  fi

  echo "Precomputing demo runs against ${API_URL} ..."
  CITYLENS_ADMIN_API_KEY="${ADMIN_API_KEY}" \
    python3 "${ROOT_DIR}/scripts/precompute_demo_runs.py" \
      --api-base "${API_URL}"

  echo "Rebuilding + redeploying API to bake updated deploy/demo_runs.json ..."
  (
    cd "${ROOT_DIR}"
    gcloud builds submit . \
      --region "${REGION}" \
      --project "${PROJECT_ID}" \
      --config api/cloudbuild.yaml \
      --substitutions _IMAGE=${API_IMAGE}
  )

  gcloud run deploy "${API_SERVICE_NAME}" \
    --image "${API_IMAGE}" \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --service-account "${API_SA_EMAIL}" \
    --allow-unauthenticated \
    --update-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_RUNS_COLLECTION=${RUNS_COLLECTION},CITYLENS_USERS_COLLECTION=${USERS_COLLECTION},CITYLENS_SIGN_URLS=${SIGN_URLS},CITYLENS_SIGN_URL_TTL_SECONDS=${SIGN_URL_TTL_SECONDS},CITYLENS_ALLOW_ADMIN_API_KEYS=${CITYLENS_ALLOW_ADMIN_API_KEYS:-false},CITYLENS_ADMIN_API_KEY_HASHES=${CITYLENS_ADMIN_API_KEY_HASHES:-},CITYLENS_ALLOW_USER_API_KEYS=${CITYLENS_ALLOW_USER_API_KEYS:-true}"

  gcloud run services update-traffic "${API_SERVICE_NAME}" \
    --to-latest \
    --region "${REGION}" \
    --project "${PROJECT_ID}"

  echo "Done. Remember to commit deploy/demo_runs.json if you want it in git."
fi

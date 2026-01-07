#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy/deploy_all.sh [--precompute] [--admin-api-key KEY] [--api-base URL]

Deploys the worker job and API service, then optionally precomputes demo runs and
re-deploys the API so the updated deploy/demo_runs.json is baked into the image.

Defaults:
  - Reads configuration from .env in the repo root
  - Uses citylens-core tag v0.2.0 unless CITYLENS_CORE_GIT_URL is set

Options:
  --precompute           Run scripts/precompute_demo_runs.py after first API deploy
  --admin-api-key KEY    API key used for POST /v1/runs (or set CITYLENS_ADMIN_API_KEY; otherwise uses first key in CITYLENS_API_KEYS)
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
: "${CITYLENS_API_KEYS:?Missing CITYLENS_API_KEYS}"

if [[ -z "${ADMIN_API_KEY}" ]]; then
  ADMIN_API_KEY="${CITYLENS_API_KEYS%%,*}"
  # trim whitespace
  ADMIN_API_KEY="${ADMIN_API_KEY#"${ADMIN_API_KEY%%[![:space:]]*}"}"
  ADMIN_API_KEY="${ADMIN_API_KEY%"${ADMIN_API_KEY##*[![:space:]]}"}"
fi

PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
REGION="${CITYLENS_REGION}"
BUCKET_NAME="${CITYLENS_BUCKET}"
JOB_NAME="${CITYLENS_JOB_NAME}"

CITYLENS_CORE_GIT_URL="${CITYLENS_CORE_GIT_URL:-git+https://github.com/joshvern/citylens-core.git@v0.2.0}"

API_SERVICE_NAME="${API_SERVICE_NAME:-citylens-api}"
API_SA_NAME="${API_SA_NAME:-citylens-api}"
API_SA_EMAIL="${API_SA_EMAIL:-${API_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
API_IMAGE="${API_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-api:latest}"

WORKER_SA_NAME="${WORKER_SA_NAME:-citylens-worker}"
WORKER_SA_EMAIL="${WORKER_SA_EMAIL:-${WORKER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
WORKER_IMAGE="${WORKER_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-worker:latest}"

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
echo "Core:    ${CITYLENS_CORE_GIT_URL}"
echo "API:     ${API_SERVICE_NAME} (${API_IMAGE})"
echo "Worker:  ${JOB_NAME} (${WORKER_IMAGE})"

# --- Deploy worker ---
ensure_service_account "${WORKER_SA_EMAIL}" "${WORKER_SA_NAME}" "CityLens worker"

(
  cd "${ROOT_DIR}/worker"
  gcloud builds submit . \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --config cloudbuild.yaml \
    --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${WORKER_IMAGE}
)

gcloud run jobs deploy "${JOB_NAME}" \
  --image "${WORKER_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${WORKER_SA_EMAIL}" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME}"

# --- Deploy API ---
ensure_service_account "${API_SA_EMAIL}" "${API_SA_NAME}" "CityLens API"

(
  cd "${ROOT_DIR}"
  gcloud builds submit . \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --config api/cloudbuild.yaml \
    --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${API_IMAGE}
)

gcloud run deploy "${API_SERVICE_NAME}" \
  --image "${API_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${API_SA_EMAIL}" \
  --allow-unauthenticated \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_API_KEYS=${CITYLENS_API_KEYS}"

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
      --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${API_IMAGE}
  )

  gcloud run deploy "${API_SERVICE_NAME}" \
    --image "${API_IMAGE}" \
    --region "${REGION}" \
    --project "${PROJECT_ID}" \
    --service-account "${API_SA_EMAIL}" \
    --allow-unauthenticated \
    --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_API_KEYS=${CITYLENS_API_KEYS}"

  echo "Done. Remember to commit deploy/demo_runs.json if you want it in git." 
fi

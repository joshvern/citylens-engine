#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo "Missing ${ROOT_DIR}/.env (this file is git-ignored)." >&2
  exit 1
fi

set -a
source "${ROOT_DIR}/.env"
set +a

: "${GOOGLE_CLOUD_PROJECT:?Missing GOOGLE_CLOUD_PROJECT}"
: "${CITYLENS_REGION:?Missing CITYLENS_REGION}"
: "${CITYLENS_BUCKET:?Missing CITYLENS_BUCKET}"
: "${CITYLENS_JOB_NAME:?Missing CITYLENS_JOB_NAME}"
: "${CITYLENS_API_KEYS:?Missing CITYLENS_API_KEYS}"

PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
REGION="${CITYLENS_REGION}"
BUCKET_NAME="${CITYLENS_BUCKET}"
JOB_NAME="${CITYLENS_JOB_NAME}"

API_SERVICE_NAME="${API_SERVICE_NAME:-citylens-api}"
API_SA_NAME="${API_SA_NAME:-citylens-api}"
API_SA_EMAIL="${API_SA_EMAIL:-${API_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

CITYLENS_CORE_GIT_URL="${CITYLENS_CORE_GIT_URL:-git+https://github.com/joshvern/citylens-core.git@v0.1.0}"
API_IMAGE="${API_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-api:latest}"

DEPLOYER_ACCOUNT="${DEPLOYER_ACCOUNT:-}"
if [[ -z "${DEPLOYER_ACCOUNT}" ]]; then
  DEPLOYER_ACCOUNT="$(gcloud config get-value account 2>/dev/null || true)"
fi
DEPLOYER_MEMBER="${DEPLOYER_MEMBER:-}"
if [[ -z "${DEPLOYER_MEMBER}" && -n "${DEPLOYER_ACCOUNT}" ]]; then
  DEPLOYER_MEMBER="user:${DEPLOYER_ACCOUNT}"
fi

echo "Project: ${PROJECT_ID}"
echo "Region:  ${REGION}"
echo "API:     ${API_SERVICE_NAME}"
echo "Image:   ${API_IMAGE}"

if ! gcloud iam service-accounts describe "${API_SA_EMAIL}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Creating service account: ${API_SA_EMAIL}"
  gcloud iam service-accounts create "${API_SA_NAME}" \
    --project "${PROJECT_ID}" \
    --display-name "CityLens API"
fi

if [[ -n "${DEPLOYER_MEMBER}" ]]; then
  echo "Ensuring deployer can actAs ${API_SA_EMAIL} (${DEPLOYER_MEMBER})"
  if ! gcloud iam service-accounts add-iam-policy-binding "${API_SA_EMAIL}" \
    --project "${PROJECT_ID}" \
    --member "${DEPLOYER_MEMBER}" \
    --role "roles/iam.serviceAccountUser" >/dev/null; then
    echo "" >&2
    echo "ERROR: Unable to grant roles/iam.serviceAccountUser on ${API_SA_EMAIL}." >&2
    echo "Run this with a project owner/admin account:" >&2
    echo "  gcloud iam service-accounts add-iam-policy-binding ${API_SA_EMAIL} --project ${PROJECT_ID} --member ${DEPLOYER_MEMBER} --role roles/iam.serviceAccountUser" >&2
    exit 1
  fi
else
  echo "NOTE: Could not determine gcloud account; if deploy fails with iam.serviceaccounts.actAs, grant roles/iam.serviceAccountUser on ${API_SA_EMAIL}." >&2
fi

cd "${ROOT_DIR}"

gcloud builds submit . \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --config api/cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${API_IMAGE}

gcloud run deploy "${API_SERVICE_NAME}" \
  --image "${API_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${API_SA_EMAIL}" \
  --allow-unauthenticated \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME},CITYLENS_JOB_NAME=${JOB_NAME},CITYLENS_API_KEYS=${CITYLENS_API_KEYS}"

API_URL="$(gcloud run services describe "${API_SERVICE_NAME}" --region "${REGION}" --project "${PROJECT_ID}" --format='value(status.url)')"
echo "Deployed API: ${API_URL}"

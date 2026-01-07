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

PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
REGION="${CITYLENS_REGION}"
BUCKET_NAME="${CITYLENS_BUCKET}"
JOB_NAME="${CITYLENS_JOB_NAME}"

WORKER_SA_NAME="${WORKER_SA_NAME:-citylens-worker}"
WORKER_SA_EMAIL="${WORKER_SA_EMAIL:-${WORKER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

CITYLENS_CORE_GIT_URL="${CITYLENS_CORE_GIT_URL:-git+https://github.com/joshvern/citylens-core.git@v0.2.1}"
WORKER_IMAGE="${WORKER_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/citylens-worker:latest}"

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
echo "Job:     ${JOB_NAME}"
echo "Image:   ${WORKER_IMAGE}"

if ! gcloud iam service-accounts describe "${WORKER_SA_EMAIL}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Creating service account: ${WORKER_SA_EMAIL}"
  gcloud iam service-accounts create "${WORKER_SA_NAME}" \
    --project "${PROJECT_ID}" \
    --display-name "CityLens worker"
fi

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

if [[ -n "${DEPLOYER_MEMBER}" ]]; then
  echo "Ensuring deployer can actAs ${WORKER_SA_EMAIL} (${DEPLOYER_MEMBER})"
  if ! gcloud iam service-accounts add-iam-policy-binding "${WORKER_SA_EMAIL}" \
    --project "${PROJECT_ID}" \
    --member "${DEPLOYER_MEMBER}" \
    --role "roles/iam.serviceAccountUser" >/dev/null; then
    echo "" >&2
    echo "ERROR: Unable to grant roles/iam.serviceAccountUser on ${WORKER_SA_EMAIL}." >&2
    echo "Run this with a project owner/admin account:" >&2
    echo "  gcloud iam service-accounts add-iam-policy-binding ${WORKER_SA_EMAIL} --project ${PROJECT_ID} --member ${DEPLOYER_MEMBER} --role roles/iam.serviceAccountUser" >&2
    exit 1
  fi
else
  echo "NOTE: Could not determine gcloud account; if deploy fails with iam.serviceaccounts.actAs, grant roles/iam.serviceAccountUser on ${WORKER_SA_EMAIL}." >&2
fi

cd "${ROOT_DIR}/worker"

gcloud builds submit . \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --config cloudbuild.yaml \
  --substitutions _CITYLENS_CORE_GIT_URL=${CITYLENS_CORE_GIT_URL},_IMAGE=${WORKER_IMAGE}

gcloud run jobs deploy "${JOB_NAME}" \
  --image "${WORKER_IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --service-account "${WORKER_SA_EMAIL}" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},CITYLENS_REGION=${REGION},CITYLENS_BUCKET=${BUCKET_NAME}"

echo "Deployed worker job: ${JOB_NAME}"

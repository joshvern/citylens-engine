# Security

## Secrets

- No secrets are committed. Use `.env.example` as a template.
- API keys are provided via `CITYLENS_API_KEYS` and are never stored in plaintext.

## Auth

- Requests to `/v1/runs*` require `X-API-Key`.
- User identity is `sha256(api_key)`.

## Storage

- GCS bucket is private.
- API can optionally return short-lived signed URLs (`CITYLENS_SIGN_URLS=1`).

## IAM

- API service account needs permission to:
  - read/write Firestore documents
  - trigger Cloud Run Job executions
  - (optional) sign URLs / read GCS metadata

- Worker service account needs permission to:
  - read/write Firestore documents
  - write objects to the artifacts bucket

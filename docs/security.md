# Security

## Three distinct credential surfaces

CityLens has three different credentials. They protect different things and never substitute for each other:

| Surface | Header | Configured by | Purpose |
| --- | --- | --- | --- |
| **User login** | `Authorization: Bearer <token>` | Neon Auth (or any OIDC issuer via JWKS) | Authenticates real dashboard users for `/v1/runs*`, `/v1/me` |
| **Admin API keys (optional)** | `X-API-Key` | `CITYLENS_ALLOW_ADMIN_API_KEYS=true` + `CITYLENS_ADMIN_API_KEYS` / `CITYLENS_ADMIN_API_KEY_HASHES` | Internal scripts only (e.g. `scripts/precompute_demo_runs.py`). Off by default. |
| **Docs access key** | `X-Docs-Key` | `CITYLENS_DOCS_ACCESS_KEY_SHA256` | Gates `/docs`, `/redoc`, `/openapi.json`. Cannot create runs or access user data. |

## Secrets

- No secrets are committed. `.env.example` only lists names + placeholders.
- For `CITYLENS_DOCS_ACCESS_KEY_SHA256`, store the SHA-256 of the docs key, not the key itself. Compute with `python -c 'import hashlib,sys;print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "<your-key>"`.
- The deprecated `CITYLENS_API_KEYS` no longer authenticates dashboard users and is ignored by the auth dependency.

## Auth flow for `/v1/runs*` and `/v1/me`

1. Browser obtains an auth token from Neon Auth (or whichever OIDC issuer the deploy targets).
2. Browser calls the API with `Authorization: Bearer <token>`.
3. The API verifies the token signature against the configured JWKS URL, validates `exp`/`iat`/issuer/audience, and extracts `sub`/`email`/`email_verified`.
4. The API maps `(provider, sub)` → `app_user_id` in Firestore (`auth_identities/{sha256(provider:sub)}` → `users/{app_user_id}`). New users are auto-provisioned with `plan_type="free"`.
5. Admin promotion: `sub ∈ CITYLENS_ADMIN_AUTH_SUBS` OR (`email ∈ CITYLENS_ADMIN_EMAILS` AND `email_verified=true`).
6. `plan_type` is never trusted from the token; it lives in Firestore and is reconciled from env-driven admin overrides.

## Quotas

- Free plan: 5 runs / UTC calendar month (overridable via `CITYLENS_FREE_MONTHLY_RUNS`), 1 concurrent run.
- Admin plan: unlimited.
- Counters live in `usage_months/{app_user_id}_{YYYY-MM}` and are incremented inside a Firestore transaction at run create time. If the Cloud Run job trigger fails, the counter is decremented.

## Mock auth (local/test only)

- `CITYLENS_AUTH_PROVIDER=mock` requires `CITYLENS_ALLOW_MOCK_AUTH=true` to take effect. Without the explicit allow flag, the verifier returns 503.
- Production deploys must use `CITYLENS_AUTH_PROVIDER=neon` (or another real OIDC provider) and leave `CITYLENS_ALLOW_MOCK_AUTH=false`.

## Storage

- GCS bucket is private.
- API can optionally return short-lived signed URLs (`CITYLENS_SIGN_URLS=1`).

## IAM

- API service account needs permission to:
  - read/write Firestore documents (`users`, `auth_identities`, `runs`, `usage_months`)
  - trigger Cloud Run Job executions
  - (optional) sign URLs / read GCS metadata
- Worker service account needs permission to:
  - read/write Firestore documents
  - write objects to the artifacts bucket

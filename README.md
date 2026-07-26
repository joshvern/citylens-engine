# citylens-engine

The CityLens **API, worker, auth, quotas, and artifact storage**. Powers
the live product at **https://www.citylens.dev**, served via the API at
**https://api.citylens.dev**.

Companion repos:

- [`citylens-web`](https://github.com/joshvern/citylens-web) — Next.js
  product frontend.
- [`citylens-core`](https://github.com/joshvern/citylens-core) — reusable
  Python pipeline library (segmentation, change detection, mesh).

This repo is independently runnable under the shared `/home/josh/citylens`
workspace. For local development, open `citylens-engine/` directly in VS Code, or
use a multi-root workspace that includes this folder so the editor resolves the
repo-local tooling correctly.

It has two separate deployment surfaces:

- **API**: FastAPI service on Cloud Run (login-gated `/v1/runs*`,
  `/v1/me`, plus public `/v1/demo/*` and `/v1/run-options`).
- **Worker**: Python Cloud Run Job that runs the citylens-core pipeline.
- **Metadata**: Firestore (`runs`, `users`, `auth_identities`,
  `usage_months`).
- **Artifacts**: GCS (private bucket; API can optionally return signed URLs).
- **Auth**: OIDC/JWKS verification (signature, `iss`, `aud`, `exp`); Neon
  Auth tokens accepted out of the box. Optional admin API keys for
  internal scripts only.

Critical constraint: this repo **does not define its own pipeline request schema**.
It imports and uses the canonical `CitylensRequest` and pipeline entrypoint from `citylens-core`.

Local development uses the repo-local `.venv` at the engine root. The API service
and worker job share that same root environment for local checks. The API image
uses a digest-pinned Python 3.11 / Alpine 3.23 multi-stage build. The worker
uses a digest-pinned Python 3.11 / Debian-slim multi-stage build because CPU
Torch requires glibc. Both install their exact `uv.lock` production graph,
exclude Git/compilers/package managers from the runtime, and run as UID/GID
`10001`.

`Urban3D-DeepRecon` is treated as a read-only reference repo. This repo is the active
runtime/API layer for the productized system.

The repo uses a single workspace lockfile at `uv.lock` for the `api/` and `worker/`
packages. Regenerate it from the repo root with `uv lock`, then sync the root
environment with `uv sync --all-packages --all-extras`.

`citylens-core` lives in the sibling repo. Both API and worker pin the production
release in their package manifests and the shared `uv.lock`. CI and container
builds install only that locked graph.

Use `make sync` to perform the workspace sync plus the sibling-core install when the
neighboring repo is available. Without the sibling checkout, it falls back to the
public GitHub repo using `CITYLENS_CORE_REF` (default `v0.3.25`). An explicit
`CITYLENS_CORE_GIT_URL` is a local-development escape hatch only and never
changes CI or production images.

## Runtime supply chain

Pull requests must pass independent API and worker supply-chain gates:

- `pip-audit` checks each locked public dependency graph. Git/private packages
  and CPU Torch wheels that are not discoverable through PyPI are covered by
  the image scan instead; their locked public dependencies remain included.
- Trivy scans both built production images. CI rejects every critical
  vulnerability and every fixable high/critical vulnerability.

CI also uploads CycloneDX dependency SBOMs and high/critical image reports for
30 days. Dependabot checks uv, Docker, and GitHub Actions weekly.
See [docs/supply_chain.md](docs/supply_chain.md) for the release policy and
local verification commands.

Current pinned release tag:

- `citylens-core@v0.3.25`

## Auth & quotas

- Real run endpoints (`POST /v1/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/me`) require `Authorization: Bearer <token>` from Neon Auth (or any compatible OIDC issuer).
- Admin promotion is via env: `CITYLENS_ADMIN_AUTH_SUBS` (sub allowlist) or `CITYLENS_ADMIN_EMAILS` (verified-email allowlist).
- Free users get 5 real runs per UTC calendar month (override with `CITYLENS_FREE_MONTHLY_RUNS`); admins are unlimited.
- Run options are server-locked: `imagery_year=2024`, `baseline_year=2017`, `segmentation_backend=sam2`, `aoi_radius_m=250`, outputs ⊂ `{previews, change, mesh}`. Discover via `GET /v1/run-options`.
- Demo endpoints (`/v1/demo/*`), `/v1/health`, `/v1/health/ready`, and
  `/v1/parcel-intel/index` remain public. Parcel Intelligence progressively
  loads `/v1/parcel-intel/map`, fetches full selected-parcel detail from
  `/v1/parcel-intel/parcel/{bbl}`, and reserves `/v1/parcel-intel/sweep` for
  CSV/export and compatibility. Public inventory is capped at 25 rows per
  borough with premium fields stripped; authenticated users can load 1,000
  rows per borough. Large JSON responses are gzip-compressed.
- The public index carries a strict, parcel-free
  `prospective-validation-status@v1` only when its source generation matches
  the active atomic feed. It separates the live cohort's
  awaiting/collecting/mature state from historical forward-test metrics,
  keeps immature final precision null, and exposes only aggregate hit-rate
  evidence, maturity dates, official DOB source dates, and a digest-bound
  private-report reference. Invalid, stale-generation, or parcel-bearing
  status payloads are discarded rather than served. The API separately derives
  `prospective_validation_health` from the accepted observation date. The
  weekly monitor remains `current` through an eight-day lag, becomes `stale`
  after that deadline, and is `unavailable` when the pointer is missing or
  invalid. Future or timezone-ambiguous source evidence is rejected.
- Parcel Intelligence accepts the `published_sweep@v5` contract: separate
  historical-model, borough-acquisition, and citywide-acquisition ranks;
  explicit eligibility/exclusion evidence; owner provenance; current-project
  context; historical NYC DOF final lien-sale diligence; current DOB
  Safety/OATH/HPD violation snapshots; adopted-2007 and preliminary-2015
  PLUTO/FEMA 1% annual-chance floodplain tax-lot screens; current PLUTO
  E-designation/restrictive-declaration diligence; current adopted NYC
  Planning Mandatory Inclusionary Housing mapped-area overlap; current MTA
  station-complex proximity, routes, ADA status, and 400/800 m counts;
  exact-name,
  current-PLUTO legal-entity portfolio summaries; and a publisher quality-gate
  summary plus a generation-to-generation drift report exposed by the
  index/sweep endpoints. The drift report covers inventory turnover, top-rank
  retention, retained-lead rank movement, score PSI, source vintages, model
  identity, required-field coverage, and all 142 accepted-model input columns.
  The index also exposes aggregate score-replay evidence proving all 5,000
  published scores came from the profiled matrix. Failed thresholds require a
  recorded reviewed override before publication. Owner, lien-sale, violation,
  portfolio, floodplain, environmental-designation, MIH, and transit fields are stripped from anonymous
  map, sweep, and detail responses. The authenticated compact map carries portfolio counts,
  `critical_violation_count`, `floodplain_1pct`, and the boolean
  `environmental_review_required` and
  `mandatory_inclusionary_housing`; detailed agency/map fields, designation
  type/number, and dates load on parcel selection. An E-designation or
  restrictive declaration is an
  air/noise/hazardous-materials diligence requirement, not a contamination
  finding or ranking input. An MIH overlap is a dated spatial reference, not
  a tax-lot legal determination; applicability and the controlling option
  still require current Appendix F and project-specific review. Portfolio
  matching preserves legal form,
  never groups natural-person names, and does not infer beneficial ownership or
  related LLCs.
  Transit distance is a great-circle tax-lot-centroid screen, not a walking
  route, entrance distance, travel-time estimate, frequency measure, or
  zoning determination; it is never a rank or eligibility input.
- Selected parcel detail includes a server-built
  `citylens/parcel-decision-audit@v1` explanation. It keeps four evidence
  concepts separate: historical model signal, deterministic acquisition
  eligibility, current post-score diligence, and source provenance. The
  historical validation block reports the accepted next-year DOB
  new-building-filing target and its forward-test precision; it explicitly is
  not seller intent, transaction probability, or acquisition conversion.
  Anonymous audit responses explain the policy while withholding owner,
  diligence, and workflow evidence. Authenticated clients may show those
  private facts without changing the stored score or rank. The same response
  includes a server-derived `readiness` block that classifies the next
  diligence state as blocked, incomplete, review-required, initial-review
  ready, or limited preview. It lists evidence blockers/review items and one
  conservative workflow action; it is explicitly not a purchase
  recommendation, appraisal, title opinion, or seller-intent score.
- The authenticated acquisition workflow preserves an immutable,
  value-minimized event history and soft-archives removed leads. The
  user-scoped `/v1/parcel-intel/workflow/analytics` endpoint reports
  maturity-qualified 30-day contact, 90-day qualification, 180-day offer,
  270-day contract, and 365-day close rates with 95% Wilson intervals. A lead
  enters a denominator only
  after its full observation window; late-recorded milestones do not count as
  on-time outcomes. The public, data-free
  `/v1/parcel-intel/workflow/analytics/methodology` endpoint publishes this
  contract for deployment verification. These are selected, user-saved
  workflow outcomes—not model accuracy, seller intent, or transaction
  probability. The authenticated
  `/v1/parcel-intel/workflow/outcomes/export` endpoint provides a versioned,
  integrity-hashed JSON evidence artifact for user-controlled offline
  validation. It exports only immutable saved-model context and
  maturity-qualified fixed-horizon labels. Pending observations remain null,
  legacy rows without observed event history cannot become negatives, and
  notes, tags, assignees, contacts, addresses, owner names, reminders, and
  raw custom disposition text are excluded. See
  [`docs/prospective_outcomes.md`](docs/prospective_outcomes.md). Per-lead
  audit events are available from
  `/v1/parcel-intel/workflow/{bbl}/events`. The authenticated
  `GET /v1/parcel-intel/workflow/{bbl}` endpoint loads one active workflow
  record (or `null`) without scanning the user's full pipeline; archived rows
  remain hidden from this operational lookup while staying in analytics
  denominators. The authenticated
  `/v1/parcel-intel/workflow/actions` endpoint turns each open lead's
  structured next action and due date into a server-derived queue: overdue,
  due today, due within seven days, scheduled, or unscheduled. It also reports
  missing assignees, unknown outcomes after 30 days, and adoption coverage for
  complete plans, assignees, and current outcome reviews. The authenticated
  `/v1/parcel-intel/workflow/{bbl}/reminder` endpoint can snooze the current
  reminder identity for a bounded interval or restore it. The server binds a
  snooze to the lead's current action, due date, assignee, stage, and outcome;
  editing any of those fields resurfaces the changed commitment immediately.
  Repeat requests are transactionally deduplicated, and terminal records clear
  stale reminders and leave the action queue automatically. These are private
  in-product reminders, not email or webhook delivery. The authenticated
  `/v1/parcel-intel/workflow/alerts` endpoint compares watched leads' saved
  snapshots with the current atomic feed and reports owner, newer-sale,
  zoning, opportunity, rank/tier, lien, violation, flood, environmental
  designation, MIH, transit-complex/tier, imagery, exact-name portfolio, and
  feed-removal changes. Small transit distance fluctuations do not alert when
  the nearest complex and access tier are unchanged. A removed lead is
  deliberately labeled for current-record verification rather than being
  called sold, built, or completed without authoritative evidence.
- Authenticated users can persist the complete citywide explorer state through
  `GET|PUT|DELETE /v1/parcel-intel/saved-searches`. The v2 saved-view contract
  stores borough scope, search text, priority/opportunity filters, optional
  owner-portfolio focus, and map overlay. Responses are private/no-store.
  `alert_frequency` is intentionally limited to `off`; scheduled saved-search
  delivery is not implemented and the API does not imply otherwise.
- Authenticated Parcel Intelligence clients may submit the strict
  `citylens/parcel-product-event@v1` contract to
  `POST /v1/parcel-intel/product-events`. The endpoint accepts only coarse
  parcel-open, decision-audit-open, underwriting-open/first-adjustment, and
  saved-view-apply sources and rejects workflow lifecycle claims, parcel IDs,
  addresses, owners, URLs, underwriting values or results, notes, tags,
  assignees, contacts, and arbitrary properties. Decision-audit opens identify
  only whether the user entered through the overview posture or the Audit tab;
  underwriting events identify only the Underwrite tab or the first base-input
  adjustment for a parcel/session.
  Workflow and saved-view lifecycle
  counters are instead derived by the API inside the same Firestore
  transaction as the canonical mutation. Effective no-op retries do not add
  events or counters. Firestore stores one aggregate counter document per
  user/day under `product_usage_days`; it does not store event-level product
  telemetry.
  Counters are capped at 1,000 per user/day, client parcel-open events are
  rate-limited at the API, and aggregate documents expire after 90 days
  through the `expires_at` TTL field. Run
  `scripts/report_product_adoption.py` for an aggregate-only 30-day operator
  report. The report also counts active and archived canonical workflow
  records and publishes an explicit `collecting`/`ready` activation-evidence
  gate. That gate requires at least 30 workflow records across at least three
  users. Its open-to-save ratio combines directional client opens with
  authoritative workflow creates; neither the ratio nor the gate is model
  accuracy, unique-parcel conversion, lead quality, seller intent, or a
  substitute for canonical workflow records.
- Public design-partner intake uses
  `POST /v1/pilot-requests` with the bounded
  `citylens/pilot-request@v1` contract and an opaque `Idempotency-Key`.
  Requests require explicit consent, are honeypot-filtered and per-IP
  throttled, store no IP address or user agent, and expire after 365 days.
  `GET /v1/pilot-requests` and
  `PATCH /v1/pilot-requests/{request_id}` are admin-only, private/no-store
  queue operations. Status is controlled (`new`, `contacted`, `qualified`,
  `declined`, `converted`, or `spam`); the queue does not claim automated
  outreach or CRM synchronization.
- Production Parcel Intelligence manifests may use
  `atomic-publication@v1`: immutable `generations/<id>/` borough/map objects
  plus one stable manifest pointer. New generations also carry a private
  `screening-ledger.jsonl` for source-backed watched-lead exit explanations.
  The ledger excludes addresses, owners, model scores, and user workflow data;
  it is loaded only by the authenticated workflow-alert path. The API validates
  every referenced path, SHA-256, byte length, row count, ledger schema, and
  unique BBL before serving a generation and fails closed on missing, corrupt,
  partial, private-field, or path-injection metadata. Legacy flat
  `published_sweep@v5` objects remain readable during migration and yield
  explicitly unresolved feed-exit alerts.
- Interactive docs (`/docs`, `/redoc`, `/openapi.json`) are off by default. Set `CITYLENS_DOCS_ACCESS_KEY_SHA256` and call with `X-Docs-Key`. The docs key cannot create runs and cannot bypass quotas.
- `CITYLENS_API_KEYS` is deprecated and ignored by auth. The optional admin `X-API-Key` path (internal scripts only) is hash-only: `CITYLENS_ALLOW_ADMIN_API_KEYS=true` + `CITYLENS_ADMIN_API_KEY_HASHES` (SHA-256 of each key).

See [docs/security.md](docs/security.md) for the full credential model.

## Demo Mode

The API exposes unauthenticated demo endpoints:

- `GET /v1/demo/featured`
- `GET /v1/demo/runs/{run_id}`
- `GET /v1/demo/artifacts/{run_id}/{artifact_name}`

These routes are backed by the allowlist under:

- [deploy/demo_runs.json](deploy/demo_runs.json)

`deploy/demo_runs.json` is not a placeholder bundle. It must contain only real,
successful precomputed runs whose artifacts already exist in Firestore + GCS.

The supported publish flow is:

1. Deploy the worker and API.
2. Run `scripts/precompute_demo_runs.py` against the deployed API with an admin API key.
3. Inspect and commit the generated `deploy/demo_runs.json`.
4. Redeploy the API so `GET /v1/demo/featured` serves the updated allowlist.

When the API returns a demo run, its artifact URLs are rewritten to same-origin API
paths like `/v1/demo/artifacts/<run_id>/<artifact_name>`. The browser never needs
direct GCS URLs for demo mode.

## Production verification

The secret-free production verifier exercises the live API, all five parcel
generation objects, and the web route:

```bash
./.venv/bin/python scripts/verify_production.py \
  --output production-verification.json
```

It fails on stale/missing feeds (including every required source SLA),
an unavailable or overdue prospective-validation monitor,
quality-gate regressions, missing or failed
generation-diff evidence, input-feature drift, score-replay mismatch,
unreviewed drift overrides, model-provenance drift,
missing/invalid authoritative ZAP source reconciliation or any directly
matched blocked-BBL leakage, incomplete project-to-BBL coverage, invalid
current-PLUTO or reviewed official-filed-document evidence,
borough or rank gaps, anonymous premium-field exposure, missing gzip,
unavailable Firestore, public workflow access, a missing/misleading parcel
decision audit, missing API/web browser-security headers, framework disclosure,
or a broken Parcel Intelligence page. The verifier also checks
that the public audit metrics match the accepted model metadata and that
anonymous ownership/diligence evidence remains withheld. Public readiness must
remain a limited preview and cannot reveal protected lien, violation, flood,
environmental, MIH, transit, or imagery signals.
[production-smoke.yml](.github/workflows/production-smoke.yml) runs the
same verifier every six hours and on demand, publishes a job summary, and
retains the JSON report for 30 days. Scheduled failures create or update one
deduplicated `[Production] Scheduled verification failing` issue; the next
successful scheduled run records recovery and closes it. Manual checks do not
create or close incidents unless the operator explicitly enables the
`manage_incident` dispatch input. A failure is an incident signal; do not
weaken a contract assertion merely to make the scheduled check green.

## Independent Cloud Monitoring

Google Cloud uptime checks provide an external availability signal that does
not depend on GitHub Actions. The managed contract covers:

- `https://api.citylens.dev/v1/health/ready`: HTTPS, valid TLS, HTTP 200, and
  the content marker `"status":"current"` so an overdue prospective monitor is
  treated as a failure.
- `https://www.citylens.dev/parcel-intel`: HTTPS, valid TLS, HTTP 200, and the
  Parcel Intelligence product heading.
- Six Google probe regions on a five-minute cadence.
- An alert after at least two regions fail, plus a TLS-expiry alert at 15 days.

The configuration command is read-only unless `--apply` is passed:

```bash
./.venv/bin/python scripts/configure_production_monitoring.py \
  --project citylens-001

./.venv/bin/python scripts/configure_production_monitoring.py \
  --project citylens-001 \
  --apply
```

Existing notification channels are preserved. Attach a verified channel
additively by passing its full Cloud Monitoring resource name:

```bash
./.venv/bin/python scripts/configure_production_monitoring.py \
  --project citylens-001 \
  --notification-channel \
  projects/citylens-001/notificationChannels/<CHANNEL_ID> \
  --apply
```

Policies without a notification channel still create incidents in Cloud
Monitoring, but they do not page an operator. Do not attach an unverified
address or webhook merely to make the channel list non-empty.

After applying, prove that Google is actually collecting healthy observations
from multiple locations:

```bash
./.venv/bin/python scripts/verify_production_monitoring.py \
  --project citylens-001
```

The verifier fails if a managed check is missing, fewer than three locations
report within 20 minutes, any latest observation fails, or any observation is
too old. See [docs/deploy_gcp.md](docs/deploy_gcp.md) for required permissions,
channel setup, and rollback.

## Production data recovery

The production recovery baseline protects the state that cannot be rebuilt
from source feeds:

- Firestore database delete protection
- seven-day Firestore point-in-time recovery
- daily Firestore backups retained for 14 days
- Sunday Firestore backups retained for 14 weeks
- enforced private access and at least seven days of soft delete on the GCS
  artifact bucket

The named-database restore drill has a mandatory IAM preflight. Do not restore
while either runtime identity has an unconditional project-level
`roles/datastore.user` binding: it would inherit access to the drill database.
The deployment guide requires tested per-database conditions scoped to
`(default)` before a restore can count as isolated recovery evidence. The
runtime IAM verifier fails closed on missing, unconditional, duplicate, or
wrong-database Firestore bindings and reports the retained condition for each
identity.

The first isolated production restore drill completed successfully on
2026-07-25. See
[the versioned drill evidence](docs/firestore_restore_drill_2026-07-25.md) for
the source backup, operation duration, metadata-only data/index comparison,
runtime 200/403 isolation proof, TTL reapplication, and guarded cleanup.

The configuration command is read-only by default:

```bash
./.venv/bin/python scripts/configure_production_recovery.py \
  --project citylens-001 \
  --bucket citylens-001-artifacts

./.venv/bin/python scripts/configure_production_recovery.py \
  --project citylens-001 \
  --bucket citylens-001-artifacts \
  --apply
```

After the first scheduled backup exists, require a recent restorable backup:

```bash
./.venv/bin/python scripts/verify_production_recovery.py \
  --project citylens-001 \
  --bucket citylens-001-artifacts \
  --location us-central1
```

During the first 26 hours after creating schedules, operators may use
`--allow-collecting` to accept healthy configuration while the first backup is
pending. That flag does not accept configuration drift or an overdue backup.
The daily
[recovery-verification.yml](.github/workflows/recovery-verification.yml)
workflow runs the same check through a repository- and `master`-restricted
keyless identity. It retains the JSON evidence for 90 days and reconciles a
deduplicated GitHub production incident on scheduled failure and recovery.
Its custom Google Cloud role has only seven database, backup, and bucket
metadata permissions; it cannot read Firestore documents or GCS objects.
See [docs/deploy_gcp.md](docs/deploy_gcp.md) for billing, restore-drill, and
post-restore TTL/security requirements.

Verify that the deployed API and worker use the canonical keyless runtime
identities, have no user-managed keys, and that quarantined legacy identities
cannot retain project, bucket, or self-impersonation grants:

```bash
./.venv/bin/python scripts/verify_runtime_iam.py \
  --project citylens-001 \
  --region us-central1 \
  --bucket citylens-001-artifacts
```

The verifier is read-only and fails closed on runtime-account substitution,
missing required roles, redundant worker Firestore viewer access, any
user-managed service-account key, or a re-enabled/re-authorized legacy
identity.

Create the managed production operations dashboard only after previewing and
API-validating the exact definition:

```bash
./.venv/bin/python scripts/configure_production_dashboard.py \
  --project citylens-001

./.venv/bin/python scripts/configure_production_dashboard.py \
  --project citylens-001 \
  --validate-only

./.venv/bin/python scripts/configure_production_dashboard.py \
  --project citylens-001 \
  --apply
```

The dashboard combines incidents, filtered API/worker errors, API request
rate and p95 latency, instance count, worker execution results, Firestore
document operations, and both independent uptime signals. Re-running the
command is idempotent and reports dashboard drift before changing it.

## Product adoption report

After deploying the product-event endpoint and enabling Firestore TTL, operators
can inspect aggregate adoption without exporting user or parcel identifiers:

```bash
./.venv/bin/python scripts/report_product_adoption.py \
  --project citylens-001 \
  --days 30 \
  --output product-adoption-report.json
```

The v6 report contains only window totals, event/source counts, active-user and
active-user-day counts, aggregate canonical workflow and saved-view inventory,
directional parcel-open to decision-audit and workflow-create ratios, separate
decision-audit, underwriting-engagement, activation, and saved-view-reuse
evidence gates, and aggregate pilot-intake plan/status counts. Parcel opens,
decision-audit opens, underwriting opens/first adjustments, and saved-view
applies are best-effort client-side directional counters. Underwriting events
contain no parcel, assumption, range, result, or valuation data. Workflow
lifecycle and saved-view create/update/delete counts are
transactionally derived from their canonical server mutations, so a dropped
follow-up browser request cannot erase a real save and an unchanged retry
cannot inflate the counters. The workflow
[adoption-report.yml](.github/workflows/adoption-report.yml) runs this report
daily through a repository- and branch-restricted keyless Google identity,
publishes warnings while evidence is collecting, and retains the aggregate-only
artifact for 90 days. The inventory query projects only saved-view schema
version and derives user counts from the document parent; it never reads view
names, search text, filters, or owners. The pilot-intake query selects only
status, plan, and creation time, raises a workflow warning when requests await
review, and never reads contact fields, request IDs, boroughs, or workflow
text. Do not publish raw `product_usage_days`, `parcel_workflow`,
`parcel_saved_searches`, or `pilot_requests` documents, and do not use this
report as a model-accuracy or lead-quality claim.

## Pilot intake operations

The public contact form submits to the API with an opaque idempotency key.
Administrators can inspect and advance the private queue without exporting it
to browser analytics:

```bash
curl -sS \
  -H "X-API-Key: ${CITYLENS_ADMIN_API_KEY}" \
  "https://api.citylens.dev/v1/pilot-requests?status=new&limit=100"

curl -sS -X PATCH \
  -H "X-API-Key: ${CITYLENS_ADMIN_API_KEY}" \
  -H "Content-Type: application/json" \
  "https://api.citylens.dev/v1/pilot-requests/<REQUEST_ID>" \
  --data '{"schema_version":"citylens/pilot-request-status@v1","status":"contacted"}'
```

Do not place request bodies in logs or analytics. The collection uses the
`expires_at` field as a 365-day Firestore TTL boundary; convert any active
commercial relationship into its governed customer record before expiry.

### VS Code folder expectations

- Open `citylens-engine/` as its own folder when you want engine-specific Python
  tooling, interpreter selection, or test execution.
- If you keep `/home/josh/citylens` open as the parent folder, use a proper
  multi-root workspace so VS Code does not blur repo boundaries between
  `citylens-core`, `citylens-engine`, and `citylens-web`.
- The engine repo should resolve its interpreter from `citylens-engine/.venv`,
  not from any parent-level environment.

## Fixed Reference Case

The acceptance case for modular parity is:

- `100 E 21st St Brooklyn, NY 11226`

Run the parity harness from the repo root:

```bash
./.venv/bin/python scripts/parity_reference_case.py
```

This writes `parity_report.json` and compares the modular outputs against the
`Urban3D-DeepRecon` reference repo.

See [docs/architecture.md](docs/architecture.md) and [docs/deploy_gcp.md](docs/deploy_gcp.md).

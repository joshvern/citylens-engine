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
  probability. See
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
- Authenticated Parcel Intelligence clients may submit the strict
  `citylens/parcel-product-event@v1` contract to
  `POST /v1/parcel-intel/product-events`. The endpoint accepts only a small
  allowlist of coarse event/source pairs and rejects parcel IDs, addresses,
  owners, URLs, notes, tags, assignees, contacts, and arbitrary properties.
  Firestore stores one aggregate counter document per user/day under
  `product_usage_days`; it does not store event-level records. Counters are
  capped at 1,000 per user/day, rate-limited at the API, and expire after 90
  days through the `expires_at` TTL field. Run
  `scripts/report_product_adoption.py` for an aggregate-only 30-day operator
  report. Its open-to-save ratio is directional product-adoption evidence,
  not model accuracy, unique-parcel conversion, seller intent, or a substitute
  for canonical workflow records.
- Production Parcel Intelligence manifests may use
  `atomic-publication@v1`: immutable `generations/<id>/` borough/map objects
  plus one stable manifest pointer. The API validates the pointer path,
  SHA-256, byte length, and row count before serving a generation and fails
  closed on missing, corrupt, partial, or path-injection metadata. Legacy flat
  `published_sweep@v5` objects remain readable during migration.
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
quality-gate regressions, missing or failed
generation-diff evidence, input-feature drift, score-replay mismatch,
unreviewed drift overrides, model-provenance drift,
missing/invalid authoritative ZAP source reconciliation or any directly
matched blocked-BBL leakage,
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
retains the JSON report for 30 days. A failure is an incident signal; do not
weaken a contract assertion merely to make the scheduled check green.

## Product adoption report

After deploying the product-event endpoint and enabling Firestore TTL, operators
can inspect aggregate adoption without exporting user or parcel identifiers:

```bash
./.venv/bin/python scripts/report_product_adoption.py \
  --project citylens-001 \
  --days 30 \
  --output product-adoption-report.json
```

The report contains only window totals, event/source counts, active-user and
active-user-day counts, and a directional parcel-open to workflow-create ratio.
Do not publish raw `product_usage_days` documents or use this report as a model
accuracy claim.

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

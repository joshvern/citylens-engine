# Architecture

`citylens-engine` is an independently runnable repo inside the shared
`/home/josh/citylens` workspace. It owns the runtime/API layer for the product,
while `citylens-core` owns the canonical pipeline contract and `citylens-web`
owns the browser UI.

## Components

- **API (Cloud Run)**
  - Auth: `Authorization: Bearer <token>` — a Neon Auth (OIDC/JWKS) user JWT, or
    a `clk_live_` user API key resolved against Firestore. An optional admin
    `X-API-Key` surface (hash-only, `CITYLENS_ADMIN_API_KEY_HASHES`) exists for
    internal scripts. See [security.md](security.md).
  - Creates Firestore run docs and triggers a Cloud Run Job execution.
  - Serves the public read endpoints `/v1/demo/*` and
    `/v1/parcel-intel/index`. Parcel Intelligence progressively loads a
    compact `/v1/parcel-intel/map` projection, fetches a full record from
    `/v1/parcel-intel/parcel/{bbl}` only when selected, and reserves
    `/v1/parcel-intel/sweep` for CSV/export and compatibility. Public inventory
    is capped at 25 rows per borough with premium fields stripped;
    authenticated users can load 1,000 per borough. Large JSON responses are
    gzip-compressed. Historical NYC DOF final lien-sale and current DOB
    Safety/OATH/HPD violation fields, NYC Planning MIH overlap, and current MTA
    station-complex proximity are premium diligence context and never exposed
    anonymously. These overlays are post-score and do not modify acquisition
    ranks. MTA distance is straight-line centroid-to-complex context rather
    than entrance-level walking or service-frequency analysis.
  - Selected parcel detail adds a read-time
    `citylens/parcel-decision-audit@v1` object. The API, rather than the
    browser, assigns each explanation to historical model signal,
    deterministic eligibility, current diligence, or source provenance and
    declares whether it can affect model rank or acquisition eligibility.
    Public responses retain methodology and current-gate explanations but
    replace owner and diligence evidence with sign-in gates. User workflow
    evidence remains a separate authenticated contract. A server-derived
    `readiness` block converts those same checks into a conservative next
    diligence state and suggested workflow action without changing the rank or
    making a purchase, seller-intent, or transaction-probability claim.
  - Parcel feed generations are immutable. A stable GCS manifest pointer names
    the active generation and records each object's SHA-256, byte size, and row
    count; readers validate all of these and retain a legacy-flat fallback.
    Generation-keyed caches prevent an in-flight old reader from repopulating a
    new generation's cache.
  - The public index may include one
    `prospective-validation-status@v1` projection produced by the independent
    weekly ranking monitor. The API validates the complete maturity contract,
    requires the status source generation to equal the active feed, rejects
    unknown/private fields, and strips the private GCS object name before
    serialization. A missing or invalid status is explicit `null`; historical
    metrics never substitute for live-cohort evidence.
  - New manifests carry `generation_diff` evidence from the publisher:
    inventory turnover, top-rank retention, rank movement, score PSI, source
    vintages, model identity, cohorts, required-field coverage, full 142-column
    inference drift, and any explicitly reviewed override. Aggregate
    `inference_replay` evidence proves all 5,000 stored scores match the
    reconstructed matrix. The index/sweep contracts expose this
    operational provenance, and the scheduled verifier fails when the report
    is absent, failed, or overridden without a recorded reason.
  - Health: `/v1/health` is the dependency-free keep-warm ping;
    `/v1/health/ready` additionally probes Firestore (503 if unreachable) and
    reports parcel-intel presence/freshness flags. It also reports the
    generation-bound prospective monitor as `current`, `stale`, or
    `unavailable`; the weekly evidence pointer has an eight-day maximum
    observation lag so a stopped monitor cannot silently look current.
  - Authenticated parcel workflow alerts compare each watched lead's saved
    baseline with the current generation. The contract is computed on request,
    user scoped, and never exposed anonymously. It reports decision-relevant
    differences without changing model scores. Generation manifests may also
    reference a private, value-minimized `screening-ledger.jsonl`; when a
    watched lead leaves the 5,000-row published inventory, the API uses that
    same-generation ledger to distinguish an eligible lead below the cutoff
    from a source-backed current-project, completed-project, constraint, or
    incomplete-data exclusion. Those explanations include source dates and
    official links. If the ledger has no row, the API explicitly reports the
    exit as unresolved rather than guessing. Transit alerts require a
    station-complex or access-tier change; distance-only centroid noise is
    ignored.
  - Authenticated parcel workflow actions are also computed on request from
    user-owned records. The server classifies next-action due dates as overdue,
    due today, due soon, scheduled, or unscheduled; flags missing assignees and
    30-day-old records without an outcome; reports workflow-plan, assignee, and
    outcome-review coverage; and excludes terminal records. Reminder snoozes
    are transactional, user scoped, and fingerprinted to the current action,
    due date, assignee, stage, and outcome, so changed commitments cannot stay
    hidden behind stale snoozes. Browser clients do not own these
    classifications or reminder identity.
  - Active, nonterminal workflow records may carry a bounded
    `evidence_reviews` map for six current decision-audit checks. Each entry is
    server-bound to the exact status, source, source date, and feed generation
    the user reviewed. The write endpoint re-reads the current parcel and
    rejects stale optimistic-concurrency inputs; clients therefore surface old
    entries as stale instead of silently carrying them onto new evidence.
    These markers mean “version considered,” never “risk resolved” or
    “diligence cleared,” and are excluded from prospective outcome exports.
    The authenticated workflow-alert response groups stale review markers by
    parcel and compares their exact status/source/as-of/generation identity
    with the current premium decision audit. It includes active reviewed
    records even when `watching=false`, but never archived records. The
    service prioritizes material source/status changes above source-date and
    generation-only changes so routine refreshes do not masquerade as new
    acquisition conclusions.
  - Any non-archived workflow may carry one latest `evidence_issues` request
    per reviewable check. Submission binds a correction or suppression review
    to the current server citation with the same optimistic-concurrency
    identity used by reviews. It writes an opaque, separately indexed
    `parcel_evidence_issues` governance record plus a user-visible workflow
    mirror in one transaction. Open requests cannot overwrite one another,
    withdrawal never deletes the citation, and admin resolution mirrors
    status plus a bounded response without mutating source facts, snapshots,
    scores, ranks, or review markers. The evidence-change center includes
    submitted requests until they are resolved, dismissed, or withdrawn.
  - Aggregate product-adoption evidence is stored as one value-minimized
    `product_usage_days` counter document per user/day. Workflow lifecycle
    and saved-view mutation counters are updated in the same transaction as
    their canonical mutations; parcel opens, comparison opens, decision-audit
    opens, underwriting opens/first adjustments, and saved-view applies remain
    directional client counters. Comparison events contain no parcel IDs or
    compared values. Decision-audit events identify only the posture or tab
    entry point. Underwriting events identify only the Underwrite tab or first
    base-assumption adjustment and never contain a parcel, input value, range,
    or result. A daily keyless GitHub workflow
    combines the retained counters with field-projected aggregate
    `parcel_workflow` and `parcel_saved_searches` inventory. It labels the
    comparison and decision-audit engagement gates `collecting` until at least
    10 opens exist across at least three users. A separate
    comparison-to-workflow handoff gate requires five canonical workflow
    creates across at least three users. Those creates are atomically sourced
    by the bounded comparison-advance mutation and contain no parcel, action,
    due-date, or value data in the aggregate ledger. The underwriting
    engagement gate
    `collecting` until at least 10 opens and five first adjustments exist,
    with each behavior spanning at least three users. The source-bound review
    gate stays `collecting` until at least 10 canonical review markers exist
    across at least three users; its aggregate record contains no parcel,
    check, citation, source date, or review time. The report labels the activation gate
    `collecting` until at least 30 workflow records exist across at least three
    users, and the saved-view-reuse gate `collecting` until at least 10 applies
    exist across at least three users. No user, parcel, saved-view name, search
    text, filter, or owner identifier is emitted, and neither gate is a model
    or lead-quality metric.
  - Public pilot intake is a separate bounded conversion contract. An opaque
    idempotency key produces a non-identifying request ID; explicit consent,
    honeypot filtering, per-IP throttling, field length limits, and a 365-day
    TTL bound the surface. The record intentionally excludes IP address,
    user-agent, referrer, arbitrary metadata, and analytics payloads.
    Listing and status transitions are admin-only and private/no-store.
  - A `lifespan` handler pre-warms the demo + parcel-intel registries; only
    `CitylensRequest` is imported from `citylens-core` (the heavy pipeline import
    is lazy, kept off the API cold-start path — the worker runs the pipeline).
  - Optionally returns signed URLs for artifacts.

- **Worker (Cloud Run Job)**
  - Reads `CITYLENS_RUN_ID`.
  - Resolves address-driven inputs into `orthophoto.tif`, `baseline.tif`,
    `baseline_footprints.geojson`, and `lidar.las` in the run's `work_dir`.
  - Loads run doc, executes `citylens_core.pipeline.run_citylens`.
  - Uploads returned standard artifacts to GCS and writes artifact docs.

## Data

Firestore:
- `users/{app_user_id}`: user record (`plan_type`, `email`, `is_admin`)
- `auth_identities/{sha256(provider:sub)}` → `app_user_id` (OIDC identity map)
- `usage_months/{app_user_id}_{YYYY-MM}`: monthly run-quota counter (transactional)
- `runs/{run_id}`: run status/progress/request
- `runs/{run_id}/artifacts/{artifact_id}`: artifact metadata + GCS URI
- `users/{app_user_id}/product_usage_days/{day}`: expiring aggregate adoption
  counters, with no row-level event or parcel payload
- `users/{app_user_id}/parcel_workflow/{bbl}`: canonical user-owned acquisition
  workflow state, including optional source-bound evidence-review markers and
  latest evidence-issue mirrors;
  reporting reads only aggregate record/user counts and archive state
- `users/{app_user_id}/parcel_saved_searches/{search_id}`: private
  `citylens/parcel-saved-view@v2` explorer state. It restores the citywide
  borough/filter/search/overlay context and is never shared-cacheable. Saved
  views are persistence only; no scheduled-alert delivery is claimed.
  Reporting selects only the schema marker to count current inventory and does
  not read the saved state.
- `pilot_requests/{request_id}`: consented design-partner intake with an
  opaque idempotency-derived ID, controlled operational status, and 365-day
  `expires_at` boundary. Records are private and are not product telemetry.
- `parcel_evidence_issues/{issue_id}`: private, admin-triaged correction and
  suppression-review requests with exact citation identity, opaque submitter
  ID, bounded note, governed status, and 730-day `expires_at`.

GCS:
- `gs://<CITYLENS_BUCKET>/runs/<run_id>/<artifact_filename>`

## Core contract

- Request schema: `citylens_core.models.CitylensRequest`
- Pipeline entrypoint: `citylens_core.pipeline.run_citylens(request, work_dir, progress_cb)`
- Standard artifact filenames written in `work_dir`:
  - `preview.png`
  - `change.geojson`
  - `mesh.ply`
  - `run_summary.json`

Workspace/runtime notes:

- Use the repo-root `uv.lock` for dependency resolution across both `api/` and `worker/`.
- Recreate the root `.venv` with Python 3.11 via `uv sync --all-packages --all-extras`.
- API and worker production builds install the exact `citylens-core` release
  recorded in their manifests and the shared lockfile. `make sync` may overlay
  the sibling checkout only for local development.
- Open this repo directly in VS Code, or use a multi-root workspace that keeps
  `citylens-engine`, `citylens-core`, and `citylens-web` as distinct folders.
- Do not depend on a parent-folder Python environment for engine development.

Fixed parity/reference case:

- `100 E 21st St Brooklyn, NY 11226`
- parity harness: `scripts/parity_reference_case.py`
- reference repo: `../Urban3D-DeepRecon`

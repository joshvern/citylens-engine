"""Parcel Intelligence — tiered read endpoints.

Serves per-borough rankings of redevelopment candidates. The data comes
from the citylens-parcel-intel repo's `scripts/publish_sweep.py`, which
uploads immutable generation-addressed JSONL and commits a stable
`manifest.json` pointer under `gs://<bucket>/parcel-intel/v1/`. Legacy flat
publishes remain readable during the migration.

Access tiers:
- `/v1/parcel-intel/index` is fully public (aggregate metadata only) and
  stays search-indexable.
- `/v1/parcel-intel/sweep` is a public *preview* + authenticated *full
  feed*. Anonymous callers get at most `_ANON_TOP_CAP` rows with premium
  fields stripped (calibration bands, SHAP attributions, change signal,
  owner of record). Any valid credential — Neon Auth JWT, `clk_live_`
  user API key, or admin X-API-Key — unlocks the full feed. Invalid
  credentials 401 rather than silently downgrading.

Caching strategy:
- Process-level: a module-level registry caches parsed JSONL keyed on
  `manifest.artifact_generation` (or legacy `generated_at`). Immutable
  generation paths prevent mixed-feed reads during a publish.
- Edge: anonymous responses keep Cache-Control headers tuned for
  ~10-minute revalidation since the sweep cadence is monthly.
  Authenticated sweep responses are `private, no-store` — the full feed
  must never land in a shared cache.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import ValidationError

from ..models.schemas import (
    ParcelIntelBorough,
    ParcelIntelIndex,
    ParcelIntelMapResponse,
    ParcelIntelMapRow,
    ParcelIntelParcelResponse,
    ParcelIntelRow,
    ParcelIntelSweepResponse,
    ParcelProspectiveValidationHealth,
    ParcelProspectiveValidationStatus,
    ParcelScreeningLedgerRow,
)
from ..services.auth import maybe_parcel_read_auth
from ..services.auth_context import AuthContext
from ..services.gcs_artifacts import GcsArtifacts
from ..services.parcel_decision_audit import build_parcel_decision_audit
from ..services.rate_limit import demo_rate_limit
from ..services.settings import Settings, get_settings

log = logging.getLogger(__name__)

router = APIRouter(tags=["parcel-intel"])

# 10-minute edge cache for anonymous/public responses. Sweep is monthly,
# but a tighter window keeps stale-after-republish blast radius small.
_INDEX_CACHE = "public, s-maxage=600, stale-while-revalidate=300"
_SWEEP_CACHE = "public, s-maxage=600, stale-while-revalidate=300"
# Authenticated sweep responses carry user-tier data — keep them out of
# shared caches entirely.
_SWEEP_CACHE_AUTHED = "private, no-store"
_MAP_CACHE = "public, s-maxage=600, stale-while-revalidate=300"
_MAP_CACHE_AUTHED = "private, no-store"

# Anonymous preview cap: unauthenticated callers get at most this many
# rows per borough (silently clamped, not an error).
_ANON_TOP_CAP = 25

# Data older than this is flagged stale on the index (the sweep cadence
# is monthly; 45 days means a missed retrain/publish cycle).
_STALE_THRESHOLD_DAYS = 45.0

# Competing-project evidence is operationally different from quarterly parcel
# facts. A newly filed DOB or ZAP project can make outreach inappropriate
# within days, so the API tightens older published manifests to this cap at
# request time instead of trusting a stale-at-publish boolean for weeks.
_SOURCE_MAX_AGE_CAP_DAYS: dict[str, float] = {
    "project_activity": 8.0,
    "land_use_activity": 8.0,
}

# Five recognized boroughs. Reject anything else with 404 to avoid
# fishing the bucket for arbitrary keys.
_BOROUGH_SLUGS: frozenset[str] = frozenset(
    ("manhattan", "brooklyn", "queens", "bronx", "staten_island")
)
_BBL_BOROUGH = {
    "1": "manhattan",
    "2": "bronx",
    "3": "brooklyn",
    "4": "queens",
    "5": "staten_island",
}

_GCS_PREFIX = "parcel-intel/v1"
_PROSPECTIVE_STATUS_OBJECT = (
    f"{_GCS_PREFIX}/prospective-validation.json"
)
_ATOMIC_PUBLICATION_SCHEMA = "citylens-parcel-intel/atomic-publication@v1"
_GENERATION_RE = re.compile(
    r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PROSPECTIVE_MAX_OBSERVATION_LAG_DAYS = 8
_SCREENING_LEDGER_SCHEMA = (
    "citylens-parcel-intel/screening-ledger@v1"
)
_SCREENING_LEDGER_FIELDS = (
    "bbl",
    "borough",
    "model_rank",
    "acquisition_rank",
    "acquisition_eligible",
    "acquisition_status",
    "acquisition_exclusion_reasons",
    "published",
    "latest_project_filing_year",
    "latest_project_status",
    "latest_project_type",
    "latest_project_job_number",
    "latest_project_url",
    "property_facts_as_of",
    "ownership_as_of",
    "project_activity_as_of",
    "land_use_activity_as_of",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_gcs(settings: Settings = Depends(get_settings)) -> GcsArtifacts:
    return GcsArtifacts(bucket=settings.bucket)


class ParcelIntelRegistry:
    """Process-level cache of parsed parcel-intel JSONL + manifest.

    Threadsafe via a single lock — cold-fill races just redundantly
    fetch from GCS, no correctness impact. Invalidation key is
    ``manifest.artifact_generation`` (falling back to `generated_at` for
    legacy feeds): when the publisher commits a new pointer, we lazily reload
    that generation on next access.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._manifest: dict[str, Any] | None = None
        self._manifest_cache_key: str | None = None
        self._rows_by_borough: dict[tuple[str, str], list[dict]] = {}
        self._map_rows: dict[str, list[ParcelIntelMapRow]] = {}
        self._screening_rows: dict[
            str, dict[str, ParcelScreeningLedgerRow]
        ] = {}

    def _gcs_object_name(self, leaf: str) -> str:
        return f"{_GCS_PREFIX}/{leaf}"

    def _cache_key(self, manifest: dict[str, Any]) -> str:
        generation = manifest.get("artifact_generation")
        if (
            manifest.get("artifact_prefix") is not None
            and isinstance(generation, str)
            and generation
        ):
            return f"generation:{generation}"
        generated_at = manifest.get("generated_at")
        return f"legacy:{generated_at}" if isinstance(generated_at, str) else "legacy:"

    def _atomic_artifact_metadata(
        self, manifest: dict[str, Any], leaf: str
    ) -> dict[str, Any] | None:
        prefix = manifest.get("artifact_prefix")
        if prefix is None:
            return None
        generation = manifest.get("artifact_generation")
        expected_prefix = (
            f"{_GCS_PREFIX}/generations/{generation}"
            if isinstance(generation, str)
            else None
        )
        artifacts = manifest.get("artifacts")
        metadata = artifacts.get(leaf) if isinstance(artifacts, dict) else None
        valid = (
            manifest.get("publication_schema") == _ATOMIC_PUBLICATION_SCHEMA
            and isinstance(generation, str)
            and _GENERATION_RE.fullmatch(generation) is not None
            and prefix == expected_prefix
            and isinstance(metadata, dict)
            and metadata.get("object_name") == f"{prefix}/{leaf}"
            and isinstance(metadata.get("sha256"), str)
            and _SHA256_RE.fullmatch(metadata["sha256"]) is not None
            and isinstance(metadata.get("size_bytes"), int)
            and metadata["size_bytes"] >= 0
            and isinstance(metadata.get("row_count"), int)
            and metadata["row_count"] >= 0
        )
        if not valid:
            log.error(
                "parcel-intel atomic manifest metadata is invalid for %s",
                leaf,
            )
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence manifest is invalid",
            )
        return metadata

    def _validate_publication_manifest(
        self, manifest: dict[str, Any]
    ) -> None:
        atomic_keys = {
            "publication_schema",
            "artifact_generation",
            "artifact_prefix",
            "artifacts",
        }
        present = atomic_keys.intersection(manifest)
        if not present:
            return
        if present != atomic_keys:
            log.error(
                "parcel-intel manifest has partial atomic metadata: %s",
                sorted(present),
            )
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence manifest is invalid",
            )
        leaves = ["map.jsonl"]
        for borough in manifest.get("boroughs") or []:
            if isinstance(borough, dict) and borough.get("slug") in _BOROUGH_SLUGS:
                leaves.append(f"{borough['slug']}.jsonl")
        for leaf in leaves:
            self._atomic_artifact_metadata(manifest, leaf)
        screening_keys = {
            "screening_ledger_schema",
            "screening_ledger_fields",
            "screening_ledger_row_count",
        }
        screening_present = screening_keys.intersection(manifest)
        artifacts = manifest.get("artifacts")
        ledger_artifact_present = (
            isinstance(artifacts, dict)
            and "screening-ledger.jsonl" in artifacts
        )
        if screening_present or ledger_artifact_present:
            if screening_present != screening_keys:
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence manifest is invalid",
                )
            screening_schema = manifest.get("screening_ledger_schema")
            screening_rows = manifest.get("screening_ledger_row_count")
            if (
                screening_schema != _SCREENING_LEDGER_SCHEMA
                or manifest.get("screening_ledger_fields")
                != list(_SCREENING_LEDGER_FIELDS)
                or isinstance(screening_rows, bool)
                or not isinstance(screening_rows, int)
                or screening_rows < 1
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence manifest is invalid",
                )
            metadata = self._atomic_artifact_metadata(
                manifest,
                "screening-ledger.jsonl",
            )
            if (
                metadata is None
                or metadata.get("row_count") != screening_rows
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence manifest is invalid",
                )

    def _download_artifact(
        self,
        gcs: GcsArtifacts,
        manifest: dict[str, Any],
        leaf: str,
    ) -> tuple[bytes, int | None]:
        metadata = self._atomic_artifact_metadata(manifest, leaf)
        object_name = (
            metadata["object_name"]
            if metadata is not None
            else self._gcs_object_name(leaf)
        )
        payload, _ = gcs.download_bytes(object_name=object_name)
        if metadata is None:
            return payload, None
        actual_sha = hashlib.sha256(payload).hexdigest()
        if (
            len(payload) != metadata["size_bytes"]
            or actual_sha != metadata["sha256"]
        ):
            log.error(
                "parcel-intel artifact integrity mismatch: object=%s "
                "expected_size=%s actual_size=%s expected_sha=%s actual_sha=%s",
                object_name,
                metadata["size_bytes"],
                len(payload),
                metadata["sha256"],
                actual_sha,
            )
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence artifact integrity check failed",
            )
        return payload, metadata["row_count"]

    def _refresh_manifest(self, gcs: GcsArtifacts) -> dict[str, Any]:
        try:
            payload, _ = gcs.download_bytes(object_name=self._gcs_object_name("manifest.json"))
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence data has not been published yet.",
            ) from exc
        try:
            manifest = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("parcel-intel manifest.json is not valid JSON: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence manifest is invalid",
            ) from exc
        if not isinstance(manifest, dict):
            log.warning(
                "parcel-intel manifest.json parsed to %s, expected object",
                type(manifest).__name__,
            )
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence manifest is invalid",
            )
        self._validate_publication_manifest(manifest)
        with self._lock:
            new_key = self._cache_key(manifest)
            if new_key != self._manifest_cache_key:
                # Drop borough caches; they'll lazy-reload on next read.
                self._rows_by_borough = {}
                self._map_rows = {}
                self._screening_rows = {}
                self._manifest_cache_key = new_key
            self._manifest = manifest
        return manifest

    def index(self, gcs: GcsArtifacts) -> ParcelIntelIndex:
        manifest = self._refresh_manifest(gcs)
        now = _utc_now()
        prospective_validation = self._prospective_validation(
            gcs,
            manifest,
            now=now,
        )
        prospective_validation_health = (
            self._prospective_validation_health(
                prospective_validation,
                now=now,
            )
        )
        boroughs = [
            ParcelIntelBorough(
                slug=b["slug"],
                display_name=b["display_name"],
                count=int(b.get("count") or 0),
                top_score=b.get("top_score"),
            )
            for b in manifest.get("boroughs") or []
            if b.get("slug") in _BOROUGH_SLUGS
        ]
        generated_at = _parse_iso(manifest.get("generated_at"))
        age_days = _age_days(generated_at)
        stale = age_days is not None and age_days > _STALE_THRESHOLD_DAYS
        if stale:
            log.warning(
                "parcel-intel data is stale: generated_at=%s (%.1f days old, "
                "threshold %.0f days) — re-run the publisher",
                manifest.get("generated_at"),
                age_days,
                _STALE_THRESHOLD_DAYS,
            )
        return ParcelIntelIndex(
            boroughs=boroughs,
            generated_at=generated_at,
            feed_generation=manifest.get("artifact_generation"),
            model_metadata=manifest.get("model_metadata") or {},
            data_sources=_live_source_statuses(
                manifest.get("data_sources"),
                now=now,
            ),
            quality_gate=manifest.get("quality_gate") or {},
            generation_diff=manifest.get("generation_diff") or {},
            inference_replay=manifest.get("inference_replay") or {},
            prospective_validation=prospective_validation,
            prospective_validation_health=prospective_validation_health,
            age_days=age_days,
            stale=stale,
        )

    def _prospective_validation(
        self,
        gcs: GcsArtifacts,
        manifest: dict[str, Any],
        *,
        now: datetime,
    ) -> ParcelProspectiveValidationStatus | None:
        """Load only a strict, parcel-free status for the active generation."""
        active_generation = manifest.get("artifact_generation")
        if not isinstance(active_generation, str):
            return None
        try:
            body, content_type = gcs.download_bytes(
                object_name=_PROSPECTIVE_STATUS_OBJECT
            )
        except FileNotFoundError:
            log.warning(
                "parcel-intel prospective validation status is not published"
            )
            return None
        if content_type not in {None, "application/json"}:
            log.error(
                "parcel-intel prospective validation has invalid content type: %s",
                content_type,
            )
            return None
        try:
            payload = json.loads(body)
            status = ParcelProspectiveValidationStatus.model_validate(payload)
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            ValidationError,
        ) as exc:
            log.error(
                "parcel-intel prospective validation is invalid: %s",
                exc,
            )
            return None
        if status.source_generation != active_generation:
            log.warning(
                "parcel-intel prospective validation is for generation %s, "
                "but active feed is %s",
                status.source_generation,
                active_generation,
            )
            return None
        if (
            status.issued_at > now + timedelta(minutes=5)
            or status.observed_through > now.date()
            or any(
                source.rows_updated_at > now + timedelta(minutes=5)
                for source in status.official_sources
            )
        ):
            log.error(
                "parcel-intel prospective validation contains future source "
                "evidence"
            )
            return None
        return status

    def _prospective_validation_health(
        self,
        status: ParcelProspectiveValidationStatus | None,
        *,
        now: datetime,
    ) -> ParcelProspectiveValidationHealth:
        if status is None:
            return ParcelProspectiveValidationHealth(
                status="unavailable",
                reason="status_missing_or_invalid",
            )
        observation_lag_days = (
            now.date() - status.observed_through
        ).days
        is_stale = (
            observation_lag_days
            > _PROSPECTIVE_MAX_OBSERVATION_LAG_DAYS
        )
        return ParcelProspectiveValidationHealth(
            status="stale" if is_stale else "current",
            reason=(
                "observation_lag_exceeded" if is_stale else "current"
            ),
            observation_lag_days=observation_lag_days,
            max_observation_lag_days=(
                _PROSPECTIVE_MAX_OBSERVATION_LAG_DAYS
            ),
            next_monitor_due_on=(
                status.observed_through
                + timedelta(
                    days=_PROSPECTIVE_MAX_OBSERVATION_LAG_DAYS
                )
            ),
            oldest_official_source_updated_at=min(
                source.rows_updated_at
                for source in status.official_sources
            ),
        )

    def borough(
        self, gcs: GcsArtifacts, slug: str
    ) -> tuple[list[ParcelIntelRow], dict[str, Any] | None]:
        if slug not in _BOROUGH_SLUGS:
            raise HTTPException(status_code=404, detail="Unknown borough")
        manifest = self._refresh_manifest(gcs)
        cache_key = self._cache_key(manifest)
        cache_id = (cache_key, slug)

        with self._lock:
            cached = self._rows_by_borough.get(cache_id)
        if cached is None:
            try:
                payload, expected_rows = self._download_artifact(
                    gcs, manifest, f"{slug}.jsonl"
                )
            except FileNotFoundError as exc:
                if self._atomic_artifact_metadata(
                    manifest, f"{slug}.jsonl"
                ) is not None:
                    raise HTTPException(
                        status_code=503,
                        detail="Parcel intelligence referenced artifact is missing",
                    ) from exc
                raise HTTPException(
                    status_code=404, detail=f"No data published for {slug}"
                ) from exc
            cached = []
            bad_lines = 0
            for line in payload.decode("utf-8", errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                if not isinstance(parsed, dict):
                    bad_lines += 1
                    continue
                cached.append(parsed)
            if bad_lines:
                log.warning(
                    "parcel-intel %s.jsonl: skipped %d unparseable line(s); "
                    "serving the remaining %d",
                    slug,
                    bad_lines,
                    len(cached),
                )
            if expected_rows is not None and (
                bad_lines or len(cached) != expected_rows
            ):
                log.error(
                    "parcel-intel atomic artifact row-count mismatch: "
                    "slug=%s expected=%d parsed=%d bad_lines=%d",
                    slug,
                    expected_rows,
                    len(cached),
                    bad_lines,
                )
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence artifact row-count check failed",
                )
            with self._lock:
                self._rows_by_borough[cache_id] = cached

        rows: list[ParcelIntelRow] = []
        bad_rows = 0
        for r in cached:
            try:
                rows.append(ParcelIntelRow(**r))
            except ValidationError as exc:
                bad_rows += 1
                log.warning(
                    "parcel-intel %s: skipping row that failed validation (bbl=%s): %s",
                    slug,
                    r.get("bbl"),
                    exc,
                )
        if bad_rows:
            if self._atomic_artifact_metadata(
                manifest, f"{slug}.jsonl"
            ) is not None:
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence artifact schema check failed",
                )
            log.warning(
                "parcel-intel %s: %d row(s) failed schema validation and were "
                "skipped; serving the remaining %d",
                slug,
                bad_rows,
                len(rows),
            )
        return rows, manifest

    def citywide_map(
        self, gcs: GcsArtifacts
    ) -> tuple[list[ParcelIntelMapRow], dict[str, Any] | None]:
        """Load the compact citywide explorer artifact."""
        manifest = self._refresh_manifest(gcs)
        cache_key = self._cache_key(manifest)
        with self._lock:
            cached = self._map_rows.get(cache_key)
        if cached is None:
            try:
                payload, expected_rows = self._download_artifact(
                    gcs, manifest, "map.jsonl"
                )
            except FileNotFoundError as exc:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Compact parcel map has not been published yet; "
                        "re-run the parcel publisher."
                    ),
                ) from exc
            validated: list[ParcelIntelMapRow] = []
            bad_lines = 0
            for line in payload.decode("utf-8", errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                if not isinstance(parsed, dict):
                    bad_lines += 1
                    continue
                try:
                    validated.append(ParcelIntelMapRow(**parsed))
                except ValidationError as exc:
                    bad_lines += 1
                    log.warning(
                        "parcel-intel map: skipping invalid row (bbl=%s): %s",
                        parsed.get("bbl"),
                        exc,
                    )
            if bad_lines:
                log.warning(
                    "parcel-intel map.jsonl: skipped %d invalid line(s)",
                    bad_lines,
                )
            if expected_rows is not None and (
                bad_lines or len(validated) != expected_rows
            ):
                log.error(
                    "parcel-intel atomic map row-count mismatch: "
                    "expected=%d parsed=%d bad_lines=%d",
                    expected_rows,
                    len(validated),
                    bad_lines,
                )
                raise HTTPException(
                    status_code=503,
                    detail="Parcel intelligence artifact row-count check failed",
                )
            with self._lock:
                self._map_rows[cache_key] = validated
            cached = validated

        return cached, manifest

    def screening_ledger(
        self,
        gcs: GcsArtifacts,
        *,
        manifest: dict[str, Any] | None = None,
    ) -> tuple[dict[str, ParcelScreeningLedgerRow], dict[str, Any]]:
        """Load the private current-screening ledger for watched-lead status."""

        manifest = manifest or self._refresh_manifest(gcs)
        if manifest.get("screening_ledger_schema") is None:
            return {}, manifest
        cache_key = self._cache_key(manifest)
        with self._lock:
            cached = self._screening_rows.get(cache_key)
        if cached is None:
            try:
                payload, expected_rows = self._download_artifact(
                    gcs,
                    manifest,
                    "screening-ledger.jsonl",
                )
            except FileNotFoundError as exc:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Parcel intelligence screening ledger is missing"
                    ),
                ) from exc
            parsed_rows: dict[str, ParcelScreeningLedgerRow] = {}
            bad_lines = 0
            for line in payload.decode(
                "utf-8", errors="replace"
            ).splitlines():
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    row = ParcelScreeningLedgerRow.model_validate(raw)
                except (json.JSONDecodeError, ValidationError):
                    bad_lines += 1
                    continue
                if row.bbl in parsed_rows:
                    bad_lines += 1
                    continue
                parsed_rows[row.bbl] = row
            if (
                bad_lines
                or expected_rows is None
                or len(parsed_rows) != expected_rows
            ):
                log.error(
                    "parcel-intel screening ledger validation failed: "
                    "expected=%s parsed=%d bad_lines=%d",
                    expected_rows,
                    len(parsed_rows),
                    bad_lines,
                )
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Parcel intelligence screening ledger validation "
                        "failed"
                    ),
                )
            with self._lock:
                self._screening_rows[cache_key] = parsed_rows
            cached = parsed_rows
        return cached, manifest

    def parcel(
        self, gcs: GcsArtifacts, bbl: str
    ) -> tuple[ParcelIntelRow, dict[str, Any] | None]:
        """Resolve a full parcel record from its BBL."""
        slug = _BBL_BOROUGH.get(bbl[:1])
        if len(bbl) != 10 or not bbl.isdigit() or slug is None:
            raise HTTPException(status_code=404, detail="Unknown parcel")
        rows, manifest = self.borough(gcs, slug)
        for row in rows:
            if row.bbl == bbl:
                return row, manifest
        raise HTTPException(status_code=404, detail="Parcel not found")


_REGISTRY = ParcelIntelRegistry()


def get_registry() -> ParcelIntelRegistry:
    return _REGISTRY


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        # Python 3.11 fromisoformat handles offsets like +00:00.
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _age_days(generated_at: datetime | None) -> float | None:
    """Age of the publish in days (1-decimal), or None when unknown."""
    if generated_at is None:
        return None
    if generated_at.tzinfo is None:
        # Publisher timestamps are UTC; treat naive values as such.
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - generated_at
    return round(max(delta.total_seconds(), 0.0) / 86400.0, 1)


def _live_source_statuses(
    value: Any,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Recompute source ages and critical caps when the index is requested.

    Manifest ``age_days`` and ``stale`` values are publish-time observations.
    Recomputing them prevents a cached monthly feed from claiming that a source
    is still current after its operating window has elapsed.
    """
    if not isinstance(value, dict):
        return {}
    out: dict[str, Any] = {}
    for key, raw in value.items():
        if not isinstance(raw, dict):
            out[key] = raw
            continue
        status = dict(raw)
        retrieved_at = _parse_iso(raw.get("retrieved_at"))
        if retrieved_at is not None and retrieved_at.tzinfo is None:
            retrieved_at = retrieved_at.replace(tzinfo=timezone.utc)

        published_max_age = raw.get("max_age_days")
        valid_published_max_age = (
            float(published_max_age)
            if isinstance(published_max_age, (int, float))
            and not isinstance(published_max_age, bool)
            and published_max_age > 0
            else None
        )
        cap = _SOURCE_MAX_AGE_CAP_DAYS.get(str(key))
        max_age = (
            min(valid_published_max_age, cap)
            if valid_published_max_age is not None and cap is not None
            else valid_published_max_age or cap
        )
        # Some manifest entries are provenance-only metadata rather than
        # refreshable operational inputs. The CityLens imagery baseline, for
        # example, records the historical comparison year but has no retrieval
        # timestamp or freshness SLA. Treating that absence as a stale source
        # produces a false acquisition warning in the product.
        if retrieved_at is None and max_age is None:
            status["age_days"] = None
            status["max_age_days"] = None
            status["stale"] = False
            status["freshness_status"] = "not_applicable"
            out[str(key)] = status
            continue
        age_days = (
            max((now - retrieved_at).total_seconds(), 0.0) / 86400.0
            if retrieved_at is not None
            else None
        )
        future = retrieved_at is not None and retrieved_at > now
        stale = (
            future
            or age_days is None
            or max_age is None
            or age_days > max_age
        )

        status["age_days"] = (
            round(age_days, 2) if age_days is not None else None
        )
        status["max_age_days"] = max_age
        status["stale"] = stale
        if (
            valid_published_max_age is not None
            and max_age is not None
            and valid_published_max_age != max_age
        ):
            status["published_max_age_days"] = valid_published_max_age
        out[str(key)] = status
    return out


# Premium fields never leave the API on anonymous responses. Keep in sync
# with ParcelIntelRow in models/schemas.py.
_ANON_STRIPPED_FIELDS: dict[str, Any] = {
    "score_calibrated_p10": None,
    "score_calibrated_p90": None,
    "top_features": [],
    "change_added_count": 0,
    "change_demolished_count": 0,
    "change_modified_count": 0,
    "change_latest_imagery_year": None,
    "observed_imagery_year": None,
    "recent_change": False,
    "owner_name": None,
    "owner_name_source": None,
    "owner_type": None,
    "owner_entity_type": None,
    "owner_portfolio_id": None,
    "owner_portfolio_match_method": None,
    "owner_portfolio_lot_count": None,
    "owner_portfolio_borough_count": None,
    "owner_portfolio_total_lot_area_sqft": None,
    "owner_portfolio_candidate_count": None,
    "owner_portfolio_data_as_of": None,
    "tax_lien_sale_date": None,
    "tax_lien_sale_year": None,
    "tax_lien_water_debt_only": None,
    "tax_lien_data_as_of": None,
    "dob_safety_active_count": 0,
    "dob_safety_latest_issue_date": None,
    "ecb_active_count": 0,
    "ecb_class_1_count": 0,
    "ecb_balance_due": 0.0,
    "ecb_latest_issue_date": None,
    "hpd_open_count": 0,
    "hpd_class_c_count": 0,
    "hpd_latest_inspection_date": None,
    "critical_violation_count": None,
    "violation_data_as_of": None,
    "firm07_floodplain": None,
    "pfirm15_floodplain": None,
    "floodplain_1pct": None,
    "floodplain_data_as_of": None,
    "environmental_review_required": None,
    "environmental_designation_number": None,
    "environmental_designation_kind": None,
    "environmental_designation_data_as_of": None,
    "mandatory_inclusionary_housing": None,
    "mih_options": None,
    "mih_area_count": None,
    "mih_data_as_of": None,
    "nearest_transit_complex_id": None,
    "nearest_transit_station_name": None,
    "nearest_transit_station_distance_m": None,
    "nearest_transit_routes": None,
    "nearest_transit_ada_status": None,
    "transit_station_count_400m": None,
    "transit_station_count_800m": None,
    "transit_access_tier": None,
    "transit_data_as_of": None,
    "assemblage_id": None,
    "assemblage_lot_count": None,
    "assemblage_combined_lot_area_sqft": None,
    "assemblage_combined_buildable_sqft": None,
    "assemblage_member_bbls": [],
}


def _strip_premium_fields(row: ParcelIntelRow) -> ParcelIntelRow:
    return row.model_copy(update=dict(_ANON_STRIPPED_FIELDS))


def _strip_map_premium_fields(row: ParcelIntelMapRow) -> ParcelIntelMapRow:
    return row.model_copy(
        update={
            "owner_name": None,
            "owner_entity_type": None,
            "owner_portfolio_id": None,
            "owner_portfolio_lot_count": None,
            "owner_portfolio_borough_count": None,
            "owner_portfolio_candidate_count": None,
            "recent_change": False,
            "tax_lien_sale_year": None,
            "critical_violation_count": None,
            "floodplain_1pct": None,
            "environmental_review_required": None,
            "mandatory_inclusionary_housing": None,
            "nearest_transit_station_name": None,
            "nearest_transit_station_distance_m": None,
            "nearest_transit_routes": None,
            "nearest_transit_ada_status": None,
            "transit_station_count_800m": None,
            "transit_access_tier": None,
        }
    )


@router.get("/parcel-intel/index", response_model=ParcelIntelIndex)
def parcel_intel_index(
    response: Response,
    _rate_limit: None = Depends(demo_rate_limit),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> ParcelIntelIndex:
    out = registry.index(gcs)
    response.headers["Cache-Control"] = _INDEX_CACHE
    return out


@router.get("/parcel-intel/map", response_model=ParcelIntelMapResponse)
def parcel_intel_map(
    response: Response,
    top_per_borough: int = Query(
        1000,
        ge=1,
        le=1000,
        description=(
            "Maximum rows per borough. Unauthenticated requests are silently "
            f"capped at {_ANON_TOP_CAP}."
        ),
    ),
    auth: Optional[AuthContext] = Depends(maybe_parcel_read_auth),
    _rate_limit: None = Depends(demo_rate_limit),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> ParcelIntelMapResponse:
    rows, manifest = registry.citywide_map(gcs)
    cap = top_per_borough if auth is not None else min(
        top_per_borough, _ANON_TOP_CAP
    )
    counts: dict[str, int] = {}
    selected: list[ParcelIntelMapRow] = []
    for row in rows:
        count = counts.get(row.borough, 0)
        if count >= cap:
            continue
        counts[row.borough] = count + 1
        selected.append(
            row if auth is not None else _strip_map_premium_fields(row)
        )
    response.headers["Cache-Control"] = (
        _MAP_CACHE_AUTHED if auth is not None else _MAP_CACHE
    )
    # The same endpoint has a cacheable public representation and a private
    # authenticated representation. Without this variance declaration, a
    # browser or intermediary can reuse the 25-per-borough preview after the
    # user signs in and make 125 rows look like the full inventory.
    response.headers["Vary"] = (
        "Authorization, X-API-Key, X-CityLens-Parcel-Smoke-Key"
    )
    response.headers["X-CityLens-Inventory-Scope"] = (
        "authenticated_full" if auth is not None else "public_preview"
    )
    response.headers["X-CityLens-Inventory-Count"] = str(len(selected))
    response.headers["X-CityLens-Inventory-Available"] = str(len(rows))
    return ParcelIntelMapResponse(
        rows=selected,
        generated_at=_parse_iso((manifest or {}).get("generated_at")),
        access_scope=(
            "authenticated_full" if auth is not None else "public_preview"
        ),
        requested_top_per_borough=top_per_borough,
        returned_count=len(selected),
        available_count=len(rows),
        inventory_complete=len(selected) == len(rows),
    )


@router.get(
    "/parcel-intel/parcel/{bbl}",
    response_model=ParcelIntelParcelResponse,
)
def parcel_intel_parcel(
    bbl: str,
    response: Response,
    auth: Optional[AuthContext] = Depends(maybe_parcel_read_auth),
    _rate_limit: None = Depends(demo_rate_limit),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> ParcelIntelParcelResponse:
    row, manifest = registry.parcel(gcs, bbl)
    if auth is None:
        rank = row.acquisition_rank
        if not isinstance(rank, int) or rank > _ANON_TOP_CAP:
            raise HTTPException(status_code=404, detail="Parcel not found")
        response.headers["Cache-Control"] = _SWEEP_CACHE
        served_row = _strip_premium_fields(row)
    else:
        response.headers["Cache-Control"] = _SWEEP_CACHE_AUTHED
        served_row = row
    response.headers["Vary"] = (
        "Authorization, X-API-Key, X-CityLens-Parcel-Smoke-Key"
    )
    return ParcelIntelParcelResponse(
        **served_row.model_dump(),
        decision_audit=build_parcel_decision_audit(
            served_row,
            manifest,
            premium_access=auth is not None,
        ),
    )


@router.get("/parcel-intel/sweep", response_model=ParcelIntelSweepResponse)
def parcel_intel_sweep(
    response: Response,
    borough: str = Query(..., description="One of manhattan/brooklyn/queens/bronx/staten_island"),
    top: int = Query(
        20,
        ge=1,
        le=1000,
        description=(
            "How many rows to return (1-1000). Unauthenticated requests are "
            f"silently capped at {_ANON_TOP_CAP}."
        ),
    ),
    auth: Optional[AuthContext] = Depends(maybe_parcel_read_auth),
    _rate_limit: None = Depends(demo_rate_limit),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> ParcelIntelSweepResponse:
    rows, manifest = registry.borough(gcs, borough)

    if auth is None:
        # Anonymous preview tier: clamp row count + strip premium fields.
        rows = [_strip_premium_fields(r) for r in rows[: min(top, _ANON_TOP_CAP)]]
        response.headers["Cache-Control"] = _SWEEP_CACHE
    else:
        rows = rows[:top]
        response.headers["Cache-Control"] = _SWEEP_CACHE_AUTHED
    response.headers["Vary"] = (
        "Authorization, X-API-Key, X-CityLens-Parcel-Smoke-Key"
    )

    generated_at = _parse_iso((manifest or {}).get("generated_at"))
    return ParcelIntelSweepResponse(
        borough=borough,
        rows=rows,
        generated_at=generated_at,
        model_metadata=(manifest or {}).get("model_metadata") or {},
        data_sources=(manifest or {}).get("data_sources") or {},
        quality_gate=(manifest or {}).get("quality_gate") or {},
        generation_diff=(manifest or {}).get("generation_diff") or {},
        inference_replay=(manifest or {}).get("inference_replay") or {},
    )

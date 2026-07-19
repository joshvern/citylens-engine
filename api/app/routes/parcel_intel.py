"""Parcel Intelligence — tiered read endpoints.

Serves per-borough rankings of redevelopment candidates. The data comes
from the citylens-parcel-intel repo's `scripts/publish_sweep.py`, which
uploads trimmed JSONL + a manifest.json to
`gs://<bucket>/parcel-intel/v1/`. We read those bytes through the
existing `GcsArtifacts` plumbing — no new GCS code needed.

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
  manifest.generated_at. Cheap re-loads on each Cloud Run instance
  (~80 KB per borough). Invalidated automatically when the publisher
  re-uploads.
- Edge: anonymous responses keep Cache-Control headers tuned for
  ~10-minute revalidation since the sweep cadence is monthly.
  Authenticated sweep responses are `private, no-store` — the full feed
  must never land in a shared cache.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import ValidationError

from ..models.schemas import (
    ParcelIntelBorough,
    ParcelIntelIndex,
    ParcelIntelRow,
    ParcelIntelSweepResponse,
)
from ..services.auth import maybe_auth
from ..services.auth_context import AuthContext
from ..services.gcs_artifacts import GcsArtifacts
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

# Anonymous preview cap: unauthenticated callers get at most this many
# rows per borough (silently clamped, not an error).
_ANON_TOP_CAP = 25

# Data older than this is flagged stale on the index (the sweep cadence
# is monthly; 45 days means a missed retrain/publish cycle).
_STALE_THRESHOLD_DAYS = 45.0

# Five recognized boroughs. Reject anything else with 404 to avoid
# fishing the bucket for arbitrary keys.
_BOROUGH_SLUGS: frozenset[str] = frozenset(
    ("manhattan", "brooklyn", "queens", "bronx", "staten_island")
)

_GCS_PREFIX = "parcel-intel/v1"


def get_gcs(settings: Settings = Depends(get_settings)) -> GcsArtifacts:
    return GcsArtifacts(bucket=settings.bucket)


class ParcelIntelRegistry:
    """Process-level cache of parsed parcel-intel JSONL + manifest.

    Threadsafe via a single lock — cold-fill races just redundantly
    fetch from GCS, no correctness impact. Invalidation key is
    ``manifest.generated_at``: when the publisher re-uploads, the
    timestamp changes and we lazily reload that borough on next access.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._manifest: dict[str, Any] | None = None
        self._manifest_generated_at: str | None = None
        self._rows_by_borough: dict[str, list[dict]] = {}

    def _gcs_object_name(self, leaf: str) -> str:
        return f"{_GCS_PREFIX}/{leaf}"

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
        with self._lock:
            new_at = manifest.get("generated_at")
            if new_at != self._manifest_generated_at:
                # Drop borough caches; they'll lazy-reload on next read.
                self._rows_by_borough = {}
                self._manifest_generated_at = new_at
            self._manifest = manifest
        return manifest

    def index(self, gcs: GcsArtifacts) -> ParcelIntelIndex:
        manifest = self._refresh_manifest(gcs)
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
            model_metadata=manifest.get("model_metadata") or {},
            data_sources=manifest.get("data_sources") or {},
            quality_gate=manifest.get("quality_gate") or {},
            age_days=age_days,
            stale=stale,
        )

    def borough(
        self, gcs: GcsArtifacts, slug: str
    ) -> tuple[list[ParcelIntelRow], dict[str, Any] | None]:
        if slug not in _BOROUGH_SLUGS:
            raise HTTPException(status_code=404, detail="Unknown borough")
        manifest = self._refresh_manifest(gcs)

        with self._lock:
            cached = self._rows_by_borough.get(slug)
        if cached is None:
            try:
                payload, _ = gcs.download_bytes(object_name=self._gcs_object_name(f"{slug}.jsonl"))
            except FileNotFoundError as exc:
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
            with self._lock:
                self._rows_by_borough[slug] = cached

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
            log.warning(
                "parcel-intel %s: %d row(s) failed schema validation and were "
                "skipped; serving the remaining %d",
                slug,
                bad_rows,
                len(rows),
            )
        return rows, manifest


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
    "assemblage_id": None,
    "assemblage_lot_count": None,
    "assemblage_combined_lot_area_sqft": None,
    "assemblage_combined_buildable_sqft": None,
    "assemblage_member_bbls": [],
}


def _strip_premium_fields(row: ParcelIntelRow) -> ParcelIntelRow:
    return row.model_copy(update=dict(_ANON_STRIPPED_FIELDS))


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
    auth: Optional[AuthContext] = Depends(maybe_auth),
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

    generated_at = _parse_iso((manifest or {}).get("generated_at"))
    return ParcelIntelSweepResponse(
        borough=borough,
        rows=rows,
        generated_at=generated_at,
        model_metadata=(manifest or {}).get("model_metadata") or {},
        data_sources=(manifest or {}).get("data_sources") or {},
        quality_gate=(manifest or {}).get("quality_gate") or {},
    )

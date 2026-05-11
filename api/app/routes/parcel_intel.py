"""Parcel Intelligence — public read endpoints.

Serves per-borough rankings of redevelopment candidates. The data comes
from the citylens-parcel-intel repo's `scripts/publish_sweep.py`, which
uploads trimmed JSONL + a manifest.json to
`gs://<bucket>/parcel-intel/v1/`. We read those bytes through the
existing `GcsArtifacts` plumbing — no new GCS code needed.

Caching strategy:
- Process-level: a module-level registry caches parsed JSONL keyed on
  manifest.generated_at. Cheap re-loads on each Cloud Run instance
  (~80 KB per borough). Invalidated automatically when the publisher
  re-uploads.
- Edge: Cache-Control headers tuned for ~10-minute revalidation since
  the sweep cadence is monthly. `stale-while-revalidate` lets the CDN
  serve a slightly-stale response while it revalidates in the
  background.

The endpoints are public — same shape as `/v1/demo/*`. No Bearer token
required so the page is search-indexable.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from ..models.schemas import (
    ParcelIntelBorough,
    ParcelIntelIndex,
    ParcelIntelRow,
    ParcelIntelSweepResponse,
)
from ..services.gcs_artifacts import GcsArtifacts
from ..services.rate_limit import demo_rate_limit
from ..services.settings import Settings, get_settings

log = logging.getLogger(__name__)

router = APIRouter(tags=["parcel-intel"])

# 10-minute edge cache. Sweep is monthly, but a tighter window keeps
# stale-after-republish blast radius small.
_INDEX_CACHE = "public, s-maxage=600, stale-while-revalidate=300"
_SWEEP_CACHE = "public, s-maxage=600, stale-while-revalidate=300"

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
            payload, _ = gcs.download_bytes(
                object_name=self._gcs_object_name("manifest.json")
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail="Parcel intelligence data has not been published yet.",
            ) from exc
        manifest = json.loads(payload.decode("utf-8"))
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
        return ParcelIntelIndex(
            boroughs=boroughs,
            generated_at=generated_at,
            model_metadata=manifest.get("model_metadata") or {},
        )

    def borough(self, gcs: GcsArtifacts, slug: str) -> tuple[list[ParcelIntelRow], dict[str, Any] | None]:
        if slug not in _BOROUGH_SLUGS:
            raise HTTPException(status_code=404, detail="Unknown borough")
        manifest = self._refresh_manifest(gcs)

        with self._lock:
            cached = self._rows_by_borough.get(slug)
        if cached is None:
            try:
                payload, _ = gcs.download_bytes(
                    object_name=self._gcs_object_name(f"{slug}.jsonl")
                )
            except FileNotFoundError as exc:
                raise HTTPException(
                    status_code=404, detail=f"No data published for {slug}"
                ) from exc
            cached = [
                json.loads(line)
                for line in payload.decode("utf-8").splitlines()
                if line.strip()
            ]
            with self._lock:
                self._rows_by_borough[slug] = cached

        rows = [ParcelIntelRow(**r) for r in cached]
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
    top: int = Query(20, ge=1, le=1000, description="How many rows to return (1-1000)."),
    _rate_limit: None = Depends(demo_rate_limit),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
) -> ParcelIntelSweepResponse:
    rows, manifest = registry.borough(gcs, borough)
    response.headers["Cache-Control"] = _SWEEP_CACHE
    generated_at = _parse_iso((manifest or {}).get("generated_at"))
    return ParcelIntelSweepResponse(
        borough=borough,
        rows=rows[:top],
        generated_at=generated_at,
        model_metadata=(manifest or {}).get("model_metadata") or {},
    )

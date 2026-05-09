from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ArtifactResponse(BaseModel):
    name: str
    type: str
    gcs_uri: str
    gcs_object: str
    sha256: str
    size_bytes: int
    created_at: datetime
    signed_url: Optional[str] = None


class RunErrorResponse(BaseModel):
    code: str
    message: str
    stage: Optional[str] = None
    traceback_summary: list[str] = Field(default_factory=list)


class RunRecordBase(BaseModel):
    run_id: str
    user_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    stage: str
    progress: int = Field(ge=0, le=100)
    request: dict[str, Any] = Field(default_factory=dict)
    error: Optional[RunErrorResponse] = None
    execution_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class RunListItem(RunRecordBase):
    pass


class RunResponse(RunRecordBase):
    artifacts: list[ArtifactResponse] = Field(default_factory=list)


class RunListResponse(BaseModel):
    items: list[RunListItem]
    next_cursor: Optional[str] = None


class DemoRunFeatured(BaseModel):
    run_id: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str] = Field(default_factory=list)


# --- Parcel Intelligence (per-borough redev candidate ranking) ---
#
# These models are populated from JSONL files published to GCS by the
# `citylens-parcel-intel` repo's `scripts/publish_sweep.py`. The schema
# is intentionally narrow — only fields the public UI needs. New
# internal model features get added to the parcel-intel sidecar
# without breaking this contract.


class TopFeature(BaseModel):
    """A single SHAP-derived feature contribution (logit space).

    Computed once per row by ``citylens-parcel-intel`` at publish time and
    served through the API verbatim. ``value`` is heterogeneous because
    the underlying feature can be a numeric (lot area, allowed FAR), a
    categorical label (zoning district, building class), or a boolean
    flag (landmark) — clients render whatever shape comes through.
    """

    name: str
    value: Optional[Any] = None
    contribution_logit: float
    contribution_pct: float


class ParcelIntelRow(BaseModel):
    bbl: str
    address: Optional[str] = None
    borough: Optional[str] = None
    score_calibrated: Optional[float] = None
    score_calibrated_p10: Optional[float] = None
    score_calibrated_p90: Optional[float] = None
    lot_area_sqft: Optional[float] = None
    allowed_far: Optional[float] = None
    max_floor_area_sqft: Optional[float] = None
    unused_floor_area_sqft: Optional[float] = None
    far_utilization_pct: Optional[float] = None
    zoning_district_1: Optional[str] = None
    land_use: Optional[str] = None
    year_built: Optional[int] = None
    num_floors: Optional[float] = None
    # Tax-lot centroid (WGS84). Some parcels lack polygon geometry
    # (condo billing units, transit ROW); those come through with
    # lat/lng = None and the UI skips them on the map.
    lat: Optional[float] = None
    lng: Optional[float] = None
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    years_held: Optional[int] = None
    has_recent_sale_5yr: bool = False
    is_landmark: bool = False
    is_historic_district: bool = False
    block_id: Optional[str] = None
    block_rank: Optional[int] = None
    # Validation status against the latest PLUTO snapshot + labels:
    # "still_vacant" — never built; safe redev candidate
    # "active"       — NB-permitted 2019-2024 OR year_built bumped post-2018
    # "already_built" — completed redev; the publisher filters these out
    # before reaching here, so this should rarely (never) be the value
    # in a published row.
    redev_status: Literal["still_vacant", "active", "already_built"] = "still_vacant"
    # Per-row SHAP feature attributions, top-K by absolute contribution.
    # Defaults to an empty list — older publishes (sweep schema v1) and
    # rows where SHAP failed flow through cleanly.
    top_features: list[TopFeature] = Field(default_factory=list)


class ParcelIntelBorough(BaseModel):
    slug: str
    display_name: str
    count: int
    top_score: Optional[float] = None


class ParcelIntelIndex(BaseModel):
    boroughs: list[ParcelIntelBorough] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)


class ParcelIntelSweepResponse(BaseModel):
    borough: str
    rows: list[ParcelIntelRow] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)

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
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    years_held: Optional[int] = None
    has_recent_sale_5yr: bool = False
    is_landmark: bool = False
    is_historic_district: bool = False
    block_id: Optional[str] = None
    block_rank: Optional[int] = None


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

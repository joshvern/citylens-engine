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
    priority_rank: Optional[int] = None
    priority_tier: Literal["highest", "high", "medium", "watch"] = "watch"
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
    parcel_geometry: Optional[dict[str, Any]] = None
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    years_held: Optional[int] = None
    has_recent_sale_5yr: bool = False
    is_landmark: bool = False
    is_historic_district: bool = False
    block_id: Optional[str] = None
    block_rank: Optional[int] = None
    # Validation status against the latest PLUTO snapshot + current DOB:
    # "still_vacant" — never built; safe redev candidate
    # "active"       — recent non-closed NB activity OR year_built bumped
    # "already_built" — completed redev; the publisher filters these out
    # before reaching here, so this should rarely (never) be the value
    # in a published row.
    redev_status: Literal["still_vacant", "active", "already_built"] = "still_vacant"
    latest_nb_filing_year: Optional[int] = None
    latest_nb_status: Optional[str] = None
    opportunity_category: Literal[
        "vacant_site",
        "ground_up_candidate",
        "conversion_or_overbuilt",
        "active_project",
        "completed_project",
    ] = "ground_up_candidate"
    property_facts_current: bool = False
    property_facts_as_of: Optional[str] = None
    ownership_as_of: Optional[str] = None
    project_activity_as_of: Optional[str] = None
    data_warnings: list[str] = Field(default_factory=list)
    assemblage_id: Optional[str] = None
    assemblage_lot_count: Optional[int] = None
    assemblage_combined_lot_area_sqft: Optional[float] = None
    assemblage_combined_buildable_sqft: Optional[float] = None
    assemblage_member_bbls: list[str] = Field(default_factory=list)
    # Per-row SHAP feature attributions, top-K by absolute contribution.
    # Defaults to an empty list — older publishes (sweep schema v1) and
    # rows where SHAP failed flow through cleanly.
    top_features: list[TopFeature] = Field(default_factory=list)
    # --- Change-signal + ownership (premium fields) ---
    # Populated by publisher v4, which joins aerial change-detection output
    # and ACRIS owner-of-record onto each lot. Pydantic
    # strips unknown JSONL fields silently, so the API must know these
    # names before the publisher starts emitting them. Defaults keep old
    # publishes validating unchanged. All of these are stripped from
    # anonymous sweep responses.
    change_added_count: int = 0
    change_demolished_count: int = 0
    change_modified_count: int = 0
    change_latest_imagery_year: Optional[int] = None
    observed_imagery_year: Optional[int] = None
    recent_change: bool = False
    owner_name: Optional[str] = None


class ParcelIntelBorough(BaseModel):
    slug: str
    display_name: str
    count: int
    top_score: Optional[float] = None


class ParcelIntelIndex(BaseModel):
    boroughs: list[ParcelIntelBorough] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    data_sources: dict[str, Any] = Field(default_factory=dict)
    # Freshness telemetry, derived from `generated_at` at request time.
    # Defaults keep older clients (and cached responses) unaffected.
    age_days: Optional[float] = None
    stale: bool = False


class ParcelIntelSweepResponse(BaseModel):
    borough: str
    rows: list[ParcelIntelRow] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    data_sources: dict[str, Any] = Field(default_factory=dict)


ParcelWorkflowStage = Literal[
    "new", "reviewing", "contacted", "underwriting", "pursue", "pass"
]


class ParcelWorkflowUpdate(BaseModel):
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    stage: ParcelWorkflowStage = "new"
    notes: str = Field(default="", max_length=4000)
    tags: list[str] = Field(default_factory=list, max_length=10)
    assignee: Optional[str] = Field(default=None, max_length=128)
    watching: bool = True
    decision_reason: Optional[str] = Field(default=None, max_length=80)
    outcome: Optional[
        Literal[
            "unknown",
            "owner_contacted",
            "meeting_scheduled",
            "offer_submitted",
            "under_contract",
            "closed",
            "lost",
        ]
    ] = "unknown"
    snapshot: "ParcelWorkflowSnapshot" = Field(
        default_factory=lambda: ParcelWorkflowSnapshot()
    )


class ParcelWorkflowSnapshot(BaseModel):
    """Small, typed baseline used to detect decision-relevant parcel changes."""

    property_facts_as_of: Optional[str] = Field(default=None, max_length=32)
    zoning_district_1: Optional[str] = Field(default=None, max_length=32)
    land_use: Optional[str] = Field(default=None, max_length=8)
    year_built: Optional[int] = Field(default=None, ge=0, le=2100)
    allowed_far: Optional[float] = Field(default=None, ge=0, le=100)
    unused_floor_area_sqft: Optional[float] = None
    owner_name: Optional[str] = Field(default=None, max_length=256)
    last_sale_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    latest_nb_filing_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    latest_nb_status: Optional[str] = Field(default=None, max_length=256)
    redev_status: Optional[
        Literal["still_vacant", "active", "already_built"]
    ] = None
    observed_imagery_year: Optional[int] = Field(default=None, ge=1900, le=2100)


class ParcelWorkflowItem(ParcelWorkflowUpdate):
    bbl: str
    saved_at: datetime
    updated_at: datetime


class ParcelSavedSearchUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    filters: "ParcelSavedSearchFilters" = Field(
        default_factory=lambda: ParcelSavedSearchFilters()
    )
    alert_frequency: Literal["off", "daily", "weekly"] = "weekly"


class ParcelSavedSearchFilters(BaseModel):
    landUseFilter: Literal[
        "all", "residential", "commercial", "industrial", "vacant"
    ] = "all"
    priorityFilter: Literal[
        "all", "highest", "high_or_better", "medium_or_better"
    ] = "all"
    opportunityFilter: Literal[
        "all",
        "ground_up",
        "vacant_site",
        "ground_up_candidate",
        "conversion_or_overbuilt",
        "active_project",
    ] = "ground_up"
    hideLandmarked: bool = False
    recentSaleOnly: bool = False
    recentChangeOnly: bool = False
    pipelineOnly: bool = False
    zoningFamilies: list[Literal["R", "C", "M", "Other"]] = Field(
        default_factory=lambda: ["R", "C", "M", "Other"], max_length=4
    )
    sortKey: Literal[
        "score_calibrated",
        "lot_area_sqft",
        "last_sale_price",
        "years_held",
        "year_built",
        "num_floors",
        "allowed_far",
        "far_utilization_pct",
    ] = "score_calibrated"
    direction: Literal["asc", "desc"] = "desc"


class ParcelSavedSearch(ParcelSavedSearchUpdate):
    search_id: str
    created_at: datetime
    updated_at: datetime

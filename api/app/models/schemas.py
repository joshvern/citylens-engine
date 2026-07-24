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
    model_rank: Optional[int] = None
    acquisition_rank: Optional[int] = None
    citywide_rank: Optional[int] = None
    # Optional during the v4 -> v5 feed rollout. ``None`` lets clients use
    # the legacy opportunity-category fallback until every borough object has
    # been replaced by the v5 publisher.
    acquisition_eligible: Optional[bool] = None
    acquisition_status: Optional[Literal[
        "eligible",
        "active_project",
        "completed_project",
        "constrained",
        "incomplete_data",
    ]] = None
    acquisition_exclusion_reasons: list[str] = Field(default_factory=list)
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
    # Historical NYC DOF final tax-lien sale record. This is a diligence
    # signal, not an assertion that a balance is still unpaid.
    tax_lien_sale_date: Optional[str] = None
    tax_lien_sale_year: Optional[int] = None
    tax_lien_water_debt_only: Optional[bool] = None
    tax_lien_data_as_of: Optional[str] = None
    # Current official NYC violation snapshots. These authenticated diligence
    # fields are post-score and never change rank or acquisition eligibility.
    dob_safety_active_count: int = 0
    dob_safety_latest_issue_date: Optional[str] = None
    ecb_active_count: int = 0
    ecb_class_1_count: int = 0
    ecb_balance_due: float = 0.0
    ecb_latest_issue_date: Optional[str] = None
    hpd_open_count: int = 0
    hpd_class_c_count: int = 0
    hpd_latest_inspection_date: Optional[str] = None
    critical_violation_count: Optional[int] = 0
    violation_data_as_of: Optional[str] = None
    # Current PLUTO parcel intersections with FEMA's 1% annual-chance
    # floodplains. These authenticated diligence fields are post-score.
    firm07_floodplain: Optional[bool] = None
    pfirm15_floodplain: Optional[bool] = None
    floodplain_1pct: Optional[bool] = None
    floodplain_data_as_of: Optional[str] = None
    # PLUTO EDesigNum includes E-designations and R-prefixed restrictive
    # declarations. These are diligence requirements, not contamination proof.
    environmental_review_required: Optional[bool] = None
    environmental_designation_number: Optional[str] = None
    environmental_designation_kind: Optional[
        Literal["e_designation", "restrictive_declaration", "other"]
    ] = None
    environmental_designation_data_as_of: Optional[str] = None
    is_landmark: bool = False
    is_historic_district: bool = False
    block_id: Optional[str] = None
    block_rank: Optional[int] = None
    # Validation status against the latest PLUTO snapshot + current DOB:
    # "still_vacant" — never built; safe redev candidate
    # "active"       — recent non-terminated project activity OR year_built bumped
    # "already_built" — completed redev; the publisher filters these out
    # before reaching here, so this should rarely (never) be the value
    # in a published row.
    redev_status: Literal["still_vacant", "active", "already_built"] = "still_vacant"
    latest_nb_filing_year: Optional[int] = None
    latest_nb_status: Optional[str] = None
    latest_project_filing_year: Optional[int] = None
    latest_project_status: Optional[str] = None
    latest_project_type: Literal[
        "new_building",
        "alt_co_new_building",
        "demolition",
        "land_use_entitlement",
    ] | None = None
    latest_project_job_number: Optional[str] = None
    latest_project_url: Optional[str] = None
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
    land_use_activity_as_of: Optional[str] = None
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
    owner_name_source: Literal["acris", "pluto"] | None = None
    owner_type: Optional[str] = None
    owner_entity_type: Literal[
        "unknown",
        "individual",
        "llc",
        "corp",
        "partnership",
        "trust",
        "estate",
        "government",
        "religious",
        "nonprofit",
        "hdfc",
    ] | None = None
    owner_portfolio_id: Optional[str] = None
    owner_portfolio_match_method: Literal[
        "exact_normalized_pluto_owner_name"
    ] | None = None
    owner_portfolio_lot_count: Optional[int] = None
    owner_portfolio_borough_count: Optional[int] = None
    owner_portfolio_total_lot_area_sqft: Optional[float] = None
    owner_portfolio_candidate_count: Optional[int] = None
    owner_portfolio_data_as_of: Optional[str] = None


class ParcelIntelMapRow(BaseModel):
    """Compact citywide explorer row.

    Polygon geometry, SHAP explanations, and full diligence fields stay in the
    per-parcel response and are fetched only when the user opens a site.
    """

    bbl: str
    address: Optional[str] = None
    borough: str
    score_calibrated: Optional[float] = None
    priority_rank: Optional[int] = None
    priority_tier: Literal["highest", "high", "medium", "watch"] = "watch"
    model_rank: Optional[int] = None
    acquisition_rank: Optional[int] = None
    citywide_rank: Optional[int] = None
    acquisition_eligible: Optional[bool] = None
    acquisition_status: Optional[
        Literal[
            "eligible",
            "active_project",
            "completed_project",
            "constrained",
            "incomplete_data",
        ]
    ] = None
    lot_area_sqft: Optional[float] = None
    unused_floor_area_sqft: Optional[float] = None
    far_utilization_pct: Optional[float] = None
    zoning_district_1: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    years_held: Optional[int] = None
    tax_lien_sale_year: Optional[int] = None
    critical_violation_count: Optional[int] = 0
    floodplain_1pct: Optional[bool] = None
    environmental_review_required: Optional[bool] = None
    owner_name: Optional[str] = None
    owner_entity_type: Optional[str] = None
    owner_portfolio_id: Optional[str] = None
    owner_portfolio_lot_count: Optional[int] = None
    owner_portfolio_borough_count: Optional[int] = None
    owner_portfolio_candidate_count: Optional[int] = None
    recent_change: bool = False
    opportunity_category: Literal[
        "vacant_site",
        "ground_up_candidate",
        "conversion_or_overbuilt",
        "active_project",
        "completed_project",
    ] = "ground_up_candidate"
    assemblage_lot_count: Optional[int] = None


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
    quality_gate: dict[str, Any] = Field(default_factory=dict)
    generation_diff: dict[str, Any] = Field(default_factory=dict)
    inference_replay: dict[str, Any] = Field(default_factory=dict)
    # Freshness telemetry, derived from `generated_at` at request time.
    # Defaults keep older clients (and cached responses) unaffected.
    age_days: Optional[float] = None
    stale: bool = False


class ParcelIntelMapResponse(BaseModel):
    rows: list[ParcelIntelMapRow] = Field(default_factory=list)
    generated_at: Optional[datetime] = None


class ParcelIntelSweepResponse(BaseModel):
    borough: str
    rows: list[ParcelIntelRow] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    data_sources: dict[str, Any] = Field(default_factory=dict)
    quality_gate: dict[str, Any] = Field(default_factory=dict)
    generation_diff: dict[str, Any] = Field(default_factory=dict)
    inference_replay: dict[str, Any] = Field(default_factory=dict)


ParcelWorkflowStage = Literal[
    "new", "reviewing", "contacted", "underwriting", "pursue", "pass"
]

ParcelWorkflowOutcome = Literal[
    "unknown",
    "owner_contacted",
    "meeting_scheduled",
    "qualified",
    "offer_submitted",
    "under_contract",
    "closed",
    "rejected",
    "lost",
]


class ParcelWorkflowUpdate(BaseModel):
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    stage: ParcelWorkflowStage = "new"
    notes: str = Field(default="", max_length=4000)
    tags: list[str] = Field(default_factory=list, max_length=10)
    assignee: Optional[str] = Field(default=None, max_length=128)
    watching: bool = True
    decision_reason: Optional[str] = Field(default=None, max_length=80)
    outcome: Optional[ParcelWorkflowOutcome] = "unknown"
    snapshot: "ParcelWorkflowSnapshot" = Field(
        default_factory=lambda: ParcelWorkflowSnapshot(),
        description=(
            "Deprecated client hint. The API captures the canonical current "
            "feed snapshot on first save and preserves it immutably."
        ),
    )


class ParcelWorkflowSnapshot(BaseModel):
    """Small, typed baseline used to detect decision-relevant parcel changes."""

    feed_generated_at: Optional[str] = Field(default=None, max_length=40)
    property_facts_as_of: Optional[str] = Field(default=None, max_length=32)
    citywide_rank: Optional[int] = Field(default=None, ge=1, le=1_000_000)
    acquisition_rank: Optional[int] = Field(default=None, ge=1, le=1_000_000)
    priority_tier: Optional[
        Literal["highest", "high", "medium", "watch"]
    ] = None
    opportunity_category: Optional[
        Literal[
            "vacant_site",
            "ground_up_candidate",
            "conversion_or_overbuilt",
            "active_project",
            "completed_project",
        ]
    ] = None
    score_calibrated: Optional[float] = Field(default=None, ge=0, le=1)
    zoning_district_1: Optional[str] = Field(default=None, max_length=32)
    land_use: Optional[str] = Field(default=None, max_length=8)
    year_built: Optional[int] = Field(default=None, ge=0, le=2100)
    allowed_far: Optional[float] = Field(default=None, ge=0, le=100)
    unused_floor_area_sqft: Optional[float] = None
    owner_name: Optional[str] = Field(default=None, max_length=256)
    owner_entity_type: Optional[
        Literal[
            "unknown",
            "individual",
            "llc",
            "corp",
            "partnership",
            "trust",
            "estate",
            "government",
            "religious",
            "nonprofit",
            "hdfc",
        ]
    ] = None
    owner_portfolio_lot_count: Optional[int] = Field(default=None, ge=1)
    last_sale_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    latest_nb_filing_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    latest_nb_status: Optional[str] = Field(default=None, max_length=256)
    redev_status: Optional[
        Literal["still_vacant", "active", "already_built"]
    ] = None
    observed_imagery_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    tax_lien_sale_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    critical_violation_count: Optional[int] = Field(default=None, ge=0)
    floodplain_1pct: Optional[bool] = None
    environmental_review_required: Optional[bool] = None
    environmental_designation_number: Optional[str] = Field(
        default=None, max_length=32
    )
    environmental_designation_kind: Optional[
        Literal["e_designation", "restrictive_declaration", "other"]
    ] = None
    recent_change: Optional[bool] = None


class ParcelWorkflowItem(ParcelWorkflowUpdate):
    bbl: str
    saved_at: datetime
    updated_at: datetime


class ParcelWorkflowEvent(BaseModel):
    event_id: str
    schema_version: Literal["citylens/parcel-workflow-event@v1"]
    bbl: str
    event_type: Literal["created", "updated", "archived", "restored"]
    occurred_at: datetime
    from_stage: Optional[ParcelWorkflowStage] = None
    to_stage: Optional[ParcelWorkflowStage] = None
    from_outcome: Optional[ParcelWorkflowOutcome] = None
    to_outcome: Optional[ParcelWorkflowOutcome] = None
    from_decision_reason: Optional[str] = None
    to_decision_reason: Optional[str] = None
    changed_fields: list[str] = Field(default_factory=list)


class ParcelWorkflowRate(BaseModel):
    numerator: int
    denominator: int
    rate: Optional[float] = None
    sufficient_denominator: bool


ParcelWorkflowMilestone = Literal[
    "owner_contacted",
    "qualified",
    "offer_submitted",
    "under_contract",
    "closed",
]


class ParcelWorkflowMaturityWindow(BaseModel):
    milestone: ParcelWorkflowMilestone
    label: str
    horizon_days: int = Field(ge=1)
    eligible_records: int = Field(ge=0)
    reached_within_horizon: int = Field(ge=0)
    pending_records: int = Field(ge=0)
    rate: Optional[float] = Field(default=None, ge=0, le=1)
    sufficient_denominator: bool


class ParcelWorkflowHorizonDefinition(BaseModel):
    milestone: ParcelWorkflowMilestone
    label: str
    horizon_days: int = Field(ge=1)


class ParcelWorkflowAnalyticsMethodology(BaseModel):
    schema_version: Literal[
        "citylens/parcel-workflow-analytics-methodology@v1"
    ]
    analytics_schema_version: Literal[
        "citylens/parcel-workflow-analytics@v2"
    ]
    horizons: list[ParcelWorkflowHorizonDefinition]
    minimum_cohort_size: int = Field(ge=1)
    minimum_rate_denominator: int = Field(ge=1)
    selection_scope: str
    timestamp_semantics: str
    model_accuracy_claim: Literal[False]


class ParcelWorkflowFunnel(BaseModel):
    saved: int
    contacted: int
    meeting_scheduled: int
    qualified: int
    offer_submitted: int
    under_contract: int
    closed: int
    rejected: int
    lost: int
    contacted_per_saved: ParcelWorkflowRate
    qualified_per_contacted: ParcelWorkflowRate
    offer_per_qualified: ParcelWorkflowRate
    contract_per_offer: ParcelWorkflowRate
    close_per_contract: ParcelWorkflowRate


class ParcelWorkflowCohort(BaseModel):
    dimension: Literal["borough", "rank_band", "opportunity"]
    value: str
    total: int
    contacted: int
    qualified: int
    offer_submitted: int
    under_contract: int
    closed: int
    rejected: int
    lost: int
    contacted_rate_denominator: int = 0
    qualified_rate_denominator: int = 0
    close_rate_denominator: int = 0
    contacted_rate: Optional[float] = None
    qualified_rate: Optional[float] = None
    close_rate: Optional[float] = None


class ParcelWorkflowAnalytics(BaseModel):
    schema_version: Literal["citylens/parcel-workflow-analytics@v2"]
    generated_at: datetime
    measurement_status: Literal["collecting", "directional", "usable"]
    measurement_label: str
    total_records: int
    active_records: int
    archived_records: int
    event_history_records: int
    rank_snapshot_records: int
    valid_saved_at_records: int
    oldest_followup_days: Optional[int] = None
    median_followup_days: Optional[float] = None
    minimum_cohort_size: int
    minimum_rate_denominator: int
    stage_counts: dict[str, int]
    outcome_counts: dict[str, int]
    decision_reason_counts: dict[str, int]
    funnel: ParcelWorkflowFunnel
    maturity_windows: list[ParcelWorkflowMaturityWindow] = Field(
        default_factory=list
    )
    cohorts: list[ParcelWorkflowCohort] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ParcelWorkflowAlert(BaseModel):
    bbl: str
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    code: Literal[
        "removed_from_current_feed",
        "owner_changed",
        "newer_sale_record",
        "zoning_changed",
        "opportunity_changed",
        "priority_tier_changed",
        "material_rank_move",
        "tax_lien_history_changed",
        "critical_violations_changed",
        "flood_overlay_changed",
        "environmental_review_changed",
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    ]
    severity: Literal["urgent", "high", "medium", "low"]
    title: str
    detail: str
    field: str
    before: Optional[Any] = None
    after: Optional[Any] = None


class ParcelWorkflowAlerts(BaseModel):
    schema_version: Literal["citylens/parcel-workflow-alerts@v1"]
    generated_at: datetime
    feed_generated_at: Optional[datetime] = None
    watched_count: int
    changed_lead_count: int
    alert_count: int
    removed_from_feed_count: int
    severity_counts: dict[str, int]
    alerts: list[ParcelWorkflowAlert] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


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

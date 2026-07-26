from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any, Literal, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_core import PydanticCustomError


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


PilotPlan = Literal["acquisitions", "concierge"]
PilotRequestStatus = Literal[
    "new",
    "contacted",
    "qualified",
    "declined",
    "converted",
    "spam",
]
PilotBorough = Literal[
    "manhattan",
    "brooklyn",
    "queens",
    "bronx",
    "staten_island",
]


class PilotRequestCreate(BaseModel):
    """Bounded public design-partner request.

    The honeypot is accepted by the schema but excluded from persistence.
    Network metadata is intentionally absent from this contract.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["citylens/pilot-request@v1"]
    plan: PilotPlan
    name: str = Field(..., min_length=2, max_length=100)
    work_email: str = Field(..., min_length=5, max_length=254)
    company: str = Field(..., min_length=2, max_length=120)
    role: str = Field(default="", max_length=100)
    team_size: Literal["1", "2-5", "6-20", "21+"]
    target_boroughs: list[PilotBorough] = Field(
        ..., min_length=1, max_length=5
    )
    workflow_summary: str = Field(..., min_length=20, max_length=1_200)
    consent: Literal[True]
    website: str = Field(default="", max_length=200, exclude=True)

    @model_validator(mode="after")
    def normalize_contact_fields(self) -> "PilotRequestCreate":
        self.name = " ".join(self.name.split())
        self.work_email = self.work_email.strip().lower()
        self.company = " ".join(self.company.split())
        self.role = " ".join(self.role.split())
        self.workflow_summary = " ".join(self.workflow_summary.split())
        self.website = self.website.strip()
        self.target_boroughs = list(dict.fromkeys(self.target_boroughs))
        if len(self.name) < 2:
            raise PydanticCustomError(
                "invalid_pilot_name",
                "name must contain at least two non-whitespace characters",
            )
        if len(self.company) < 2:
            raise PydanticCustomError(
                "invalid_pilot_company",
                "company must contain at least two non-whitespace characters",
            )
        if len(self.workflow_summary) < 20:
            raise PydanticCustomError(
                "invalid_pilot_workflow_summary",
                "workflow_summary must contain at least 20 non-whitespace characters",
            )
        if not re.fullmatch(
            r"[^@\s]+@[^@\s]+\.[^@\s]+",
            self.work_email,
        ):
            raise PydanticCustomError(
                "invalid_pilot_email",
                "work_email must be a valid email address",
            )
        return self


class PilotRequestReceipt(BaseModel):
    schema_version: Literal["citylens/pilot-request-receipt@v1"]
    request_id: str
    status: Literal["received"]
    created_at: datetime


class PilotRequestAdminRecord(BaseModel):
    schema_version: Literal["citylens/pilot-request@v1"]
    request_id: str
    status: PilotRequestStatus
    plan: PilotPlan
    name: str
    work_email: str
    company: str
    role: str = ""
    team_size: Literal["1", "2-5", "6-20", "21+"]
    target_boroughs: list[PilotBorough]
    workflow_summary: str
    consent: Literal[True]
    created_at: datetime
    updated_at: datetime
    expires_at: datetime


class PilotRequestAdminList(BaseModel):
    items: list[PilotRequestAdminRecord] = Field(default_factory=list)


class PilotRequestStatusUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["citylens/pilot-request-status@v1"]
    status: PilotRequestStatus


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


class ParcelDecisionAuditValidation(BaseModel):
    target: str
    evaluation_scope: str
    precision_at_100: Optional[float] = Field(default=None, ge=0, le=1)
    precision_at_1000: Optional[float] = Field(default=None, ge=0, le=1)
    base_rate: Optional[float] = Field(default=None, ge=0, le=1)
    prospective_validated: bool = False
    disclaimer: str


class ParcelDecisionAuditCheck(BaseModel):
    key: str
    layer: Literal[
        "model_signal",
        "eligibility_gate",
        "current_diligence",
        "source_freshness",
    ]
    label: str
    status: Literal[
        "verified",
        "review",
        "excluded",
        "unavailable",
        "informational",
    ]
    summary: str
    source: str
    as_of: Optional[str] = None
    affects_model_rank: bool = False
    affects_acquisition_eligibility: bool = False


class ParcelDecisionReadiness(BaseModel):
    """Conservative, evidence-derived next step for an acquisition screen.

    This is workflow guidance, not a model output or purchase recommendation.
    The builder must preserve the same anonymous/private boundary as the
    underlying audit checks.
    """

    status: Literal[
        "blocked",
        "incomplete",
        "review_required",
        "initial_review_ready",
        "limited_preview",
    ]
    label: str
    recommended_action: str
    blockers: list[str] = Field(default_factory=list)
    review_items: list[str] = Field(default_factory=list)
    cleared_items: list[str] = Field(default_factory=list)
    disclaimer: str


class ParcelDecisionAudit(BaseModel):
    schema_version: Literal["citylens/parcel-decision-audit@v1"]
    overall_status: Literal[
        "screened",
        "screened_with_flags",
        "excluded",
        "incomplete",
    ]
    overall_label: str
    validation: ParcelDecisionAuditValidation
    readiness: ParcelDecisionReadiness
    checks: list[ParcelDecisionAuditCheck] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


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
    # Current adopted NYC Planning MIH mapped-area overlap. This is
    # authenticated post-score diligence, not a zoning opinion or rank input.
    mandatory_inclusionary_housing: Optional[bool] = None
    mih_options: Optional[list[str]] = None
    mih_area_count: Optional[int] = None
    mih_data_as_of: Optional[str] = None
    # Current MTA subway/SIR station-complex proximity. Straight-line
    # authenticated diligence only; not a walking route or rank input.
    nearest_transit_complex_id: Optional[str] = None
    nearest_transit_station_name: Optional[str] = None
    nearest_transit_station_distance_m: Optional[int] = Field(
        default=None, ge=0
    )
    nearest_transit_routes: Optional[list[str]] = None
    nearest_transit_ada_status: Optional[
        Literal["full", "partial", "none"]
    ] = None
    transit_station_count_400m: Optional[int] = Field(default=None, ge=0)
    transit_station_count_800m: Optional[int] = Field(default=None, ge=0)
    transit_access_tier: Optional[
        Literal["very_close", "walkable", "limited", "distant"]
    ] = None
    transit_data_as_of: Optional[str] = None
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


class ParcelIntelParcelResponse(ParcelIntelRow):
    decision_audit: ParcelDecisionAudit


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
    mandatory_inclusionary_housing: Optional[bool] = None
    nearest_transit_station_name: Optional[str] = None
    nearest_transit_station_distance_m: Optional[int] = Field(
        default=None, ge=0
    )
    nearest_transit_routes: Optional[list[str]] = None
    nearest_transit_ada_status: Optional[
        Literal["full", "partial", "none"]
    ] = None
    transit_station_count_800m: Optional[int] = Field(default=None, ge=0)
    transit_access_tier: Optional[
        Literal["very_close", "walkable", "limited", "distant"]
    ] = None
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


class ParcelProspectiveValidationMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    eligible_parcels: int = Field(ge=1, le=1000)
    observed_nb_filing_hits: Optional[int] = Field(default=None, ge=0)
    observed_precision_lower_bound: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
    )
    final_precision: Optional[float] = Field(default=None, ge=0, le=1)
    final_precision_95ci: Optional[tuple[float, float]] = None


class ParcelProspectiveValidationMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    top_100: ParcelProspectiveValidationMetric
    top_1000: ParcelProspectiveValidationMetric


class ParcelProspectiveHistoricalBenchmark(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope: Optional[str] = None
    evaluation_window: Optional[str] = None
    precision_at_100: Optional[float] = Field(default=None, ge=0, le=1)
    precision_at_1000: Optional[float] = Field(default=None, ge=0, le=1)
    not_current_cohort_accuracy: Literal[True]


class ParcelProspectiveOfficialSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: Literal["ic3t-wcy2", "w9ak-ipjd"]
    rows_updated_at: datetime


class ParcelProspectiveReportReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observation_id: str = Field(pattern=r"^[0-9]{8}-[0-9a-f]{12}$")
    # Required to validate the private producer pointer, but never serialized
    # through the public API.
    object_name: str = Field(min_length=1, max_length=512, exclude=True)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ParcelProspectiveValidationStatus(BaseModel):
    """Public-safe maturity state for one exact production ranking cohort."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[
        "citylens-parcel-intel/prospective-validation-status@v1"
    ] = Field(alias="schema")
    cohort_id: str = Field(
        pattern=r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
    )
    source_generation: str = Field(
        pattern=r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
    )
    label_definition: Literal["dob_nb_job_filing"]
    measurement_status: Literal[
        "awaiting_post_issue_data",
        "collecting",
        "mature",
    ]
    issued_at: datetime
    observation_starts_on: date
    observed_through: date
    matures_at: datetime
    elapsed_days: int = Field(ge=0, le=365)
    maturity_fraction: float = Field(ge=0, le=1)
    metrics: ParcelProspectiveValidationMetrics
    historical_benchmark: ParcelProspectiveHistoricalBenchmark
    official_sources: list[ParcelProspectiveOfficialSource] = Field(
        min_length=2,
        max_length=2,
    )
    report_reference: ParcelProspectiveReportReference
    interpretation: str = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def validate_maturity_contract(
        self,
    ) -> ParcelProspectiveValidationStatus:
        if (
            self.issued_at.utcoffset() is None
            or self.matures_at.utcoffset() is None
            or any(
                source.rows_updated_at.utcoffset() is None
                for source in self.official_sources
            )
        ):
            raise PydanticCustomError(
                "prospective_timezone",
                "prospective timestamps must be timezone-aware",
            )
        if self.cohort_id != self.source_generation:
            raise PydanticCustomError(
                "prospective_generation_mismatch",
                "cohort_id and source_generation must match",
            )
        if self.observation_starts_on != self.issued_at.date() + timedelta(
            days=1
        ):
            raise PydanticCustomError(
                "prospective_observation_start",
                "observation must start after cohort issuance",
            )
        if self.matures_at != self.issued_at + timedelta(days=365):
            raise PydanticCustomError(
                "prospective_maturity_horizon",
                "prospective cohort must use the fixed 365-day horizon",
            )
        if (
            self.measurement_status == "awaiting_post_issue_data"
            and self.observed_through >= self.observation_starts_on
        ):
            raise PydanticCustomError(
                "prospective_status_date",
                "awaiting status requires a pre-observation source date",
            )
        if (
            self.measurement_status == "collecting"
            and not (
                self.observation_starts_on
                <= self.observed_through
                < self.matures_at.date()
            )
        ):
            raise PydanticCustomError(
                "prospective_status_date",
                "collecting status date is outside the observation window",
            )
        if (
            self.measurement_status == "mature"
            and self.observed_through < self.matures_at.date()
        ):
            raise PydanticCustomError(
                "prospective_status_date",
                "mature status requires the complete observation horizon",
            )
        expected_elapsed_days = min(
            365,
            max(0, (self.observed_through - self.issued_at.date()).days),
        )
        if self.elapsed_days != expected_elapsed_days or not math.isclose(
            self.maturity_fraction,
            expected_elapsed_days / 365,
            abs_tol=1e-12,
        ):
            raise PydanticCustomError(
                "prospective_maturity_telemetry",
                "elapsed days and maturity fraction disagree with source dates",
            )
        expected_counts = {"top_100": 100, "top_1000": 1000}
        for name, expected in expected_counts.items():
            metric = getattr(self.metrics, name)
            if metric.eligible_parcels != expected:
                raise PydanticCustomError(
                    "prospective_metric_population",
                    f"{name} eligible_parcels must equal {expected}",
                )
            if self.measurement_status == "awaiting_post_issue_data":
                if (
                    metric.observed_nb_filing_hits is not None
                    or metric.observed_precision_lower_bound is not None
                ):
                    raise PydanticCustomError(
                        "prospective_premature_observation",
                        "pre-observation metrics must remain null",
                    )
            elif (
                metric.observed_nb_filing_hits is None
                or metric.observed_precision_lower_bound is None
            ):
                raise PydanticCustomError(
                    "prospective_missing_observation",
                    "started cohorts require observed lower-bound metrics",
                )
            elif (
                metric.observed_nb_filing_hits > expected
                or not math.isclose(
                    metric.observed_precision_lower_bound,
                    metric.observed_nb_filing_hits / expected,
                    abs_tol=1e-12,
                )
            ):
                raise PydanticCustomError(
                    "prospective_metric_consistency",
                    "observed hits and precision lower bound disagree",
                )
            if self.measurement_status != "mature":
                if (
                    metric.final_precision is not None
                    or metric.final_precision_95ci is not None
                ):
                    raise PydanticCustomError(
                        "prospective_premature_final",
                        "immature final metrics must remain null",
                    )
            elif (
                metric.final_precision is None
                or metric.final_precision_95ci is None
            ):
                raise PydanticCustomError(
                    "prospective_missing_final",
                    "mature cohorts require final precision and interval",
                )
            elif (
                not math.isclose(
                    metric.final_precision,
                    metric.observed_precision_lower_bound or 0.0,
                    abs_tol=1e-12,
                )
                or not 0 <= metric.final_precision_95ci[0] <= 1
                or not 0 <= metric.final_precision_95ci[1] <= 1
                or not (
                    metric.final_precision_95ci[0]
                    <= metric.final_precision
                    <= metric.final_precision_95ci[1]
                )
            ):
                raise PydanticCustomError(
                    "prospective_final_consistency",
                    "final precision and confidence interval disagree",
                )
        if {item.dataset_id for item in self.official_sources} != {
            "ic3t-wcy2",
            "w9ak-ipjd",
        }:
            raise PydanticCustomError(
                "prospective_official_sources",
                "both official DOB datasets are required",
            )
        expected_suffix = (
            f"/{self.cohort_id}/reports/"
            f"{self.report_reference.observation_id}.json"
        )
        if (
            not self.report_reference.object_name.endswith(
                expected_suffix
            )
            or not self.report_reference.observation_id.startswith(
                self.observed_through.strftime("%Y%m%d") + "-"
            )
        ):
            raise PydanticCustomError(
                "prospective_report_reference",
                "report identity does not match cohort and observation date",
            )
        return self


class ParcelProspectiveValidationHealth(BaseModel):
    """API-derived freshness state for the weekly prospective monitor."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["current", "stale", "unavailable"]
    reason: Literal[
        "current",
        "observation_lag_exceeded",
        "status_missing_or_invalid",
    ]
    observation_lag_days: Optional[int] = Field(default=None, ge=0)
    max_observation_lag_days: Literal[8] = 8
    next_monitor_due_on: Optional[date] = None
    oldest_official_source_updated_at: Optional[datetime] = None

    @model_validator(mode="after")
    def validate_freshness_contract(
        self,
    ) -> ParcelProspectiveValidationHealth:
        if self.status == "unavailable":
            if (
                self.reason != "status_missing_or_invalid"
                or self.observation_lag_days is not None
                or self.next_monitor_due_on is not None
                or self.oldest_official_source_updated_at is not None
            ):
                raise PydanticCustomError(
                    "prospective_health_unavailable",
                    "unavailable health must not imply source freshness",
                )
            return self
        if (
            self.observation_lag_days is None
            or self.next_monitor_due_on is None
            or self.oldest_official_source_updated_at is None
        ):
            raise PydanticCustomError(
                "prospective_health_incomplete",
                "available health requires lag, due date, and source timestamp",
            )
        expected_status = (
            "stale"
            if self.observation_lag_days
            > self.max_observation_lag_days
            else "current"
        )
        expected_reason = (
            "observation_lag_exceeded"
            if expected_status == "stale"
            else "current"
        )
        if self.status != expected_status or self.reason != expected_reason:
            raise PydanticCustomError(
                "prospective_health_consistency",
                "health status and observation lag disagree",
            )
        return self


class ParcelIntelIndex(BaseModel):
    boroughs: list[ParcelIntelBorough] = Field(default_factory=list)
    generated_at: Optional[datetime] = None
    feed_generation: Optional[str] = None
    model_metadata: dict[str, Any] = Field(default_factory=dict)
    data_sources: dict[str, Any] = Field(default_factory=dict)
    quality_gate: dict[str, Any] = Field(default_factory=dict)
    generation_diff: dict[str, Any] = Field(default_factory=dict)
    inference_replay: dict[str, Any] = Field(default_factory=dict)
    prospective_validation: Optional[
        ParcelProspectiveValidationStatus
    ] = None
    prospective_validation_health: ParcelProspectiveValidationHealth = Field(
        default_factory=lambda: ParcelProspectiveValidationHealth(
            status="unavailable",
            reason="status_missing_or_invalid",
        )
    )
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

ParcelProductEventName = Literal[
    "parcel_opened",
    "saved_view_applied",
    "decision_audit_opened",
    "underwriting_opened",
    "underwriting_assumptions_changed",
]

ParcelProductEventSource = Literal[
    "direct",
    "map",
    "ranking",
    "action_queue",
    "watchlist",
    "saved_views",
    "decision_posture",
    "audit_tab",
    "underwrite_tab",
    "base_assumptions",
]

_PARCEL_PRODUCT_EVENT_SOURCES: dict[str, set[str]] = {
    "parcel_opened": {"direct", "map", "ranking", "action_queue", "watchlist"},
    "saved_view_applied": {"saved_views"},
    "decision_audit_opened": {"decision_posture", "audit_tab"},
    "underwriting_opened": {"underwrite_tab"},
    "underwriting_assumptions_changed": {"base_assumptions"},
}


class ParcelProductEventCreate(BaseModel):
    """Value-minimized client event.

    Workflow and saved-view mutation counters are derived transactionally by
    the API from canonical writes. Only parcel opens, decision-audit opens,
    underwriting opens/first adjustments, and saved-view applies remain
    client-reported.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["citylens/parcel-product-event@v1"]
    event: ParcelProductEventName
    source: ParcelProductEventSource

    @model_validator(mode="after")
    def validate_source_for_event(self) -> "ParcelProductEventCreate":
        if self.source not in _PARCEL_PRODUCT_EVENT_SOURCES[self.event]:
            raise PydanticCustomError(
                "invalid_product_event_source",
                "source {source!r} is invalid for event {event!r}",
                {"source": self.source, "event": self.event},
            )
        return self


class ParcelWorkflowUpdate(BaseModel):
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    stage: ParcelWorkflowStage = "new"
    notes: str = Field(default="", max_length=4000)
    tags: list[str] = Field(default_factory=list, max_length=10)
    assignee: Optional[str] = Field(default=None, max_length=128)
    watching: bool = True
    decision_reason: Optional[str] = Field(default=None, max_length=80)
    outcome: Optional[ParcelWorkflowOutcome] = "unknown"
    next_action: Optional[str] = Field(default=None, max_length=240)
    next_action_due_date: Optional[date] = None
    snapshot: "ParcelWorkflowSnapshot" = Field(
        default_factory=lambda: ParcelWorkflowSnapshot(),
        description=(
            "Deprecated client hint. The API captures the canonical current "
            "feed snapshot on first save and preserves it immutably."
        ),
    )


class ParcelWorkflowSnapshot(BaseModel):
    """Small, typed baseline used to detect decision-relevant parcel changes."""

    address: Optional[str] = Field(default=None, max_length=256)
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
    mandatory_inclusionary_housing: Optional[bool] = None
    nearest_transit_complex_id: Optional[str] = Field(default=None, max_length=32)
    nearest_transit_station_name: Optional[str] = Field(
        default=None, max_length=160
    )
    nearest_transit_station_distance_m: Optional[int] = Field(
        default=None, ge=0
    )
    transit_access_tier: Optional[
        Literal["very_close", "walkable", "limited", "distant"]
    ] = None
    transit_data_as_of: Optional[str] = Field(default=None, max_length=32)
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


class ParcelWorkflowConfidenceInterval(BaseModel):
    confidence_level: Literal[0.95]
    lower: float = Field(ge=0, le=1)
    upper: float = Field(ge=0, le=1)


class ParcelWorkflowRate(BaseModel):
    numerator: int
    denominator: int
    rate: Optional[float] = None
    confidence_interval: Optional[ParcelWorkflowConfidenceInterval] = None
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
    confidence_interval: Optional[ParcelWorkflowConfidenceInterval] = None
    sufficient_denominator: bool


class ParcelWorkflowHorizonDefinition(BaseModel):
    milestone: ParcelWorkflowMilestone
    label: str
    horizon_days: int = Field(ge=1)


class ParcelWorkflowAnalyticsMethodology(BaseModel):
    schema_version: Literal[
        "citylens/parcel-workflow-analytics-methodology@v2"
    ]
    analytics_schema_version: Literal[
        "citylens/parcel-workflow-analytics@v3"
    ]
    horizons: list[ParcelWorkflowHorizonDefinition]
    minimum_cohort_size: int = Field(ge=1)
    minimum_rate_denominator: int = Field(ge=1)
    confidence_level: Literal[0.95]
    selection_scope: str
    timestamp_semantics: str
    uncertainty_semantics: str
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
    contacted_confidence_interval: Optional[
        ParcelWorkflowConfidenceInterval
    ] = None
    qualified_rate: Optional[float] = None
    qualified_confidence_interval: Optional[
        ParcelWorkflowConfidenceInterval
    ] = None
    close_rate: Optional[float] = None
    close_confidence_interval: Optional[
        ParcelWorkflowConfidenceInterval
    ] = None


class ParcelWorkflowAnalytics(BaseModel):
    schema_version: Literal["citylens/parcel-workflow-analytics@v3"]
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


ParcelWorkflowOutcomeLabelState = Literal[
    "pending",
    "positive",
    "negative",
    "unavailable_history",
]


class ParcelWorkflowOutcomeLabel(BaseModel):
    milestone: ParcelWorkflowMilestone
    horizon_days: int = Field(ge=1)
    state: ParcelWorkflowOutcomeLabelState
    eligible: bool
    value: Optional[bool] = None
    reached_at: Optional[datetime] = None
    days_to_milestone: Optional[int] = Field(default=None, ge=0)


class ParcelWorkflowOutcomeExportRow(BaseModel):
    """Value-minimized, maturity-safe prospective label row."""

    bbl: str = Field(pattern=r"^[1-5][0-9]{9}$")
    borough: Literal[
        "manhattan", "brooklyn", "queens", "bronx", "staten_island"
    ]
    saved_at: datetime
    archived_at: Optional[datetime] = None
    followup_days: int = Field(ge=0)
    stage: ParcelWorkflowStage
    outcome: ParcelWorkflowOutcome
    decision_reason_category: Optional[str] = None
    event_history_observed: bool
    event_count: int = Field(ge=0)
    feed_generated_at: Optional[str] = None
    property_facts_as_of: Optional[str] = None
    citywide_rank: Optional[int] = Field(default=None, ge=1)
    acquisition_rank: Optional[int] = Field(default=None, ge=1)
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
    saved_model_score: Optional[float] = Field(default=None, ge=0, le=1)
    labels: list[ParcelWorkflowOutcomeLabel]


class ParcelWorkflowOutcomeExport(BaseModel):
    schema_version: Literal["citylens/parcel-workflow-outcome-export@v1"]
    methodology_schema_version: Literal[
        "citylens/parcel-workflow-analytics-methodology@v2"
    ]
    generated_at: datetime
    input_record_count: int = Field(ge=0)
    exported_record_count: int = Field(ge=0)
    excluded_invalid_saved_at_count: int = Field(ge=0)
    event_history_observed_count: int = Field(ge=0)
    rank_snapshot_count: int = Field(ge=0)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_semantics: str
    score_semantics: str
    privacy_contract: str
    excluded_private_fields: list[str]
    rows: list[ParcelWorkflowOutcomeExportRow]


ParcelWorkflowActionState = Literal[
    "overdue",
    "due_today",
    "due_soon",
    "scheduled",
    "unscheduled",
]


class ParcelWorkflowActionItem(BaseModel):
    bbl: str
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    address: Optional[str] = None
    stage: ParcelWorkflowStage
    outcome: ParcelWorkflowOutcome
    assignee: Optional[str] = None
    next_action: Optional[str] = None
    next_action_due_date: Optional[date] = None
    action_state: ParcelWorkflowActionState
    days_overdue: int = Field(default=0, ge=0)
    days_since_update: int = Field(ge=0)
    needs_assignee: bool
    needs_outcome_update: bool
    requires_attention: bool
    reminder_snoozed_until: Optional[datetime] = None
    is_snoozed: bool
    citywide_rank: Optional[int] = Field(default=None, ge=1)
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
    saved_at: datetime
    updated_at: datetime


class ParcelWorkflowActions(BaseModel):
    schema_version: Literal["citylens/parcel-workflow-actions@v1"]
    generated_at: datetime
    total_records: int = Field(ge=0)
    open_records: int = Field(ge=0)
    completed_records: int = Field(ge=0)
    overdue_count: int = Field(ge=0)
    due_today_count: int = Field(ge=0)
    due_soon_count: int = Field(ge=0)
    scheduled_count: int = Field(ge=0)
    unscheduled_count: int = Field(ge=0)
    unassigned_count: int = Field(ge=0)
    outcome_update_due_count: int = Field(ge=0)
    attention_count: int = Field(ge=0)
    snoozed_count: int = Field(ge=0)
    complete_plan_count: int = Field(ge=0)
    plan_coverage_rate: Optional[float] = Field(default=None, ge=0, le=1)
    assigned_count: int = Field(ge=0)
    assignee_coverage_rate: Optional[float] = Field(default=None, ge=0, le=1)
    outcome_current_count: int = Field(ge=0)
    outcome_current_rate: Optional[float] = Field(default=None, ge=0, le=1)
    items: list[ParcelWorkflowActionItem] = Field(default_factory=list)


class ParcelWorkflowReminderSnoozeRequest(BaseModel):
    days: Literal[0, 1, 3, 7, 14]


class ParcelWorkflowReminderSnoozeResponse(BaseModel):
    bbl: str
    reminder_snoozed_until: Optional[datetime] = None
    is_snoozed: bool


class ParcelWorkflowAlertSource(BaseModel):
    source: str
    as_of: Optional[str] = None
    url: Optional[str] = None
    supports: str


class ParcelWorkflowAlert(BaseModel):
    bbl: str
    borough: Literal["manhattan", "brooklyn", "queens", "bronx", "staten_island"]
    code: Literal[
        "removed_from_current_feed",
        "screened_out_of_current_feed",
        "eligible_below_published_cutoff",
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
        "mih_overlay_changed",
        "transit_access_changed",
        "imagery_change_signal_changed",
        "owner_portfolio_size_changed",
    ]
    severity: Literal["urgent", "high", "medium", "low"]
    title: str
    detail: str
    field: str
    before: Optional[Any] = None
    after: Optional[Any] = None
    current_disposition: Optional[
        Literal[
            "published",
            "eligible_below_cutoff",
            "screened_out",
            "not_evaluated",
        ]
    ] = None
    reason_codes: list[str] = Field(default_factory=list)
    recommended_action: Optional[str] = None
    source_evidence: list[ParcelWorkflowAlertSource] = Field(
        default_factory=list
    )
    parcel_available: bool = True


class ParcelWorkflowAlerts(BaseModel):
    schema_version: Literal[
        "citylens/parcel-workflow-alerts@v1",
        "citylens/parcel-workflow-alerts@v2",
    ]
    generated_at: datetime
    feed_generated_at: Optional[datetime] = None
    watched_count: int
    changed_lead_count: int
    alert_count: int
    removed_from_feed_count: int
    resolved_exit_count: int = 0
    unresolved_exit_count: int = 0
    screened_out_count: int = 0
    eligible_below_cutoff_count: int = 0
    severity_counts: dict[str, int]
    alerts: list[ParcelWorkflowAlert] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ParcelScreeningLedgerRow(BaseModel):
    """Value-minimized current status for one evaluated model candidate."""

    model_config = ConfigDict(extra="forbid")

    bbl: str = Field(pattern=r"^[1-5][0-9]{9}$")
    borough: Literal[
        "manhattan", "brooklyn", "queens", "bronx", "staten_island"
    ]
    model_rank: int = Field(ge=1)
    acquisition_rank: Optional[int] = Field(default=None, ge=1)
    acquisition_eligible: bool
    acquisition_status: Literal[
        "eligible",
        "active_project",
        "completed_project",
        "constrained",
        "incomplete_data",
    ]
    acquisition_exclusion_reasons: list[str] = Field(default_factory=list)
    published: bool
    latest_project_filing_year: Optional[int] = Field(
        default=None, ge=1900, le=2100
    )
    latest_project_status: Optional[str] = None
    latest_project_type: Optional[
        Literal[
            "new_building",
            "alt_co_new_building",
            "demolition",
            "land_use_entitlement",
        ]
    ] = None
    latest_project_job_number: Optional[str] = None
    latest_project_url: Optional[str] = None
    property_facts_as_of: Optional[str] = None
    ownership_as_of: Optional[str] = None
    project_activity_as_of: Optional[str] = None
    land_use_activity_as_of: Optional[str] = None

    @model_validator(mode="after")
    def validate_screening_classification(
        self,
    ) -> "ParcelScreeningLedgerRow":
        if self.acquisition_eligible:
            if (
                self.acquisition_status != "eligible"
                or self.acquisition_rank is None
                or self.acquisition_exclusion_reasons
            ):
                raise ValueError(
                    "eligible screening rows require eligible status, a "
                    "positive acquisition rank, and no exclusion reasons"
                )
        elif (
            self.acquisition_status == "eligible"
            or self.acquisition_rank is not None
            or not self.acquisition_exclusion_reasons
        ):
            raise ValueError(
                "screened-out rows require a non-eligible status, no "
                "acquisition rank, and at least one exclusion reason"
            )
        if self.latest_project_url is not None:
            parsed = urlsplit(self.latest_project_url)
            hostname = parsed.hostname
            if (
                parsed.scheme != "https"
                or hostname is None
                or not (
                    hostname == "nyc.gov"
                    or hostname.endswith(".nyc.gov")
                )
            ):
                raise ValueError(
                    "latest_project_url must be an official NYC HTTPS URL"
                )
        return self


class ParcelSavedSearchFilters(BaseModel):
    """The complete, restorable state of the citywide parcel explorer."""

    query: str = Field(default="", max_length=160)
    priority: Literal["all", "highest", "high_or_better"] = "all"
    opportunity: Literal[
        "all",
        "uncommitted",
        "assemblage",
        "tax_lien",
        "violations",
        "floodplain",
        "environmental_review",
        "mih",
        "transit_800m",
        "portfolio",
        "vacant_site",
        "ground_up_candidate",
        "conversion_or_overbuilt",
        "active_project",
    ] = "uncommitted"
    owner_portfolio_id: Optional[str] = Field(default=None, max_length=128)
    overlay: Literal["priority", "opportunity", "borough"] = "borough"

    @model_validator(mode="after")
    def normalize_and_validate(self) -> ParcelSavedSearchFilters:
        self.query = self.query.strip()
        if self.opportunity != "portfolio" and self.owner_portfolio_id is not None:
            raise PydanticCustomError(
                "invalid_saved_view_owner_focus",
                "owner_portfolio_id requires opportunity='portfolio'",
            )
        if self.owner_portfolio_id is not None:
            self.owner_portfolio_id = self.owner_portfolio_id.strip()
            if not self.owner_portfolio_id:
                self.owner_portfolio_id = None
        return self


class ParcelSavedSearchUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    borough: Literal[
        "all", "manhattan", "brooklyn", "queens", "bronx", "staten_island"
    ]
    filters: ParcelSavedSearchFilters = Field(
        default_factory=ParcelSavedSearchFilters
    )
    # Saved views are persistence only. CityLens does not yet deliver scheduled
    # saved-search alerts, so accepting daily/weekly would be a false promise.
    alert_frequency: Literal["off"] = "off"

    @model_validator(mode="after")
    def normalize_name(self) -> ParcelSavedSearchUpdate:
        self.name = self.name.strip()
        if not self.name:
            raise PydanticCustomError(
                "blank_saved_view_name",
                "name must not be blank",
            )
        return self


class ParcelSavedSearch(ParcelSavedSearchUpdate):
    schema_version: Literal["citylens/parcel-saved-view@v2"]
    search_id: str
    created_at: datetime
    updated_at: datetime

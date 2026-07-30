"""Private, generation-bound practitioner reviews of ranked parcel leads."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Response

from ..models.schemas import (
    ParcelLeadReview,
    ParcelLeadReviewIndex,
    ParcelLeadReviewRequest,
    ParcelLeadReviewState,
    ParcelLeadReviewVerdictCounts,
)
from ..services.auth import require_auth
from ..services.auth_context import AuthContext
from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.rate_limit import enforce_token_bucket
from .parcel_intel import ParcelIntelRegistry, get_gcs, get_registry
from .parcel_workflow import get_store

router = APIRouter(tags=["parcel-lead-reviews"])

_GENERATION_RE = re.compile(
    r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
)
_PRIVATE_CACHE = "private, no-store"
_PRIVATE_VARY = "Authorization, X-API-Key"


def _current_parcel(
    *,
    bbl: str,
    gcs: GcsArtifacts,
    registry: ParcelIntelRegistry,
):
    row, manifest = registry.parcel(gcs, bbl)
    generation = (
        manifest.get("artifact_generation")
        if isinstance(manifest, dict)
        else None
    )
    if (
        not isinstance(generation, str)
        or _GENERATION_RE.fullmatch(generation) is None
    ):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "PARCEL_REVIEW_GENERATION_UNAVAILABLE",
                "message": (
                    "The current parcel feed has no immutable generation "
                    "receipt. Review recording is temporarily unavailable."
                ),
            },
        )
    return row, generation


def _set_private_headers(response: Response) -> None:
    response.headers["Cache-Control"] = _PRIVATE_CACHE
    response.headers["Vary"] = _PRIVATE_VARY


@router.get(
    "/parcel-intel/lead-reviews",
    response_model=ParcelLeadReviewIndex,
)
def list_lead_reviews(
    response: Response,
    auth: AuthContext = Depends(require_auth),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
    store: FirestoreStore = Depends(get_store),
) -> ParcelLeadReviewIndex:
    """Return this user's coverage for the current immutable feed only."""

    enforce_token_bucket(
        key=f"parcel-lead-review-index:{auth.app_user_id}",
        capacity=30,
        refill_per_second=0.1,
    )
    index = registry.index(gcs)
    generation = index.feed_generation
    if (
        not isinstance(generation, str)
        or _GENERATION_RE.fullmatch(generation) is None
    ):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "PARCEL_REVIEW_GENERATION_UNAVAILABLE",
                "message": (
                    "The current parcel feed has no immutable generation "
                    "receipt. Review coverage is temporarily unavailable."
                ),
            },
        )
    available_count = sum(
        max(0, borough.count) for borough in index.boroughs
    )
    items = [
        ParcelLeadReview.model_validate(review)
        for review in store.list_parcel_lead_reviews(
            app_user_id=auth.app_user_id,
            feed_generation=generation,
        )
    ]
    verdict_counts = {
        "pursue": 0,
        "watch": 0,
        "pass": 0,
        "unclear": 0,
    }
    for item in items:
        verdict_counts[item.verdict] += 1
    reviewed_count = len(items)
    _set_private_headers(response)
    return ParcelLeadReviewIndex(
        schema_version="citylens/parcel-lead-review-index@v1",
        current_feed_generation=generation,
        available_count=available_count,
        reviewed_count=reviewed_count,
        unreviewed_count=available_count - reviewed_count,
        verdict_counts=ParcelLeadReviewVerdictCounts.model_validate(
            verdict_counts
        ),
        items=items,
    )


@router.get(
    "/parcel-intel/lead-reviews/{bbl}",
    response_model=ParcelLeadReviewState,
)
def get_lead_review(
    bbl: str,
    response: Response,
    auth: AuthContext = Depends(require_auth),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
    store: FirestoreStore = Depends(get_store),
) -> ParcelLeadReviewState:
    """Return this user's review for the current immutable feed only."""

    enforce_token_bucket(
        key=f"parcel-lead-review-read:{auth.app_user_id}",
        capacity=120,
        refill_per_second=1.0,
    )
    _row, generation = _current_parcel(
        bbl=bbl,
        gcs=gcs,
        registry=registry,
    )
    review = store.get_parcel_lead_review(
        app_user_id=auth.app_user_id,
        bbl=bbl,
        feed_generation=generation,
    )
    _set_private_headers(response)
    return ParcelLeadReviewState(
        schema_version="citylens/parcel-lead-review-state@v1",
        current_feed_generation=generation,
        review=(
            ParcelLeadReview.model_validate(review)
            if review is not None
            else None
        ),
    )


@router.put(
    "/parcel-intel/lead-reviews/{bbl}",
    response_model=ParcelLeadReview,
)
def put_lead_review(
    bbl: str,
    request: ParcelLeadReviewRequest,
    response: Response,
    auth: AuthContext = Depends(require_auth),
    gcs: GcsArtifacts = Depends(get_gcs),
    registry: ParcelIntelRegistry = Depends(get_registry),
    store: FirestoreStore = Depends(get_store),
) -> ParcelLeadReview:
    """Record relevance feedback without changing rank or workflow state."""

    enforce_token_bucket(
        key=f"parcel-lead-review:{auth.app_user_id}",
        capacity=30,
        refill_per_second=0.25,
    )
    row, generation = _current_parcel(
        bbl=bbl,
        gcs=gcs,
        registry=registry,
    )
    if request.expected_feed_generation != generation:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PARCEL_REVIEW_GENERATION_CHANGED",
                "message": (
                    "The parcel ranking changed after this panel opened. "
                    "Reload the current feed before reviewing the lead."
                ),
                "current_feed_generation": generation,
            },
        )

    review, mutation = store.upsert_parcel_lead_review(
        app_user_id=auth.app_user_id,
        bbl=bbl,
        feed_generation=generation,
        verdict=request.verdict,
        reason_codes=list(request.reason_codes),
        snapshot={
            "citywide_rank": row.citywide_rank,
            "acquisition_rank": row.acquisition_rank,
            "priority_tier": row.priority_tier,
            "opportunity_category": row.opportunity_category,
        },
    )
    _set_private_headers(response)
    response.headers["X-CityLens-Lead-Review-Mutation"] = mutation
    return ParcelLeadReview.model_validate(review)

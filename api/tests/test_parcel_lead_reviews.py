from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.models.schemas import (
    ParcelIntelBorough,
    ParcelIntelIndex,
    ParcelIntelRow,
)
from app.routes import parcel_reviews
from app.services.firestore_store import parcel_lead_review_id

GENERATION = "20260730T092749819158Z-daf06394d35b"
BBL = "3058920038"


class FakeLeadReviewStore:
    def __init__(self) -> None:
        self.reviews: dict[tuple[str, str], dict] = {}
        self.upsert_calls = 0

    def get_parcel_lead_review(
        self,
        *,
        app_user_id: str,
        bbl: str,
        feed_generation: str,
    ) -> dict | None:
        del app_user_id
        return self.reviews.get((feed_generation, bbl))

    def upsert_parcel_lead_review(
        self,
        *,
        app_user_id: str,
        bbl: str,
        feed_generation: str,
        verdict: str,
        reason_codes: list[str],
        snapshot: dict,
    ) -> tuple[dict, str]:
        del app_user_id
        self.upsert_calls += 1
        key = (feed_generation, bbl)
        existing = self.reviews.get(key)
        normalized = sorted(set(reason_codes))
        if (
            existing
            and existing["verdict"] == verdict
            and existing["reason_codes"] == normalized
        ):
            return existing, "unchanged"
        now = datetime.now(timezone.utc)
        review = {
            "schema_version": "citylens/parcel-lead-review@v1",
            "review_id": parcel_lead_review_id(
                feed_generation=feed_generation,
                bbl=bbl,
            ),
            "bbl": bbl,
            "feed_generation": feed_generation,
            "verdict": verdict,
            "reason_codes": normalized,
            **snapshot,
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
            "revision": (existing["revision"] if existing else 0) + 1,
        }
        self.reviews[key] = review
        return review, "updated" if existing else "created"

    def list_parcel_lead_reviews(
        self,
        *,
        app_user_id: str,
        feed_generation: str,
    ) -> list[dict]:
        del app_user_id
        return sorted(
            (
                review
                for (generation, _bbl), review in self.reviews.items()
                if generation == feed_generation
            ),
            key=lambda review: review["citywide_rank"],
        )


class FakeRegistry:
    def index(self, _gcs) -> ParcelIntelIndex:
        return ParcelIntelIndex(
            boroughs=[
                ParcelIntelBorough(
                    slug=borough,
                    display_name=borough.replace("_", " ").title(),
                    count=1_000,
                )
                for borough in (
                    "manhattan",
                    "bronx",
                    "brooklyn",
                    "queens",
                    "staten_island",
                )
            ],
            feed_generation=GENERATION,
        )

    def parcel(self, _gcs, bbl: str):
        if bbl != BBL:
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail="Parcel not found")
        return (
            ParcelIntelRow(
                bbl=BBL,
                borough="brooklyn",
                citywide_rank=42,
                acquisition_rank=39,
                priority_tier="highest",
                opportunity_category="ground_up_candidate",
            ),
            {"artifact_generation": GENERATION},
        )


def _install_dependencies(store: FakeLeadReviewStore) -> None:
    app.dependency_overrides[parcel_reviews.get_store] = lambda: store
    app.dependency_overrides[parcel_reviews.get_gcs] = lambda: object()
    app.dependency_overrides[parcel_reviews.get_registry] = (
        lambda: FakeRegistry()
    )


def _payload(
    *,
    verdict: str = "pass",
    reasons: list[str] | None = None,
    generation: str = GENERATION,
) -> dict:
    return {
        "schema_version": "citylens/parcel-lead-review-request@v1",
        "expected_feed_generation": generation,
        "verdict": verdict,
        "reason_codes": reasons or ["active_or_completed_project"],
    }


def test_lead_review_requires_authentication() -> None:
    store = FakeLeadReviewStore()
    _install_dependencies(store)

    response = TestClient(app).get(
        f"/v1/parcel-intel/lead-reviews/{BBL}"
    )

    assert response.status_code == 401


def test_lead_review_state_is_private_and_generation_bound(
    auth_override,
) -> None:
    auth_override()
    store = FakeLeadReviewStore()
    _install_dependencies(store)

    response = TestClient(app).get(
        f"/v1/parcel-intel/lead-reviews/{BBL}"
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "schema_version": "citylens/parcel-lead-review-state@v1",
        "current_feed_generation": GENERATION,
        "review": None,
    }
    assert response.headers["cache-control"] == "private, no-store"
    assert "authorization" in response.headers["vary"].lower()


def test_lead_review_index_returns_only_current_generation_coverage(
    auth_override,
) -> None:
    auth_override()
    store = FakeLeadReviewStore()
    _install_dependencies(store)
    client = TestClient(app)

    created = client.put(
        f"/v1/parcel-intel/lead-reviews/{BBL}",
        json=_payload(),
    )
    assert created.status_code == 200, created.text
    store.reviews[
        ("20260729T092749819158Z-daf06394d35b", BBL)
    ] = created.json() | {
        "feed_generation": (
            "20260729T092749819158Z-daf06394d35b"
        )
    }

    response = client.get("/v1/parcel-intel/lead-reviews")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "schema_version": "citylens/parcel-lead-review-index@v1",
        "current_feed_generation": GENERATION,
        "available_count": 5_000,
        "reviewed_count": 1,
        "unreviewed_count": 4_999,
        "verdict_counts": {
            "pursue": 0,
            "watch": 0,
            "pass": 1,
            "unclear": 0,
        },
        "items": [created.json()],
    }
    assert response.headers["cache-control"] == "private, no-store"
    assert "authorization" in response.headers["vary"].lower()


def test_lead_review_records_server_owned_snapshot_idempotently(
    auth_override,
) -> None:
    auth_override()
    store = FakeLeadReviewStore()
    _install_dependencies(store)
    client = TestClient(app)

    created = client.put(
        f"/v1/parcel-intel/lead-reviews/{BBL}",
        json=_payload(),
    )
    repeated = client.put(
        f"/v1/parcel-intel/lead-reviews/{BBL}",
        json=_payload(),
    )

    assert created.status_code == 200, created.text
    body = created.json()
    assert body["review_id"] == parcel_lead_review_id(
        feed_generation=GENERATION,
        bbl=BBL,
    )
    assert body["feed_generation"] == GENERATION
    assert body["verdict"] == "pass"
    assert body["reason_codes"] == ["active_or_completed_project"]
    assert body["citywide_rank"] == 42
    assert body["acquisition_rank"] == 39
    assert body["priority_tier"] == "highest"
    assert body["opportunity_category"] == "ground_up_candidate"
    assert body["revision"] == 1
    assert created.headers[
        "x-citylens-lead-review-mutation"
    ] == "created"
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["revision"] == 1
    assert repeated.headers[
        "x-citylens-lead-review-mutation"
    ] == "unchanged"


def test_lead_review_rejects_stale_generation(auth_override) -> None:
    auth_override()
    store = FakeLeadReviewStore()
    _install_dependencies(store)

    response = TestClient(app).put(
        f"/v1/parcel-intel/lead-reviews/{BBL}",
        json=_payload(
            generation="20260729T092749819158Z-daf06394d35b"
        ),
    )

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == (
        "PARCEL_REVIEW_GENERATION_CHANGED"
    )
    assert response.json()["detail"][
        "current_feed_generation"
    ] == GENERATION
    assert store.upsert_calls == 0


def test_lead_review_rejects_reason_from_another_verdict(
    auth_override,
) -> None:
    auth_override()
    store = FakeLeadReviewStore()
    _install_dependencies(store)

    response = TestClient(app).put(
        f"/v1/parcel-intel/lead-reviews/{BBL}",
        json=_payload(
            verdict="pursue",
            reasons=["active_or_completed_project"],
        ),
    )

    assert response.status_code == 422
    assert store.upsert_calls == 0

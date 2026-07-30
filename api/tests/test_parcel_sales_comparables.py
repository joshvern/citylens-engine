from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
from fastapi import HTTPException

from app.models.schemas import ParcelSalesComparablesResponse
from app.services.parcel_sales_comparables import (
    PLUTO_DATASET_ID,
    SALES_DATASET_ID,
    ParcelSalesComparableService,
)

SUBJECT_BBL = "3058920038"
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def _sale(
    bbl: str,
    *,
    address: str,
    sale_price: str = "1500000",
    sale_date: str = "2025-10-15T00:00:00.000",
    latitude: str = "40.6380",
    longitude: str = "-74.0300",
    apartment_number: str = "",
    land_square_feet: str = "9000",
    gross_square_feet: str = "3200",
    building_class: str = "B2",
) -> dict[str, str]:
    return {
        "bbl": bbl,
        "address": address,
        "zip_code": "11209",
        "building_class_category": "01 ONE FAMILY DWELLINGS",
        "building_class_as_of_final": building_class,
        "apartment_number": apartment_number,
        "land_square_feet": land_square_feet,
        "gross_square_feet": gross_square_feet,
        "residential_units": "2",
        "commercial_units": "0",
        "total_units": "2",
        "year_built": "1910",
        "sale_price": sale_price,
        "sale_date": sale_date,
        "latitude": latitude,
        "longitude": longitude,
    }


def _subject_row(*, bbl: str = SUBJECT_BBL) -> dict[str, str]:
    return {
        "bbl": bbl,
        "zipcode": "11209",
        "latitude": "40.6368",
        "longitude": "-74.0312",
        "bldgclass": "B3",
        "lotarea": "9260",
        "bldgarea": "3006",
    }


def _service(
    *,
    subject_rows: list[dict[str, str]] | None = None,
    sales_rows: list[dict[str, str]] | None = None,
) -> tuple[ParcelSalesComparableService, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if PLUTO_DATASET_ID in request.url.path:
            return httpx.Response(
                200,
                json=(
                    [_subject_row()]
                    if subject_rows is None
                    else subject_rows
                ),
            )
        if SALES_DATASET_ID in request.url.path:
            return httpx.Response(
                200,
                json=[] if sales_rows is None else sales_rows,
                headers={
                    "X-SODA2-Truth-Last-Modified": (
                        "Tue, 09 Jun 2026 14:00:00 GMT"
                    )
                },
            )
        raise AssertionError(f"Unexpected URL: {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    return (
        ParcelSalesComparableService(
            http_client=client,
            now=lambda: NOW,
        ),
        requests,
    )


def test_selects_explainable_recent_tax_lot_sales_and_caches() -> None:
    service, requests = _service(
        sales_rows=[
            _sale(
                "3058900040",
                address="450 OVINGTON AVENUE",
            ),
            _sale(
                "3058900041",
                address="452 OVINGTON AVENUE",
                sale_date="2024-04-10T00:00:00.000",
                land_square_feet="12000",
                gross_square_feet="4100",
            ),
            _sale(
                SUBJECT_BBL,
                address="SUBJECT",
            ),
            _sale(
                "3058900042",
                address="UNIT SALE",
                apartment_number="4A",
            ),
            _sale(
                "3058900043",
                address="NOMINAL TRANSFER",
                sale_price="10",
            ),
            _sale(
                "3058900044",
                address="TOO FAR",
                latitude="40.7500",
                longitude="-73.8500",
            ),
            _sale(
                "3058900045",
                address="PROXIMITY ONLY",
                building_class="G2",
                land_square_feet="20000",
                gross_square_feet="9000",
            ),
        ]
    )

    first = service.get(
        SUBJECT_BBL,
        dossier_row={"la": 9260, "ba": 3006, "bc": "B3"},
    )
    second = service.get(
        SUBJECT_BBL,
        dossier_row={"la": 9260, "ba": 3006, "bc": "B3"},
    )
    payload = ParcelSalesComparablesResponse.model_validate(first)

    assert second is first
    assert len(requests) == 2
    assert payload.status == "available"
    assert [item.bbl for item in payload.comparables] == [
        "3058900040",
        "3058900041",
    ]
    assert payload.summary is not None
    assert payload.summary.comparable_count == 2
    assert payload.source_dataset_id == SALES_DATASET_ID
    assert payload.source_data_updated_at == datetime(
        2026,
        6,
        9,
        14,
        0,
        tzinfo=timezone.utc,
    )
    assert payload.comparables[0].match_reasons[0] == (
        "Same building-class family"
    )
    assert "sale_price >= 100000" in requests[1].url.params["$where"]


def test_current_pluto_row_must_match_requested_bbl() -> None:
    service, requests = _service(
        subject_rows=[_subject_row(bbl="3058920039")],
    )

    payload = ParcelSalesComparablesResponse.model_validate(
        service.get(
            SUBJECT_BBL,
            dossier_row={"la": 9260, "ba": 3006, "bc": "B3"},
        )
    )

    assert payload.status == "insufficient_source_facts"
    assert payload.comparables == []
    assert payload.summary is None
    assert len(requests) == 1


def test_source_failures_are_explicit_service_unavailable() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "rate limited"})

    service = ParcelSalesComparableService(
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler),
        ),
        now=lambda: NOW,
    )

    with pytest.raises(HTTPException) as exc_info:
        service.get(
            SUBJECT_BBL,
            dossier_row={"la": 9260, "ba": 3006, "bc": "B3"},
        )

    assert exc_info.value.status_code == 503
    assert "temporarily unavailable" in str(exc_info.value.detail)

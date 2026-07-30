"""Bounded, official-source comparable-sale screening for one NYC tax lot.

This service deliberately does not estimate value. It selects recent priced
tax-lot-level records from the NYC Department of Finance annualized sales
dataset using current PLUTO location facts, then explains why each record was
selected. Unit sales, zero-consideration transfers, malformed records, and
the subject BBL are excluded.
"""

from __future__ import annotations

import math
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from statistics import median
from typing import Any, Callable

import httpx
from fastapi import HTTPException

PLUTO_DATASET_ID = "64uk-42ks"
SALES_DATASET_ID = "w2pb-icbu"
PLUTO_RESOURCE_URL = (
    f"https://data.cityofnewyork.us/resource/{PLUTO_DATASET_ID}.json"
)
SALES_RESOURCE_URL = (
    f"https://data.cityofnewyork.us/resource/{SALES_DATASET_ID}.json"
)
SALES_SOURCE_URL = (
    "https://data.cityofnewyork.us/City-Government/"
    "NYC-Citywide-Annualized-Calendar-Sales-Update/w2pb-icbu"
)
COMPARABLE_SCHEMA = "citylens/parcel-sales-comparables@v1"
MAX_SOURCE_ROWS = 2_000
MAX_COMPARABLES = 5
MAX_DISTANCE_MILES = 2.0
MIN_SALE_PRICE = 100_000
_BBL_RE = re.compile(r"^[1-5][0-9]{9}$")


@dataclass(frozen=True)
class _Subject:
    bbl: str
    zip_code: str
    latitude: float
    longitude: float
    lot_area_sqft: float | None
    building_area_sqft: float | None
    building_class: str | None


@dataclass(frozen=True)
class _Candidate:
    payload: dict[str, Any]
    score: float
    sale_date: date
    distance_miles: float


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.replace(",", "").strip())
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _positive(value: Any) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def _text(value: Any, maximum: int = 200) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    return normalized[:maximum] if normalized else None


def _haversine_miles(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    radius_miles = 3958.7613
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    delta_lat = lat_b - lat_a
    delta_lon = math.radians(longitude_b - longitude_a)
    value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat_a)
        * math.cos(lat_b)
        * math.sin(delta_lon / 2) ** 2
    )
    return 2 * radius_miles * math.asin(min(1.0, math.sqrt(value)))


def _relative_gap(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or left <= 0 or right <= 0:
        return None
    return abs(math.log(left / right))


def _ratio_reason(
    label: str,
    subject: float | None,
    candidate: float | None,
) -> str | None:
    if subject is None or candidate is None or subject <= 0:
        return None
    difference = abs(candidate - subject) / subject
    if difference <= 0.15:
        return f"{label} within 15%"
    if difference <= 0.35:
        return f"{label} within 35%"
    return None


def _last_modified(headers: httpx.Headers) -> datetime | None:
    value = headers.get("x-soda2-truth-last-modified") or headers.get(
        "last-modified"
    )
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


class ParcelSalesComparableService:
    """Fetch and cache a transparent official comparable-sale screen."""

    def __init__(
        self,
        *,
        http_client: httpx.Client | None = None,
        now: Callable[[], datetime] = _utc_now,
        cache_ttl: timedelta = timedelta(hours=6),
        max_cached_parcels: int = 512,
    ) -> None:
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(6.0),
            headers={"User-Agent": "CityLens parcel evidence/1.0"},
        )
        self._now = now
        self._cache_ttl = cache_ttl
        self._max_cached_parcels = max_cached_parcels
        self._lock = threading.Lock()
        self._cache: OrderedDict[
            str, tuple[datetime, dict[str, Any]]
        ] = OrderedDict()

    @staticmethod
    def _unavailable() -> HTTPException:
        return HTTPException(
            status_code=503,
            detail=(
                "Official comparable-sale evidence is temporarily unavailable"
            ),
        )

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, str],
    ) -> tuple[list[dict[str, Any]], httpx.Headers]:
        try:
            response = self._http.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        except (
            httpx.HTTPError,
            TypeError,
            ValueError,
        ) as exc:
            raise self._unavailable() from exc
        if not isinstance(payload, list) or not all(
            isinstance(row, dict) for row in payload
        ):
            raise self._unavailable()
        return payload, response.headers

    def _subject(
        self,
        bbl: str,
        *,
        dossier_row: dict[str, Any],
    ) -> _Subject | None:
        block = str(int(bbl[1:6]))
        lot = str(int(bbl[6:]))
        rows, _ = self._get_json(
            PLUTO_RESOURCE_URL,
            params={
                "$select": (
                    "bbl,zipcode,latitude,longitude,bldgclass,"
                    "lotarea,bldgarea"
                ),
                "$where": (
                    f"borocode='{bbl[0]}' AND block='{block}' AND lot='{lot}'"
                ),
                "$limit": "2",
            },
        )
        if len(rows) != 1:
            return None
        row = rows[0]
        source_bbl = _text(row.get("bbl"), 10)
        latitude = _number(row.get("latitude"))
        longitude = _number(row.get("longitude"))
        zip_code = _text(row.get("zipcode"), 10)
        if (
            source_bbl != bbl
            or latitude is None
            or longitude is None
            or not zip_code
            or not (-90 <= latitude <= 90)
            or not (-180 <= longitude <= 180)
        ):
            return None
        return _Subject(
            bbl=bbl,
            zip_code=zip_code,
            latitude=latitude,
            longitude=longitude,
            lot_area_sqft=(
                _positive(dossier_row.get("la"))
                or _positive(row.get("lotarea"))
            ),
            building_area_sqft=(
                _positive(dossier_row.get("ba"))
                or _positive(row.get("bldgarea"))
            ),
            building_class=(
                _text(dossier_row.get("bc"), 12)
                or _text(row.get("bldgclass"), 12)
            ),
        )

    def _candidate(
        self,
        raw: dict[str, Any],
        *,
        subject: _Subject,
    ) -> _Candidate | None:
        bbl = _text(raw.get("bbl"), 10)
        sale_date = _date(raw.get("sale_date"))
        sale_price = _positive(raw.get("sale_price"))
        lot_area = _positive(raw.get("land_square_feet"))
        gross_area = _positive(raw.get("gross_square_feet"))
        latitude = _number(raw.get("latitude"))
        longitude = _number(raw.get("longitude"))
        apartment = _text(raw.get("apartment_number"), 40)
        if (
            bbl is None
            or _BBL_RE.fullmatch(bbl) is None
            or bbl == subject.bbl
            or sale_date is None
            or sale_price is None
            or lot_area is None
            or latitude is None
            or longitude is None
            or apartment is not None
            or sale_date > self._now().date()
            or sale_price < MIN_SALE_PRICE
            or not (-90 <= latitude <= 90)
            or not (-180 <= longitude <= 180)
        ):
            return None

        distance = _haversine_miles(
            subject.latitude,
            subject.longitude,
            latitude,
            longitude,
        )
        if distance > MAX_DISTANCE_MILES:
            return None

        lot_gap = _relative_gap(subject.lot_area_sqft, lot_area)
        if lot_gap is not None and lot_gap > math.log(3):
            return None

        candidate_class = _text(
            raw.get("building_class_as_of_final"),
            12,
        )
        same_class_family = bool(
            subject.building_class
            and candidate_class
            and subject.building_class[0] == candidate_class[0]
        )
        building_gap = _relative_gap(
            subject.building_area_sqft,
            gross_area,
        )
        lot_similarity_score = (
            1.0
            if lot_gap is None
            else 3.0 - min(3.0, lot_gap * 2.0)
        )
        building_similarity_score = (
            0.5
            if building_gap is None
            else 2.0 - min(2.0, building_gap)
        )
        recency_days = max(0, (self._now().date() - sale_date).days)
        score = (
            (4.0 if same_class_family else 0.0)
            + lot_similarity_score
            + building_similarity_score
            + (2.0 - min(2.0, distance))
            + max(0.0, 1.0 - recency_days / (365.25 * 4))
        )

        reasons: list[str] = []
        if same_class_family:
            reasons.append("Same building-class family")
        lot_reason = _ratio_reason(
            "Lot area",
            subject.lot_area_sqft,
            lot_area,
        )
        if lot_reason:
            reasons.append(lot_reason)
        building_reason = _ratio_reason(
            "Building area",
            subject.building_area_sqft,
            gross_area,
        )
        if building_reason:
            reasons.append(building_reason)
        if (
            subject.building_class
            and candidate_class
            and not same_class_family
        ):
            return None
        if lot_reason is None and building_reason is None:
            return None
        reasons.append(
            (
                f"Within {distance:.1f} miles"
                if distance >= 0.1
                else "Within 0.1 miles"
            )
        )

        payload = {
            "bbl": bbl,
            "address": _text(raw.get("address")) or f"BBL {bbl}",
            "sale_date": sale_date,
            "sale_price": sale_price,
            "distance_miles": round(distance, 2),
            "lot_area_sqft": lot_area,
            "gross_area_sqft": gross_area,
            "residential_units": _integer(raw.get("residential_units")),
            "commercial_units": _integer(raw.get("commercial_units")),
            "total_units": _integer(raw.get("total_units")),
            "year_built": _integer(raw.get("year_built")),
            "building_class": candidate_class,
            "building_class_category": _text(
                raw.get("building_class_category")
            ),
            "price_per_land_sqft": round(sale_price / lot_area, 2),
            "price_per_gross_sqft": (
                round(sale_price / gross_area, 2)
                if gross_area is not None
                else None
            ),
            "match_reasons": reasons[:3],
        }
        return _Candidate(
            payload=payload,
            score=score,
            sale_date=sale_date,
            distance_miles=distance,
        )

    def _build(
        self,
        bbl: str,
        *,
        dossier_row: dict[str, Any],
    ) -> dict[str, Any]:
        if _BBL_RE.fullmatch(bbl) is None:
            raise HTTPException(status_code=404, detail="Parcel not found")
        retrieved_at = self._now()
        subject = self._subject(bbl, dossier_row=dossier_row)
        if subject is None:
            return {
                "schema_version": COMPARABLE_SCHEMA,
                "status": "insufficient_source_facts",
                "subject_bbl": bbl,
                "search_zip_code": None,
                "query_window_start": date(retrieved_at.year - 3, 1, 1),
                "source_candidate_count": 0,
                "eligible_candidate_count": 0,
                "source_limit_reached": False,
                "comparables": [],
                "summary": None,
                "source_name": (
                    "NYC Department of Finance annualized property sales"
                ),
                "source_dataset_id": SALES_DATASET_ID,
                "source_url": SALES_SOURCE_URL,
                "source_data_updated_at": None,
                "source_retrieved_at": retrieved_at,
                "selection_method": (
                    "No comparable set was produced because current PLUTO "
                    "location facts were unavailable."
                ),
                "interpretation": (
                    "No value conclusion is available. Verify the parcel and "
                    "review recorded transactions manually."
                ),
            }

        start_date = date(retrieved_at.year - 3, 1, 1)
        rows, headers = self._get_json(
            SALES_RESOURCE_URL,
            params={
                "$select": (
                    "bbl,address,zip_code,building_class_category,"
                    "building_class_as_of_final,apartment_number,"
                    "land_square_feet,gross_square_feet,residential_units,"
                    "commercial_units,total_units,year_built,sale_price,"
                    "sale_date,latitude,longitude"
                ),
                "$where": (
                    f"borough='{bbl[0]}' AND "
                    f"zip_code='{subject.zip_code}' AND "
                    f"sale_price >= {MIN_SALE_PRICE} AND "
                    f"sale_date >= '{start_date.isoformat()}T00:00:00' AND "
                    "(apartment_number IS NULL OR apartment_number='')"
                ),
                "$order": "sale_date DESC",
                "$limit": str(MAX_SOURCE_ROWS),
            },
        )
        candidates = [
            candidate
            for raw in rows
            if (
                candidate := self._candidate(
                    raw,
                    subject=subject,
                )
            )
            is not None
        ]
        # Keep at most the most recent record for one BBL before ranking.
        latest_by_bbl: dict[str, _Candidate] = {}
        for candidate in sorted(
            candidates,
            key=lambda item: item.sale_date,
            reverse=True,
        ):
            latest_by_bbl.setdefault(candidate.payload["bbl"], candidate)
        ranked = sorted(
            latest_by_bbl.values(),
            key=lambda item: (
                -item.score,
                -item.sale_date.toordinal(),
                item.distance_miles,
                item.payload["bbl"],
            ),
        )
        selected = ranked[:MAX_COMPARABLES]
        comparables = [candidate.payload for candidate in selected]
        land_rates = [
            item["price_per_land_sqft"]
            for item in comparables
            if item["price_per_land_sqft"] is not None
        ]
        gross_rates = [
            item["price_per_gross_sqft"]
            for item in comparables
            if item["price_per_gross_sqft"] is not None
        ]
        prices = [item["sale_price"] for item in comparables]
        summary = (
            {
                "comparable_count": len(comparables),
                "median_sale_price": float(median(prices)),
                "median_price_per_land_sqft": (
                    round(float(median(land_rates)), 2)
                    if land_rates
                    else None
                ),
                "median_price_per_gross_sqft": (
                    round(float(median(gross_rates)), 2)
                    if gross_rates
                    else None
                ),
                "minimum_sale_price": min(prices),
                "maximum_sale_price": max(prices),
            }
            if comparables
            else None
        )
        return {
            "schema_version": COMPARABLE_SCHEMA,
            "status": "available" if comparables else "insufficient_sales",
            "subject_bbl": bbl,
            "search_zip_code": subject.zip_code,
            "query_window_start": start_date,
            "source_candidate_count": len(rows),
            "eligible_candidate_count": len(ranked),
            "source_limit_reached": len(rows) == MAX_SOURCE_ROWS,
            "comparables": comparables,
            "summary": summary,
            "source_name": (
                "NYC Department of Finance annualized property sales"
            ),
            "source_dataset_id": SALES_DATASET_ID,
            "source_url": SALES_SOURCE_URL,
            "source_data_updated_at": _last_modified(headers),
            "source_retrieved_at": retrieved_at,
            "selection_method": (
                "Recent priced records in the subject ZIP with a blank unit "
                f"field, at least ${MIN_SALE_PRICE:,.0f} consideration, "
                "reported land area, valid coordinates, and no subject BBL "
                "match. When both classes are reported, candidates must share "
                "the subject building-class family; every candidate must also "
                "have lot or building area within 35%. Eligible records are "
                "ranked by class family, physical scale, distance, and recency."
            ),
            "interpretation": (
                "This is a comparable-transaction screen, not an appraisal "
                "or land-value estimate. NYC DOF records may include related, "
                "partial, or otherwise non-arm's-length transfers. Verify the "
                "deed, interest transferred, physical condition, zoning, and "
                "development rights before relying on any record."
            ),
        }

    def get(
        self,
        bbl: str,
        *,
        dossier_row: dict[str, Any],
    ) -> dict[str, Any]:
        now = self._now()
        with self._lock:
            cached = self._cache.get(bbl)
            if cached is not None and now - cached[0] <= self._cache_ttl:
                self._cache.move_to_end(bbl)
                return cached[1]

        result = self._build(bbl, dossier_row=dossier_row)
        with self._lock:
            self._cache[bbl] = (now, result)
            self._cache.move_to_end(bbl)
            while len(self._cache) > self._max_cached_parcels:
                self._cache.popitem(last=False)
        return result

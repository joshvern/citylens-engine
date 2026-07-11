from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from pyproj import CRS, Transformer

from services.imagery_inputs import (
    _CURRENT_FOOTPRINTS_DEFAULT_URL,
    _current_footprints_url,
    _fetch_current_footprints,
    _stage_current_footprints,
    _stage_current_footprints_optional,
)


class FakeResponse:
    def __init__(self, payload: Any, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.status_checked = False

    def raise_for_status(self) -> None:
        self.status_checked = True
        if self.error is not None:
            raise self.error

    def json(self) -> Any:
        return self.payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response


class MemoryBlob:
    def __init__(self, objects: dict[str, bytes], object_name: str) -> None:
        self.objects = objects
        self.object_name = object_name

    def exists(self) -> bool:
        return self.object_name in self.objects

    def download_to_filename(self, filename: str) -> None:
        Path(filename).write_bytes(self.objects[self.object_name])

    def upload_from_filename(self, filename: str) -> None:
        self.objects[self.object_name] = Path(filename).read_bytes()


class MemoryBucket:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def blob(self, object_name: str) -> MemoryBlob:
        return MemoryBlob(self.objects, object_name)


class MemoryGcsClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def bucket(self, bucket: str) -> MemoryBucket:  # noqa: ARG002
        return MemoryBucket(self.objects)


def _polygon_feature(
    *,
    lon: float = -73.98,
    lat: float = 40.75,
    construction_year: Any = "2020",
    status: str = "Constructed",
    base_bbl: str = "1000010001",
) -> dict[str, Any]:
    delta = 0.0001
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [lon, lat],
                    [lon + delta, lat],
                    [lon + delta, lat + delta],
                    [lon, lat + delta],
                    [lon, lat],
                ]
            ],
        },
        "properties": {
            "construction_year": construction_year,
            "last_status_type": status,
            "geom_source": "Photogrammetric",
            "base_bbl": base_bbl,
            "mappluto_bbl": base_bbl,
        },
    }


def _ortho_bbox() -> tuple[float, float, float, float]:
    transformer = Transformer.from_crs(4326, 3857, always_xy=True)
    west, south = transformer.transform(-74.0, 40.73)
    east, north = transformer.transform(-73.96, 40.77)
    return west, south, east, north


def test_current_footprints_url_has_official_default_and_env_override(monkeypatch) -> None:
    monkeypatch.delenv("CITYLENS_CURRENT_FOOTPRINTS_URL", raising=False)
    assert _current_footprints_url() == _CURRENT_FOOTPRINTS_DEFAULT_URL

    monkeypatch.setenv("CITYLENS_CURRENT_FOOTPRINTS_URL", "https://mirror.example/current.geojson")
    assert _current_footprints_url() == "https://mirror.example/current.geojson"


def test_fetch_current_footprints_queries_filters_reprojects_and_uses_token(
    monkeypatch,
) -> None:
    monkeypatch.setenv("NYC_OPENDATA_APP_TOKEN", "test-app-token")
    response = FakeResponse(
        {
            "type": "FeatureCollection",
            "features": [
                _polygon_feature(base_bbl="1000010001"),
                _polygon_feature(construction_year="2025", base_bbl="1000010002"),
                _polygon_feature(status="Removed", base_bbl="1000010003"),
                _polygon_feature(construction_year=None, base_bbl="1000010004"),
            ],
        }
    )
    session = FakeSession(response)

    result, query_bbox = _fetch_current_footprints(
        url="https://data.cityofnewyork.us/resource/5zhs-2jue.geojson",
        bbox=_ortho_bbox(),
        target_crs=CRS.from_epsg(3857),
        imagery_year=2024,
        session=session,
    )

    assert response.status_checked is True
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("/5zhs-2jue.geojson")
    assert call["timeout"] == 60
    assert call["headers"] == {"X-App-Token": "test-app-token"}
    assert call["params"]["$select"] == (
        "the_geom,construction_year,last_status_type,geom_source,base_bbl,mappluto_bbl"
    )
    where = call["params"]["$where"]
    assert where.startswith("within_box(the_geom,") and where.endswith(")")
    north, west, south, east = [
        float(value) for value in where.removeprefix("within_box(the_geom,")[:-1].split(",")
    ]
    assert west == pytest.approx(query_bbox[0])
    assert south == pytest.approx(query_bbox[1])
    assert east == pytest.approx(query_bbox[2])
    assert north == pytest.approx(query_bbox[3])

    assert result["type"] == "FeatureCollection"
    assert result["crs"]["properties"]["name"] == "EPSG:3857"
    assert [feature["properties"]["base_bbl"] for feature in result["features"]] == [
        "1000010001",
        "1000010004",
    ]
    assert set(result["features"][0]["properties"]) == {
        "construction_year",
        "last_status_type",
        "geom_source",
        "base_bbl",
        "mappluto_bbl",
        "source_dataset",
    }
    assert result["features"][0]["properties"]["construction_year"] == 2020
    assert result["features"][0]["properties"]["source_dataset"] == "5zhs-2jue"
    assert result["features"][1]["properties"]["construction_year"] is None

    expected_x, expected_y = Transformer.from_crs(4326, 3857, always_xy=True).transform(
        -73.98, 40.75
    )
    actual_x, actual_y = result["features"][0]["geometry"]["coordinates"][0][0]
    assert actual_x == pytest.approx(expected_x)
    assert actual_y == pytest.approx(expected_y)


@pytest.mark.parametrize(
    ("response", "error_match"),
    [
        (FakeResponse({}, error=RuntimeError("HTTP 503")), "HTTP 503"),
        (FakeResponse({"error": "not geojson"}), "not a FeatureCollection"),
    ],
)
def test_fetch_current_footprints_rejects_http_and_payload_errors(
    response: FakeResponse, error_match: str
) -> None:
    with pytest.raises(RuntimeError, match=error_match):
        _fetch_current_footprints(
            url="https://example.test/current.geojson",
            bbox=_ortho_bbox(),
            target_crs=CRS.from_epsg(3857),
            imagery_year=2024,
            session=FakeSession(response),
        )


def test_stage_current_footprints_reuses_gcs_input_cache(tmp_path: Path) -> None:
    gcs = MemoryGcsClient()
    first_session = FakeSession(
        FakeResponse(
            {
                "type": "FeatureCollection",
                "features": [_polygon_feature()],
            }
        )
    )
    first_dir = tmp_path / "first"
    first_dir.mkdir()

    first = _stage_current_footprints(
        bbox=_ortho_bbox(),
        target_crs=CRS.from_epsg(3857),
        imagery_year=2024,
        work_dir=first_dir,
        gcs_client=gcs,
        bucket="test-bucket",
        cache_prefix="inputs",
        session=first_session,
    )

    assert first["cache_hit"] is False
    assert first["cache_object"].startswith("inputs/current-footprints/")
    assert first["cache_object"] in gcs.objects
    assert len(first_session.calls) == 1

    class NoNetworkSession:
        def get(self, *args: Any, **kwargs: Any) -> None:
            raise AssertionError("cache hit should avoid the NYC API")

    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = _stage_current_footprints(
        bbox=_ortho_bbox(),
        target_crs=CRS.from_epsg(3857),
        imagery_year=2024,
        work_dir=second_dir,
        gcs_client=gcs,
        bucket="test-bucket",
        cache_prefix="inputs",
        session=NoNetworkSession(),
    )

    assert second["cache_hit"] is True
    assert second["feature_count"] == 1
    assert json.loads((second_dir / "current_footprints.geojson").read_text())["type"] == (
        "FeatureCollection"
    )


def test_stage_current_footprints_survives_gcs_cache_failure(tmp_path: Path) -> None:
    class BrokenGcsClient:
        def bucket(self, bucket: str) -> None:  # noqa: ARG002
            raise RuntimeError("cache unavailable")

    session = FakeSession(
        FakeResponse(
            {
                "type": "FeatureCollection",
                "features": [_polygon_feature()],
            }
        )
    )

    result = _stage_current_footprints(
        bbox=_ortho_bbox(),
        target_crs=CRS.from_epsg(3857),
        imagery_year=2024,
        work_dir=tmp_path,
        gcs_client=BrokenGcsClient(),
        bucket="test-bucket",
        cache_prefix="inputs",
        session=session,
    )

    assert result["cache_hit"] is False
    assert result["feature_count"] == 1
    assert (tmp_path / "current_footprints.geojson").exists()
    assert len(session.calls) == 1


def test_current_footprints_api_failure_is_soft(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    class BrokenSession:
        def get(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("NYC API unavailable")

    caplog.set_level(logging.WARNING)
    result, warning = _stage_current_footprints_optional(
        bbox=_ortho_bbox(),
        target_crs=CRS.from_epsg(3857),
        imagery_year=2024,
        work_dir=tmp_path,
        gcs_client=MemoryGcsClient(),
        bucket="test-bucket",
        cache_prefix="inputs",
        session=BrokenSession(),
    )

    assert result is None
    assert warning is not None
    assert warning["code"] == "CURRENT_FOOTPRINTS_UNAVAILABLE"
    assert "NYC API unavailable" in warning["message"]
    assert not (tmp_path / "current_footprints.geojson").exists()
    assert "current_footprints_unavailable" in caplog.text

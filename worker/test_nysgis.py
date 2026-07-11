from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from services.nysgis import LAS_INDEX_LAYER_URL, NYSGISAPI
from services.run_errors import LidarCoverageError


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.last_url: str | None = None
        self.last_params: dict | None = None

    def get(self, url, params=None, timeout=None):  # noqa: ARG002
        self.last_url = url
        self.last_params = params
        return _FakeResponse(self._payload)


def test_default_las_index_is_nyc_topobathymetric_2017_layer() -> None:
    assert LAS_INDEX_LAYER_URL.endswith("/las_indexes/MapServer/10")
    assert NYSGISAPI().las_index_layer_url == LAS_INDEX_LAYER_URL


def test_las_index_environment_override_is_preserved(monkeypatch) -> None:
    import services.nysgis as nysgis

    override = "https://example.test/arcgis/rest/services/las/MapServer/42/"
    with monkeypatch.context() as context:
        context.setenv("CITYLENS_LAS_INDEX_LAYER_URL", override)
        reloaded = importlib.reload(nysgis)
        assert reloaded.NYSGISAPI().las_index_layer_url == override.rstrip("/")

    # Avoid leaving the reloaded module bound to the temporary override for
    # later worker tests in this process.
    importlib.reload(nysgis)


def test_get_lidar_tile_by_point_raises_typed_error_when_no_features() -> None:
    """An empty index response should raise a typed LidarCoverageError
    carrying the failing point and layer URL, not a generic ValueError."""

    fake_session = _FakeSession(payload={"features": []})
    api = NYSGISAPI(session=SimpleNamespace(get=fake_session.get))

    # The point from the failing run, in EPSG:3857.
    x, y = -8235305.5902816355, 4976726.264007133

    with pytest.raises(LidarCoverageError) as excinfo:
        api.get_lidar_tile_by_point(x, y, wkid=3857)

    err = excinfo.value
    assert err.x == x
    assert err.y == y
    assert err.wkid == 3857
    assert err.layer_url == api.las_index_layer_url
    # Plain `except ValueError` callers must still catch this, since we
    # intentionally subclass ValueError.
    assert isinstance(err, ValueError)


def test_get_lidar_tile_by_point_handles_missing_features_key() -> None:
    """Defensive: ESRI sometimes omits the `features` key entirely on
    error responses; make sure we still raise the typed error rather than
    a KeyError or AttributeError."""

    fake_session = _FakeSession(payload={})
    api = NYSGISAPI(session=SimpleNamespace(get=fake_session.get))

    with pytest.raises(LidarCoverageError):
        api.get_lidar_tile_by_point(-8235305.59, 4976726.26, wkid=3857)

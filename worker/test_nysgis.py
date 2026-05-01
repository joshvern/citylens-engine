from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.nysgis import NYSGISAPI
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


def test_get_lidar_tile_by_point_raises_typed_error_when_no_features() -> None:
    """Mid-Manhattan point with no LAS coverage in /MapServer/9 should raise
    a typed LidarCoverageError carrying the failing point and layer URL,
    not a generic ValueError. Reproduces run e65200a5..."""

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

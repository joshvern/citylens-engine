from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
from PIL import Image
from rasterio.crs import CRS
from rasterio.transform import from_origin

from services.imagery_inputs import (
    _crop_ortho_to_data_coverage,
    _get_config,
    ensure_work_dir_inputs,
)


class FakeBlob:
    def exists(self) -> bool:
        return False

    def download_to_filename(self, filename: str) -> None:
        raise AssertionError("unexpected download")

    def upload_from_filename(self, filename: str) -> None:
        return None


class FakeBucket:
    def blob(self, object_name: str) -> FakeBlob:
        return FakeBlob()


class FakeGcsClient:
    def bucket(self, bucket: str) -> FakeBucket:
        return FakeBucket()


def test_ensure_work_dir_inputs_writes_manifest_and_materializes_georef_inputs(
    monkeypatch, tmp_path: Path
) -> None:
    class FakeResolver:
        def __init__(self) -> None:
            self.session = SimpleNamespace()

        def get_assets_for_address(self, address: str):
            from services.nysgis import AddressAssets, LidarTile

            return AddressAssets(
                normalized_address="1 Main St",
                x=100.0,
                y=200.0,
                lidar_tile=LidarTile(
                    tile_id="123456",
                    filename="123456.las",
                    direct_url="https://example.test/123456.las",
                ),
                ortho_zip_url="https://example.test/123456.zip",
            )

        def build_ortho_wms_getmap_url(self, bbox, **kwargs):
            return "https://example.test/ortho.png"

    def fake_ortho(**kwargs):
        ortho_tif = tmp_path / "orthophoto.tif"
        ortho_png = tmp_path / "orthophoto.png"
        arr = np.zeros((8, 8, 3), dtype=np.uint8)
        arr[:, :, 0] = 120
        with rasterio.open(
            ortho_tif,
            "w",
            driver="GTiff",
            height=8,
            width=8,
            count=3,
            dtype="uint8",
            crs=CRS.from_epsg(3857),
            transform=from_origin(100.0, 200.0, 1.0, 1.0),
        ) as dst:
            dst.write(np.moveaxis(arr, -1, 0))
        Image.fromarray(arr).save(ortho_png)
        return {
            "canonical_path": str(ortho_tif),
            "compat_path": str(ortho_png),
            "source_url": "https://example.test/ortho.png",
            "sha256": "deadbeef",
            "crs": "EPSG:3857",
            "transform": from_origin(100.0, 200.0, 1.0, 1.0),
            "bbox": (100.0, 192.0, 108.0, 200.0),
            "cache_key": "cache-key",
        }

    def fake_counties(*args, **kwargs):
        return {"Kings": tmp_path / "Kings_Building_Footprints.gdb"}

    def fake_baseline_footprints(**kwargs):
        path = tmp_path / "baseline_footprints.geojson"
        path.write_text('{"type":"FeatureCollection","features":[]}')
        return {
            "path": path,
            "sha256": "feedface",
            "feature_count": 0,
            "county_sources": {"Kings": str(tmp_path / "Kings_Building_Footprints.gdb")},
            "source_urls": {"Kings": "https://example.test/kings.zip"},
        }

    def fake_rasterize_baseline(**kwargs):
        baseline_tif = tmp_path / "baseline.tif"
        baseline_png = tmp_path / "baseline.png"
        with rasterio.open(
            baseline_tif,
            "w",
            driver="GTiff",
            height=8,
            width=8,
            count=1,
            dtype="uint8",
            crs=CRS.from_epsg(3857),
            transform=from_origin(100.0, 200.0, 1.0, 1.0),
        ) as dst:
            dst.write(np.zeros((8, 8), dtype=np.uint8), 1)
        Image.fromarray(np.zeros((8, 8), dtype=np.uint8)).save(baseline_png)
        return {
            "canonical_path": str(baseline_tif),
            "compat_path": str(baseline_png),
            "sha256": "cafebabe",
            "mask_sha256": "abc123",
            "feature_count": 0,
        }

    def fake_lidar(url, dest_path, **kwargs):
        Path(dest_path).write_bytes(b"LAS")
        return Path(dest_path)

    monkeypatch.setattr("services.imagery_inputs.NYSGISAPI", FakeResolver)
    monkeypatch.setattr("services.imagery_inputs._download_orthophoto_tif", fake_ortho)
    monkeypatch.setattr("services.imagery_inputs._ensure_county_footprints_gdbs", fake_counties)
    monkeypatch.setattr(
        "services.imagery_inputs._build_baseline_footprints", fake_baseline_footprints
    )
    monkeypatch.setattr("services.imagery_inputs._rasterize_baseline", fake_rasterize_baseline)
    monkeypatch.setattr("services.imagery_inputs._download_lidar_tile", fake_lidar)

    manifest = ensure_work_dir_inputs(
        request=SimpleNamespace(address="1 Main St"),
        work_dir=tmp_path,
        gcs_client=FakeGcsClient(),
        bucket="test-bucket",
    )

    manifest_path = tmp_path / "input_manifest.json"
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert data["orthophoto_path"].endswith("orthophoto.tif")
    assert data["orthophoto_png_path"].endswith("orthophoto.png")
    assert data["baseline_path"].endswith("baseline.tif")
    assert data["baseline_png_path"].endswith("baseline.png")
    assert data["baseline_footprints_path"].endswith("baseline_footprints.geojson")
    assert data["lidar_path"].endswith("lidar.las")
    assert data["geocode"]["x"] == 100.0
    assert data["tile"]["tile_id"] == "123456"
    assert data["assets"]["baseline_footprints"]["feature_count"] == 0
    assert (tmp_path / "orthophoto.tif").exists()
    assert (tmp_path / "orthophoto.png").exists()
    assert (tmp_path / "baseline.tif").exists()
    assert (tmp_path / "baseline.png").exists()
    assert (tmp_path / "baseline_footprints.geojson").exists()
    assert (tmp_path / "lidar.las").exists()
    assert manifest["input_manifest_path"] == str(manifest_path)


def test_get_config_prefers_request_radius_over_env(monkeypatch) -> None:
    """Regression test: request.aoi_radius_m must override the env fallback.

    Original bug: _get_config ignored the request entirely and read
    CITYLENS_ORTHO_BBOX_HALF_SIZE_M (default 120 m), producing a 240x240 m
    ortho instead of the requested 500x500 m. On the Brooklyn reference run
    this clipped baseline GDB coverage to ~half the visible buildings.
    """
    monkeypatch.setenv("CITYLENS_ORTHO_BBOX_HALF_SIZE_M", "120")
    # Env-only path still works for ad-hoc CLI scripts.
    cfg_env = _get_config()
    assert cfg_env.bbox_half_size_m == 120.0
    # Request value wins when present.
    cfg_req = _get_config(request_radius=250)
    assert cfg_req.bbox_half_size_m == 250.0
    # Falsy values (None / 0) fall back to env.
    cfg_zero = _get_config(request_radius=0)
    assert cfg_zero.bbox_half_size_m == 120.0


def test_request_aoi_radius_drives_wms_bbox(monkeypatch, tmp_path: Path) -> None:
    """End-to-end regression: setting request.aoi_radius_m=250 must produce a
    500m-wide WMS bbox (half-size 250), not the env-default 240m (half-size
    120). HTTP is mocked — we only check the bbox math the resolver was asked
    to build, plus the GeoTIFF transform written for that bbox.
    """
    from io import BytesIO
    from types import SimpleNamespace

    from PIL import Image as PILImage

    import services.imagery_inputs as mod

    monkeypatch.setenv("CITYLENS_ORTHO_BBOX_HALF_SIZE_M", "120")

    captured: dict = {}

    class FakeSession:
        def get(self, url, timeout=None):  # noqa: ARG002
            buf = BytesIO()
            PILImage.new("RGB", (8, 8), (10, 20, 30)).save(buf, format="PNG")
            return SimpleNamespace(
                content=buf.getvalue(),
                raise_for_status=lambda: None,
            )

    class FakeResolver:
        session = FakeSession()

        def build_ortho_wms_getmap_url(self, bbox, **kwargs):
            captured["bbox"] = bbox
            captured["kwargs"] = kwargs
            return "https://example.test/wms?fake=1"

    from services.nysgis import AddressAssets, LidarTile

    assets = AddressAssets(
        normalized_address="123 Test St",
        x=1_000_000.0,
        y=2_000_000.0,
        lidar_tile=LidarTile(
            tile_id="t1", filename="t1.las", direct_url="https://example.test/t1.las"
        ),
        ortho_zip_url="https://example.test/t1.zip",
    )

    result = mod._download_orthophoto_tif(
        resolver=FakeResolver(),
        assets=assets,
        work_dir=tmp_path,
        gcs_client=FakeGcsClient(),
        bucket="b",
        width=64,
        height=64,
        bbox_half_size_m=250.0,  # the value _get_config would have returned
    )

    minx, miny, maxx, maxy = captured["bbox"]
    assert (maxx - minx) == 500.0, f"bbox width should be 500m, got {maxx - minx}"
    assert (maxy - miny) == 500.0, f"bbox height should be 500m, got {maxy - miny}"
    assert minx == assets.x - 250.0
    assert maxx == assets.x + 250.0
    bbox_tuple = result["bbox"]
    assert (bbox_tuple[2] - bbox_tuple[0]) == 500.0

    # The cache key must include the half-size so a 250m request doesn't
    # serve a stale 120m blob. Re-run with a different size and confirm the
    # cache key changes — otherwise the GCS blob would collide.
    captured.clear()
    other_tmp = tmp_path / "other"
    other_tmp.mkdir()
    result_120 = mod._download_orthophoto_tif(
        resolver=FakeResolver(),
        assets=assets,
        work_dir=other_tmp,
        gcs_client=FakeGcsClient(),
        bucket="b",
        width=64,
        height=64,
        bbox_half_size_m=120.0,
    )
    assert result["cache_key"] != result_120["cache_key"], (
        "cache key must differ when bbox_half_size_m changes, otherwise the "
        "240m and 500m orthos would collide in GCS"
    )


def test_crop_ortho_to_data_coverage_trims_western_no_data(tmp_path: Path) -> None:
    """The NYS 2024 WMS returns black no-data pixels where coverage is missing.
    Construct a 100x100 RGB ortho with the western 30 columns set to (0,0,0)
    and assert the crop helper rewrites the file to 100x70 with the transform
    shifted east by 30 pixel-widths.
    """
    arr = np.zeros((100, 100, 3), dtype=np.uint8)
    arr[:, 30:, :] = np.array([120, 130, 140], dtype=np.uint8)  # data band

    tif_path = tmp_path / "ortho.tif"
    pixel_size_x = 0.5  # m/px (arbitrary units; only ratios matter for this test)
    pixel_size_y = 0.5
    origin_x = 1000.0
    origin_y = 2000.0
    src_transform = from_origin(origin_x, origin_y, pixel_size_x, pixel_size_y)
    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=100,
        width=100,
        count=3,
        dtype="uint8",
        crs=CRS.from_epsg(3857),
        transform=src_transform,
    ) as dst:
        dst.write(np.moveaxis(arr, -1, 0))

    rewritten = _crop_ortho_to_data_coverage(tif_path)
    assert rewritten is True

    with rasterio.open(tif_path) as src:
        assert src.width == 70
        assert src.height == 100
        # Origin shifts east by 30 pixels (30 * 0.5 m = 15 m); y origin
        # unchanged because no rows were dropped.
        assert src.transform.c == origin_x + 30 * pixel_size_x
        assert src.transform.f == origin_y
        # Pixel sizes preserved.
        assert src.transform.a == pixel_size_x
        assert src.transform.e == -pixel_size_y
        # All pixels should now be data.
        data = src.read()
        assert np.all(data[0] == 120)


def test_crop_ortho_to_data_coverage_noop_on_full_coverage(tmp_path: Path) -> None:
    arr = np.full((50, 50, 3), 200, dtype=np.uint8)
    tif_path = tmp_path / "ortho.tif"
    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=50,
        width=50,
        count=3,
        dtype="uint8",
        crs=CRS.from_epsg(3857),
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    ) as dst:
        dst.write(np.moveaxis(arr, -1, 0))

    pre_size = tif_path.stat().st_size
    assert _crop_ortho_to_data_coverage(tif_path) is False
    # File untouched.
    with rasterio.open(tif_path) as src:
        assert src.width == 50
        assert src.height == 50
    assert tif_path.stat().st_size == pre_size


def test_crop_ortho_to_data_coverage_skips_when_coverage_under_threshold(
    tmp_path: Path,
) -> None:
    """If <30% of the raster is data, leave it alone — a partial preview is
    better than a broken pipeline run."""
    arr = np.zeros((100, 100, 3), dtype=np.uint8)
    arr[:10, :10, :] = 255  # 1% coverage
    tif_path = tmp_path / "ortho.tif"
    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=100,
        width=100,
        count=3,
        dtype="uint8",
        crs=CRS.from_epsg(3857),
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    ) as dst:
        dst.write(np.moveaxis(arr, -1, 0))

    assert _crop_ortho_to_data_coverage(tif_path) is False
    with rasterio.open(tif_path) as src:
        assert src.width == 100
        assert src.height == 100


def test_features_for_bbox_reprojects_output_to_target_crs(monkeypatch) -> None:
    """Regression test for the CRS-direction bug where output features were
    left in src CRS instead of being reprojected to target CRS.

    Original bug: the function built ONE transformer target_crs -> src_crs
    (correct for the bbox query) and then reused it on output features,
    which left them in src CRS. On the Brooklyn demo this meant
    baseline_footprints.geojson features were in NYSP ftUS while the ortho
    was in EPSG:3857 -> rasterize produced an all-zero mask -> mask_iou=0.
    """
    from rasterio.crs import CRS as RioCRS

    import services.imagery_inputs as mod

    # Two polygons in the "fake GDB": one inside the query bbox, one outside.
    # We'll pretend the source CRS is EPSG:2263 (NYSP LI ftUS) and the target
    # is EPSG:3857. A well-known NYC point is (996977.21, 177499.99) in 2263
    # which is (-8232536.28, 4961419.96) in 3857.
    feat_in = {
        "type": "Feature",
        "properties": {"Source": "NYC OpenData"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [996970.0, 177490.0],
                    [996990.0, 177490.0],
                    [996990.0, 177510.0],
                    [996970.0, 177510.0],
                    [996970.0, 177490.0],
                ]
            ],
        },
    }

    class FakeFionaSrc:
        crs = "EPSG:2263"

        def filter(self, bbox):
            return iter([feat_in])

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(mod, "fiona_open", lambda *a, **kw: FakeFionaSrc())
    monkeypatch.setattr(mod, "_layer_name_from_gdb", lambda p: "layer")

    target_crs = RioCRS.from_epsg(3857)
    # bbox centered at the known NYC point (-8232536.28, 4961419.96) ± ~30m
    bbox_3857 = (-8232566.0, 4961390.0, -8232506.0, 4961450.0)

    result = mod._features_for_bbox(
        gdb_path=Path("/tmp/fake.gdb"),
        bbox=bbox_3857,
        target_crs=target_crs,
    )

    assert len(result) == 1
    coords = result[0]["geometry"]["coordinates"][0]
    xs = [pt[0] for pt in coords]
    ys = [pt[1] for pt in coords]
    # After reprojection, x should be near -8232536 (EPSG:3857) and NOT
    # near 996977 (EPSG:2263). A ~30m tolerance is plenty.
    assert -8232550 < min(xs) < -8232520, (
        f"feature x not reprojected to EPSG:3857: xs={xs}"
    )
    assert 4961400 < min(ys) < 4961440, (
        f"feature y not reprojected to EPSG:3857: ys={ys}"
    )

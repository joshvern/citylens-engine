from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
from PIL import Image
from rasterio.crs import CRS
from rasterio.transform import from_origin

from services.imagery_inputs import ensure_work_dir_inputs


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

from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import requests
from fiona import listlayers
from fiona import open as fiona_open
from PIL import Image
from pyproj import CRS, Transformer
from rasterio import open as rasterio_open
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely.geometry import mapping, shape
from shapely.ops import transform as shapely_transform

from .nysgis import NYSGISAPI, AddressAssets

_COUNTY_FOOTPRINT_ZIPS: dict[str, str] = {
    "Bronx": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Bronx_Building_Footprints.zip",
    "Kings": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Kings_Building_Footprints.zip",
    "New York": "https://gisdata.ny.gov/GISData/State/Building_Footprints/New_York_Building_Footprints.zip",
    "Queens": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Queens_Building_Footprints.zip",
    "Richmond": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Richmond_Building_Footprints.zip",
}


def _normalize_address(address: str) -> str:
    return " ".join((address or "").strip().split())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if chunk:
                h.update(chunk)
    return h.hexdigest()


def _optional_source_from_env(name: str) -> Path | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return Path(raw)


def _download_file(url: str, dest_path: Path, *, session: requests.Session | None = None) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    sess = session or requests.Session()
    with sess.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(tmp_path, dest_path)


def _download_lidar_tile(
    url: str, dest_path: Path, *, gcs_client: Any, bucket: str, cache_key: str
) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    object_name = f"inputs/{cache_key}/lidar.las"
    blob = gcs_client.bucket(bucket).blob(object_name)
    if blob.exists():
        blob.download_to_filename(str(dest_path))
        return dest_path

    _download_file(url, dest_path)
    blob.upload_from_filename(str(dest_path))
    return dest_path


def _convert_jp2_to_tif(input_path: Path, output_path: Path) -> None:
    with rasterio_open(input_path) as src:
        profile = src.profile.copy()
        profile.update(driver="GTiff")
        with rasterio_open(output_path, "w", **profile) as dst:
            for i in range(1, src.count + 1):
                dst.write(src.read(i), i)


def _download_ortho_zip_tif(*, ortho_zip_url: str, work_dir: Path, tif_path: Path) -> None:
    zip_path = work_dir / "orthophoto.zip"
    extracted_dir = work_dir / "orthophoto-zip"
    _download_file(ortho_zip_url, zip_path)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        tif_members = [m for m in members if m.lower().endswith(".tif")]
        if tif_members:
            extracted_path = Path(zf.extract(tif_members[0], extracted_dir))
            tif_path.parent.mkdir(parents=True, exist_ok=True)
            if extracted_path.resolve() != tif_path.resolve():
                os.replace(str(extracted_path), str(tif_path))
            return

        jp2_members = [m for m in members if m.lower().endswith(".jp2")]
        if not jp2_members:
            raise RuntimeError("No .tif or .jp2 file found inside orthophoto ZIP")

        jp2_member = jp2_members[0]
        base_name = Path(jp2_member).stem.lower()
        sidecar_exts = {".aux", ".j2w", ".tab"}
        for member in members:
            suffix = Path(member).suffix.lower()
            if suffix in sidecar_exts and base_name in Path(member).stem.lower():
                zf.extract(member, extracted_dir)

        jp2_path = Path(zf.extract(jp2_member, extracted_dir))
        _convert_jp2_to_tif(jp2_path, tif_path)


def _download_orthophoto_tif(
    *,
    resolver: NYSGISAPI,
    assets: AddressAssets,
    work_dir: Path,
    gcs_client: Any,
    bucket: str,
    width: int,
    height: int,
    bbox_half_size_m: float,
) -> dict[str, Any]:
    x, y = assets.x, assets.y
    d = float(bbox_half_size_m)
    bbox = (x - d, y - d, x + d, y + d)
    transform = from_bounds(*bbox, width=int(width), height=int(height))
    crs = CRS.from_epsg(3857)

    tif_path = work_dir / "orthophoto.tif"
    png_path = work_dir / "orthophoto.png"
    cache_key = hashlib.sha256(
        f"{assets.normalized_address}|{assets.lidar_tile.tile_id}|{width}|{height}|{bbox_half_size_m}".encode(
            "utf-8"
        )
    ).hexdigest()
    object_name = f"inputs/{cache_key}/orthophoto.tif"
    blob = gcs_client.bucket(bucket).blob(object_name)

    if blob.exists():
        blob.download_to_filename(str(tif_path))
        with rasterio_open(tif_path) as src:
            arr = src.read()
            if arr.shape[0] == 1:
                img = Image.fromarray(arr[0])
            else:
                img = Image.fromarray(np.moveaxis(arr[:3], 0, -1))
        img.save(png_path)
        return {
            "canonical_path": str(tif_path),
            "compat_path": str(png_path),
            "source_url": assets.ortho_zip_url,
            "sha256": _sha256_file(tif_path),
            "crs": "EPSG:3857",
            "transform": transform,
            "bbox": bbox,
            "cache_key": cache_key,
        }

    url = resolver.build_ortho_wms_getmap_url(
        bbox, width=int(width), height=int(height), transparent=False
    )
    source_url = url
    try:
        session = resolver.session
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        img = Image.open(BytesIO(resp.content)).convert("RGB")
        arr = np.array(img)

        tif_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio_open(
            tif_path,
            "w",
            driver="GTiff",
            height=arr.shape[0],
            width=arr.shape[1],
            count=3,
            dtype=arr.dtype,
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(np.moveaxis(arr, -1, 0))

        img.save(png_path)
    except Exception:
        _download_ortho_zip_tif(
            ortho_zip_url=assets.ortho_zip_url,
            work_dir=work_dir,
            tif_path=tif_path,
        )
        with rasterio_open(tif_path) as src:
            arr = src.read()
            if arr.shape[0] == 1:
                img = Image.fromarray(arr[0])
            else:
                img = Image.fromarray(np.moveaxis(arr[:3], 0, -1))
        img.save(png_path)
        source_url = assets.ortho_zip_url

    blob.upload_from_filename(str(tif_path))

    return {
        "canonical_path": str(tif_path),
        "compat_path": str(png_path),
        "source_url": source_url,
        "sha256": _sha256_file(tif_path),
        "crs": "EPSG:3857",
        "transform": transform,
        "bbox": bbox,
        "cache_key": cache_key,
    }


def _ensure_county_footprints_gdbs(data_dir: Path, *, keep_zips: bool = False) -> dict[str, Path]:
    from .reference_data import ensure_nyc_county_footprints

    return ensure_nyc_county_footprints(data_dir=data_dir, keep_zips=keep_zips)


def _layer_name_from_gdb(gdb_path: Path) -> str:
    layers = listlayers(str(gdb_path))
    if layers:
        return str(layers[0])
    return gdb_path.stem


def _features_for_bbox(
    *,
    gdb_path: Path,
    bbox: tuple[float, float, float, float],
    target_crs: CRS,
) -> list[dict[str, Any]]:
    layer = _layer_name_from_gdb(gdb_path)
    features: list[dict[str, Any]] = []
    with fiona_open(str(gdb_path), layer=layer) as src:
        src_crs = CRS.from_user_input(src.crs) if src.crs else None
        src_bbox = bbox
        transformer: Transformer | None = None
        if src_crs and str(src_crs) != str(target_crs):
            transformer = Transformer.from_crs(target_crs, src_crs, always_xy=True)
            xs = [bbox[0], bbox[2], bbox[0], bbox[2]]
            ys = [bbox[1], bbox[1], bbox[3], bbox[3]]
            tx, ty = transformer.transform(xs, ys)
            src_bbox = (float(min(tx)), float(min(ty)), float(max(tx)), float(max(ty)))

        for feat in src.filter(bbox=src_bbox):
            geom = feat.get("geometry")
            if not geom:
                continue
            try:
                parsed = shape(geom)
            except Exception:
                continue
            if parsed.is_empty:
                continue
            if transformer is not None:
                parsed = shapely_transform(transformer.transform, parsed)
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "source_gdb": gdb_path.name,
                        "source_layer": layer,
                        **{
                            k: v
                            for k, v in (feat.get("properties") or {}).items()
                            if k in {"NYSGeo_Source", "Source", "SourceDate"}
                        },
                    },
                    "geometry": mapping(parsed),
                }
            )
    return features


def _build_baseline_footprints(
    *,
    reference_data_dir: Path,
    bbox: tuple[float, float, float, float],
    target_crs: CRS,
    work_dir: Path,
    keep_zips: bool,
) -> dict[str, Any]:
    county_gdbs = _ensure_county_footprints_gdbs(reference_data_dir, keep_zips=keep_zips)
    geojson_path = work_dir / "baseline_footprints.geojson"
    all_features: list[dict[str, Any]] = []
    county_sources: dict[str, str] = {}

    for county, gdb_path in county_gdbs.items():
        county_sources[county] = str(gdb_path)
        all_features.extend(
            _features_for_bbox(gdb_path=Path(gdb_path), bbox=bbox, target_crs=target_crs)
        )

    feature_collection = {"type": "FeatureCollection", "features": all_features}
    geojson_path.write_text(json.dumps(feature_collection, indent=2, sort_keys=False))

    return {
        "path": geojson_path,
        "sha256": _sha256_file(geojson_path),
        "feature_count": len(all_features),
        "county_sources": county_sources,
        "source_urls": dict(_COUNTY_FOOTPRINT_ZIPS),
    }


def _rasterize_baseline(
    *,
    baseline_footprints: Path,
    ortho_path: Path,
    work_dir: Path,
) -> dict[str, Any]:
    baseline_tif_path = work_dir / "baseline.tif"
    baseline_png_path = work_dir / "baseline.png"

    with rasterio_open(ortho_path) as src:
        transform = src.transform
        crs = src.crs
        height = src.height
        width = src.width

    if crs is None:
        raise RuntimeError("orthophoto must have georeferencing before baseline rasterization")

    geojson = json.loads(baseline_footprints.read_text())
    shapes: list[tuple[Any, int]] = []
    for feature in geojson.get("features") or []:
        geom = feature.get("geometry")
        if not geom:
            continue
        try:
            shapes.append((shape(geom), 1))
        except Exception:
            continue

    mask = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        default_value=1,
        all_touched=False,
        dtype="uint8",
    )

    with rasterio_open(
        baseline_tif_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="uint8",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(mask, 1)

    Image.fromarray((mask * 255).astype(np.uint8)).save(baseline_png_path)

    return {
        "canonical_path": str(baseline_tif_path),
        "compat_path": str(baseline_png_path),
        "sha256": _sha256_file(baseline_tif_path),
        "mask_sha256": _sha256_bytes(mask.tobytes()),
        "feature_count": len(shapes),
    }


def _copy_source(source: Path, dest: Path) -> Path:
    source = Path(source)
    if not source.exists():
        raise FileNotFoundError(str(source))
    dest.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() != dest.resolve():
        shutil.copy2(str(source), str(dest))
    return dest


@dataclass(frozen=True)
class OrthoFetchConfig:
    wms_url: str
    cache_prefix: str
    bbox_half_size_m: float
    width: int
    height: int


def _get_config() -> OrthoFetchConfig:
    wms_url = os.getenv(
        "CITYLENS_ORTHO_WMS_URL",
        "https://orthos.its.ny.gov/arcgis/rest/services/wms/2024/MapServer/WMSServer",
    ).strip()
    cache_prefix = os.getenv("CITYLENS_IMAGERY_CACHE_PREFIX", "inputs").strip().strip("/")
    bbox_half_size_m = float(os.getenv("CITYLENS_ORTHO_BBOX_HALF_SIZE_M", "120"))
    width = int(os.getenv("CITYLENS_ORTHO_WIDTH", "1024"))
    height = int(os.getenv("CITYLENS_ORTHO_HEIGHT", "1024"))
    return OrthoFetchConfig(
        wms_url=wms_url,
        cache_prefix=cache_prefix,
        bbox_half_size_m=bbox_half_size_m,
        width=width,
        height=height,
    )


def _prepare_manifest_asset(
    *,
    name: str,
    canonical_path: Path,
    compat_path: Path | None = None,
    source_url: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "canonical_path": str(canonical_path),
        "local_path": str(canonical_path),
        "sha256": _sha256_file(canonical_path),
    }
    if compat_path is not None:
        payload["compat_path"] = str(compat_path)
    if source_url:
        payload["source_url"] = source_url
    if extra:
        payload.update(extra)
    return payload


def ensure_work_dir_inputs(
    *,
    request: Any,
    work_dir: Path,
    gcs_client: Any,
    bucket: str,
) -> dict[str, Any]:
    """Materialize orthophoto, baseline footprints, and LiDAR inputs in work_dir."""

    cfg = _get_config()
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    explicit_ortho = getattr(request, "orthophoto_path", None)
    explicit_base = getattr(request, "baseline_path", None)

    address = getattr(request, "address", None)
    if not isinstance(address, str) or not address.strip():
        raise RuntimeError("request.address is required to fetch imagery")

    resolver = NYSGISAPI()
    assets = resolver.get_assets_for_address(address)
    normalized_address = assets.normalized_address
    cache_key = hashlib.sha256(
        f"{normalized_address}|{assets.lidar_tile.tile_id}|{cfg.bbox_half_size_m}|{cfg.width}|{cfg.height}|{cfg.wms_url}".encode(
            "utf-8"
        )
    ).hexdigest()

    manifest: dict[str, Any] = {
        "address": normalized_address,
        "geocode": {"x": assets.x, "y": assets.y, "wkid": 3857},
        "tile": {
            "tile_id": assets.lidar_tile.tile_id,
            "lidar_filename": assets.lidar_tile.filename,
            "lidar_url": assets.lidar_tile.direct_url,
            "ortho_zip_url": assets.ortho_zip_url,
        },
        "work_dir": str(work_dir),
        "reference_data_dir": None,
        "reference_county_footprints": {},
        "orthophoto_path": None,
        "orthophoto_png_path": None,
        "baseline_path": None,
        "baseline_png_path": None,
        "baseline_footprints_path": None,
        "lidar_path": None,
        "assets": {},
    }

    if explicit_ortho:
        ortho_canonical = Path(explicit_ortho)
        if not ortho_canonical.exists():
            raise FileNotFoundError(str(ortho_canonical))
        ortho_compat = work_dir / "orthophoto.png"
        if (
            ortho_canonical.suffix.lower() == ".png"
            and ortho_canonical.resolve() != ortho_compat.resolve()
        ):
            shutil.copy2(str(ortho_canonical), str(ortho_compat))
        manifest["orthophoto_path"] = str(ortho_canonical)
        manifest["orthophoto_png_path"] = str(
            ortho_compat if ortho_compat.exists() else ortho_canonical
        )
        manifest["assets"]["orthophoto"] = _prepare_manifest_asset(
            name="orthophoto",
            canonical_path=ortho_canonical,
            compat_path=ortho_compat if ortho_compat.exists() else None,
            extra={"source_url": assets.ortho_zip_url},
        )
    else:
        ortho = _download_orthophoto_tif(
            resolver=resolver,
            assets=assets,
            work_dir=work_dir,
            gcs_client=gcs_client,
            bucket=bucket,
            width=cfg.width,
            height=cfg.height,
            bbox_half_size_m=cfg.bbox_half_size_m,
        )
        manifest["orthophoto_path"] = ortho["canonical_path"]
        manifest["orthophoto_png_path"] = ortho["compat_path"]
        manifest["assets"]["orthophoto"] = {
            **ortho,
            "local_path": ortho["canonical_path"],
        }

    reference_data_dir = Path(os.getenv("CITYLENS_REFERENCE_DATA_DIR", "/tmp/reference-data"))
    keep_zips = os.getenv("CITYLENS_REFERENCE_KEEP_ZIPS", "0") == "1"
    county_gdbs = _ensure_county_footprints_gdbs(reference_data_dir, keep_zips=keep_zips)
    manifest["reference_data_dir"] = str(reference_data_dir)
    manifest["reference_county_footprints"] = {k: str(v) for k, v in county_gdbs.items()}

    with rasterio_open(manifest["orthophoto_path"]) as src:
        ortho_crs = CRS.from_user_input(src.crs) if src.crs else CRS.from_epsg(3857)
        bbox = tuple(src.bounds)
        if len(bbox) != 4:
            raise RuntimeError("Could not determine orthophoto bounds")

    baseline_footprints = _build_baseline_footprints(
        reference_data_dir=reference_data_dir,
        bbox=(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])),
        target_crs=ortho_crs,
        work_dir=work_dir,
        keep_zips=keep_zips,
    )
    manifest["baseline_footprints_path"] = str(baseline_footprints["path"])

    baseline = _rasterize_baseline(
        baseline_footprints=Path(baseline_footprints["path"]),
        ortho_path=Path(manifest["orthophoto_path"]),
        work_dir=work_dir,
    )
    manifest["baseline_path"] = (
        baseline["canonical_path"] if not explicit_base else str(Path(explicit_base))
    )
    manifest["baseline_png_path"] = baseline["compat_path"]
    manifest["assets"]["baseline_footprints"] = {
        "canonical_path": str(baseline_footprints["path"]),
        "local_path": str(baseline_footprints["path"]),
        "sha256": baseline_footprints["sha256"],
        "source_urls": baseline_footprints["source_urls"],
        "feature_count": baseline_footprints["feature_count"],
    }
    manifest["assets"]["baseline"] = {
        **baseline,
        "local_path": baseline["canonical_path"],
        "source_url": "derived from baseline_footprints.geojson",
    }

    if explicit_base:
        baseline_path = Path(explicit_base)
        if not baseline_path.exists():
            raise FileNotFoundError(str(baseline_path))
        manifest["baseline_path"] = str(baseline_path)
        if (
            baseline_path.suffix.lower() == ".png"
            and baseline_path.resolve() != Path(manifest["baseline_png_path"]).resolve()
        ):
            shutil.copy2(str(baseline_path), manifest["baseline_png_path"])

    lidar_path = work_dir / "lidar.las"
    _download_lidar_tile(
        assets.lidar_tile.direct_url,
        lidar_path,
        gcs_client=gcs_client,
        bucket=bucket,
        cache_key=assets.lidar_tile.tile_id,
    )
    manifest["lidar_path"] = str(lidar_path)
    manifest["assets"]["lidar"] = _prepare_manifest_asset(
        name="lidar",
        canonical_path=lidar_path,
        source_url=assets.lidar_tile.direct_url,
        extra={
            "tile_id": assets.lidar_tile.tile_id,
            "filename": assets.lidar_tile.filename,
            "collection": assets.lidar_tile.collection,
            "ftp_path": assets.lidar_tile.ftp_path,
            "size_gb": assets.lidar_tile.size_gb,
        },
    )

    if os.getenv("CITYLENS_DOWNLOAD_REFERENCE_DATA", "0") == "1":
        manifest["reference_county_footprints"] = {k: str(v) for k, v in county_gdbs.items()}

    manifest["tile"]["cache_key"] = cache_key
    manifest["tile"]["ortho_bounds"] = [float(b) for b in bbox]
    manifest["tile"]["ortho_crs"] = str(ortho_crs)

    manifest_path = work_dir / "input_manifest.json"
    manifest["input_manifest_path"] = str(manifest_path)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def ensure_orthophoto_and_baseline_inputs(
    *,
    request: Any,
    work_dir: Path,
    gcs_client: Any,
    bucket: str,
) -> dict[str, Any]:
    return ensure_work_dir_inputs(
        request=request, work_dir=work_dir, gcs_client=gcs_client, bucket=bucket
    )

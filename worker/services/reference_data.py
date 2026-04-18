from __future__ import annotations

import logging
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Any, Mapping, Optional

import requests

logger = logging.getLogger(__name__)

NYC_COUNTY_FOOTPRINT_ZIPS: dict[str, str] = {
    "Bronx": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Bronx_Building_Footprints.zip",
    "Kings": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Kings_Building_Footprints.zip",
    "New York": "https://gisdata.ny.gov/GISData/State/Building_Footprints/New_York_Building_Footprints.zip",
    "Queens": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Queens_Building_Footprints.zip",
    "Richmond": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Richmond_Building_Footprints.zip",
}

DEFAULT_GCS_PREFIX = "reference-data/nyc-footprints"


def _download(url: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

    safe_url = url.replace(" ", "%20")
    with requests.get(safe_url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)

    os.replace(tmp_path, dest_path)


def _extract_zip(zip_path: Path, dest_dir: Path) -> None:
    with zipfile.ZipFile(str(zip_path), "r") as zf:
        zf.extractall(str(dest_dir))


def _discover_gdb_path(dest_dir: Path) -> Path | None:
    matches = sorted(dest_dir.glob("*.gdb"))
    if matches:
        return matches[0]
    nested = sorted(dest_dir.rglob("*.gdb"))
    if nested:
        return nested[0]
    return None


def _safe_slug(name: str) -> str:
    return name.replace(" ", "_")


def _tar_gdb(gdb_path: Path, tar_path: Path) -> None:
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tar_path.with_suffix(tar_path.suffix + ".part")
    with tarfile.open(str(tmp), "w:gz") as tf:
        tf.add(str(gdb_path), arcname=gdb_path.name)
    os.replace(tmp, tar_path)


def _untar_gdb(tar_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(str(tar_path), "r:gz") as tf:
        tf.extractall(str(dest_dir))


def _gcs_object_for(prefix: str, county: str) -> str:
    return f"{prefix.rstrip('/')}/{_safe_slug(county)}.tar.gz"


def _try_restore_from_gcs(
    *,
    gcs_client: Any,
    bucket: str,
    object_name: str,
    tar_path: Path,
    dest_dir: Path,
) -> Path | None:
    """Return the restored .gdb path, or None if the cache object doesn't exist."""
    try:
        blob = gcs_client.bucket(bucket).blob(object_name)
        if not blob.exists():
            return None
        tar_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(tar_path))
    except Exception as exc:
        logger.warning(
            "gcs_cache_restore_failed",
            extra={"object": object_name, "error": f"{type(exc).__name__}: {exc}"},
        )
        return None

    try:
        _untar_gdb(tar_path, dest_dir)
    finally:
        try:
            tar_path.unlink()
        except OSError:
            pass

    return _discover_gdb_path(dest_dir)


def _upload_to_gcs(
    *,
    gcs_client: Any,
    bucket: str,
    object_name: str,
    gdb_path: Path,
    staging_path: Path,
) -> None:
    try:
        _tar_gdb(gdb_path, staging_path)
        blob = gcs_client.bucket(bucket).blob(object_name)
        blob.upload_from_filename(str(staging_path))
    except Exception as exc:
        logger.warning(
            "gcs_cache_upload_failed",
            extra={"object": object_name, "error": f"{type(exc).__name__}: {exc}"},
        )
    finally:
        try:
            staging_path.unlink()
        except OSError:
            pass


def ensure_nyc_county_footprints(
    *,
    data_dir: Path,
    keep_zips: bool = False,
    gcs_client: Any | None = None,
    gcs_bucket: str | None = None,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    urls: Optional[Mapping[str, str]] = None,
) -> dict[str, Path]:
    """Ensure NYC county building footprints GDBs exist locally.

    Resolution order for each county:
      1. Already extracted on local disk -> use as-is.
      2. If gcs_client+gcs_bucket provided, try to restore a tarball from
         gs://{gcs_bucket}/{gcs_prefix}/{county}.tar.gz.
      3. Fall back to the HTTP download + extract from NY State, then push
         a tarball up to GCS so next cold-start can skip the download.

    Caching to GCS is an optimization: if it fails (missing perms, transient
    error), we still return the locally-extracted path.

    Returns a map of county -> extracted .gdb path.
    """

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    source_urls = urls or NYC_COUNTY_FOOTPRINT_ZIPS
    use_gcs = gcs_client is not None and bool(gcs_bucket)

    out: dict[str, Path] = {}
    for county, url in source_urls.items():
        dest_dir = data_dir / f"{_safe_slug(county)}_Building_Footprints"
        dest_dir.mkdir(parents=True, exist_ok=True)

        existing = _discover_gdb_path(dest_dir)
        if existing is not None:
            out[county] = existing
            continue

        object_name = _gcs_object_for(gcs_prefix, county)

        # Attempt 2: GCS restore.
        if use_gcs:
            tar_path = dest_dir / f"{_safe_slug(county)}.tar.gz"
            restored = _try_restore_from_gcs(
                gcs_client=gcs_client,
                bucket=str(gcs_bucket),
                object_name=object_name,
                tar_path=tar_path,
                dest_dir=dest_dir,
            )
            if restored is not None:
                logger.info(
                    "gcs_cache_hit",
                    extra={"county": county, "object": object_name, "gdb": str(restored)},
                )
                out[county] = restored
                continue

        # Attempt 3: fresh HTTP download + extract.
        zip_name = url.split("/")[-1]
        zip_path = dest_dir / zip_name
        if not zip_path.exists():
            _download(url, zip_path)

        _extract_zip(zip_path, dest_dir)
        gdb_path = _discover_gdb_path(dest_dir) or (
            dest_dir / f"{_safe_slug(county)}_Building_Footprints.gdb"
        )

        if (not keep_zips) and zip_path.exists():
            try:
                zip_path.unlink()
            except OSError:
                pass

        # Best-effort push to GCS so the next cold-start is fast.
        if use_gcs and gdb_path.exists():
            staging = dest_dir / f"{_safe_slug(county)}.tar.gz"
            _upload_to_gcs(
                gcs_client=gcs_client,
                bucket=str(gcs_bucket),
                object_name=object_name,
                gdb_path=gdb_path,
                staging_path=staging,
            )

        out[county] = gdb_path

    return out

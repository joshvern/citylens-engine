from __future__ import annotations

import os
import zipfile
from pathlib import Path

import requests

NYC_COUNTY_FOOTPRINT_ZIPS: dict[str, str] = {
    "Bronx": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Bronx_Building_Footprints.zip",
    "Kings": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Kings_Building_Footprints.zip",
    "New York": "https://gisdata.ny.gov/GISData/State/Building_Footprints/New_York_Building_Footprints.zip",
    "Queens": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Queens_Building_Footprints.zip",
    "Richmond": "https://gisdata.ny.gov/GISData/State/Building_Footprints/Richmond_Building_Footprints.zip",
}


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


def ensure_nyc_county_footprints(*, data_dir: Path, keep_zips: bool = False) -> dict[str, Path]:
    """Ensure NYC county building footprints GDBs exist locally.

    Mirrors Urban3D-DeepRecon/scripts/download_reference_data.py.

    Returns a map of county -> extracted .gdb path.
    """

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    out: dict[str, Path] = {}
    for county, url in NYC_COUNTY_FOOTPRINT_ZIPS.items():
        dest_dir = data_dir / f"{county}_Building_Footprints"
        dest_dir.mkdir(parents=True, exist_ok=True)

        zip_name = url.split("/")[-1]
        zip_path = dest_dir / zip_name
        gdb_path = _discover_gdb_path(dest_dir) or (dest_dir / f"{county}_Building_Footprints.gdb")

        if not zip_path.exists():
            _download(url, zip_path)

        if not gdb_path.exists():
            _extract_zip(zip_path, dest_dir)
            discovered = _discover_gdb_path(dest_dir)
            if discovered is not None:
                gdb_path = discovered

        if (not keep_zips) and zip_path.exists():
            try:
                zip_path.unlink()
            except OSError:
                pass

        out[county] = gdb_path

    return out

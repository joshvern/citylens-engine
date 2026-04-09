from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlencode

import requests

GEOCODER_FIND_URL = (
    "https://gisservices.its.ny.gov/arcgis/rest/services/"
    "Locators/Street_and_Address_Composite/GeocodeServer/findAddressCandidates"
)

LAS_INDEX_LAYER_URL = (
    "https://orthos.its.ny.gov/arcgis/rest/services/vector/las_indexes/MapServer/9"
)

ORTHO_WMS_URL = "https://orthos.its.ny.gov/arcgis/rest/services/wms/2024/MapServer/WMSServer"

LIDAR_FILE_BASE = "https://gisdata.ny.gov/elevation/LIDAR/NYC_TopoBathymetric2017"

ORTHO_TILE_BASE = "https://gisdata.ny.gov/ortho/nysdop12/new_york_city/spcs/tiles"


def _normalize_address(address: str) -> str:
    return " ".join((address or "").strip().split())


@dataclass(frozen=True)
class LidarTile:
    tile_id: str
    filename: str
    direct_url: str
    ftp_path: Optional[str] = None
    collection: Optional[str] = None
    size_gb: Optional[float] = None


@dataclass(frozen=True)
class AddressAssets:
    normalized_address: str
    x: float
    y: float
    lidar_tile: LidarTile
    ortho_zip_url: str


class NYSGISAPI:
    """Minimal helper for NYC LiDAR, ortho, and address resolution."""

    def __init__(
        self,
        las_index_layer_url: str = LAS_INDEX_LAYER_URL,
        lidar_file_base: str = LIDAR_FILE_BASE,
        ortho_tile_base: str = ORTHO_TILE_BASE,
        ortho_wms_url: str = ORTHO_WMS_URL,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.las_index_layer_url = las_index_layer_url.rstrip("/")
        self.lidar_file_base = lidar_file_base.rstrip("/")
        self.ortho_tile_base = ortho_tile_base.rstrip("/")
        self.ortho_wms_url = ortho_wms_url.rstrip("?")
        self.session = session or requests.Session()

    def geocode_address(
        self, address: str, wkid: int = 3857, min_score: float = 80.0
    ) -> Tuple[float, float]:
        address = _normalize_address(address)
        if not address:
            raise ValueError("address is required for imagery fetch")

        variants = [address]
        if "NY" not in address:
            variants.append(f"{address}, NY")

        last_err: Exception | None = None
        for candidate in variants:
            try:
                resp = self.session.get(
                    GEOCODER_FIND_URL,
                    params={
                        "SingleLine": candidate,
                        "maxLocations": 1,
                        "outSR": wkid,
                        "f": "json",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()

                candidates = data.get("candidates") or []
                if not candidates:
                    raise ValueError(f"No geocoding candidates for address={candidate!r}")

                best = candidates[0]
                score = float(best.get("score") or 0)
                if score < min_score:
                    raise ValueError(f"Low geocode score ({score}) for address={candidate!r}")

                loc = best.get("location") or {}
                return float(loc["x"]), float(loc["y"])
            except Exception as exc:
                last_err = exc

        raise last_err or ValueError(f"Geocoding failed for address={address!r}")

    def get_lidar_tile_by_point(self, x: float, y: float, wkid: int = 3857) -> LidarTile:
        geometry = {"x": x, "y": y, "spatialReference": {"wkid": wkid}}
        params = {
            "f": "json",
            "geometry": json.dumps(geometry),
            "geometryType": "esriGeometryPoint",
            "inSR": wkid,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "false",
        }

        url = f"{self.las_index_layer_url}/query"
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features") or []
        if not features:
            raise ValueError(
                f"No LAS tile found at point ({x}, {y}) in layer {self.las_index_layer_url}"
            )

        attrs = features[0].get("attributes") or {}
        filename = str(attrs.get("FILENAME") or "")
        if not filename:
            raise ValueError("LAS index feature missing FILENAME attribute")

        direct_url = str(attrs.get("DIRECT_DL") or f"{self.lidar_file_base}/{filename}")
        return LidarTile(
            tile_id=filename.rsplit(".", 1)[0],
            filename=filename,
            direct_url=direct_url,
            ftp_path=attrs.get("FTP_PATH"),
            collection=attrs.get("COLLECTION"),
            size_gb=attrs.get("LAS_GB"),
        )

    def get_ortho_zip_url(self, tile_id: str) -> str:
        return f"{self.ortho_tile_base}/{tile_id}.zip"

    def build_ortho_wms_getmap_url(
        self,
        bbox: Tuple[float, float, float, float],
        *,
        width: int = 1024,
        height: int = 1024,
        crs: str = "EPSG:3857",
        img_format: str = "image/png",
        transparent: bool = True,
        layer_id: int = 0,
    ) -> str:
        bbox_str = ",".join(map(str, bbox))
        params = {
            "service": "WMS",
            "request": "GetMap",
            "version": "1.3.0",
            "layers": str(layer_id),
            "styles": "",
            "crs": crs,
            "bbox": bbox_str,
            "width": str(width),
            "height": str(height),
            "format": img_format,
            "transparent": "TRUE" if transparent else "FALSE",
        }
        return f"{self.ortho_wms_url}?{urlencode(params)}"

    def get_assets_for_address(self, address: str) -> AddressAssets:
        try_addresses = [_normalize_address(address)]
        if "," in address:
            parts = address.split(",")
            if parts and any(ch.isdigit() for ch in parts[-1]):
                try_addresses.append(",".join(parts[:-1]).strip())
        if "NY" not in address:
            try_addresses.append(f"{address}, NY")

        last_err: Exception | None = None
        for candidate in try_addresses:
            if not candidate:
                continue
            try:
                x, y = self.geocode_address(candidate, wkid=3857)
                lidar_tile = self.get_lidar_tile_by_point(x, y, wkid=3857)
                return AddressAssets(
                    normalized_address=_normalize_address(candidate),
                    x=x,
                    y=y,
                    lidar_tile=lidar_tile,
                    ortho_zip_url=self.get_ortho_zip_url(lidar_tile.tile_id),
                )
            except Exception as exc:
                last_err = exc

        raise last_err or ValueError(f"Geocoding failed for address={address!r}")

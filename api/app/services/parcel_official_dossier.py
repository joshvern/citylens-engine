from __future__ import annotations

import gzip
import hashlib
import json
import math
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from fastapi import HTTPException

from .gcs_artifacts import GcsArtifacts

DOSSIER_PREFIX = "parcel-intel/dossiers/v1"
DOSSIER_SCHEMA = "citylens-parcel-dossier/tax-lot-index@v1"
PUBLICATION_SCHEMA = "citylens-parcel-dossier/atomic-publication@v1"
PLUTO_DATASET_ID = "64uk-42ks"
ACRIS_DATASET_IDS = {
    "master": "bnx9-e6tj",
    "legals": "8h5j-fqxa",
    "parties": "636b-3b5g",
}
ROW_FIELDS = [
    "b",
    "a",
    "po",
    "ao",
    "sd",
    "sp",
    "yh",
    "la",
    "ba",
    "u",
    "nf",
    "yb",
    "lu",
    "bc",
    "z1",
    "z2",
    "bf",
    "rf",
    "cf",
    "ff",
    "al",
    "ab",
    "at",
    "f07",
    "f15",
    "er",
    "ek",
    "en",
]
_GENERATION_RE = re.compile(
    r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BBL_RE = re.compile(r"^[1-5][0-9]{9}$")
_BOROUGHS: dict[
    str,
    Literal[
        "manhattan",
        "bronx",
        "brooklyn",
        "queens",
        "staten_island",
    ],
] = {
    "1": "manhattan",
    "2": "bronx",
    "3": "brooklyn",
    "4": "queens",
    "5": "staten_island",
}


@dataclass(frozen=True)
class OfficialParcelDossier:
    row: dict[str, Any]
    borough: Literal[
        "manhattan",
        "bronx",
        "brooklyn",
        "queens",
        "staten_island",
    ]
    generation: str
    pluto_retrieved_at: datetime
    acris_updated_at: datetime


def _unavailable(
    detail: str = "Official parcel dossier is unavailable",
) -> HTTPException:
    return HTTPException(status_code=503, detail=detail)


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("expected datetime")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _optional_string(value: Any, maximum: int) -> bool:
    return value is None or (
        isinstance(value, str)
        and bool(value.strip())
        and len(value) <= maximum
    )


def _optional_number(
    value: Any,
    *,
    minimum: float = 0,
    maximum: float = 10_000_000_000,
    integer: bool = False,
) -> bool:
    if value is None:
        return True
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        return False
    number = float(value)
    return (
        minimum <= number <= maximum
        and (not integer or number.is_integer())
    )


def _validate_row(row: Any, shard: str) -> dict[str, Any]:
    if not isinstance(row, dict) or set(row) != set(ROW_FIELDS):
        raise ValueError("invalid dossier row shape")
    bbl = row.get("b")
    valid = (
        isinstance(bbl, str)
        and _BBL_RE.fullmatch(bbl) is not None
        and hashlib.sha256(bbl.encode("ascii")).hexdigest().startswith(shard)
        and _optional_string(row.get("a"), 200)
        and _optional_string(row.get("po"), 200)
        and _optional_string(row.get("ao"), 200)
        and _optional_string(row.get("sd"), 40)
        and _optional_number(row.get("sp"))
        and _optional_number(row.get("yh"), integer=True, maximum=500)
        and _optional_number(row.get("la"), maximum=1_000_000_000)
        and _optional_number(row.get("ba"), maximum=1_000_000_000)
        and _optional_number(row.get("u"), integer=True, maximum=1_000_000)
        and _optional_number(row.get("nf"), maximum=200)
        and _optional_number(
            row.get("yb"),
            integer=True,
            minimum=1600,
            maximum=2200,
        )
        and _optional_string(row.get("lu"), 8)
        and _optional_string(row.get("bc"), 12)
        and _optional_string(row.get("z1"), 40)
        and _optional_string(row.get("z2"), 40)
        and all(
            _optional_number(row.get(key), maximum=1000)
            for key in ("bf", "rf", "cf", "ff")
        )
        and all(
            _optional_number(row.get(key))
            for key in ("al", "ab", "at")
        )
        and all(
            isinstance(row.get(key), bool)
            for key in ("f07", "f15", "er")
        )
        and _optional_string(row.get("ek"), 60)
        and _optional_string(row.get("en"), 60)
    )
    if not valid:
        raise ValueError("invalid dossier row value")
    if row["sd"] is not None:
        _parse_datetime(row["sd"])
    return row


class ParcelOfficialDossierStore:
    """Generation-aware, integrity-checked private BBL dossier reader."""

    def __init__(self, *, max_cached_shards: int = 16) -> None:
        self._max_cached_shards = max_cached_shards
        self._lock = threading.Lock()
        self._shards: OrderedDict[
            tuple[str, str],
            dict[str, dict[str, Any]],
        ] = OrderedDict()

    def _load_manifest(self, gcs: GcsArtifacts) -> dict[str, Any]:
        try:
            body, content_type = gcs.download_bytes(
                object_name=f"{DOSSIER_PREFIX}/manifest.json"
            )
        except FileNotFoundError as exc:
            raise _unavailable(
                "Official parcel dossiers have not been published yet"
            ) from exc
        if content_type not in {None, "application/json"}:
            raise _unavailable()
        try:
            manifest = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise _unavailable() from exc
        if not isinstance(manifest, dict):
            raise _unavailable()

        generation = manifest.get("artifact_generation")
        artifact_prefix = manifest.get("artifact_prefix")
        stats = manifest.get("stats")
        privacy = manifest.get("privacy")
        quality = manifest.get("quality_gate")
        sources = manifest.get("sources")
        pluto = sources.get("pluto") if isinstance(sources, dict) else None
        acris = sources.get("acris") if isinstance(sources, dict) else None
        artifacts = manifest.get("artifacts")
        valid = (
            manifest.get("schema") == DOSSIER_SCHEMA
            and manifest.get("publication_schema") == PUBLICATION_SCHEMA
            and manifest.get("shard_key") == "sha256_bbl"
            and manifest.get("shard_prefix_length") == 2
            and manifest.get("compression") == "gzip"
            and manifest.get("row_fields") == ROW_FIELDS
            and isinstance(generation, str)
            and _GENERATION_RE.fullmatch(generation) is not None
            and artifact_prefix
            == f"{DOSSIER_PREFIX}/generations/{generation}"
            and isinstance(stats, dict)
            and isinstance(stats.get("rendered_rows"), int)
            and stats["rendered_rows"] >= 800_000
            and stats.get("rejected_pluto_rows") == 0
            and stats.get("rejected_acris_rows") == 0
            and stats.get("shard_count") == 256
            and isinstance(privacy, dict)
            and privacy.get("access_contract")
            == "private_authenticated_single_parcel_only"
            and privacy.get("contact_fields_published") is False
            and privacy.get("beneficial_owner_inference_published") is False
            and privacy.get("candidate_membership_published") is False
            and privacy.get("model_fields_published") is False
            and privacy.get("workflow_fields_published") is False
            and privacy.get("permitted_row_fields") == ROW_FIELDS
            and isinstance(quality, dict)
            and quality.get("passed") is True
            and quality.get("failures") == []
            and isinstance(pluto, dict)
            and pluto.get("dataset_id") == PLUTO_DATASET_ID
            and isinstance(pluto.get("retrieved_at"), str)
            and isinstance(acris, dict)
            and acris.get("dataset_ids") == ACRIS_DATASET_IDS
            and isinstance(acris.get("feature_source_updated_at"), str)
            and isinstance(artifacts, dict)
            and len(artifacts) == 256
        )
        if not valid:
            raise _unavailable()
        try:
            _parse_datetime(pluto["retrieved_at"])
            _parse_datetime(acris["feature_source_updated_at"])
        except ValueError as exc:
            raise _unavailable() from exc
        return manifest

    def _artifact(
        self,
        manifest: dict[str, Any],
        shard: str,
    ) -> dict[str, Any]:
        metadata = manifest["artifacts"].get(shard)
        expected = (
            f"{manifest['artifact_prefix']}/shards/{shard}.jsonl.gz"
        )
        valid = (
            isinstance(metadata, dict)
            and metadata.get("object_name") == expected
            and isinstance(metadata.get("sha256"), str)
            and _SHA256_RE.fullmatch(metadata["sha256"]) is not None
            and isinstance(metadata.get("uncompressed_sha256"), str)
            and _SHA256_RE.fullmatch(
                metadata["uncompressed_sha256"]
            )
            is not None
            and isinstance(metadata.get("size_bytes"), int)
            and not isinstance(metadata.get("size_bytes"), bool)
            and metadata["size_bytes"] > 0
            and isinstance(metadata.get("uncompressed_size_bytes"), int)
            and not isinstance(
                metadata.get("uncompressed_size_bytes"),
                bool,
            )
            and metadata["uncompressed_size_bytes"] > 0
            and isinstance(metadata.get("row_count"), int)
            and not isinstance(metadata.get("row_count"), bool)
            and metadata["row_count"] > 0
        )
        if not valid:
            raise _unavailable()
        return metadata

    def _load_shard(
        self,
        gcs: GcsArtifacts,
        manifest: dict[str, Any],
        shard: str,
    ) -> dict[str, dict[str, Any]]:
        generation = manifest["artifact_generation"]
        cache_key = (generation, shard)
        with self._lock:
            cached = self._shards.get(cache_key)
            if cached is not None:
                self._shards.move_to_end(cache_key)
                return cached

        metadata = self._artifact(manifest, shard)
        try:
            compressed, content_type = gcs.download_bytes(
                object_name=metadata["object_name"]
            )
        except FileNotFoundError as exc:
            raise _unavailable() from exc
        if content_type not in {None, "application/gzip"}:
            raise _unavailable()
        if (
            len(compressed) != metadata["size_bytes"]
            or hashlib.sha256(compressed).hexdigest()
            != metadata["sha256"]
        ):
            raise _unavailable(
                "Official parcel dossier failed its compressed integrity check"
            )
        try:
            body = gzip.decompress(compressed)
        except (OSError, EOFError) as exc:
            raise _unavailable(
                "Official parcel dossier compression is invalid"
            ) from exc
        if (
            len(body) != metadata["uncompressed_size_bytes"]
            or hashlib.sha256(body).hexdigest()
            != metadata["uncompressed_sha256"]
        ):
            raise _unavailable(
                "Official parcel dossier failed its content integrity check"
            )

        parsed: dict[str, dict[str, Any]] = {}
        try:
            for raw_line in body.splitlines():
                if not raw_line:
                    continue
                row = _validate_row(json.loads(raw_line), shard)
                bbl = row["b"]
                if bbl in parsed:
                    raise ValueError("duplicate dossier BBL")
                parsed[bbl] = row
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise _unavailable(
                "Official parcel dossier failed schema validation"
            ) from exc
        if len(parsed) != metadata["row_count"]:
            raise _unavailable(
                "Official parcel dossier failed its row-count check"
            )

        with self._lock:
            self._shards[cache_key] = parsed
            self._shards.move_to_end(cache_key)
            while len(self._shards) > self._max_cached_shards:
                self._shards.popitem(last=False)
        return parsed

    def get(
        self,
        gcs: GcsArtifacts,
        bbl: str,
    ) -> OfficialParcelDossier:
        if _BBL_RE.fullmatch(bbl) is None:
            raise HTTPException(status_code=422, detail="Invalid BBL")
        manifest = self._load_manifest(gcs)
        shard = hashlib.sha256(bbl.encode("ascii")).hexdigest()[:2]
        row = self._load_shard(gcs, manifest, shard).get(bbl)
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "Tax lot was not found in the current official PLUTO "
                    "snapshot"
                ),
            )
        return OfficialParcelDossier(
            row=row,
            borough=_BOROUGHS[bbl[0]],
            generation=manifest["artifact_generation"],
            pluto_retrieved_at=_parse_datetime(
                manifest["sources"]["pluto"]["retrieved_at"]
            ),
            acris_updated_at=_parse_datetime(
                manifest["sources"]["acris"][
                    "feature_source_updated_at"
                ]
            ),
        )

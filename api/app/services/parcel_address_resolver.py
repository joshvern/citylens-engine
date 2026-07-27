from __future__ import annotations

import hashlib
import json
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from fastapi import HTTPException

from .gcs_artifacts import GcsArtifacts

RESOLVER_PREFIX = "parcel-intel/resolver/v1"
RESOLVER_SCHEMA = "citylens-parcel-resolver/address-index@v1"
PUBLICATION_SCHEMA = "citylens-parcel-resolver/atomic-publication@v1"
NORMALIZATION_SCHEMA = "citylens/address-normalization@v1"
MAX_RETURNED_CANDIDATES = 20
_GENERATION_RE = re.compile(
    r"^[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BBL_RE = re.compile(r"^[1-5][0-9]{9}$")
_UNIT_RE = re.compile(
    r"\s+(?:(?:APT\.?|APARTMENT|UNIT|SUITE|STE\.?|FLOOR|FL\.?)"
    r"(?:\s+|\s*#\s*)|#\s*)[A-Z0-9-]+\s*$",
    re.IGNORECASE,
)
_ZIP_RE = re.compile(r"\s+\d{5}(?:-\d{4})?\s*$")
_STATE_RE = re.compile(r"\s+(?:NY|NEW\s+YORK)\s*$", re.IGNORECASE)
_CITY_RE = re.compile(
    r"\s+(?:"
    r"MANHATTAN|NEW\s+YORK|BROOKLYN|BRONX|QUEENS|"
    r"STATEN\s+ISLAND"
    r")\s*$",
    re.IGNORECASE,
)
_ORDINAL_RE = re.compile(r"^(\d+)(ST|ND|RD|TH)$")
_STREET_ALIASES = {
    "ST": "STREET",
    "ST.": "STREET",
    "AVE": "AVENUE",
    "AVE.": "AVENUE",
    "RD": "ROAD",
    "RD.": "ROAD",
    "BLVD": "BOULEVARD",
    "BLVD.": "BOULEVARD",
    "PL": "PLACE",
    "PL.": "PLACE",
    "DR": "DRIVE",
    "DR.": "DRIVE",
    "CT": "COURT",
    "CT.": "COURT",
    "PKWY": "PARKWAY",
    "PKWY.": "PARKWAY",
    "E": "EAST",
    "E.": "EAST",
    "W": "WEST",
    "W.": "WEST",
    "N": "NORTH",
    "N.": "NORTH",
    "S": "SOUTH",
    "S.": "SOUTH",
}
_BBL_BOROUGH: dict[str, Literal[
    "manhattan",
    "bronx",
    "brooklyn",
    "queens",
    "staten_island",
]] = {
    "1": "manhattan",
    "2": "bronx",
    "3": "brooklyn",
    "4": "queens",
    "5": "staten_island",
}


@dataclass(frozen=True)
class NormalizedAddress:
    value: str
    unit_removed: bool
    locality_removed: bool


@dataclass(frozen=True)
class ResolvedAddressCandidate:
    bbl: str
    borough: Literal[
        "manhattan",
        "bronx",
        "brooklyn",
        "queens",
        "staten_island",
    ]


@dataclass(frozen=True)
class ResolvedAddress:
    candidates: tuple[ResolvedAddressCandidate, ...]
    candidate_count: int
    truncated: bool
    unit_removed: bool
    locality_removed: bool
    generation: str
    source_name: str
    source_dataset_id: str
    source_retrieved_at: datetime


def _strip_ordinal(token: str) -> str:
    match = _ORDINAL_RE.fullmatch(token)
    return match.group(1) if match else token


def normalize_resolver_address(value: str) -> NormalizedAddress:
    """Normalize a user-entered NYC street address without guessing.

    The canonical portion intentionally stops before borough/state/ZIP and
    unit designators because PAD is a tax-lot address directory, not a unit
    directory. Hyphenated Queens house numbers remain intact.
    """

    raw = " ".join(value.strip().split())
    if not raw:
        return NormalizedAddress("", False, False)

    # A comma cleanly separates the street address from locality in normal
    # postal input. Retain only the first component; no value is ever logged or
    # returned to the caller.
    parts = [part.strip() for part in raw.split(",")]
    street = parts[0]
    locality_removed = len(parts) > 1

    before_unit = street
    street = _UNIT_RE.sub("", street)
    unit_removed = street != before_unit

    # Comma-free input commonly ends in "Brooklyn NY 11209". Strip locality
    # only after seeing ZIP/state evidence so a real street such as
    # "NEW YORK AVENUE" is not truncated.
    if not locality_removed:
        before_zip = street
        street = _ZIP_RE.sub("", street)
        zip_removed = street != before_zip
        before_state = street
        street = _STATE_RE.sub("", street)
        state_removed = street != before_state
        if zip_removed or state_removed:
            before_city = street
            street = _CITY_RE.sub("", street)
            locality_removed = (
                zip_removed or state_removed or street != before_city
            )

    text = street.upper()
    text = re.sub(r"[,.]", " ", text)
    tokens = [token for token in re.split(r"\s+", text) if token]
    expanded = [
        _STREET_ALIASES.get(token, _strip_ordinal(token))
        for token in tokens
    ]
    normalized = " ".join(expanded)
    if (
        len(normalized) < 3
        or not any(char.isdigit() for char in normalized)
        or not any(char.isalpha() for char in normalized)
    ):
        normalized = ""
    return NormalizedAddress(
        normalized,
        unit_removed,
        locality_removed,
    )


class ParcelAddressResolver:
    """Integrity-checked, generation-aware LRU for hash-sharded addresses."""

    def __init__(self, *, max_cached_shards: int = 32) -> None:
        self._max_cached_shards = max_cached_shards
        self._lock = threading.Lock()
        self._shards: OrderedDict[
            tuple[str, str],
            dict[str, tuple[str, ...]],
        ] = OrderedDict()

    @staticmethod
    def _unavailable(detail: str = "Parcel address resolver is unavailable"):
        return HTTPException(status_code=503, detail=detail)

    def _load_manifest(self, gcs: GcsArtifacts) -> dict[str, Any]:
        try:
            body, content_type = gcs.download_bytes(
                object_name=f"{RESOLVER_PREFIX}/manifest.json"
            )
        except FileNotFoundError as exc:
            raise self._unavailable(
                "Parcel address resolver has not been published yet"
            ) from exc
        if content_type not in {None, "application/json"}:
            raise self._unavailable()
        try:
            manifest = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise self._unavailable() from exc
        if not isinstance(manifest, dict):
            raise self._unavailable()

        generation = manifest.get("artifact_generation")
        artifact_prefix = manifest.get("artifact_prefix")
        quality = manifest.get("quality_gate")
        privacy = manifest.get("privacy")
        artifacts = manifest.get("artifacts")
        valid = (
            manifest.get("schema") == RESOLVER_SCHEMA
            and manifest.get("publication_schema") == PUBLICATION_SCHEMA
            and manifest.get("normalization_schema") == NORMALIZATION_SCHEMA
            and manifest.get("hash_algorithm") == "sha256"
            and manifest.get("shard_prefix_length") == 2
            and manifest.get("row_fields") == ["h", "b", "s"]
            and isinstance(generation, str)
            and _GENERATION_RE.fullmatch(generation) is not None
            and artifact_prefix
            == f"{RESOLVER_PREFIX}/generations/{generation}"
            and isinstance(quality, dict)
            and quality.get("passed") is True
            and quality.get("failures") == []
            and isinstance(privacy, dict)
            and privacy.get("plaintext_addresses_published") is False
            and privacy.get("candidate_membership_published") is False
            and privacy.get("permitted_row_fields") == ["h", "b", "s"]
            and isinstance(artifacts, dict)
        )
        if not valid:
            raise self._unavailable()
        return manifest

    def _artifact_metadata(
        self,
        manifest: dict[str, Any],
        shard: str,
    ) -> dict[str, Any] | None:
        artifacts = manifest["artifacts"]
        metadata = artifacts.get(shard)
        if metadata is None:
            return None
        expected = (
            f"{manifest['artifact_prefix']}/shards/{shard}.jsonl"
        )
        valid = (
            isinstance(metadata, dict)
            and metadata.get("object_name") == expected
            and isinstance(metadata.get("sha256"), str)
            and _SHA256_RE.fullmatch(metadata["sha256"]) is not None
            and isinstance(metadata.get("size_bytes"), int)
            and not isinstance(metadata.get("size_bytes"), bool)
            and metadata["size_bytes"] > 0
            and isinstance(metadata.get("row_count"), int)
            and not isinstance(metadata.get("row_count"), bool)
            and metadata["row_count"] > 0
            and isinstance(metadata.get("address_hash_count"), int)
            and not isinstance(metadata.get("address_hash_count"), bool)
            and metadata["address_hash_count"] > 0
        )
        if not valid:
            raise self._unavailable()
        return metadata

    def _load_shard(
        self,
        gcs: GcsArtifacts,
        manifest: dict[str, Any],
        shard: str,
    ) -> dict[str, tuple[str, ...]]:
        generation = manifest["artifact_generation"]
        cache_key = (generation, shard)
        with self._lock:
            cached = self._shards.get(cache_key)
            if cached is not None:
                self._shards.move_to_end(cache_key)
                return cached

        metadata = self._artifact_metadata(manifest, shard)
        if metadata is None:
            return {}
        try:
            body, content_type = gcs.download_bytes(
                object_name=metadata["object_name"]
            )
        except FileNotFoundError as exc:
            raise self._unavailable() from exc
        if content_type not in {None, "application/x-ndjson"}:
            raise self._unavailable()
        if (
            len(body) != metadata["size_bytes"]
            or hashlib.sha256(body).hexdigest() != metadata["sha256"]
        ):
            raise self._unavailable(
                "Parcel address resolver failed its integrity check"
            )

        grouped: dict[str, list[str]] = {}
        row_count = 0
        try:
            for raw_line in body.splitlines():
                if not raw_line:
                    continue
                row = json.loads(raw_line)
                if not isinstance(row, dict) or set(row) != {"h", "b", "s"}:
                    raise ValueError("invalid resolver row shape")
                address_hash = row["h"]
                bbl = row["b"]
                source = row["s"]
                if (
                    not isinstance(address_hash, str)
                    or _SHA256_RE.fullmatch(address_hash) is None
                    or not address_hash.startswith(shard)
                    or not isinstance(bbl, str)
                    or _BBL_RE.fullmatch(bbl) is None
                    or source not in {"pad", "pluto_derived"}
                ):
                    raise ValueError("invalid resolver row value")
                values = grouped.setdefault(address_hash, [])
                if bbl in values:
                    raise ValueError("duplicate resolver address-BBL pair")
                values.append(bbl)
                row_count += 1
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise self._unavailable(
                "Parcel address resolver failed schema validation"
            ) from exc
        if (
            row_count != metadata["row_count"]
            or len(grouped) != metadata["address_hash_count"]
        ):
            raise self._unavailable(
                "Parcel address resolver failed its row-count check"
            )
        parsed = {
            address_hash: tuple(sorted(bbls))
            for address_hash, bbls in grouped.items()
        }
        with self._lock:
            self._shards[cache_key] = parsed
            self._shards.move_to_end(cache_key)
            while len(self._shards) > self._max_cached_shards:
                self._shards.popitem(last=False)
        return parsed

    @staticmethod
    def _source_receipt(
        manifest: dict[str, Any],
    ) -> tuple[str, str, datetime]:
        source = manifest.get("source")
        official = source.get("official_sources") if isinstance(source, dict) else None
        pad = next(
            (
                item
                for item in official or []
                if isinstance(item, dict)
                and item.get("dataset_key") == "pad"
            ),
            None,
        )
        if not isinstance(source, dict) or not isinstance(pad, dict):
            raise ParcelAddressResolver._unavailable()
        name = source.get("name")
        dataset_id = pad.get("dataset_id")
        retrieved_at = pad.get("retrieved_at")
        if (
            not isinstance(name, str)
            or not name
            or dataset_id != "bc8t-ecyu"
            or not isinstance(retrieved_at, str)
        ):
            raise ParcelAddressResolver._unavailable()
        try:
            parsed_retrieved_at = datetime.fromisoformat(retrieved_at)
        except ValueError as exc:
            raise ParcelAddressResolver._unavailable() from exc
        return name, dataset_id, parsed_retrieved_at

    def resolve(
        self,
        gcs: GcsArtifacts,
        address: str,
    ) -> ResolvedAddress:
        normalized = normalize_resolver_address(address)
        if not normalized.value:
            raise HTTPException(
                status_code=422,
                detail="Enter a complete NYC street address with a house number",
            )
        address_hash = hashlib.sha256(
            normalized.value.encode("utf-8")
        ).hexdigest()
        manifest = self._load_manifest(gcs)
        shard_rows = self._load_shard(gcs, manifest, address_hash[:2])
        all_bbls = shard_rows.get(address_hash, ())
        candidates = tuple(
            ResolvedAddressCandidate(
                bbl=bbl,
                borough=_BBL_BOROUGH[bbl[0]],
            )
            for bbl in all_bbls[:MAX_RETURNED_CANDIDATES]
        )
        source_name, dataset_id, source_retrieved_at = (
            self._source_receipt(manifest)
        )
        return ResolvedAddress(
            candidates=candidates,
            candidate_count=len(all_bbls),
            truncated=len(all_bbls) > len(candidates),
            unit_removed=normalized.unit_removed,
            locality_removed=normalized.locality_removed,
            generation=manifest["artifact_generation"],
            source_name=source_name,
            source_dataset_id=dataset_id,
            source_retrieved_at=source_retrieved_at,
        )

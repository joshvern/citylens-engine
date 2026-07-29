from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models.schemas import ArtifactResponse, RunResponse
from .artifact_contract import artifact_media_type
from .gcs_artifacts import GcsArtifacts
from .run_errors import normalize_run_record
from .settings import Settings


def _object_from_gcs_uri(gcs_uri: str, *, bucket: str) -> str:
    prefix = f"gs://{bucket}/"
    return gcs_uri[len(prefix) :] if gcs_uri.startswith(prefix) else ""


def _artifact_response(
    *,
    name: str,
    gcs_uri: str,
    detail: dict[str, Any],
    settings: Settings,
    gcs: GcsArtifacts,
) -> ArtifactResponse:
    object_from_uri = _object_from_gcs_uri(gcs_uri, bucket=settings.bucket)
    detail_object = str(detail.get("gcs_object") or "")
    detail_uri = str(detail.get("gcs_uri") or "")
    detail_matches = bool(detail) and (
        detail_uri == gcs_uri
        or (object_from_uri and detail_object == object_from_uri)
    )

    gcs_object = (
        detail_object if detail_matches and detail_object else object_from_uri
    )
    created_at = detail.get("created_at") if detail_matches else None
    if not isinstance(created_at, datetime):
        created_at = datetime.utcnow()

    signed_url = None
    if settings.sign_urls and gcs_object:
        try:
            signed_url = gcs.signed_url(
                object_name=gcs_object,
                ttl_seconds=settings.sign_url_ttl_seconds,
            )
        except Exception:
            signed_url = None

    return ArtifactResponse(
        name=name,
        type=(
            str(detail.get("type") or artifact_media_type(name))
            if detail_matches
            else artifact_media_type(name)
        ),
        gcs_uri=gcs_uri,
        gcs_object=gcs_object,
        sha256=str(detail.get("sha256") or "") if detail_matches else "",
        size_bytes=int(detail.get("size_bytes") or 0) if detail_matches else 0,
        created_at=created_at,
        signed_url=signed_url,
    )


def build_run_response(
    *,
    run: dict[str, Any],
    artifacts: list[dict[str, Any]] | None = None,
    settings: Settings,
    gcs: GcsArtifacts,
) -> RunResponse:
    out_artifacts: list[ArtifactResponse] = []
    # The Firestore run doc may contain an `artifacts` field (either legacy list or
    # the newer {name: gcs_uri} map). We always provide the API response's
    # artifacts list explicitly, so remove it from the base payload to avoid
    # passing duplicate keyword args into RunResponse.
    run_base = normalize_run_record(run)
    run_base.pop("artifacts", None)

    detailed_by_name = {
        str(artifact.get("name") or ""): artifact
        for artifact in artifacts or []
        if str(artifact.get("name") or "")
    }

    # The compact map remains authoritative for artifact membership, while
    # the subcollection owns integrity metadata. Returning early from the map
    # used to discard the SHA-256, byte count, media type, and creation time
    # that the worker had already persisted.
    run_artifacts = run.get("artifacts")
    if isinstance(run_artifacts, dict) and run_artifacts:
        for name, gcs_uri in run_artifacts.items():
            if not name or not gcs_uri:
                continue
            out_artifacts.append(
                _artifact_response(
                    name=str(name),
                    gcs_uri=str(gcs_uri),
                    detail=detailed_by_name.get(str(name), {}),
                    settings=settings,
                    gcs=gcs,
                )
            )
        return RunResponse(**run_base, artifacts=out_artifacts)

    # Fallback: read from artifacts subcollection if the map is not present.
    if artifacts is None:
        artifacts = []

    for a in artifacts:
        out_artifacts.append(
            _artifact_response(
                name=str(a.get("name") or ""),
                gcs_uri=str(a.get("gcs_uri") or ""),
                detail=a,
                settings=settings,
                gcs=gcs,
            )
        )

    return RunResponse(
        **run_base,
        artifacts=out_artifacts,
    )

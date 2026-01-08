from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models.schemas import ArtifactResponse, RunResponse
from .gcs_artifacts import GcsArtifacts
from .settings import Settings


def _infer_type(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".png"):
        return "image/png"
    if lower.endswith(".geojson"):
        return "application/geo+json"
    if lower.endswith(".ply"):
        return "model/ply"
    if lower.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


def build_run_response(
    *,
    run: dict[str, Any],
    artifacts: list[dict[str, Any]] | None = None,
    settings: Settings,
    gcs: GcsArtifacts,
) -> RunResponse:
    out_artifacts: list[ArtifactResponse] = []

    # Prefer the compact "artifacts" map from the run doc (written by worker) if present.
    # This contains {name: gcs_uri} for all successfully uploaded artifacts and is more reliable
    # than the artifacts subcollection.
    run_artifacts = run.get("artifacts")
    if isinstance(run_artifacts, dict) and run_artifacts:
        for name, gcs_uri in run_artifacts.items():
            if not name or not gcs_uri:
                continue
            gcs_uri_str = str(gcs_uri)
            obj = gcs_uri_str.replace("gs://", "").replace(f"{settings.bucket}/", "", 1)
            signed_url = None
            if settings.sign_urls:
                if "gs://" in gcs_uri_str and obj:
                    try:
                        signed_url = gcs.signed_url(
                            object_name=obj, ttl_seconds=settings.sign_url_ttl_seconds
                        )
                    except Exception:
                        signed_url = None
            out_artifacts.append(
                ArtifactResponse(
                    name=str(name),
                    type=_infer_type(str(name)),
                    gcs_uri=gcs_uri_str,
                    gcs_object=obj if "gs://" in gcs_uri_str else "",
                    sha256="",
                    size_bytes=0,
                    created_at=datetime.utcnow(),
                    signed_url=signed_url,
                )
            )
        return RunResponse(**run, artifacts=out_artifacts)

    # Fallback: read from artifacts subcollection if the map is not present.
    if artifacts is None:
        artifacts = []

    for a in artifacts:
        name = str(a.get("name") or "")
        gcs_uri = str(a.get("gcs_uri") or "")
        gcs_object = str(a.get("gcs_object") or "")
        sha256 = str(a.get("sha256") or "")
        size_bytes = int(a.get("size_bytes") or 0)
        created_at = a.get("created_at")
        if not isinstance(created_at, datetime):
            created_at = datetime.utcnow()

        signed_url = None
        if settings.sign_urls:
            obj = str(a.get("gcs_object") or "")
            if obj:
                try:
                    signed_url = gcs.signed_url(
                        object_name=obj, ttl_seconds=settings.sign_url_ttl_seconds
                    )
                except Exception:
                    signed_url = None

        out_artifacts.append(
            ArtifactResponse(
                name=name,
                type=_infer_type(name),
                gcs_uri=gcs_uri,
                gcs_object=gcs_object,
                sha256=sha256,
                size_bytes=size_bytes,
                created_at=created_at,
                signed_url=signed_url,
            )
        )

    run_out: dict[str, Any] = dict(run)
    run_out.setdefault("error", None)
    run_out.setdefault("execution_id", None)

    return RunResponse(
        **run_out,
        artifacts=out_artifacts,
    )

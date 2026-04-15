from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Request

from ..models.schemas import ArtifactResponse, RunResponse
from .demo_registry import DemoRegistry, DemoRunMeta

EXPECTED_DEMO_ARTIFACTS = (
    "preview.png",
    "change.geojson",
    "mesh.ply",
    "run_summary.json",
)


class DemoBundleInvalidError(RuntimeError):
    pass


def _default_demo_artifacts_root() -> Path:
    here = Path(__file__).resolve()

    container_path = here.parents[2] / "deploy" / "demo_artifacts"
    if container_path.exists():
        return container_path

    local_engine_root = here.parents[3]
    local_path = local_engine_root / "deploy" / "demo_artifacts"
    if local_path.exists():
        return local_path

    app_dir = here.parents[1]
    return app_dir / "demo_artifacts"


def demo_artifacts_root() -> Path:
    env_path = os.getenv("CITYLENS_DEMO_ARTIFACTS_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return _default_demo_artifacts_root()


def _artifact_type(name: str) -> str:
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


def demo_artifact_path(*, run_id: str, artifact_name: str) -> Path | None:
    if artifact_name not in EXPECTED_DEMO_ARTIFACTS:
        return None

    run_dir = demo_artifacts_root() / run_id
    path = run_dir / artifact_name
    if not path.exists() or not path.is_file():
        return None
    return path


def _bundle_paths_for_run(run_id: str) -> dict[str, Path | None]:
    return {
        artifact_name: demo_artifact_path(run_id=run_id, artifact_name=artifact_name)
        for artifact_name in EXPECTED_DEMO_ARTIFACTS
    }


def _format_missing_artifacts(run_id: str, missing: list[str]) -> str:
    return (
        f"Bundled demo artifacts missing for run_id={run_id}: {', '.join(missing)} "
        f"(root={demo_artifacts_root()})"
    )


def ensure_demo_bundle_complete(*, run_id: str, require_all: bool) -> None:
    paths = _bundle_paths_for_run(run_id)
    present = [name for name, path in paths.items() if path is not None]
    missing = [name for name, path in paths.items() if path is None]

    if not present and not require_all:
        return
    if missing:
        raise DemoBundleInvalidError(_format_missing_artifacts(run_id, missing))


def validate_demo_bundle_for_registry(registry: DemoRegistry) -> None:
    errors: list[str] = []
    for meta in registry.all():
        try:
            ensure_demo_bundle_complete(run_id=meta.run_id, require_all=True)
        except DemoBundleInvalidError as exc:
            errors.append(str(exc))

    if errors:
        raise DemoBundleInvalidError("Invalid bundled demo artifacts:\n" + "\n".join(errors))


def build_static_demo_run_response(*, request: Request, meta: DemoRunMeta) -> RunResponse | None:
    ensure_demo_bundle_complete(run_id=meta.run_id, require_all=False)

    paths = _bundle_paths_for_run(meta.run_id)
    if not any(path is not None for path in paths.values()):
        return None

    artifacts: list[ArtifactResponse] = []

    for artifact_name in EXPECTED_DEMO_ARTIFACTS:
        path = paths[artifact_name]
        if path is None:
            raise DemoBundleInvalidError(_format_missing_artifacts(meta.run_id, [artifact_name]))

        stat = path.stat()
        created_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        artifacts.append(
            ArtifactResponse(
                name=artifact_name,
                type=_artifact_type(artifact_name),
                gcs_uri=f"demo://{meta.run_id}/{artifact_name}",
                gcs_object=f"demo/{meta.run_id}/{artifact_name}",
                sha256="",
                size_bytes=stat.st_size,
                created_at=created_at,
                signed_url=str(
                    request.url_for(
                        "demo_artifact",
                        run_id=meta.run_id,
                        artifact_name=artifact_name,
                    )
                ),
            )
        )

    created_at = max(
        (artifact.created_at for artifact in artifacts),
        default=datetime.now(timezone.utc),
    )

    return RunResponse(
        run_id=meta.run_id,
        user_id="demo",
        status="succeeded",
        stage="complete",
        progress=100,
        request={
            "address": meta.address,
            "imagery_year": meta.imagery_year,
            "baseline_year": meta.baseline_year,
            "segmentation_backend": meta.segmentation_backend,
            "outputs": meta.outputs,
        },
        error=None,
        execution_id=None,
        created_at=created_at,
        updated_at=created_at,
        artifacts=artifacts,
    )

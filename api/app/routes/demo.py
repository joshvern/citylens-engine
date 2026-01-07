from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

from ..models.schemas import DemoRunFeatured, RunResponse
from ..services.demo_registry import DemoRegistry
from ..services.firestore_store import FirestoreStore
from ..services.gcs_artifacts import GcsArtifacts
from ..services.rate_limit import demo_rate_limit
from ..services.run_presenter import build_run_response
from ..services.settings import Settings, get_settings

router = APIRouter(tags=["demo"])


def _default_demo_runs_path() -> str:
    # Source-of-truth lives in citylens-engine/deploy/demo_runs.json.
    # In the API container we copy `deploy/` to `/app/deploy`.
    here = Path(__file__).resolve()

    # Container location
    container_path = here.parents[2] / "deploy" / "demo_runs.json"  # /app/deploy/demo_runs.json
    if container_path.exists():
        return str(container_path)

    # Local repo location
    local_engine_root = here.parents[3]  # .../citylens-engine
    local_path = local_engine_root / "deploy" / "demo_runs.json"
    if local_path.exists():
        return str(local_path)

    # Fallback (older behavior)
    app_dir = here.parents[1]  # api/app
    return str(app_dir / "demo_runs.json")


_DEMO_REGISTRY: DemoRegistry | None = None


def get_demo_registry() -> DemoRegistry:
    global _DEMO_REGISTRY
    if _DEMO_REGISTRY is None:
        path = os.getenv("CITYLENS_DEMO_RUNS_PATH") or _default_demo_runs_path()
        _DEMO_REGISTRY = DemoRegistry(json_path=path)
    return _DEMO_REGISTRY


def get_store(settings: Settings = Depends(get_settings)) -> FirestoreStore:
    return FirestoreStore(
        project_id=settings.project_id,
        runs_collection=settings.runs_collection,
        users_collection=settings.users_collection,
    )


def get_gcs(settings: Settings = Depends(get_settings)) -> GcsArtifacts:
    return GcsArtifacts(bucket=settings.bucket)


@router.get("/demo/featured", response_model=dict[str, list[DemoRunFeatured]])
def demo_featured(
    _rate_limit: None = Depends(demo_rate_limit),
    registry: DemoRegistry = Depends(get_demo_registry),
) -> dict[str, list[DemoRunFeatured]]:
    featured = registry.featured()

    out: dict[str, list[DemoRunFeatured]] = {}
    for category, metas in featured.items():
        out[category] = [
            DemoRunFeatured(
                run_id=m.run_id,
                label=m.label,
                address=m.address,
                imagery_year=m.imagery_year,
                baseline_year=m.baseline_year,
                segmentation_backend=m.segmentation_backend,
                outputs=m.outputs,
            )
            for m in metas
        ]

    return out


@router.get("/demo/runs/{run_id}", response_model=RunResponse)
def demo_get_run(
    run_id: str,
    _rate_limit: None = Depends(demo_rate_limit),
    registry: DemoRegistry = Depends(get_demo_registry),
    settings: Settings = Depends(get_settings),
    store: FirestoreStore = Depends(get_store),
    gcs: GcsArtifacts = Depends(get_gcs),
) -> RunResponse:
    if not registry.get(run_id):
        raise HTTPException(status_code=404, detail="Run not found")

    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    artifacts = store.list_artifacts(run_id)
    return build_run_response(run=run, artifacts=artifacts, settings=settings, gcs=gcs)

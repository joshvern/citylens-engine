from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..models.schemas import DemoRunFeatured, RunResponse
from ..services.demo_registry import DemoRegistry, get_demo_registry
from ..services.rate_limit import demo_rate_limit
from ..services.run_presenter import build_run_response
from ..services.settings import Settings, get_settings
from .runs import get_gcs, get_store

router = APIRouter(tags=["demo"])


@router.get("/demo/featured", response_model=dict[str, list[DemoRunFeatured]])
def demo_featured(
    registry: DemoRegistry = Depends(get_demo_registry),
    _: None = Depends(demo_rate_limit),
) -> dict[str, list[DemoRunFeatured]]:
    return registry.featured()


@router.get("/demo/runs/{run_id}", response_model=RunResponse)
def demo_get_run(
    run_id: str,
    registry: DemoRegistry = Depends(get_demo_registry),
    settings: Settings = Depends(get_settings),
    store=Depends(get_store),
    gcs=Depends(get_gcs),
    _: None = Depends(demo_rate_limit),
) -> RunResponse:
    if not registry.is_allowed(run_id):
        raise HTTPException(status_code=404, detail="Run not found")

    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    artifacts = store.list_artifacts(run_id)
    return build_run_response(run=run, artifacts=artifacts, settings=settings, gcs=gcs)

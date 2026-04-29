from __future__ import annotations

from fastapi import APIRouter

from ..services.run_options import run_options_payload


router = APIRouter(tags=["run-options"])


@router.get("/run-options")
def run_options() -> dict:
    return run_options_payload()

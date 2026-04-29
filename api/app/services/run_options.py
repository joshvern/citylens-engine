from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


SUPPORTED_IMAGERY_YEARS: list[int] = [2024]
SUPPORTED_BASELINE_YEARS: list[int] = [2017]
SUPPORTED_SEGMENTATION_BACKENDS: list[str] = ["sam2"]
SUPPORTED_OUTPUTS: set[str] = {"previews", "change", "mesh"}
DEFAULT_OUTPUTS: list[str] = ["previews", "change", "mesh"]
DEFAULT_AOI_RADIUS_M: int = 250


OutputName = Literal["previews", "change", "mesh"]


class PublicRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str = Field(min_length=1)
    imagery_year: Literal[2024] = 2024
    baseline_year: Literal[2017] = 2017
    segmentation_backend: Literal["sam2"] = "sam2"
    outputs: list[OutputName] = Field(default_factory=lambda: list(DEFAULT_OUTPUTS), min_length=1)
    notes: Optional[str] = None


def run_options_payload() -> dict:
    return {
        "imagery_years": list(SUPPORTED_IMAGERY_YEARS),
        "baseline_years": list(SUPPORTED_BASELINE_YEARS),
        "segmentation_backends": list(SUPPORTED_SEGMENTATION_BACKENDS),
        "outputs": sorted(SUPPORTED_OUTPUTS),
        "defaults": {
            "imagery_year": SUPPORTED_IMAGERY_YEARS[0],
            "baseline_year": SUPPORTED_BASELINE_YEARS[0],
            "segmentation_backend": SUPPORTED_SEGMENTATION_BACKENDS[0],
            "outputs": list(DEFAULT_OUTPUTS),
            "aoi_radius_m": DEFAULT_AOI_RADIUS_M,
        },
    }

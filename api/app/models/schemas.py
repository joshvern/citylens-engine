from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ArtifactResponse(BaseModel):
    name: str
    type: str
    gcs_uri: str
    gcs_object: str
    sha256: str
    size_bytes: int
    created_at: datetime
    signed_url: Optional[str] = None


class RunResponse(BaseModel):
    run_id: str
    user_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    stage: str
    progress: int = Field(ge=0, le=100)
    request: dict[str, Any]
    error: Optional[str] = None
    execution_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    artifacts: list[ArtifactResponse] = Field(default_factory=list)


class DemoRunFeatured(BaseModel):
    run_id: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str] = Field(default_factory=list)

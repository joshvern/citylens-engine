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


class RunErrorResponse(BaseModel):
    code: str
    message: str
    stage: Optional[str] = None
    traceback_summary: list[str] = Field(default_factory=list)


class RunRecordBase(BaseModel):
    run_id: str
    user_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    stage: str
    progress: int = Field(ge=0, le=100)
    request: dict[str, Any] = Field(default_factory=dict)
    error: Optional[RunErrorResponse] = None
    execution_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class RunListItem(RunRecordBase):
    pass


class RunResponse(RunRecordBase):
    artifacts: list[ArtifactResponse] = Field(default_factory=list)


class RunListResponse(BaseModel):
    items: list[RunListItem]
    next_cursor: Optional[str] = None


class DemoRunFeatured(BaseModel):
    run_id: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str] = Field(default_factory=list)

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bucket: str
    runs_collection: str = "runs"
    work_root: str = "/tmp/runs"
    download_reference_data: bool = False
    reference_data_dir: str = "/tmp/reference-data"
    reference_keep_zips: bool = False


def get_settings() -> Settings:
    return Settings(
        project_id=_env("GOOGLE_CLOUD_PROJECT"),
        region=_env("CITYLENS_REGION"),
        bucket=_env("CITYLENS_BUCKET"),
        runs_collection=os.getenv("CITYLENS_RUNS_COLLECTION", "runs"),
        work_root=os.getenv("CITYLENS_WORK_ROOT", "/tmp/runs"),
        download_reference_data=os.getenv("CITYLENS_DOWNLOAD_REFERENCE_DATA", "0") == "1",
        reference_data_dir=os.getenv("CITYLENS_REFERENCE_DATA_DIR", "/tmp/reference-data"),
        reference_keep_zips=os.getenv("CITYLENS_REFERENCE_KEEP_ZIPS", "0") == "1",
    )

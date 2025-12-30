from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    return int(raw)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bucket: str

    api_keys: list[str]

    runs_collection: str = "runs"
    users_collection: str = "users"

    sign_urls: bool = False
    sign_url_ttl_seconds: int = 300

    job_name: str = ""


def get_settings() -> Settings:
    keys_raw = os.getenv("CITYLENS_API_KEYS", "").strip()
    api_keys = [k.strip() for k in keys_raw.split(",") if k.strip()]

    return Settings(
        project_id=_env("GOOGLE_CLOUD_PROJECT"),
        region=_env("CITYLENS_REGION"),
        bucket=_env("CITYLENS_BUCKET"),
        api_keys=api_keys,
        runs_collection=os.getenv("CITYLENS_RUNS_COLLECTION", "runs"),
        users_collection=os.getenv("CITYLENS_USERS_COLLECTION", "users"),
        sign_urls=_env_bool("CITYLENS_SIGN_URLS", False),
        sign_url_ttl_seconds=_env_int("CITYLENS_SIGN_URL_TTL_SECONDS", 300),
        job_name=_env("CITYLENS_JOB_NAME"),
    )

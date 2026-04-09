from __future__ import annotations

import os
from dataclasses import dataclass

import google.auth

DEFAULT_CORS_ORIGINS = [
    "https://citylens.dev",
    "https://www.citylens.dev",
    "http://localhost:3000",
]


def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _csv_env(name: str, default: str | None = None) -> list[str]:
    raw = os.getenv(name, default if default is not None else "")
    values = [item.strip() for item in str(raw).split(",") if item.strip()]
    if not values:
        raise RuntimeError(f"Missing required env var: {name}")

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _project_id() -> str:
    env_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if env_project_id:
        return env_project_id

    # On Cloud Run, ADC is available and includes the project id.
    _, project_id = google.auth.default()
    if project_id:
        return project_id

    raise RuntimeError("Missing required env var: GOOGLE_CLOUD_PROJECT")


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
    cors_origins: list[str]

    runs_collection: str = "runs"
    users_collection: str = "users"

    sign_urls: bool = False
    sign_url_ttl_seconds: int = 300

    job_name: str = ""


def get_settings() -> Settings:
    api_keys = _csv_env("CITYLENS_API_KEYS")
    cors_origins = _csv_env("CITYLENS_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))

    return Settings(
        project_id=_project_id(),
        region=_env("CITYLENS_REGION"),
        bucket=_env("CITYLENS_BUCKET"),
        api_keys=api_keys,
        cors_origins=cors_origins,
        runs_collection=os.getenv("CITYLENS_RUNS_COLLECTION", "runs"),
        users_collection=os.getenv("CITYLENS_USERS_COLLECTION", "users"),
        sign_urls=_env_bool("CITYLENS_SIGN_URLS", False),
        sign_url_ttl_seconds=_env_int("CITYLENS_SIGN_URL_TTL_SECONDS", 300),
        job_name=_env("CITYLENS_JOB_NAME"),
    )

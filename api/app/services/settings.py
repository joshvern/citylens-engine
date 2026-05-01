from __future__ import annotations

import os
from dataclasses import dataclass, field

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


def _csv_env(name: str, default: str | None = None, *, required: bool = True) -> list[str]:
    raw = os.getenv(name, default if default is not None else "")
    values = [item.strip() for item in str(raw).split(",") if item.strip()]
    if not values:
        if required:
            raise RuntimeError(f"Missing required env var: {name}")
        return []

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


def _opt_env(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    return raw.strip()


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bucket: str

    # Deprecated for normal users. Kept for back-compat in test fixtures only.
    api_keys: list[str]
    cors_origins: list[str]

    runs_collection: str = "runs"
    users_collection: str = "users"
    auth_identities_collection: str = "auth_identities"
    usage_months_collection: str = "usage_months"
    api_keys_index_collection: str = "api_keys_by_hash"

    sign_urls: bool = False
    sign_url_ttl_seconds: int = 300

    job_name: str = ""

    # Auth
    auth_provider: str = "neon"
    auth_issuer: str | None = None
    auth_audience: str | None = None
    auth_jwks_url: str | None = None
    auth_required: bool = True
    allow_mock_auth: bool = False

    admin_auth_subs: list[str] = field(default_factory=list)
    admin_emails: list[str] = field(default_factory=list)

    allow_admin_api_keys: bool = False
    admin_api_keys: list[str] = field(default_factory=list)
    admin_api_key_hashes: list[str] = field(default_factory=list)

    # User-level programmatic API keys (Bearer `clk_live_…`).
    # Off by default so production deploys must explicitly opt in.
    allow_user_api_keys: bool = False

    # Plan
    free_monthly_runs: int = 5

    # Docs gating
    docs_access_key_sha256: str | None = None


def get_settings() -> Settings:
    api_keys = _csv_env("CITYLENS_API_KEYS", default="deprecated-unused", required=False)
    cors_origins = _csv_env("CITYLENS_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))

    return Settings(
        project_id=_project_id(),
        region=_env("CITYLENS_REGION"),
        bucket=_env("CITYLENS_BUCKET"),
        api_keys=api_keys,
        cors_origins=cors_origins,
        runs_collection=os.getenv("CITYLENS_RUNS_COLLECTION", "runs"),
        users_collection=os.getenv("CITYLENS_USERS_COLLECTION", "users"),
        auth_identities_collection=os.getenv(
            "CITYLENS_AUTH_IDENTITIES_COLLECTION", "auth_identities"
        ),
        usage_months_collection=os.getenv("CITYLENS_USAGE_MONTHS_COLLECTION", "usage_months"),
        api_keys_index_collection=os.getenv(
            "CITYLENS_API_KEYS_INDEX_COLLECTION", "api_keys_by_hash"
        ),
        sign_urls=_env_bool("CITYLENS_SIGN_URLS", False),
        sign_url_ttl_seconds=_env_int("CITYLENS_SIGN_URL_TTL_SECONDS", 300),
        job_name=_env("CITYLENS_JOB_NAME"),
        auth_provider=os.getenv("CITYLENS_AUTH_PROVIDER", "neon"),
        auth_issuer=_opt_env("CITYLENS_AUTH_ISSUER"),
        auth_audience=_opt_env("CITYLENS_AUTH_AUDIENCE"),
        auth_jwks_url=_opt_env("CITYLENS_AUTH_JWKS_URL"),
        auth_required=_env_bool("CITYLENS_AUTH_REQUIRED", True),
        allow_mock_auth=_env_bool("CITYLENS_ALLOW_MOCK_AUTH", False),
        admin_auth_subs=_csv_env("CITYLENS_ADMIN_AUTH_SUBS", default="", required=False),
        admin_emails=_csv_env("CITYLENS_ADMIN_EMAILS", default="", required=False),
        allow_admin_api_keys=_env_bool("CITYLENS_ALLOW_ADMIN_API_KEYS", False),
        admin_api_keys=_csv_env("CITYLENS_ADMIN_API_KEYS", default="", required=False),
        admin_api_key_hashes=_csv_env(
            "CITYLENS_ADMIN_API_KEY_HASHES", default="", required=False
        ),
        allow_user_api_keys=_env_bool("CITYLENS_ALLOW_USER_API_KEYS", False),
        free_monthly_runs=_env_int("CITYLENS_FREE_MONTHLY_RUNS", 5),
        docs_access_key_sha256=_opt_env("CITYLENS_DOCS_ACCESS_KEY_SHA256"),
    )

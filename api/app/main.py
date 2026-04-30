from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Awaitable, Callable
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import JSONResponse
from starlette.responses import PlainTextResponse, Response

from .routes.demo import router as demo_router
from .routes.health import router as health_router
from .routes.me import router as me_router
from .routes.run_options import router as run_options_router
from .routes.runs import router as runs_router
from .services.logging import configure_json_logging
from .services.run_options import (
    SUPPORTED_BASELINE_YEARS,
    SUPPORTED_IMAGERY_YEARS,
    SUPPORTED_OUTPUTS,
    SUPPORTED_SEGMENTATION_BACKENDS,
)
from .services.settings import DEFAULT_CORS_ORIGINS, Settings, get_settings

_DOCS_PATHS = {"/docs", "/redoc", "/openapi.json"}


app = FastAPI(
    title="citylens-engine-api",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _allowed_origins() -> list[str]:
    try:
        return list(get_settings().cors_origins)
    except Exception:
        return list(DEFAULT_CORS_ORIGINS)


def _is_demo_request(request: Request) -> bool:
    return request.url.path.startswith("/v1/demo/")


def _is_demo_origin_allowed(origin: str, allowed_origins: list[str]) -> bool:
    if origin in allowed_origins:
        return True

    parsed = urlparse(origin)
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    return parsed.hostname.endswith(".vercel.app")


@app.on_event("startup")
def validate_settings() -> None:
    settings = get_settings()
    app.state.settings = settings
    configure_json_logging(service_name="citylens-engine-api")
    logging.getLogger(__name__).info("validated settings", extra={"stage": "startup"})


@app.middleware("http")
async def cors_middleware(request: Request, call_next):
    origin = request.headers.get("origin")
    allowed_origins = _allowed_origins()
    origin_allowed = bool(origin) and (
        _is_demo_origin_allowed(origin, allowed_origins)
        if _is_demo_request(request)
        else origin in allowed_origins
    )

    if request.method == "OPTIONS" and origin:
        if not origin_allowed:
            return PlainTextResponse("CORS origin not allowed", status_code=403)
        response = Response(status_code=204)
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "false"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = request.headers.get(
            "access-control-request-headers",
            "*",
        )
        response.headers["Vary"] = "Origin"
        return response

    response = await call_next(request)
    if origin and origin_allowed:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "false"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Vary"] = "Origin"
    return response


@app.middleware("http")
async def docs_key_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
):
    if request.url.path not in _DOCS_PATHS:
        return await call_next(request)

    try:
        settings = get_settings()
    except Exception:
        return PlainTextResponse("Docs unavailable", status_code=503)

    expected = settings.docs_access_key_sha256
    if not expected:
        return PlainTextResponse("Not found", status_code=404)

    provided = request.headers.get("X-Docs-Key", "")
    if not provided:
        return PlainTextResponse("Docs key required", status_code=401)

    candidate = hashlib.sha256(provided.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(candidate.lower(), expected.lower()):
        return PlainTextResponse("Invalid docs key", status_code=401)

    if request.url.path == "/openapi.json":
        return JSONResponse(app.openapi())
    if request.url.path == "/redoc":
        return get_redoc_html(openapi_url="/openapi.json", title="citylens-engine docs")
    return get_swagger_ui_html(openapi_url="/openapi.json", title="citylens-engine docs")


_LOCKED_RUN_FIELDS = {
    "imagery_year": SUPPORTED_IMAGERY_YEARS,
    "baseline_year": SUPPORTED_BASELINE_YEARS,
    "segmentation_backend": SUPPORTED_SEGMENTATION_BACKENDS,
    "outputs": sorted(SUPPORTED_OUTPUTS),
    "sam2_cfg": [],
    "sam2_checkpoint": [],
    "aoi_radius_m": [],
    "orthophoto_path": [],
    "orthophoto_url": [],
    "baseline_path": [],
    "baseline_url": [],
}


def _is_create_run_request(request: Request) -> bool:
    return request.method == "POST" and request.url.path.rstrip("/") == "/v1/runs"


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    if not _is_create_run_request(request):
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )

    errors = exc.errors()
    primary_field = ""
    primary_message = "Invalid run options."
    for err in errors:
        loc = [str(p) for p in err.get("loc", []) if p not in ("body",)]
        primary_field = loc[0] if loc else primary_field
        primary_message = str(err.get("msg") or primary_message)
        if primary_field in _LOCKED_RUN_FIELDS:
            break

    allowed = _LOCKED_RUN_FIELDS.get(primary_field, [])
    return JSONResponse(
        status_code=400,
        content={
            "detail": {
                "code": "INVALID_RUN_OPTION",
                "field": primary_field,
                "allowed_values": allowed,
                "message": primary_message,
            }
        },
    )


app.include_router(health_router, prefix="/v1")
app.include_router(demo_router, prefix="/v1")
app.include_router(run_options_router, prefix="/v1")
app.include_router(me_router, prefix="/v1")
app.include_router(runs_router, prefix="/v1")


__all__ = ["app", "Settings"]

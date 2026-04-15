import logging
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response

from .routes.demo import get_demo_registry
from .routes.demo import router as demo_router
from .routes.health import router as health_router
from .routes.runs import router as runs_router
from .services.demo_bundle import validate_demo_bundle_for_registry
from .services.logging import configure_json_logging
from .services.settings import DEFAULT_CORS_ORIGINS, get_settings

app = FastAPI(title="citylens-engine-api", version="0.1.0")


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
    validate_demo_bundle_for_registry(get_demo_registry())
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


app.include_router(health_router, prefix="/v1")
app.include_router(demo_router, prefix="/v1")
app.include_router(runs_router, prefix="/v1")

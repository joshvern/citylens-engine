import logging

from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse, Response

from .routes.demo import router as demo_router
from .routes.health import router as health_router
from .routes.runs import router as runs_router
from .services.logging import configure_json_logging
from .services.settings import DEFAULT_CORS_ORIGINS, get_settings

app = FastAPI(title="citylens-engine-api", version="0.1.0")


def _allowed_origins() -> list[str]:
    try:
        return list(get_settings().cors_origins)
    except Exception:
        return list(DEFAULT_CORS_ORIGINS)


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

    if request.method == "OPTIONS" and origin:
        if origin not in allowed_origins:
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
    if origin and origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "false"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Vary"] = "Origin"
    return response


app.include_router(health_router, prefix="/v1")
app.include_router(demo_router, prefix="/v1")
app.include_router(runs_router, prefix="/v1")

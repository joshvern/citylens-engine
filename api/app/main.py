from fastapi import FastAPI

from .routes.health import router as health_router
from .routes.runs import router as runs_router

app = FastAPI(title="citylens-engine-api", version="0.1.0")

app.include_router(health_router, prefix="/v1")
app.include_router(runs_router, prefix="/v1")

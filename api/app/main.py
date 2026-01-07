from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes.demo import router as demo_router
from .routes.health import router as health_router
from .routes.runs import router as runs_router

app = FastAPI(title="citylens-engine-api", version="0.1.0")

# CORS: required for browser clients (e.g., citylens-web) because requests include
# the custom `X-API-Key` header which triggers an automatic preflight OPTIONS.
# Middleware must be added before routers.
app.add_middleware(
	CORSMiddleware,
	allow_origins=[
		"https://citylens.dev",
		"https://www.citylens.dev",
		"http://localhost:3000",
	],
	allow_credentials=False,
	allow_methods=["*"],
	allow_headers=["*"],
)

app.include_router(health_router, prefix="/v1")
app.include_router(runs_router, prefix="/v1")
app.include_router(demo_router, prefix="/v1")

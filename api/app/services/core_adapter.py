from __future__ import annotations

# CRITICAL: do not define engine-owned schemas.
# Import the canonical request model and pipeline entrypoint from citylens-core.
from typing import Any

_CORE_IMPORT_HELP = (
    "citylens-core is required but could not be imported.\n\n"
    "Local dev (editable install):\n"
    "  pip install -e ../citylens-core\n\n"
    'Docker builds: pass --build-arg CITYLENS_CORE_GIT_URL="git+https://..."\n'
    "(see api/Dockerfile and worker/Dockerfile)."
)

# `CitylensRequest` is a pure-pydantic model (no heavy deps) — safe to import
# eagerly; the API needs it to validate/shape run requests on every cold boot.
try:
    from citylens_core.models import CitylensRequest
except ImportError as e:  # pragma: no cover
    raise ImportError(_CORE_IMPORT_HELP) from e


# `run_citylens` pulls the full pipeline import chain (rasterio/GDAL/SAM2),
# which is multi-second to import and never executed by the API — the pipeline
# runs in the worker (`CloudRunJobTrigger`). Lazily expose it via PEP 562 so
# `from ...core_adapter import run_citylens` still resolves for any caller, but
# the heavy chain stays off the API container's cold-start import graph.
def __getattr__(name: str) -> Any:
    if name == "run_citylens":
        try:
            from citylens_core.pipeline import run_citylens
        except ImportError as e:  # pragma: no cover
            raise ImportError(_CORE_IMPORT_HELP) from e
        return run_citylens
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# `run_citylens` is supplied lazily by module-level `__getattr__` above.
__all__ = ["CitylensRequest", "run_citylens"]  # noqa: F822

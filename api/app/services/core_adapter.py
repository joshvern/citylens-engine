from __future__ import annotations

# CRITICAL: do not define engine-owned schemas.
# Import the canonical request model and pipeline entrypoint from citylens-core.

try:
    from citylens_core.models import CitylensRequest
    from citylens_core.pipeline import run_citylens
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "citylens-core is required but could not be imported.\n\n"
        "Local dev (editable install):\n"
        "  pip install -e ../citylens-core\n\n"
        "Docker builds: pass --build-arg CITYLENS_CORE_GIT_URL=\"git+https://...\"\n"
        "(see api/Dockerfile and worker/Dockerfile)."
    ) from e

__all__ = ["CitylensRequest", "run_citylens"]

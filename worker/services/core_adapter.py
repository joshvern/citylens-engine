from __future__ import annotations

try:
    from citylens_core.models import CitylensRequest
    from citylens_core.pipeline import run_citylens
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "citylens-core is required but could not be imported.\n\n"
        "Local dev (editable install):\n"
        "  pip install -e ../citylens-core\n\n"
        "Docker builds: pass --build-arg CITYLENS_CORE_GIT_URL=\"git+https://...\"\n"
        "(see worker/Dockerfile)."
    ) from e

__all__ = ["CitylensRequest", "run_citylens"]

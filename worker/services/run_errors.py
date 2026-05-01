from __future__ import annotations

from datetime import datetime
from traceback import TracebackException
from typing import Any


class LidarCoverageError(ValueError):
    """Raised when the NYS LAS index has no tile covering the requested point.

    Subclasses ``ValueError`` so existing ``except ValueError`` paths still
    catch it. Carries the failing point and the layer URL so callers can
    surface a stable user-facing error code (``LIDAR_NO_COVERAGE``) without
    having to parse the message.
    """

    def __init__(
        self,
        message: str,
        *,
        x: float | None = None,
        y: float | None = None,
        wkid: int | None = None,
        layer_url: str | None = None,
    ) -> None:
        super().__init__(message)
        self.x = x
        self.y = y
        self.wkid = wkid
        self.layer_url = layer_url


def build_error_payload(
    exc: BaseException,
    *,
    code: str = "WORKER_FAILED",
    stage: str = "failed",
    limit: int = 8,
) -> dict[str, Any]:
    tb = TracebackException.from_exception(exc)
    summary: list[str] = []
    for line in tb.format(chain=True):
        text = line.rstrip()
        if text:
            summary.append(text)
        if len(summary) >= max(1, int(limit)):
            break

    return {
        "code": code,
        "message": str(exc),
        "stage": stage,
        "traceback_summary": summary,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

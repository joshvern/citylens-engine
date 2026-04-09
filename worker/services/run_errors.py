from __future__ import annotations

from datetime import datetime
from traceback import TracebackException
from typing import Any


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

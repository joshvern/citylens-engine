from __future__ import annotations

from typing import Any

from ..models.schemas import RunErrorResponse


def _normalize_traceback_summary(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [line for line in value.splitlines() if line.strip()]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def coerce_run_error(raw: Any, *, stage: str | None = None) -> RunErrorResponse | None:
    if raw is None:
        return None
    if isinstance(raw, RunErrorResponse):
        return raw
    if isinstance(raw, dict):
        code = str(raw.get("code") or raw.get("error_code") or "UNKNOWN")
        message = str(
            raw.get("message")
            or raw.get("error_message")
            or raw.get("detail")
            or raw.get("error")
            or code
        )
        raw_stage = raw.get("stage")
        if raw_stage is None and stage is not None:
            raw_stage = stage
        return RunErrorResponse(
            code=code,
            message=message,
            stage=str(raw_stage) if raw_stage is not None else None,
            traceback_summary=_normalize_traceback_summary(
                raw.get("traceback_summary") or raw.get("traceback")
            ),
        )
    text = str(raw).strip()
    return RunErrorResponse(
        code="UNKNOWN",
        message=text or "Unknown error",
        stage=str(stage) if stage is not None else None,
        traceback_summary=[],
    )


def normalize_run_record(run: dict[str, Any]) -> dict[str, Any]:
    out = dict(run)
    stage = out.get("stage")
    out["error"] = coerce_run_error(
        out.get("error"),
        stage=str(stage) if stage is not None else None,
    )
    return out

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional, TypedDict


class PlanPolicy(TypedDict):
    monthly_run_limit: Optional[int]
    max_concurrent_runs: Optional[int]


def _free_monthly_limit() -> int:
    raw = os.getenv("CITYLENS_FREE_MONTHLY_RUNS")
    if raw is None or raw.strip() == "":
        return 5
    try:
        return int(raw)
    except ValueError:
        return 5


def get_policy(plan_type: str) -> PlanPolicy:
    if plan_type == "admin":
        return {"monthly_run_limit": None, "max_concurrent_runs": None}
    return {"monthly_run_limit": _free_monthly_limit(), "max_concurrent_runs": 1}


def month_key(now: datetime) -> str:
    dt = now.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"

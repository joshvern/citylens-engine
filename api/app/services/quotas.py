from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException


@dataclass(frozen=True)
class UserQuota:
    quota_per_day: int = 10
    max_concurrent_runs: int = 1


def _utc_midnight(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def enforce_quotas(*, store, user_id: str, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    user = store.get_or_create_user(user_id)
    quota = UserQuota(
        quota_per_day=int(user.get("quota_per_day", 10)),
        max_concurrent_runs=int(user.get("max_concurrent_runs", 1)),
    )

    since = _utc_midnight(now)
    runs_today = store.count_user_runs_since(user_id=user_id, since=since)
    if runs_today >= quota.quota_per_day:
        raise HTTPException(status_code=429, detail="Quota exceeded (per-day)")

    concurrent = store.count_user_concurrent_runs(user_id=user_id)
    if concurrent >= quota.max_concurrent_runs:
        raise HTTPException(status_code=429, detail="Quota exceeded (concurrent)")

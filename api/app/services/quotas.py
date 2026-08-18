from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import HTTPException

from .firestore_store import FirestoreStore, MonthlyQuotaExceeded
from .plans import get_policy, month_key


def get_quota_state(
    *,
    store: FirestoreStore,
    app_user_id: str,
    plan_type: str,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    policy = get_policy(plan_type)
    mk = month_key(now)
    runs_used = store.get_monthly_usage(app_user_id=app_user_id, month_key=mk)

    monthly_limit = policy["monthly_run_limit"]
    unlimited = monthly_limit is None
    runs_remaining: Optional[int]
    if unlimited:
        runs_remaining = None
    else:
        runs_remaining = max(0, int(monthly_limit) - int(runs_used))

    return {
        "month_key": mk,
        "monthly_run_limit": monthly_limit,
        "runs_used": int(runs_used),
        "runs_remaining": runs_remaining,
        "unlimited": unlimited,
        "max_concurrent_runs": policy["max_concurrent_runs"],
    }


def enforce_concurrent_quota(*, store: FirestoreStore, app_user_id: str, plan_type: str) -> None:
    policy = get_policy(plan_type)
    max_concurrent = policy["max_concurrent_runs"]
    if max_concurrent is None:
        return
    currently_running = store.count_user_concurrent_runs(user_id=app_user_id)
    if currently_running >= int(max_concurrent):
        raise HTTPException(
            status_code=429,
            detail={
                "code": "CONCURRENT_LIMIT_EXCEEDED",
                "message": (
                    f"Plan '{plan_type}' allows {max_concurrent} concurrent run(s); "
                    f"{currently_running} already queued or running."
                ),
                "plan_type": plan_type,
                "max_concurrent_runs": int(max_concurrent),
                "currently_running": int(currently_running),
            },
            # A slot frees when an active run finishes; the pipeline runs in
            # minutes, so 60s is a sensible client poll interval.
            headers={"Retry-After": "60"},
        )


def monthly_quota_exceeded_http(
    exc: MonthlyQuotaExceeded, *, plan_type: str
) -> HTTPException:
    """Map MonthlyQuotaExceeded onto the canonical 429 response.

    Shared by the keyless reservation path (``reserve_monthly_run``) and the
    idempotent create transaction in routes/runs.py, so both surface an
    identical error shape.
    """

    return HTTPException(
        status_code=429,
        detail={
            "code": "MONTHLY_QUOTA_EXCEEDED",
            "message": (
                f"{plan_type.capitalize()} plan includes "
                f"{exc.monthly_run_limit} runs per month."
            ),
            "plan_type": plan_type,
            "monthly_run_limit": exc.monthly_run_limit,
            "runs_used": exc.runs_used,
            "runs_remaining": 0,
            "month_key": exc.month_key,
        },
        # The monthly window resets at the UTC month boundary — hours to
        # days away. A constant 1h Retry-After steers well-behaved
        # clients to a sane re-check cadence without computing the exact
        # month-end (the response's month_key carries that context).
        headers={"Retry-After": "3600"},
    )


def reserve_monthly_run(
    *,
    store: FirestoreStore,
    app_user_id: str,
    plan_type: str,
    now: Optional[datetime] = None,
) -> str:
    now = now or datetime.now(timezone.utc)
    policy = get_policy(plan_type)
    mk = month_key(now)
    monthly_limit = policy["monthly_run_limit"]

    try:
        store.try_increment_monthly_usage(
            app_user_id=app_user_id,
            month_key=mk,
            limit=monthly_limit,
        )
    except MonthlyQuotaExceeded as exc:
        raise monthly_quota_exceeded_http(exc, plan_type=plan_type) from exc
    return mk


def release_monthly_run(*, store: FirestoreStore, app_user_id: str, month_key: str) -> None:
    try:
        store.decrement_monthly_usage(app_user_id=app_user_id, month_key=month_key)
    except Exception:  # pragma: no cover - best-effort release
        pass

"""Plan registry: run quotas + parcel-feed entitlements, table-driven.

This module is the single policy table for what each plan may do:

- Run quotas (``get_policy``): monthly run limit + concurrent-run ceiling,
  consumed by ``services/quotas.py``.
- Feed entitlements (``feed_entitlement``): what the tiered
  ``/v1/parcel-intel/{map,parcel,sweep}`` endpoints serve — row cap and
  whether premium fields (calibration bands, SHAP attributions, change
  signal, owner of record) are included.

IMPORTANT: the shipped defaults reproduce the live product's behavior
exactly. Every authenticated plan — including "free" — currently receives
the complete published inventory with premium fields; the Vercel SSR
`clk_live_` "vercel-server-feed" key and signed-in Neon JWTs depend on it,
and the production monitors assert exactly 5,000 authenticated parcels.
Changing what a tier receives (for example tightening the free feed) is an
edit to the tables below plus a redeploy — never a route-code change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, TypedDict

from .auth_context import AuthContext


class PlanPolicy(TypedDict):
    monthly_run_limit: Optional[int]
    max_concurrent_runs: Optional[int]


@dataclass(frozen=True)
class FeedEntitlement:
    """What one caller tier may read from the tiered parcel-intel feed.

    ``feed_row_cap`` of ``None`` means the complete published inventory;
    ``include_premium_fields`` of ``False`` means premium values are
    stripped/defaulted before serving.
    """

    plan_type: Optional[str]  # None == anonymous (no credential at all)
    feed_row_cap: Optional[int]
    include_premium_fields: bool

    @property
    def access_scope(self) -> str:
        """Scope label for response metadata, derived from the entitlement.

        Deriving from the entitlement itself (not from mere credential
        presence) means the label can never disagree with what the response
        actually contains: anonymous stays "public_preview", a full-
        inventory + premium entitlement is "authenticated_full", and any
        authenticated tier whose entitlement is row-capped or premium-
        stripped is labeled "authenticated_limited". Under today's policy
        table only the first two values are ever emitted.
        """

        if self.plan_type is None:
            return "public_preview"
        if self.feed_row_cap is None and self.include_premium_fields:
            return "authenticated_full"
        return "authenticated_limited"


# Plan types an AuthContext may carry. "smoke_read_only" is internal: it is
# derived from the production-smoke header credential in services/auth.py,
# never persisted on a user document, and never assignable.
PLAN_TYPES: tuple[str, ...] = (
    "free",
    "acquisitions",
    "concierge",
    "admin",
    "smoke_read_only",
)

# Plans the admin assignment endpoint may persist on a user document.
ASSIGNABLE_PLAN_TYPES: tuple[str, ...] = (
    "free",
    "acquisitions",
    "concierge",
    "admin",
)

# Unknown or missing plan_type values fall back to the free plan so a
# malformed user document can never escalate itself.
FALLBACK_PLAN_TYPE = "free"


def _env_int_or(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _free_monthly_limit() -> int:
    return _env_int_or("CITYLENS_FREE_MONTHLY_RUNS", 5)


# ---- Run quotas ----------------------------------------------------------
#
# Env vars are read at call time (matching the pre-registry behavior of the
# free limit) so tests and deploys can tune limits without process restarts
# beyond the usual Cloud Run revision rollout.


def _run_policy_registry() -> dict[str, PlanPolicy]:
    return {
        # Free tier: the monthly limit stays on the exact env var it has
        # always used; 1 concurrent run matches the pre-registry hardcode.
        "free": {
            "monthly_run_limit": _free_monthly_limit(),
            "max_concurrent_runs": _env_int_or(
                "CITYLENS_FREE_MAX_CONCURRENT_RUNS", 1
            ),
        },
        "acquisitions": {
            "monthly_run_limit": _env_int_or(
                "CITYLENS_ACQUISITIONS_MONTHLY_RUNS", 25
            ),
            "max_concurrent_runs": _env_int_or(
                "CITYLENS_ACQUISITIONS_MAX_CONCURRENT_RUNS", 2
            ),
        },
        "concierge": {
            "monthly_run_limit": _env_int_or(
                "CITYLENS_CONCIERGE_MONTHLY_RUNS", 100
            ),
            "max_concurrent_runs": _env_int_or(
                "CITYLENS_CONCIERGE_MAX_CONCURRENT_RUNS", 4
            ),
        },
        "admin": {"monthly_run_limit": None, "max_concurrent_runs": None},
        # The smoke credential is read-only by construction — it is only
        # accepted by the tiered parcel read routes and can never reach
        # /v1/runs — so a zero run quota is defense-in-depth, not a
        # behavior change.
        "smoke_read_only": {"monthly_run_limit": 0, "max_concurrent_runs": 0},
    }


def get_policy(plan_type: str) -> PlanPolicy:
    registry = _run_policy_registry()
    return registry.get(plan_type, registry[FALLBACK_PLAN_TYPE])


# ---- Feed entitlements ---------------------------------------------------
#
# TODAY'S LIVE BEHAVIOR, encoded as policy: every authenticated plan gets
# the full feed. "smoke_read_only" full access is deliberate and explicit —
# the scheduled production smoke asserts the complete authenticated
# inventory (exactly 5,000 rows), so it must keep full-feed read
# entitlements. Tightening the free tier later is an edit to this table
# (e.g. feed_row_cap=100, include_premium_fields=False), not a code change.

# Anonymous preview cap, also surfaced by the routes' OpenAPI descriptions.
ANONYMOUS_FEED_ROW_CAP = 25

_FEED_ENTITLEMENTS: dict[Optional[str], FeedEntitlement] = {
    # Anonymous preview: capped rows, premium values stripped.
    None: FeedEntitlement(
        plan_type=None,
        feed_row_cap=ANONYMOUS_FEED_ROW_CAP,
        include_premium_fields=False,
    ),
    "free": FeedEntitlement("free", None, True),
    "acquisitions": FeedEntitlement("acquisitions", None, True),
    "concierge": FeedEntitlement("concierge", None, True),
    "admin": FeedEntitlement("admin", None, True),
    "smoke_read_only": FeedEntitlement("smoke_read_only", None, True),
}


def get_feed_entitlement(plan_type: Optional[str]) -> FeedEntitlement:
    if plan_type is None:
        return _FEED_ENTITLEMENTS[None]
    return _FEED_ENTITLEMENTS.get(
        plan_type, _FEED_ENTITLEMENTS[FALLBACK_PLAN_TYPE]
    )


def feed_entitlement(auth: Optional[AuthContext]) -> FeedEntitlement:
    """Single enforcement point for the tiered parcel-intel feed.

    All three tiered routes consult this instead of checking whether a
    credential is merely present, so the plan table above — not route
    code — decides what each tier receives.
    """

    return get_feed_entitlement(auth.plan_type if auth is not None else None)


def month_key(now: datetime) -> str:
    dt = now.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"

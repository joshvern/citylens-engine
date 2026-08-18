"""Plan registry: run-quota policies + feed entitlements.

The shipped defaults must reproduce the live product's behavior exactly:
free keeps the env-driven monthly limit + 1 concurrent run, admin stays
unlimited, and EVERY authenticated plan (including the internal
smoke_read_only credential) keeps full-feed read entitlements while the
anonymous tier stays a 25-row stripped preview.
"""

from __future__ import annotations

import pytest

from app.services.auth_context import AuthContext
from app.services.plans import (
    ASSIGNABLE_PLAN_TYPES,
    PLAN_TYPES,
    FeedEntitlement,
    feed_entitlement,
    get_feed_entitlement,
    get_policy,
)


def _ctx(plan_type: str) -> AuthContext:
    return AuthContext(
        app_user_id="user-plans-1",
        auth_provider="mock",
        auth_subject="sub-user-plans-1",
        email="user-plans-1@example.com",
        email_verified=True,
        is_admin=plan_type == "admin",
        plan_type=plan_type,
    )


# ---- Run quotas ----------------------------------------------------------


def test_free_policy_preserves_env_driven_limit_and_one_concurrent(
    monkeypatch,
) -> None:
    # conftest sets CITYLENS_FREE_MONTHLY_RUNS=5.
    assert get_policy("free") == {
        "monthly_run_limit": 5,
        "max_concurrent_runs": 1,
    }
    monkeypatch.setenv("CITYLENS_FREE_MONTHLY_RUNS", "7")
    assert get_policy("free")["monthly_run_limit"] == 7


def test_admin_policy_stays_unlimited() -> None:
    assert get_policy("admin") == {
        "monthly_run_limit": None,
        "max_concurrent_runs": None,
    }


def test_paid_plan_default_quotas() -> None:
    assert get_policy("acquisitions") == {
        "monthly_run_limit": 25,
        "max_concurrent_runs": 2,
    }
    assert get_policy("concierge") == {
        "monthly_run_limit": 100,
        "max_concurrent_runs": 4,
    }


def test_paid_plan_quotas_are_env_tunable(monkeypatch) -> None:
    monkeypatch.setenv("CITYLENS_ACQUISITIONS_MONTHLY_RUNS", "40")
    monkeypatch.setenv("CITYLENS_ACQUISITIONS_MAX_CONCURRENT_RUNS", "3")
    monkeypatch.setenv("CITYLENS_CONCIERGE_MONTHLY_RUNS", "250")
    monkeypatch.setenv("CITYLENS_CONCIERGE_MAX_CONCURRENT_RUNS", "8")
    assert get_policy("acquisitions") == {
        "monthly_run_limit": 40,
        "max_concurrent_runs": 3,
    }
    assert get_policy("concierge") == {
        "monthly_run_limit": 250,
        "max_concurrent_runs": 8,
    }


def test_bad_env_values_fall_back_to_defaults(monkeypatch) -> None:
    monkeypatch.setenv("CITYLENS_FREE_MONTHLY_RUNS", "not-a-number")
    monkeypatch.setenv("CITYLENS_ACQUISITIONS_MONTHLY_RUNS", "")
    assert get_policy("free")["monthly_run_limit"] == 5
    assert get_policy("acquisitions")["monthly_run_limit"] == 25


def test_unknown_or_missing_plan_falls_back_to_free() -> None:
    assert get_policy("platinum") == get_policy("free")
    assert get_policy("") == get_policy("free")


def test_smoke_read_only_has_zero_run_quota() -> None:
    # Defense-in-depth: the smoke credential never reaches /v1/runs, but if
    # it ever did, it must not be able to create runs.
    assert get_policy("smoke_read_only") == {
        "monthly_run_limit": 0,
        "max_concurrent_runs": 0,
    }


# ---- Feed entitlements ---------------------------------------------------


def test_anonymous_feed_entitlement_is_capped_preview() -> None:
    ent = feed_entitlement(None)
    assert ent.plan_type is None
    assert ent.feed_row_cap == 25
    assert ent.include_premium_fields is False


@pytest.mark.parametrize(
    "plan_type",
    ["free", "acquisitions", "concierge", "admin", "smoke_read_only"],
)
def test_every_authenticated_plan_currently_gets_full_feed(
    plan_type: str,
) -> None:
    """Live-behavior lock: any credential unlocks the full premium feed.

    Production monitors assert exactly 5,000 authenticated parcels; the
    Vercel SSR key is a plain `clk_live_` free-plan key. Tightening any
    tier is a plans.py table edit — update this test alongside it.
    """

    ent = feed_entitlement(_ctx(plan_type))
    assert ent.feed_row_cap is None
    assert ent.include_premium_fields is True


def test_unknown_plan_feed_entitlement_falls_back_to_free() -> None:
    assert (
        feed_entitlement(_ctx("platinum"))
        == get_feed_entitlement("free")
    )


def test_access_scope_derives_from_entitlement_not_credential_presence() -> None:
    """/map's scope metadata (X-CityLens-Inventory-Scope + access_scope)
    comes from FeedEntitlement.access_scope so it can never disagree with
    what the response actually contains. Today's policy emits exactly the
    historical values; a tightened tier would surface as
    "authenticated_limited" instead of lying "authenticated_full"."""

    # Byte-identical to the pre-entitlement labels under today's policy.
    assert feed_entitlement(None).access_scope == "public_preview"
    for plan_type in PLAN_TYPES:
        assert feed_entitlement(_ctx(plan_type)).access_scope == (
            "authenticated_full"
        )

    # A future tightened tier — row-capped and/or premium-stripped — gets a
    # distinct, truthful label.
    capped = FeedEntitlement(
        plan_type="free", feed_row_cap=100, include_premium_fields=True
    )
    stripped = FeedEntitlement(
        plan_type="free", feed_row_cap=None, include_premium_fields=False
    )
    assert capped.access_scope == "authenticated_limited"
    assert stripped.access_scope == "authenticated_limited"


def test_registry_shape() -> None:
    # smoke_read_only is internal-only: recognized, never assignable.
    assert set(ASSIGNABLE_PLAN_TYPES) == {
        "free",
        "acquisitions",
        "concierge",
        "admin",
    }
    assert "smoke_read_only" in PLAN_TYPES
    assert "smoke_read_only" not in ASSIGNABLE_PLAN_TYPES

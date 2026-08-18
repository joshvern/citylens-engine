from __future__ import annotations

import math
import time
from dataclasses import dataclass
from threading import Lock

from fastapi import Depends, HTTPException, Request

from .auth import require_auth
from .auth_context import AuthContext

# KNOWN LIMITATION (deliberate, structural fix tracked separately): these
# buckets live in process memory, so every Cloud Run instance enforces its
# own copy — under autoscaling the effective limit multiplies by the
# instance count. Do NOT try to fix this here; a shared backend
# (e.g. Redis/Firestore counters) is a later architectural change.


@dataclass
class _Bucket:
    tokens: float
    last_refill_s: float


_lock = Lock()
_buckets: dict[str, _Bucket] = {}


def _client_ip(request: Request) -> str:
    # Cloud Run / reverse proxies typically set X-Forwarded-For.
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # Use the left-most IP (original client).
        ip = xff.split(",")[0].strip()
        if ip:
            return ip

    if request.client and request.client.host:
        return request.client.host

    return "unknown"


def enforce_token_bucket(*, key: str, capacity: int, refill_per_second: float) -> None:
    now = time.monotonic()

    with _lock:
        bucket = _buckets.get(key)
        if not bucket:
            bucket = _Bucket(tokens=float(capacity), last_refill_s=now)
            _buckets[key] = bucket

        elapsed = max(0.0, now - bucket.last_refill_s)
        bucket.tokens = min(float(capacity), bucket.tokens + elapsed * refill_per_second)
        bucket.last_refill_s = now

        if bucket.tokens < 1.0:
            # Seconds (ceil) until one full token has refilled — every 429
            # this limiter emits carries a Retry-After so well-behaved
            # clients back off instead of hammering.
            if refill_per_second > 0:
                retry_after = math.ceil((1.0 - bucket.tokens) / refill_per_second)
            else:  # pragma: no cover - no bucket is configured with 0 refill
                retry_after = 60
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(max(1, int(retry_after)))},
            )

        bucket.tokens -= 1.0


def demo_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    # Basic, in-memory rate limiting per instance.
    # ~60 requests/min with a small burst.
    enforce_token_bucket(key=f"demo:{ip}", capacity=30, refill_per_second=1.0)


def pilot_request_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    # Public conversion endpoint: allow a short retry burst, then roughly one
    # additional submission every 20 minutes per API instance.
    enforce_token_bucket(
        key=f"pilot-request:{ip}",
        capacity=3,
        refill_per_second=1 / 1_200,
    )


# ---- Per-credential buckets for expensive authenticated paths ------------
#
# Keyed on the resolved app user id, which covers both Neon JWTs and
# `clk_live_` API keys (a key resolves to its owning user, so rotating keys
# does not reset the bucket). Anonymous callers fall back to client IP —
# on these routes `require_auth` rejects them with 401 first, so the
# fallback is defensive only.


def _credential_key(request: Request, auth: AuthContext | None) -> str:
    if auth is not None and auth.app_user_id:
        return f"user:{auth.app_user_id}"
    return f"ip:{_client_ip(request)}"


def create_run_rate_limit(
    request: Request, auth: AuthContext = Depends(require_auth)
) -> None:
    """POST /v1/runs: the most expensive path (Cloud Run job per call).

    Monthly quota already caps volume; this stops burst abuse. Sustained
    ~6 runs/min per credential, with a burst capacity of 10 so a small
    legitimate flurry (retries, scripted backfills) is not punished.
    """

    enforce_token_bucket(
        key=f"runs-create:{_credential_key(request, auth)}",
        capacity=10,
        refill_per_second=6 / 60,
    )


def me_rate_limit(
    request: Request, auth: AuthContext = Depends(require_auth)
) -> None:
    # /v1/me does two Firestore reads per call; 60/min per credential.
    enforce_token_bucket(
        key=f"me:{_credential_key(request, auth)}",
        capacity=60,
        refill_per_second=1.0,
    )


def api_keys_rate_limit(
    request: Request, auth: AuthContext = Depends(require_auth)
) -> None:
    # /v1/api-keys mint/list/revoke share one 20/min bucket per credential —
    # key minting writes two documents and should never be a hot path.
    enforce_token_bucket(
        key=f"api-keys:{_credential_key(request, auth)}",
        capacity=20,
        refill_per_second=20 / 60,
    )

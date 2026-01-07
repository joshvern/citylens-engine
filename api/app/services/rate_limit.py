from __future__ import annotations

<<<<<<< HEAD
import os
=======
>>>>>>> 40da1628b40164ed42e14c918f81e26d62c1320f
import time
from dataclasses import dataclass
from threading import Lock

from fastapi import HTTPException, Request


@dataclass
class _Bucket:
    tokens: float
<<<<<<< HEAD
    updated_at: float


_LOCK = Lock()
_BUCKETS: dict[str, _Bucket] = {}


def _client_ip(request: Request) -> str:
    # Prefer X-Forwarded-For when behind Cloud Run / proxies.
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # take first
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def demo_rate_limit(request: Request) -> None:
    """Basic in-memory token bucket for unauthenticated /v1/demo/* endpoints.

    Env overrides (optional):
      - CITYLENS_DEMO_RL_PER_MIN (default 60)
      - CITYLENS_DEMO_RL_BURST (default 60)
    """

    per_min = int(os.getenv("CITYLENS_DEMO_RL_PER_MIN", "60") or 60)
    burst = int(os.getenv("CITYLENS_DEMO_RL_BURST", str(per_min)) or per_min)

    refill_per_sec = max(per_min, 1) / 60.0
    capacity = max(burst, 1)

    key = _client_ip(request)
    now = time.time()

    with _LOCK:
        b = _BUCKETS.get(key)
        if b is None:
            b = _Bucket(tokens=float(capacity), updated_at=now)
            _BUCKETS[key] = b

        elapsed = max(now - b.updated_at, 0.0)
        b.tokens = min(float(capacity), b.tokens + elapsed * refill_per_sec)
        b.updated_at = now

        if b.tokens < 1.0:
            raise HTTPException(status_code=429, detail="Too Many Requests")

        b.tokens -= 1.0
=======
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
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

        bucket.tokens -= 1.0


def demo_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    # Basic, in-memory rate limiting per instance.
    # ~60 requests/min with a small burst.
    enforce_token_bucket(key=f"demo:{ip}", capacity=30, refill_per_second=1.0)
>>>>>>> 40da1628b40164ed42e14c918f81e26d62c1320f

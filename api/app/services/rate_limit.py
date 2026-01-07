from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock

from fastapi import HTTPException, Request


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
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

        bucket.tokens -= 1.0


def demo_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    # Basic, in-memory rate limiting per instance.
    # ~60 requests/min with a small burst.
    enforce_token_bucket(key=f"demo:{ip}", capacity=30, refill_per_second=1.0)

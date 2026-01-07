from __future__ import annotations

import os
import time
from dataclasses import dataclass
from threading import Lock

from fastapi import HTTPException, Request


@dataclass
class _Bucket:
    tokens: float
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

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

from google.api_core import exceptions as gexc

T = TypeVar("T")

_TRANSIENT_EXCEPTIONS = tuple(
    exc
    for exc in (
        getattr(gexc, "Aborted", None),
        getattr(gexc, "DeadlineExceeded", None),
        getattr(gexc, "InternalServerError", None),
        getattr(gexc, "ResourceExhausted", None),
        getattr(gexc, "ServiceUnavailable", None),
        getattr(gexc, "TooManyRequests", None),
        getattr(gexc, "Unavailable", None),
        getattr(gexc, "RetryError", None),
        TimeoutError,
        ConnectionError,
    )
    if exc is not None
)


def retry_transient(
    fn: Callable[[], T],
    *,
    attempts: int = 4,
    base_delay_s: float = 0.25,
    max_delay_s: float = 2.0,
) -> T:
    last_exc: Exception | None = None
    max_attempts = max(1, int(attempts))

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except _TRANSIENT_EXCEPTIONS as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(float(max_delay_s), float(base_delay_s) * (2 ** (attempt - 1)))
            time.sleep(sleep_s)

    assert last_exc is not None
    raise last_exc

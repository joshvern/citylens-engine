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

_EXPIRED_TRANSACTION_DETAIL = (
    "the referenced transaction has expired or is no longer valid"
)


def _is_transient_exception(exc: Exception) -> bool:
    if isinstance(exc, _TRANSIENT_EXCEPTIONS):
        return True

    # Firestore occasionally reports an expired transaction as INVALID_ARGUMENT
    # rather than ABORTED. Retrying every InvalidArgument would hide real request
    # defects, so recognize only the service's exact transient transaction detail.
    return isinstance(exc, gexc.InvalidArgument) and (
        _EXPIRED_TRANSACTION_DETAIL in str(exc).casefold()
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
        except Exception as exc:
            if not _is_transient_exception(exc):
                raise
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(float(max_delay_s), float(base_delay_s) * (2 ** (attempt - 1)))
            time.sleep(sleep_s)

    assert last_exc is not None
    raise last_exc

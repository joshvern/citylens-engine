from __future__ import annotations

import pytest
from google.api_core import exceptions as gexc

from app.services import retry


def test_retry_transient_restarts_an_expired_firestore_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    monkeypatch.setattr(retry.time, "sleep", lambda _seconds: None)

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise gexc.InvalidArgument(
                "The referenced transaction has expired or is no longer valid."
            )
        return "committed"

    assert retry.retry_transient(operation) == "committed"
    assert calls == 2


def test_retry_transient_does_not_retry_other_invalid_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    monkeypatch.setattr(retry.time, "sleep", lambda _seconds: None)

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise gexc.InvalidArgument("A field path is invalid.")

    with pytest.raises(gexc.InvalidArgument, match="field path"):
        retry.retry_transient(operation)

    assert calls == 1

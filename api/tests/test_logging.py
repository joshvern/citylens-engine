"""JsonFormatter must emit Cloud-Logging-native structured records.

GCP keys log lines on `severity` (not `level`): without it every line lands
at DEFAULT severity, invisible to severity filters and log-based alerts.
Error Reporting additionally requires the traceback inside `message` and the
ReportedErrorEvent `@type` marker to ingest and group exceptions.
"""

from __future__ import annotations

import json
import logging

from app.services.logging import JsonFormatter


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter(service_name="test-svc").format(record))


def _record(
    level: int,
    message: str = "boom happened",
    *,
    exc_info=None,
) -> logging.LogRecord:
    return logging.LogRecord(
        name="app.test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=exc_info,
    )


def _exc_info():
    try:
        raise ValueError("kaboom")
    except ValueError:
        import sys

        return sys.exc_info()


def test_plain_record_uses_severity_not_level() -> None:
    payload = _format(_record(logging.INFO, "hello"))

    assert payload["severity"] == "INFO"
    assert "level" not in payload
    assert payload["message"] == "hello"
    assert payload["service"] == "test-svc"
    assert payload["logger"] == "app.test"
    assert "@type" not in payload


def test_severity_matches_python_level_names() -> None:
    for level, name in (
        (logging.WARNING, "WARNING"),
        (logging.ERROR, "ERROR"),
        (logging.CRITICAL, "CRITICAL"),
    ):
        assert _format(_record(level))["severity"] == name


def test_error_with_exception_appends_traceback_and_error_reporting_type() -> None:
    payload = _format(_record(logging.ERROR, exc_info=_exc_info()))

    assert payload["severity"] == "ERROR"
    # Error Reporting groups by the traceback embedded in `message`.
    assert payload["message"].startswith("boom happened\n")
    assert "ValueError: kaboom" in payload["message"]
    assert "Traceback (most recent call last)" in payload["message"]
    assert "ValueError: kaboom" in payload["exception"]
    assert payload["@type"] == (
        "type.googleapis.com/google.devtools.clouderrorreporting."
        "v1beta1.ReportedErrorEvent"
    )


def test_warning_with_exception_has_traceback_but_no_error_reporting_type() -> None:
    payload = _format(_record(logging.WARNING, exc_info=_exc_info()))

    assert payload["severity"] == "WARNING"
    assert "ValueError: kaboom" in payload["message"]
    assert "@type" not in payload


def test_error_without_exception_has_no_error_reporting_type() -> None:
    payload = _format(_record(logging.ERROR))

    assert payload["severity"] == "ERROR"
    assert payload["message"] == "boom happened"
    assert "@type" not in payload
    assert "exception" not in payload


def test_context_extras_still_pass_through() -> None:
    record = _record(logging.INFO, "with context")
    record.run_id = "run-123"
    record.stage = "startup"
    payload = _format(record)

    assert payload["run_id"] == "run-123"
    assert payload["stage"] == "startup"


def test_audit_extras_pass_through() -> None:
    """The whitelist must carry the audit extras emitted by
    admin_users.py's plan-assignment log (target_user_id, plan_type,
    changed_by) and admin_runs.py's reconciler log (previous_status,
    stale_minutes) — a key missing from the whitelist is silently dropped
    from Cloud Logging."""

    record = _record(logging.INFO, "admin plan assignment")
    record.target_user_id = "u-target"
    record.plan_type = "acquisitions"
    record.changed_by = "u-admin"
    payload = _format(record)
    assert payload["target_user_id"] == "u-target"
    assert payload["plan_type"] == "acquisitions"
    assert payload["changed_by"] == "u-admin"

    record = _record(logging.WARNING, "reconciled stuck run")
    record.run_id = "run-9"
    record.user_id = "u-owner"
    record.previous_status = "running"
    record.stale_minutes = 35
    payload = _format(record)
    assert payload["run_id"] == "run-9"
    assert payload["user_id"] == "u-owner"
    assert payload["previous_status"] == "running"
    assert payload["stale_minutes"] == 35

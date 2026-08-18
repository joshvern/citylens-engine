"""Worker JsonFormatter must emit Cloud-Logging-native structured records.

Mirror of the API formatter tests: `severity` (not `level`) so severity
filters and log-based alerts see the line, the traceback appended to
`message`, and the ReportedErrorEvent `@type` on ERROR-and-above records with
exception info so Error Reporting ingests worker failures.
"""

from __future__ import annotations

import json
import logging
import sys

from services.logging import JsonFormatter


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter(service_name="test-worker").format(record))


def _record(level: int, message: str = "boom happened", *, exc_info=None) -> logging.LogRecord:
    return logging.LogRecord(
        name="worker.test",
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
        return sys.exc_info()


def test_plain_record_uses_severity_not_level() -> None:
    payload = _format(_record(logging.INFO, "hello"))

    assert payload["severity"] == "INFO"
    assert "level" not in payload
    assert payload["message"] == "hello"
    assert payload["service"] == "test-worker"


def test_error_with_exception_appends_traceback_and_error_reporting_type() -> None:
    payload = _format(_record(logging.ERROR, exc_info=_exc_info()))

    assert payload["severity"] == "ERROR"
    assert payload["message"].startswith("boom happened\n")
    assert "ValueError: kaboom" in payload["message"]
    assert payload["@type"] == (
        "type.googleapis.com/google.devtools.clouderrorreporting."
        "v1beta1.ReportedErrorEvent"
    )


def test_warning_with_exception_has_no_error_reporting_type() -> None:
    payload = _format(_record(logging.WARNING, exc_info=_exc_info()))

    assert "ValueError: kaboom" in payload["message"]
    assert "@type" not in payload


def test_worker_context_extras_still_pass_through() -> None:
    record = _record(logging.INFO, "with context")
    record.run_id = "run-123"
    record.work_root = "/tmp/work"
    payload = _format(record)

    assert payload["run_id"] == "run-123"
    assert payload["work_root"] == "/tmp/work"

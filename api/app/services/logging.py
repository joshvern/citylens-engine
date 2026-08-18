from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

# Structured-log field consumed by Cloud Error Reporting. Emitting it on an
# ERROR-and-above record that carries exception info makes Error Reporting
# ingest and group the event even without the Error Reporting API.
_ERROR_REPORTING_TYPE = (
    "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
)


class JsonFormatter(logging.Formatter):
    def __init__(self, *, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        # Cloud Logging keys structured payloads on `severity` — a `level`
        # field lands every line at DEFAULT severity, invisible to severity
        # filters, log-based alerts, and Error Reporting. Python level names
        # (DEBUG/INFO/WARNING/ERROR/CRITICAL) map 1:1 onto LogSeverity.
        message = record.getMessage()
        payload: dict[str, object] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "severity": record.levelname,
            "logger": record.name,
        }

        # Pass-through whitelist for structured extras. The audit extras
        # match exactly what admin_users.py's plan-assignment log
        # (target_user_id, plan_type, changed_by) and admin_runs.py's
        # reconciler log (previous_status, stale_minutes) emit — an extra
        # missing here is silently dropped from Cloud Logging.
        for key in (
            "run_id",
            "stage",
            "execution_id",
            "job_name",
            "user_id",
            "target_user_id",
            "plan_type",
            "changed_by",
            "previous_status",
            "stale_minutes",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            payload["exception"] = exc_text
            # Error Reporting groups exceptions by the traceback embedded in
            # `message`; keep the human-readable message as the first line.
            message = f"{message}\n{exc_text}" if message else exc_text
            if record.levelno >= logging.ERROR:
                payload["@type"] = _ERROR_REPORTING_TYPE

        payload["message"] = message
        return json.dumps(payload, sort_keys=True)


def configure_json_logging(*, service_name: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(service_name=service_name))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)

from __future__ import annotations

from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession


class CloudRunJobTrigger:
    def __init__(self, *, project_id: str, region: str, job_name: str) -> None:
        self.project_id = project_id
        self.region = region
        self.job_name = job_name

    def run(self, *, run_id: str) -> str:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        session = AuthorizedSession(creds)

        name = f"projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}"
        url = f"https://run.googleapis.com/v2/{name}:run"

        body: dict[str, Any] = {
            "overrides": {
                "containerOverrides": [
                    {"env": [{"name": "CITYLENS_RUN_ID", "value": run_id}]}
                ]
            }
        }

        resp = session.post(url, json=body, timeout=30)
        if resp.status_code >= 300:
            raise RuntimeError(f"Cloud Run Job trigger failed: {resp.status_code} {resp.text}")

        data = resp.json() if resp.content else {}
        execution_name = data.get("name") or data.get("execution") or ""
        return str(execution_name)

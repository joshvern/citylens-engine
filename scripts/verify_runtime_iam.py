#!/usr/bin/env python3
"""Verify the least-privilege CityLens production runtime identity boundary."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from typing import Any

try:
    from scripts.configure_production_monitoring import Gcloud, JsonCommand
except ModuleNotFoundError:  # Direct execution: python scripts/<file>.py
    from configure_production_monitoring import (  # type: ignore[no-redef]
        Gcloud,
        JsonCommand,
    )

SCHEMA = "citylens/runtime-iam-verification@v1"
LEGACY_ACCOUNT_NAMES = ("citylens-api-sa", "citylens-worker-sa")


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _members_for_role(
    policy: dict[str, Any],
    role: str,
) -> set[str]:
    return {
        str(member)
        for binding in policy.get("bindings") or []
        if binding.get("role") == role
        for member in binding.get("members") or []
    }


def _roles_for_member(
    policy: dict[str, Any],
    member: str,
) -> set[str]:
    return {
        str(binding.get("role"))
        for binding in policy.get("bindings") or []
        if member in set(binding.get("members") or [])
    }


def _service_account_email(name: str, project: str) -> str:
    return f"{name}@{project}.iam.gserviceaccount.com"


def _runtime_service_account(
    row: dict[str, Any],
    *,
    job: bool,
) -> str | None:
    spec = row.get("spec") or {}
    template = spec.get("template") or {}
    if job:
        template = (template.get("spec") or {}).get("template") or {}
    value = (template.get("spec") or {}).get("serviceAccountName")
    return str(value) if value else None


def verify(
    *,
    project: str,
    region: str,
    bucket: str,
    api_service: str,
    worker_job: str,
    api_account_name: str,
    worker_account_name: str,
    now: datetime,
    command: JsonCommand,
) -> dict[str, Any]:
    api_email = _service_account_email(api_account_name, project)
    worker_email = _service_account_email(worker_account_name, project)
    legacy_emails = [
        _service_account_email(name, project)
        for name in LEGACY_ACCOUNT_NAMES
    ]
    emails = [api_email, worker_email, *legacy_emails]

    service = command.json(
        "run",
        "services",
        "describe",
        api_service,
        f"--region={region}",
    )
    job = command.json(
        "run",
        "jobs",
        "describe",
        worker_job,
        f"--region={region}",
    )
    project_policy = command.json("projects", "get-iam-policy", project)
    bucket_policy = command.json(
        "storage",
        "buckets",
        "get-iam-policy",
        f"gs://{bucket}",
    )
    accounts = {
        email: command.json(
            "iam",
            "service-accounts",
            "describe",
            email,
        )
        for email in emails
    }
    account_policies = {
        email: command.json(
            "iam",
            "service-accounts",
            "get-iam-policy",
            email,
        )
        for email in emails
    }
    user_keys = {
        email: command.json(
            "iam",
            "service-accounts",
            "keys",
            "list",
            f"--iam-account={email}",
            "--managed-by=user",
        )
        for email in emails
    }

    failures: list[str] = []
    warnings: list[str] = []
    expected_runtime = {
        "api": api_email,
        "worker": worker_email,
    }
    actual_runtime = {
        "api": _runtime_service_account(service, job=False),
        "worker": _runtime_service_account(job, job=True),
    }
    for key, expected in expected_runtime.items():
        actual = actual_runtime[key]
        if actual != expected:
            failures.append(
                f"{key} runtime service account is {actual!r}, "
                f"expected {expected!r}"
            )

    current_project_roles = {
        "api": _roles_for_member(
            project_policy,
            f"serviceAccount:{api_email}",
        ),
        "worker": _roles_for_member(
            project_policy,
            f"serviceAccount:{worker_email}",
        ),
    }
    required_project_roles = {
        "api": {"roles/datastore.user", "roles/run.developer"},
        "worker": {"roles/datastore.user"},
    }
    for key, required in required_project_roles.items():
        missing = required - current_project_roles[key]
        if missing:
            failures.append(
                f"{key} runtime identity lacks project role(s): "
                f"{', '.join(sorted(missing))}"
            )
    if "roles/datastore.viewer" in current_project_roles["worker"]:
        failures.append(
            "worker runtime identity still has redundant "
            "roles/datastore.viewer"
        )

    api_bucket_member = f"serviceAccount:{api_email}"
    worker_bucket_member = f"serviceAccount:{worker_email}"
    if api_bucket_member not in _members_for_role(
        bucket_policy,
        "roles/storage.objectViewer",
    ):
        failures.append("API runtime identity lacks artifact object viewer")
    if worker_bucket_member not in _members_for_role(
        bucket_policy,
        "roles/storage.objectAdmin",
    ):
        failures.append("worker runtime identity lacks artifact object admin")

    token_creators = _members_for_role(
        account_policies[api_email],
        "roles/iam.serviceAccountTokenCreator",
    )
    if f"serviceAccount:{api_email}" not in token_creators:
        failures.append(
            "API runtime identity cannot self-sign private artifact URLs"
        )

    for key, email in (
        ("api", api_email),
        ("worker", worker_email),
    ):
        if accounts[email].get("disabled") is True:
            failures.append(f"{key} runtime identity is disabled")
        keys = user_keys[email]
        if not isinstance(keys, list):
            raise TypeError("service-account key response is not a list")
        if keys:
            failures.append(
                f"{key} runtime identity has {len(keys)} user-managed key(s)"
            )

    legacy_results: list[dict[str, Any]] = []
    for email in legacy_emails:
        project_roles = _roles_for_member(
            project_policy,
            f"serviceAccount:{email}",
        )
        bucket_roles = _roles_for_member(
            bucket_policy,
            f"serviceAccount:{email}",
        )
        self_roles = _roles_for_member(
            account_policies[email],
            f"serviceAccount:{email}",
        )
        keys = user_keys[email]
        if not isinstance(keys, list):
            raise TypeError("service-account key response is not a list")
        disabled = accounts[email].get("disabled") is True
        if not disabled:
            failures.append(f"legacy identity {email} is not disabled")
        if project_roles:
            failures.append(
                f"legacy identity {email} retains project role(s): "
                f"{', '.join(sorted(project_roles))}"
            )
        if bucket_roles:
            failures.append(
                f"legacy identity {email} retains bucket role(s): "
                f"{', '.join(sorted(bucket_roles))}"
            )
        if self_roles:
            failures.append(
                f"legacy identity {email} retains self role(s): "
                f"{', '.join(sorted(self_roles))}"
            )
        if keys:
            failures.append(
                f"legacy identity {email} has {len(keys)} user-managed key(s)"
            )
        legacy_results.append(
            {
                "email": email,
                "disabled": disabled,
                "project_roles": sorted(project_roles),
                "bucket_roles": sorted(bucket_roles),
                "self_roles": sorted(self_roles),
                "user_managed_keys": len(keys),
            }
        )

    return {
        "schema": SCHEMA,
        "project": project,
        "region": region,
        "artifact_bucket": bucket,
        "verified_at": _format_time(now),
        "healthy": not failures,
        "runtime_identities": {
            "api": {
                "expected": api_email,
                "actual": actual_runtime["api"],
                "project_roles": sorted(current_project_roles["api"]),
            },
            "worker": {
                "expected": worker_email,
                "actual": actual_runtime["worker"],
                "project_roles": sorted(current_project_roles["worker"]),
            },
        },
        "legacy_identities": legacy_results,
        "failures": failures,
        "warnings": warnings,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default=(
            os.environ.get("CITYLENS_GCP_PROJECT")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
        ),
        help="Google Cloud project ID",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("CITYLENS_REGION", "us-central1"),
        help="Cloud Run region",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("CITYLENS_BUCKET"),
        help="Production artifact bucket",
    )
    parser.add_argument(
        "--api-service",
        default=os.environ.get("API_SERVICE_NAME", "citylens-api"),
        help="Production API Cloud Run service",
    )
    parser.add_argument(
        "--worker-job",
        default=os.environ.get("CITYLENS_JOB_NAME", "citylens-worker"),
        help="Production worker Cloud Run job",
    )
    parser.add_argument(
        "--api-account",
        default=os.environ.get("API_SA_NAME", "citylens-api"),
        help="Expected API service-account name",
    )
    parser.add_argument(
        "--worker-account",
        default=os.environ.get("WORKER_SA_NAME", "citylens-worker"),
        help="Expected worker service-account name",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.project:
        raise SystemExit(
            "--project or CITYLENS_GCP_PROJECT/GOOGLE_CLOUD_PROJECT is required"
        )
    if not args.bucket:
        raise SystemExit("--bucket or CITYLENS_BUCKET is required")
    result = verify(
        project=args.project,
        region=args.region,
        bucket=args.bucket,
        api_service=args.api_service,
        worker_job=args.worker_job,
        api_account_name=args.api_account,
        worker_account_name=args.worker_account,
        now=datetime.now(UTC),
        command=Gcloud(args.project),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

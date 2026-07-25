from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from scripts.verify_runtime_iam import verify

NOW = datetime(2026, 7, 25, 4, 0, tzinfo=UTC)
PROJECT = "citylens-001"
API = f"citylens-api@{PROJECT}.iam.gserviceaccount.com"
WORKER = f"citylens-worker@{PROJECT}.iam.gserviceaccount.com"
LEGACY_API = f"citylens-api-sa@{PROJECT}.iam.gserviceaccount.com"
LEGACY_WORKER = f"citylens-worker-sa@{PROJECT}.iam.gserviceaccount.com"


def _policy(bindings: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "bindings": [
            {"role": role, "members": members}
            for role, members in bindings.items()
        ]
    }


def _responses() -> list[Any]:
    project_policy = _policy(
        {
            "roles/datastore.user": [
                f"serviceAccount:{API}",
                f"serviceAccount:{WORKER}",
            ],
            "roles/run.developer": [f"serviceAccount:{API}"],
        }
    )
    bucket_policy = _policy(
        {
            "roles/storage.objectViewer": [f"serviceAccount:{API}"],
            "roles/storage.objectAdmin": [f"serviceAccount:{WORKER}"],
        }
    )
    accounts = [
        {"email": API},
        {"email": WORKER},
        {"email": LEGACY_API, "disabled": True},
        {"email": LEGACY_WORKER, "disabled": True},
    ]
    policies = [
        _policy(
            {
                "roles/iam.serviceAccountTokenCreator": [
                    f"serviceAccount:{API}"
                ]
            }
        ),
        {},
        {},
        {},
    ]
    return [
        {
            "spec": {
                "template": {
                    "spec": {"serviceAccountName": API},
                }
            }
        },
        {
            "spec": {
                "template": {
                    "spec": {
                        "template": {
                            "spec": {"serviceAccountName": WORKER},
                        }
                    }
                }
            }
        },
        project_policy,
        bucket_policy,
        *accounts,
        *policies,
        [],
        [],
        [],
        [],
    ]


class FakeCommand:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, ...]] = []

    def json(self, *args: str) -> Any:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError(f"unexpected command: {args}")
        return self.responses.pop(0)


def _verify(responses: list[Any]) -> dict[str, Any]:
    return verify(
        project=PROJECT,
        region="us-central1",
        bucket="citylens-001-artifacts",
        api_service="citylens-api",
        worker_job="citylens-worker",
        api_account_name="citylens-api",
        worker_account_name="citylens-worker",
        now=NOW,
        command=FakeCommand(responses),
    )


def test_runtime_identity_contract_passes_without_keys_or_legacy_grants() -> None:
    result = _verify(_responses())

    assert result["healthy"] is True
    assert result["failures"] == []
    assert result["runtime_identities"]["api"]["actual"] == API
    assert result["runtime_identities"]["worker"]["actual"] == WORKER
    assert all(
        row["disabled"] is True
        for row in result["legacy_identities"]
    )


def test_runtime_identity_contract_fails_closed_on_privilege_drift() -> None:
    responses = _responses()
    responses[0]["spec"]["template"]["spec"]["serviceAccountName"] = LEGACY_API
    responses[2]["bindings"].append(
        {
            "role": "roles/datastore.viewer",
            "members": [f"serviceAccount:{WORKER}"],
        }
    )
    responses[2]["bindings"].append(
        {
            "role": "roles/datastore.user",
            "members": [f"serviceAccount:{LEGACY_WORKER}"],
        }
    )
    responses[3]["bindings"].append(
        {
            "role": "roles/storage.objectAdmin",
            "members": [f"serviceAccount:{LEGACY_WORKER}"],
        }
    )
    responses[6]["disabled"] = False
    responses[10] = _policy(
        {
            "roles/iam.serviceAccountTokenCreator": [
                f"serviceAccount:{LEGACY_API}"
            ]
        }
    )
    responses[14] = [{"name": "legacy-key"}]

    result = _verify(deepcopy(responses))

    assert result["healthy"] is False
    assert any(
        "api runtime service account" in failure
        for failure in result["failures"]
    )
    assert any(
        "redundant roles/datastore.viewer" in failure
        for failure in result["failures"]
    )
    assert any(
        "legacy identity" in failure and "not disabled" in failure
        for failure in result["failures"]
    )
    assert any(
        "retains project role" in failure
        for failure in result["failures"]
    )
    assert any(
        "retains bucket role" in failure
        for failure in result["failures"]
    )
    assert any(
        "retains self role" in failure
        for failure in result["failures"]
    )
    assert any(
        "user-managed key" in failure
        for failure in result["failures"]
    )

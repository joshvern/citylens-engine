from __future__ import annotations

from copy import deepcopy
from typing import Any

from scripts.configure_production_monitoring import (
    API_REGIONS,
    SPECS,
    _canonical_policy,
    build_policy,
    configure,
    ensure_policy,
    ensure_uptime,
    uptime_drift,
)


def _uptime(spec_index: int = 0) -> dict[str, Any]:
    spec = SPECS[spec_index]
    return {
        "name": (
            "projects/citylens-001/uptimeCheckConfigs/"
            f"existing-{spec.key}"
        ),
        "displayName": spec.uptime_display_name,
        "monitoredResource": {
            "type": "uptime_url",
            "labels": {
                "host": spec.host,
                "project_id": "citylens-001",
            },
        },
        "httpCheck": {
            "acceptedResponseStatusCodes": [{"statusValue": 200}],
            "path": spec.path,
            "port": 443,
            "requestMethod": "GET",
            "useSsl": True,
            "validateSsl": True,
        },
        "contentMatchers": [
            {"content": spec.matcher, "matcher": "CONTAINS_STRING"}
        ],
        "period": "300s",
        "selectedRegions": list(API_REGIONS),
        "timeout": "10s",
        "userLabels": {
            "environment": "production",
            "managed_by": "citylens",
            "service": spec.service_label,
        },
    }


class FakeCommand:
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.responses = list(responses or [])
        self.calls: list[tuple[str, ...]] = []

    def json(self, *args: str) -> Any:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError(f"unexpected command: {args}")
        return self.responses.pop(0)


def test_uptime_contract_detects_security_and_content_drift() -> None:
    spec = SPECS[0]
    row = _uptime()
    assert uptime_drift(row, project="citylens-001", spec=spec) == []

    row["httpCheck"]["validateSsl"] = False
    row["contentMatchers"][0]["content"] = '"ok":true'
    row["selectedRegions"] = ["USA_IOWA"]
    drift = uptime_drift(row, project="citylens-001", spec=spec)
    assert any("validate_ssl" in reason for reason in drift)
    assert any("matcher" in reason for reason in drift)
    assert any("six-region" in reason for reason in drift)


def test_uptime_dry_run_is_non_mutating_and_reports_drift() -> None:
    row = _uptime()
    row["timeout"] = "60s"
    command = FakeCommand()
    result, resolved = ensure_uptime(
        command,
        project="citylens-001",
        spec=SPECS[0],
        existing=row,
        apply=False,
    )
    assert result["action"] == "update"
    assert result["drift"] == ["timeout: '60s' != '10s'"]
    assert resolved is row
    assert command.calls == []


def test_uptime_apply_uses_exact_safe_update_contract() -> None:
    row = _uptime()
    row["timeout"] = "60s"
    updated = _uptime()
    command = FakeCommand([updated])
    result, resolved = ensure_uptime(
        command,
        project="citylens-001",
        spec=SPECS[0],
        existing=row,
        apply=True,
    )
    assert result["action"] == "updated"
    assert resolved == updated
    args = command.calls[0]
    assert "--validate-ssl=true" in args
    assert "--set-status-codes=200" in args
    expected_regions = (
        "--set-regions=asia-pacific,europe,south-america,"
        "usa-iowa,usa-oregon,usa-virginia"
    )
    assert expected_regions in args


def test_policy_requires_two_failed_regions_and_preserves_channels() -> None:
    spec = SPECS[0]
    channel = (
        "projects/citylens-001/notificationChannels/existing-channel"
    )
    existing = build_policy(
        spec=spec,
        check_id="existing-api",
        notification_channels=[channel],
        name="projects/citylens-001/alertPolicies/existing-policy",
    )
    existing["conditions"][0]["name"] = "server-generated-condition"
    desired = build_policy(
        spec=spec,
        check_id="existing-api",
        notification_channels=[channel],
        name="projects/citylens-001/alertPolicies/existing-policy",
    )
    assert _canonical_policy(existing) == _canonical_policy(desired)

    failure = desired["conditions"][0]["conditionThreshold"]
    assert failure["thresholdValue"] == 1
    assert failure["comparison"] == "COMPARISON_GT"
    assert failure["aggregations"][0]["crossSeriesReducer"] == (
        "REDUCE_COUNT_FALSE"
    )
    tls = desired["conditions"][1]["conditionThreshold"]
    assert tls["thresholdValue"] == 15

    command = FakeCommand()
    result = ensure_policy(
        command,
        spec=spec,
        check_id="existing-api",
        existing=existing,
        requested_channels=[],
        apply=True,
    )
    assert result["action"] == "unchanged"
    assert result["notification_channels"] == [channel]
    assert command.calls == []


def test_configure_dry_run_plans_missing_resources_without_writes() -> None:
    command = FakeCommand([[], []])
    result = configure(
        project="citylens-001",
        apply=False,
        notification_channels=[],
        command=command,
    )
    assert [row["action"] for row in result["uptime_checks"]] == [
        "create",
        "create",
    ]
    assert [row["action"] for row in result["alert_policies"]] == [
        "blocked_until_uptime_check_exists",
        "blocked_until_uptime_check_exists",
    ]
    assert len(command.calls) == 2


def test_duplicate_display_names_fail_closed() -> None:
    duplicate = _uptime()
    command = FakeCommand([[duplicate, deepcopy(duplicate)], []])
    try:
        configure(
            project="citylens-001",
            apply=False,
            notification_channels=[],
            command=command,
        )
    except RuntimeError as exc:
        assert "multiple uptime check resources" in str(exc)
    else:
        raise AssertionError("duplicate managed resources were accepted")

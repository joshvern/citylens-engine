from __future__ import annotations

from copy import deepcopy
from typing import Any

from scripts.configure_production_dashboard import (
    DISPLAY_NAME,
    _canonical,
    build_dashboard,
    configure,
)

PROJECT = "citylens-001"


class FakeCommand:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, ...]] = []

    def json(self, *args: str) -> Any:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError(f"unexpected command: {args}")
        return self.responses.pop(0)


def _dashboard() -> dict[str, Any]:
    row = build_dashboard(project=PROJECT)
    row["name"] = f"projects/{PROJECT}/dashboards/citylens-operations"
    row["etag"] = "etag"
    return row


def _configure(
    command: FakeCommand,
    *,
    apply: bool = False,
    validate_only: bool = False,
) -> dict[str, Any]:
    return configure(
        project=PROJECT,
        api_service="citylens-api",
        worker_job="citylens-worker",
        apply=apply,
        validate_only=validate_only,
        command=command,
    )


def test_dashboard_covers_runtime_data_recovery_and_incidents() -> None:
    dashboard = build_dashboard(project=PROJECT)

    assert dashboard["displayName"] == DISPLAY_NAME
    assert dashboard["labels"] == {
        "environment": "production",
        "managed_by": "citylens",
    }
    titles = {
        tile["widget"]["title"]
        for tile in dashboard["mosaicLayout"]["tiles"]
    }
    assert {
        "Open production incidents",
        "API and worker errors",
        "API request rate by response class",
        "API request latency · p95",
        "API container instances by state",
        "Worker executions by result",
        "Firestore document operations",
        "API readiness · fraction passing",
        "Web availability · fraction passing",
    }.issubset(titles)
    encoded = str(dashboard)
    assert "citylens-api" in encoded
    assert "citylens-worker" in encoded
    assert "severity>=ERROR" in encoded
    assert "firestore.googleapis.com/document/read_count" in encoded


def test_dashboard_dry_run_plans_create_without_mutation() -> None:
    command = FakeCommand([[]])

    result = _configure(command)

    assert result["action"] == "create"
    assert command.calls == [("monitoring", "dashboards", "list")]


def test_dashboard_validate_only_uses_cloud_api_without_saving() -> None:
    command = FakeCommand([{}])

    result = _configure(command, validate_only=True)

    assert result["action"] == "validated"
    args = command.calls[0]
    assert args[:3] == ("monitoring", "dashboards", "create")
    assert "--validate-only" in args
    assert any(value.startswith("--config=") for value in args)


def test_dashboard_is_idempotent_when_server_fields_differ() -> None:
    existing = _dashboard()
    tiles = existing["mosaicLayout"]["tiles"]
    existing.pop("dashboardFilters")
    tiles[0].pop("xPos")
    tiles[0].pop("yPos")
    tiles[0]["widget"]["text"]["style"] = {}
    tiles[0]["widget"]["id"] = "server-id"
    incident_list = tiles[1]["widget"]["incidentList"]
    incident_list.pop("monitoredResources")
    incident_list.pop("policyNames")
    for tile in tiles:
        chart = tile["widget"].get("xyChart")
        if chart is not None:
            chart.pop("thresholds")
    command = FakeCommand([[existing]])

    result = _configure(command, apply=True)

    assert result["action"] == "unchanged"
    assert len(command.calls) == 1
    assert _canonical(existing) == _canonical(
        build_dashboard(project=PROJECT)
    )


def test_dashboard_update_preserves_name_and_etag() -> None:
    existing = _dashboard()
    existing["mosaicLayout"]["tiles"][0]["height"] = 99
    command = FakeCommand([[existing], _dashboard()])

    result = _configure(command, apply=True)

    assert result["action"] == "updated"
    update_args = command.calls[1]
    assert update_args[:4] == (
        "monitoring",
        "dashboards",
        "update",
        "citylens-operations",
    )
    config = next(
        value.removeprefix("--config=")
        for value in update_args
        if value.startswith("--config=")
    )
    assert '"etag":"etag"' in config
    assert (
        f'"name":"projects/{PROJECT}/dashboards/citylens-operations"'
        in config
    )


def test_duplicate_dashboard_names_fail_closed() -> None:
    existing = _dashboard()
    command = FakeCommand([[existing, deepcopy(existing)]])

    try:
        _configure(command)
    except RuntimeError as exc:
        assert "multiple dashboards" in str(exc)
    else:
        raise AssertionError("duplicate managed dashboards were accepted")

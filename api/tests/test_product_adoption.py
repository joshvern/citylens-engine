from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from scripts.report_product_adoption import (
    _read_saved_view_rows,
    _read_workflow_rows,
)

from app.services.product_adoption import build_product_adoption_report


def test_report_is_aggregate_only_and_uses_explicit_window() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": "private-user-a",
                "day": "2026-07-24",
                "events": {
                    "parcel_opened": 3,
                    "saved_view_applied": 2,
                    "saved_view_created": 1,
                    "workflow_created": 1,
                },
                "sources": {
                    "parcel_opened:map": 2,
                    "parcel_opened:ranking": 1,
                    "saved_view_applied:saved_views": 2,
                    "saved_view_created:saved_views": 1,
                    "workflow_created:header": 1,
                },
                "bbl": "3020960069",
            },
            {
                "_user_id": "private-user-b",
                "day": "2026-07-23",
                "events": {
                    "parcel_opened": 1,
                    "saved_view_applied": 1,
                    "workflow_updated": 2,
                },
                "sources": {
                    "parcel_opened:direct": 1,
                    "saved_view_applied:saved_views": 1,
                    "workflow_updated:workflow": 2,
                },
            },
            {
                "_user_id": "private-user-a",
                "day": "2026-01-01",
                "events": {"parcel_opened": 99},
            },
        ],
        workflow_rows=[
            {"_user_id": "private-user-a", "archived_at": None},
            {
                "_user_id": "private-user-b",
                "archived_at": "2026-07-20T00:00:00Z",
            },
            {"_user_id": "", "archived_at": None},
        ],
        saved_view_rows=[
            {
                "_user_id": "private-user-a",
                "schema_version": "citylens/parcel-saved-view@v2",
            },
            {
                "_user_id": "private-user-b",
                "schema_version": "citylens/parcel-saved-view@v2",
            },
            {"_user_id": "", "schema_version": "citylens/parcel-saved-view@v2"},
            {
                "_user_id": "private-user-c",
                "schema_version": "citylens/parcel-saved-search@v1",
            },
        ],
        as_of=datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc),
        days=30,
    )

    assert report["window"] == {
        "days": 30,
        "start": "2026-06-25",
        "end": "2026-07-24",
    }
    assert report["active_users"] == 2
    assert report["active_user_days"] == 2
    assert report["total_events"] == 11
    assert report["events"] == {
        "parcel_opened": 4,
        "saved_view_applied": 3,
        "saved_view_created": 1,
        "workflow_created": 1,
        "workflow_updated": 2,
    }
    assert report["parcel_open_to_workflow_create_rate"] == 0.25
    assert report["model_accuracy_claim"] is False
    assert report["excluded_or_invalid_rows"] == 1
    assert report["schema_version"] == "citylens/product-adoption-report@v3"
    assert report["workflow_inventory"] == {
        "records": 2,
        "active": 1,
        "archived": 1,
        "users": 2,
        "excluded_or_invalid_rows": 1,
    }
    assert report["saved_view_inventory"] == {
        "records": 2,
        "users": 2,
        "excluded_or_invalid_rows": 2,
    }
    assert report["saved_view_reuse"]["created"] == 1
    assert report["saved_view_reuse"]["updated"] == 0
    assert report["saved_view_reuse"]["deleted"] == 0
    assert report["saved_view_reuse"]["applied"] == 3
    assert report["saved_view_reuse"]["event_users"] == 2
    assert report["saved_view_reuse"]["apply_users"] == 2
    assert report["saved_view_reuse"]["evidence_gate"]["status"] == "collecting"
    assert report["activation_evidence_gate"] == {
        "status": "collecting",
        "minimum_workflow_records": 30,
        "minimum_workflow_users": 3,
        "records_remaining": 28,
        "users_remaining": 1,
        "claim": (
            "Directional activation evidence only; this gate does not "
            "establish lead quality, seller intent, or model accuracy."
        ),
    }
    assert any(
        "saved-view mutation counts are derived transactionally" in warning
        for warning in report["warnings"]
    )
    rendered = json.dumps(report)
    assert "private-user" not in rendered
    assert "3020960069" not in rendered


def test_report_handles_empty_window_without_false_rate() -> None:
    report = build_product_adoption_report(
        [],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
        days=7,
    )
    assert report["active_users"] == 0
    assert report["total_events"] == 0
    assert report["parcel_open_to_workflow_create_rate"] is None
    assert report["workflow_inventory"]["records"] == 0
    assert report["saved_view_inventory"]["records"] == 0
    assert report["saved_view_reuse"]["evidence_gate"]["status"] == "collecting"
    assert report["activation_evidence_gate"]["status"] == "collecting"
    assert any("No qualifying" in warning for warning in report["warnings"])


def test_activation_gate_requires_records_across_multiple_users() -> None:
    rows = [
        {"_user_id": f"user-{index % 3}", "archived_at": None}
        for index in range(30)
    ]
    report = build_product_adoption_report([], workflow_rows=rows)

    assert report["workflow_inventory"]["records"] == 30
    assert report["workflow_inventory"]["users"] == 3
    assert report["activation_evidence_gate"]["status"] == "ready"
    assert report["activation_evidence_gate"]["records_remaining"] == 0
    assert report["activation_evidence_gate"]["users_remaining"] == 0


def test_saved_view_reuse_gate_requires_applies_across_multiple_users() -> None:
    report = build_product_adoption_report(
        [
            {
                "_user_id": f"user-{index % 3}",
                "day": "2026-07-24",
                "events": {"saved_view_applied": 1},
                "sources": {"saved_view_applied:saved_views": 1},
            }
            for index in range(10)
        ],
        as_of=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )

    gate = report["saved_view_reuse"]["evidence_gate"]
    assert report["saved_view_reuse"]["applied"] == 10
    assert report["saved_view_reuse"]["apply_users"] == 3
    assert gate["status"] == "ready"
    assert gate["applies_remaining"] == 0
    assert gate["users_remaining"] == 0


def test_workflow_inventory_query_reads_only_archive_state_and_user_parent() -> None:
    class FakeUserReference:
        id = "private-user-a"

    class FakeCollectionReference:
        parent = FakeUserReference()

    class FakeDocumentReference:
        parent = FakeCollectionReference()

    class FakeSnapshot:
        reference = FakeDocumentReference()

        @staticmethod
        def to_dict() -> dict[str, str]:
            return {
                "archived_at": "2026-07-20T00:00:00Z",
                "bbl": "3020960069",
                "notes": "must never enter the report",
            }

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None

        def select(self, field_paths: list[str]) -> FakeQuery:
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection_group(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    rows = _read_workflow_rows(client)  # type: ignore[arg-type]

    assert client.collection_id == "parcel_workflow"
    assert client.query.field_paths == ["archived_at"]
    assert rows == [
        {
            "_user_id": "private-user-a",
            "archived_at": "2026-07-20T00:00:00Z",
        }
    ]
    assert "3020960069" not in json.dumps(rows)
    assert "must never enter" not in json.dumps(rows)


def test_saved_view_inventory_query_reads_only_schema_and_user_parent() -> None:
    class FakeUserReference:
        id = "private-user-a"

    class FakeCollectionReference:
        parent = FakeUserReference()

    class FakeDocumentReference:
        parent = FakeCollectionReference()

    class FakeSnapshot:
        reference = FakeDocumentReference()

        @staticmethod
        def to_dict() -> dict[str, str]:
            return {
                "schema_version": "citylens/parcel-saved-view@v2",
                "name": "must never enter the report",
                "query": "confidential owner",
                "owner": "PRIVATE OWNER LLC",
            }

    class FakeQuery:
        def __init__(self) -> None:
            self.field_paths: list[str] | None = None

        def select(self, field_paths: list[str]) -> FakeQuery:
            self.field_paths = field_paths
            return self

        @staticmethod
        def stream() -> list[FakeSnapshot]:
            return [FakeSnapshot()]

    class FakeClient:
        def __init__(self) -> None:
            self.query = FakeQuery()
            self.collection_id: str | None = None

        def collection_group(self, collection_id: str) -> FakeQuery:
            self.collection_id = collection_id
            return self.query

    client = FakeClient()
    rows = _read_saved_view_rows(client)  # type: ignore[arg-type]

    assert client.collection_id == "parcel_saved_searches"
    assert client.query.field_paths == ["schema_version"]
    assert rows == [
        {
            "_user_id": "private-user-a",
            "schema_version": "citylens/parcel-saved-view@v2",
        }
    ]
    rendered = json.dumps(rows)
    assert "must never enter" not in rendered
    assert "confidential owner" not in rendered
    assert "PRIVATE OWNER" not in rendered


@pytest.mark.parametrize("days", [0, 91])
def test_report_rejects_windows_outside_retention(days: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 90"):
        build_product_adoption_report([], days=days)

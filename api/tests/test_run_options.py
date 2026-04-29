from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.routes import runs as runs_routes
from app.services.firestore_store import MonthlyQuotaExceeded


class _OkStore:
    def __init__(self) -> None:
        self.created: list[dict] = []

    def get_or_create_user_by_identity(self, **_kwargs):
        return {"user_id": "u1", "plan_type": "free", "is_admin": False}

    def count_user_concurrent_runs(self, *, user_id: str):
        return 0

    def try_increment_monthly_usage(self, *, app_user_id, month_key, limit):
        return 1

    def decrement_monthly_usage(self, *, app_user_id, month_key):
        return 0

    def create_run(self, *, user_id: str, request_dict: dict):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        doc = {
            "run_id": "r1",
            "user_id": user_id,
            "status": "queued",
            "stage": "queued",
            "progress": 0,
            "request": request_dict,
            "error": None,
            "execution_id": None,
            "created_at": now,
            "updated_at": now,
        }
        self.created.append(doc)
        return doc

    def set_execution_id(self, *_args, **_kwargs):
        return None

    def mark_failed(self, *_args, **_kwargs):
        return None


class _Trigger:
    def run(self, *, run_id: str) -> str:
        return "exec-1"


def _install(store) -> None:
    app.dependency_overrides[runs_routes.get_store] = lambda: store
    app.dependency_overrides[runs_routes.get_job_trigger] = lambda: _Trigger()


def test_run_options_endpoint_returns_canonical_shape() -> None:
    client = TestClient(app)
    resp = client.get("/v1/run-options")
    assert resp.status_code == 200
    body = resp.json()
    assert body["imagery_years"] == [2024]
    assert body["baseline_years"] == [2017]
    assert body["segmentation_backends"] == ["sam2"]
    assert sorted(body["outputs"]) == ["change", "mesh", "previews"]
    assert body["defaults"]["aoi_radius_m"] == 250
    assert body["defaults"]["imagery_year"] == 2024


def test_unsupported_imagery_year_returns_400(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St", "imagery_year": 2023})
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["code"] == "INVALID_RUN_OPTION"
    assert detail["field"] == "imagery_year"
    assert detail["allowed_values"] == [2024]


def test_unsupported_baseline_year_returns_400(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "baseline_year": 2018},
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["field"] == "baseline_year"
    assert detail["allowed_values"] == [2017]


def test_unsupported_segmentation_backend_returns_400(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "segmentation_backend": "unet"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "segmentation_backend"


def test_unknown_output_returns_400(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "outputs": ["weird"]},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "outputs"


def test_empty_outputs_returns_400(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St", "outputs": []})
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "outputs"


def test_sam2_cfg_field_rejected(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "sam2_cfg": "configs/sam2.yaml"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "sam2_cfg"


def test_sam2_checkpoint_field_rejected(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "sam2_checkpoint": "weights/x.pt"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "sam2_checkpoint"


def test_aoi_radius_field_rejected(auth_override) -> None:
    auth_override()
    _install(_OkStore())
    client = TestClient(app)
    resp = client.post(
        "/v1/runs",
        json={"address": "1 Main St", "aoi_radius_m": 999},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "aoi_radius_m"


def test_valid_minimal_request_succeeds_and_persists_canonical(auth_override) -> None:
    auth_override(app_user_id="u-ok", plan_type="free")
    store = _OkStore()
    _install(store)
    client = TestClient(app)
    resp = client.post("/v1/runs", json={"address": "1 Main St"})
    assert resp.status_code == 200, resp.text
    persisted = store.created[0]["request"]
    assert persisted["aoi_radius_m"] == 250
    assert persisted["imagery_year"] == 2024
    assert persisted["baseline_year"] == 2017
    assert persisted["segmentation_backend"] == "sam2"
    # Canonical request supplies the SAM2 paths from citylens-core defaults
    assert "sam2_cfg" in persisted
    assert "sam2_checkpoint" in persisted

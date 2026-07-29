from __future__ import annotations

import base64
import hashlib
import json
import struct

from scripts import verify_demo_artifacts as verifier

RUN_ID = "private-run-id"
WEB_ORIGIN = "https://www.citylens.dev"


def _png() -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n"
        + struct.pack(">I", 13)
        + b"IHDR"
        + struct.pack(">II", 512, 512)
        + b"\x08\x06\x00\x00\x00"
        + (b"\x00" * 1_100)
    )


def _geojson() -> bytes:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-73.96, 40.65],
                            [-73.95, 40.65],
                            [-73.95, 40.66],
                            [-73.96, 40.65],
                        ]
                    ],
                },
                "properties": {
                    "crs": "EPSG:4326",
                    "change_type": "added",
                },
            }
        ],
    }
    return json.dumps(payload).encode() + (b" " * 1_024)


def _ply() -> bytes:
    return (
        b"ply\n"
        b"format ascii 1.0\n"
        b"element vertex 3\n"
        b"property float x\n"
        b"property float y\n"
        b"property float z\n"
        b"element face 1\n"
        b"property list uchar int vertex_indices\n"
        b"end_header\n"
        b"0 0 0\n1 0 0\n0 1 0\n3 0 1 2\n"
        + (b" " * 1_024)
    )


def _metadata(name: str, body: bytes) -> dict:
    return {
        "name": name,
        "type": verifier.REQUIRED_ARTIFACTS[name],
        "sha256": hashlib.sha256(body).hexdigest(),
        "size_bytes": len(body),
        "signed_url": f"/v1/demo/artifacts/{RUN_ID}/{name}",
    }


def _summary(
    outer_artifacts: dict[str, dict],
    *,
    metrics: float = 0.95,
) -> bytes:
    payload = {
        "ok": True,
        "errors": [],
        "missing_paths": [],
        "stage_status": {
            stage: "ok" for stage in verifier.REQUIRED_STAGES
        },
        "qa": {
            "reference_case_id": "reference-case",
            "baseline_footprints_used": True,
            "lidar_used": True,
            "sam2_used": True,
            "parity_status": "complete",
            "mask_iou": metrics,
            "change_polygon_f1": metrics,
            "mesh_footprint_iou": metrics,
            "change_counts": {"added": 1, "removed": 0},
            "mesh_stats": {"count": 1},
        },
        "performance": {
            "total_runtime_seconds": 12.5,
            "stage_timings_seconds": {
                stage: 0.5 for stage in verifier.REQUIRED_STAGES
            },
        },
        "artifacts": {
            key: {
                "sha256": outer_artifacts[name]["sha256"],
                "size_bytes": outer_artifacts[name]["size_bytes"],
            }
            for key, name in (
                ("preview", "preview.png"),
                ("change", "change.geojson"),
                ("mesh", "mesh.ply"),
            )
        },
    }
    return json.dumps(payload).encode() + (b" " * 512)


def _fixture() -> tuple[dict[str, bytes], dict[str, dict]]:
    bodies = {
        "preview.png": _png(),
        "change.geojson": _geojson(),
        "mesh.ply": _ply(),
    }
    metadata = {
        name: _metadata(name, body) for name, body in bodies.items()
    }
    bodies["run_summary.json"] = _summary(metadata)
    metadata["run_summary.json"] = _metadata(
        "run_summary.json",
        bodies["run_summary.json"],
    )
    return bodies, metadata


def _delivery(name: str, body: bytes) -> verifier.HttpResult:
    sha256 = hashlib.sha256(body).hexdigest()
    digest = base64.b64encode(bytes.fromhex(sha256)).decode()
    return verifier.HttpResult(
        status=200,
        headers={
            "content-type": verifier.REQUIRED_ARTIFACTS[name],
            "content-length": str(len(body)),
            "x-content-sha256": sha256,
            "etag": f'"{sha256}"',
            "content-digest": f"sha-256=:{digest}:",
            "content-disposition": f'inline; filename="{name}"',
            "cache-control": "public, max-age=31536000, immutable",
            "access-control-allow-origin": WEB_ORIGIN,
            "access-control-expose-headers": (
                "Content-Digest, Content-Disposition, ETag, X-Content-SHA256"
            ),
        },
        body=body,
        elapsed_seconds=0.01,
    )


def test_featured_registry_requires_nonempty_unique_run_ids() -> None:
    failures: list[str] = []
    entries = verifier.flatten_featured(
        {
            "Featured": [
                {"run_id": "same"},
                {"run_id": "same"},
                {},
            ]
        },
        failures=failures,
    )

    assert len(entries) == 3
    assert any("no run_id" in failure for failure in failures)
    assert any("duplicate run_id" in failure for failure in failures)

    failures = []
    assert verifier.flatten_featured({}, failures=failures) == []
    assert failures == ["featured demos: no curated demos were returned"]


def test_run_and_delivery_contract_accept_valid_artifacts() -> None:
    bodies, metadata = _fixture()
    failures: list[str] = []
    artifacts = verifier.validate_run(
        {
            "run_id": RUN_ID,
            "status": "succeeded",
            "stage": "done",
            "progress": 100,
            "error": None,
            "artifacts": list(metadata.values()),
        },
        expected_run_id=RUN_ID,
        ordinal=1,
        failures=failures,
    )
    for name, body in bodies.items():
        receipt = verifier.validate_artifact_delivery(
            name=name,
            metadata=artifacts[name],
            result=_delivery(name, body),
            web_origin=WEB_ORIGIN,
            label="demo 1",
            failures=failures,
        )
        assert receipt["sha256"] == hashlib.sha256(body).hexdigest()

    assert failures == []


def test_run_and_delivery_contract_rejects_corruption_and_bad_proxy_path() -> None:
    bodies, metadata = _fixture()
    metadata["preview.png"]["signed_url"] = "https://storage.example/preview.png"
    failures: list[str] = []
    verifier.validate_run(
        {
            "run_id": RUN_ID,
            "status": "succeeded",
            "stage": "done",
            "progress": 100,
            "error": None,
            "artifacts": list(metadata.values()),
        },
        expected_run_id=RUN_ID,
        ordinal=1,
        failures=failures,
    )
    bad_delivery = _delivery("preview.png", bodies["preview.png"] + b"corrupt")
    verifier.validate_artifact_delivery(
        name="preview.png",
        metadata=metadata["preview.png"],
        result=bad_delivery,
        web_origin=WEB_ORIGIN,
        label="demo 1",
        failures=failures,
    )

    assert any("canonical API proxy path" in failure for failure in failures)
    assert any("byte count" in failure for failure in failures)
    assert any("SHA-256" in failure for failure in failures)


def test_artifact_shapes_reject_empty_or_pixel_space_outputs() -> None:
    failures: list[str] = []
    assert verifier.validate_png(
        b"\x89PNG\r\n\x1a\n",
        label="preview",
        failures=failures,
    ) == {"width": 0, "height": 0}
    verifier.validate_geojson(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [0, 0]]],
                        },
                        "properties": {
                            "crs": "pixel",
                            "change_type": "added",
                        },
                    }
                ],
            }
        ).encode(),
        label="change",
        failures=failures,
    )
    verifier.validate_ply(
        b"ply\nformat ascii 1.0\nelement vertex 0\nelement face 0\nend_header\n",
        label="mesh",
        failures=failures,
    )

    assert any("PNG dimensions" in failure for failure in failures)
    assert any("not georeferenced" in failure for failure in failures)
    assert any("no vertices" in failure for failure in failures)
    assert any("no faces" in failure for failure in failures)


def test_summary_coherence_treats_quality_targets_as_advisory() -> None:
    _, metadata = _fixture()
    failures: list[str] = []
    quality, warnings = verifier.validate_summary(
        _summary(metadata, metrics=0.5),
        label="summary",
        outer_artifacts=metadata,
        change_counts={"added": 1},
        failures=failures,
    )

    assert failures == []
    assert len(warnings) == 3
    assert quality["target_status"] == "below_documented_parity_targets"
    assert "not current accuracy" in quality["quality_scope"]


def test_end_to_end_report_is_value_minimized(monkeypatch) -> None:
    bodies, metadata = _fixture()

    def fake_request(url: str, **_: object) -> verifier.HttpResult:
        if url.endswith("/v1/demo/featured"):
            body = json.dumps(
                {
                    "Featured": [
                        {
                            "run_id": RUN_ID,
                            "address": "private address",
                            "owner": "private owner",
                        }
                    ]
                }
            ).encode()
            return verifier.HttpResult(200, {}, body, 0.01)
        if url.endswith(f"/v1/demo/runs/{RUN_ID}"):
            body = json.dumps(
                {
                    "run_id": RUN_ID,
                    "status": "succeeded",
                    "stage": "done",
                    "progress": 100,
                    "error": None,
                    "artifacts": list(metadata.values()),
                }
            ).encode()
            return verifier.HttpResult(200, {}, body, 0.01)
        name = url.rsplit("/", 1)[-1]
        return _delivery(name, bodies[name])

    monkeypatch.setattr(verifier, "_request", fake_request)
    report = verifier.verify_demo_artifacts(
        api_base="https://api.example.test",
        web_origin=WEB_ORIGIN,
        timeout=1,
    )
    serialized = json.dumps(report)

    assert report["passed"] is True
    assert report["verified_artifact_count"] == 4
    assert RUN_ID not in serialized
    assert "private address" not in serialized
    assert "private owner" not in serialized
    assert "/v1/demo/" not in serialized

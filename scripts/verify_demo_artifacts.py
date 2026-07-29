#!/usr/bin/env python3
"""Verify every curated production demo and its browser-delivered artifacts.

The verifier is intentionally secret-free and stores no run IDs, addresses,
URLs, or user identifiers in its report. Delivery and structural failures are
release-blocking. Pipeline QA metrics are retained as advisory evidence because
they are not current real-world accuracy or seller-intent measurements.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import struct
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

REQUIRED_ARTIFACTS = {
    "preview.png": "image/png",
    "change.geojson": "application/geo+json",
    "mesh.ply": "model/ply",
    "run_summary.json": "application/json",
}
MINIMUM_ARTIFACT_BYTES = {
    "preview.png": 1_024,
    "change.geojson": 1_024,
    "mesh.ply": 1_024,
    "run_summary.json": 512,
}
REQUIRED_STAGES = {
    "resolve",
    "fetch",
    "segment",
    "refine",
    "change",
    "reconstruct",
    "render",
}
DOCUMENTED_PARITY_TARGETS = {
    "mask_iou": 0.90,
    "mesh_footprint_iou": 0.85,
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PLY_COUNT_RE = re.compile(r"^element\s+(vertex|face)\s+(\d+)$", re.MULTILINE)


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes
    elapsed_seconds: float


def _request(
    url: str,
    *,
    timeout: float,
    accept: str,
    origin: str | None = None,
    attempts: int = 3,
) -> HttpResult:
    headers = {
        "Accept": accept,
        "Cache-Control": "no-cache",
        "User-Agent": "citylens-demo-artifact-verifier/1.0",
    }
    if origin:
        headers["Origin"] = origin
    last_error: Exception | None = None
    for attempt in range(attempts):
        started = time.monotonic()
        try:
            with urlopen(Request(url, headers=headers), timeout=timeout) as response:
                return HttpResult(
                    status=int(response.status),
                    headers={
                        key.lower(): value
                        for key, value in response.headers.items()
                    },
                    body=response.read(),
                    elapsed_seconds=time.monotonic() - started,
                )
        except HTTPError as exc:
            body = exc.read()
            if exc.code < 500 or attempt == attempts - 1:
                return HttpResult(
                    status=int(exc.code),
                    headers={
                        key.lower(): value
                        for key, value in exc.headers.items()
                    },
                    body=body,
                    elapsed_seconds=time.monotonic() - started,
                )
            last_error = exc
        except (TimeoutError, URLError) as exc:
            last_error = exc
        if attempt < attempts - 1:
            time.sleep(0.5 * (2**attempt))
    error_type = type(last_error).__name__ if last_error else "unknown error"
    raise RuntimeError(
        f"request failed after {attempts} attempts: {error_type}"
    )


def _json_object(
    result: HttpResult,
    *,
    label: str,
    failures: list[str],
) -> dict[str, Any]:
    if result.status != 200:
        failures.append(f"{label}: expected HTTP 200, got {result.status}")
        return {}
    try:
        value = json.loads(result.body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        failures.append(f"{label}: response was not valid JSON")
        return {}
    if not isinstance(value, dict):
        failures.append(f"{label}: expected a JSON object")
        return {}
    return value


def _expect(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def flatten_featured(
    payload: dict[str, Any],
    *,
    failures: list[str],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for category, raw_entries in payload.items():
        if not isinstance(category, str) or not category.strip():
            failures.append("featured demos: category name is invalid")
            continue
        if not isinstance(raw_entries, list):
            failures.append(
                f"featured demos: category {category!r} is not a list"
            )
            continue
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, dict):
                failures.append(
                    f"featured demos: category {category!r} contains a non-object"
                )
                continue
            entries.append(raw_entry)
    _expect(bool(entries), "featured demos: no curated demos were returned", failures)
    run_ids = [str(entry.get("run_id") or "").strip() for entry in entries]
    _expect(
        all(run_ids),
        "featured demos: one or more entries have no run_id",
        failures,
    )
    _expect(
        len(set(run_ids)) == len(run_ids),
        "featured demos: duplicate run_id entries were returned",
        failures,
    )
    return entries


def validate_run(
    payload: dict[str, Any],
    *,
    expected_run_id: str,
    ordinal: int,
    failures: list[str],
) -> dict[str, dict[str, Any]]:
    label = f"demo {ordinal}"
    _expect(
        payload.get("run_id") == expected_run_id,
        f"{label}: run detail identity does not match the featured entry",
        failures,
    )
    _expect(
        payload.get("status") == "succeeded"
        and payload.get("stage") == "done"
        and payload.get("progress") == 100,
        f"{label}: run is not a completed success",
        failures,
    )
    _expect(payload.get("error") is None, f"{label}: run has an error", failures)
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list):
        failures.append(f"{label}: artifacts is not a list")
        return {}

    artifacts: dict[str, dict[str, Any]] = {}
    for raw_artifact in raw_artifacts:
        if not isinstance(raw_artifact, dict):
            failures.append(f"{label}: artifact entry is not an object")
            continue
        name = str(raw_artifact.get("name") or "").strip()
        if not name:
            failures.append(f"{label}: artifact entry has no name")
            continue
        if name in artifacts:
            failures.append(f"{label}: duplicate artifact {name}")
            continue
        artifacts[name] = raw_artifact

    missing = sorted(set(REQUIRED_ARTIFACTS) - set(artifacts))
    _expect(
        not missing,
        f"{label}: missing required artifacts: {', '.join(missing)}",
        failures,
    )
    for name, expected_type in REQUIRED_ARTIFACTS.items():
        metadata = artifacts.get(name)
        if metadata is None:
            continue
        sha256 = str(metadata.get("sha256") or "").lower()
        size_bytes = metadata.get("size_bytes")
        expected_path = (
            f"/v1/demo/artifacts/{quote(expected_run_id, safe='')}/"
            f"{quote(name, safe='')}"
        )
        _expect(
            metadata.get("type") == expected_type,
            f"{label} {name}: metadata media type is invalid",
            failures,
        )
        _expect(
            SHA256_RE.fullmatch(sha256) is not None,
            f"{label} {name}: metadata SHA-256 is invalid",
            failures,
        )
        _expect(
            isinstance(size_bytes, int)
            and not isinstance(size_bytes, bool)
            and size_bytes >= MINIMUM_ARTIFACT_BYTES[name],
            f"{label} {name}: metadata byte count is missing or implausibly small",
            failures,
        )
        _expect(
            metadata.get("signed_url") == expected_path,
            f"{label} {name}: browser URL is not the canonical API proxy path",
            failures,
        )
    return artifacts


def validate_artifact_delivery(
    *,
    name: str,
    metadata: dict[str, Any],
    result: HttpResult,
    web_origin: str,
    label: str,
    failures: list[str],
) -> dict[str, Any]:
    expected_type = REQUIRED_ARTIFACTS[name]
    expected_sha256 = str(metadata.get("sha256") or "").lower()
    expected_size = metadata.get("size_bytes")
    actual_sha256 = hashlib.sha256(result.body).hexdigest()
    actual_size = len(result.body)
    content_type = result.headers.get("content-type", "").split(";", 1)[0].strip()
    expected_digest = (
        "sha-256=:"
        + base64.b64encode(bytes.fromhex(actual_sha256)).decode("ascii")
        + ":"
    )
    exposed = {
        value.strip().lower()
        for value in result.headers.get(
            "access-control-expose-headers", ""
        ).split(",")
        if value.strip()
    }
    required_exposed = {
        "content-digest",
        "content-disposition",
        "etag",
        "x-content-sha256",
    }
    prefix = f"{label} {name}"
    _expect(
        result.status == 200,
        f"{prefix}: expected HTTP 200, got {result.status}",
        failures,
    )
    _expect(
        content_type == expected_type,
        f"{prefix}: expected media type {expected_type}, got {content_type or 'missing'}",
        failures,
    )
    _expect(
        actual_size == expected_size,
        f"{prefix}: delivered byte count does not match metadata",
        failures,
    )
    _expect(
        actual_size >= MINIMUM_ARTIFACT_BYTES[name],
        f"{prefix}: delivered payload is implausibly small",
        failures,
    )
    _expect(
        actual_sha256 == expected_sha256,
        f"{prefix}: delivered SHA-256 does not match metadata",
        failures,
    )
    _expect(
        result.headers.get("x-content-sha256") == actual_sha256,
        f"{prefix}: X-Content-SHA256 is missing or invalid",
        failures,
    )
    _expect(
        result.headers.get("etag") == f'"{actual_sha256}"',
        f"{prefix}: ETag is missing or invalid",
        failures,
    )
    _expect(
        result.headers.get("content-digest") == expected_digest,
        f"{prefix}: Content-Digest is missing or invalid",
        failures,
    )
    _expect(
        result.headers.get("content-length") == str(actual_size),
        f"{prefix}: Content-Length is missing or invalid",
        failures,
    )
    _expect(
        result.headers.get("access-control-allow-origin") == web_origin,
        f"{prefix}: production web origin is not allowed by CORS",
        failures,
    )
    _expect(
        required_exposed.issubset(exposed),
        f"{prefix}: browser integrity headers are not all exposed",
        failures,
    )
    _expect(
        name in result.headers.get("content-disposition", ""),
        f"{prefix}: Content-Disposition does not preserve the filename",
        failures,
    )
    cache_control = result.headers.get("cache-control", "").lower()
    _expect(
        "immutable" in cache_control and "max-age=" in cache_control,
        f"{prefix}: immutable cache contract is missing",
        failures,
    )
    return {
        "name": name,
        "content_type": content_type,
        "size_bytes": actual_size,
        "sha256": actual_sha256,
        "elapsed_seconds": round(result.elapsed_seconds, 3),
    }


def validate_png(
    body: bytes,
    *,
    label: str,
    failures: list[str],
) -> dict[str, int]:
    width = height = 0
    if (
        len(body) >= 24
        and body[:8] == b"\x89PNG\r\n\x1a\n"
        and body[12:16] == b"IHDR"
    ):
        width, height = struct.unpack(">II", body[16:24])
    _expect(
        width >= 64 and height >= 64,
        f"{label}: PNG dimensions are invalid",
        failures,
    )
    return {"width": width, "height": height}


def validate_geojson(
    body: bytes,
    *,
    label: str,
    failures: list[str],
) -> tuple[dict[str, Any], dict[str, int]]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        failures.append(f"{label}: change artifact is not valid JSON")
        return {}, {}
    if not isinstance(payload, dict):
        failures.append(f"{label}: change artifact is not a JSON object")
        return {}, {}
    features = payload.get("features")
    _expect(
        payload.get("type") == "FeatureCollection" and isinstance(features, list),
        f"{label}: change artifact is not a GeoJSON FeatureCollection",
        failures,
    )
    if not isinstance(features, list):
        return payload, {}
    _expect(bool(features), f"{label}: change artifact contains no features", failures)
    change_counts: dict[str, int] = {}
    for index, feature in enumerate(features):
        if not isinstance(feature, dict):
            failures.append(f"{label}: feature {index} is not an object")
            continue
        geometry = feature.get("geometry")
        properties = feature.get("properties")
        _expect(
            feature.get("type") == "Feature"
            and isinstance(geometry, dict)
            and bool(geometry.get("coordinates")),
            f"{label}: feature {index} has no usable geometry",
            failures,
        )
        if not isinstance(properties, dict):
            failures.append(f"{label}: feature {index} has no properties")
            continue
        _expect(
            properties.get("crs") != "pixel",
            f"{label}: feature {index} is not georeferenced",
            failures,
        )
        change_type = str(
            properties.get("change_type") or properties.get("kind") or ""
        ).strip()
        _expect(
            bool(change_type),
            f"{label}: feature {index} has no change classification",
            failures,
        )
        if change_type:
            change_counts[change_type] = change_counts.get(change_type, 0) + 1
    return payload, change_counts


def validate_ply(
    body: bytes,
    *,
    label: str,
    failures: list[str],
) -> dict[str, int]:
    header_end = body.find(b"end_header")
    _expect(
        body.startswith(b"ply\n") and 0 < header_end < 65_536,
        f"{label}: mesh is not a parseable PLY document",
        failures,
    )
    if header_end <= 0:
        return {"vertex_count": 0, "face_count": 0}
    header = body[:header_end].decode("ascii", errors="replace")
    counts = {name: int(value) for name, value in PLY_COUNT_RE.findall(header)}
    vertex_count = counts.get("vertex", 0)
    face_count = counts.get("face", 0)
    _expect(vertex_count > 0, f"{label}: mesh contains no vertices", failures)
    _expect(face_count > 0, f"{label}: mesh contains no faces", failures)
    return {"vertex_count": vertex_count, "face_count": face_count}


def validate_summary(
    body: bytes,
    *,
    label: str,
    outer_artifacts: dict[str, dict[str, Any]],
    change_counts: dict[str, int],
    failures: list[str],
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    try:
        summary = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        failures.append(f"{label}: summary is not valid JSON")
        return {}, warnings
    if not isinstance(summary, dict):
        failures.append(f"{label}: summary is not a JSON object")
        return {}, warnings
    _expect(summary.get("ok") is True, f"{label}: summary is not successful", failures)
    _expect(summary.get("errors") == [], f"{label}: summary contains errors", failures)
    _expect(
        summary.get("missing_paths") == [],
        f"{label}: summary records missing paths",
        failures,
    )
    stage_status = summary.get("stage_status")
    _expect(
        isinstance(stage_status, dict)
        and REQUIRED_STAGES.issubset(stage_status)
        and all(stage_status.get(stage) == "ok" for stage in REQUIRED_STAGES),
        f"{label}: one or more required stages are not successful",
        failures,
    )

    qa = summary.get("qa")
    if not isinstance(qa, dict):
        failures.append(f"{label}: QA block is missing")
        qa = {}
    for flag in ("baseline_footprints_used", "lidar_used", "sam2_used"):
        _expect(qa.get(flag) is True, f"{label}: QA flag {flag} is not true", failures)
    _expect(
        isinstance(qa.get("reference_case_id"), str)
        and bool(qa["reference_case_id"].strip()),
        f"{label}: QA reference case is missing",
        failures,
    )
    _expect(
        qa.get("parity_status") == "complete",
        f"{label}: parity computation is incomplete",
        failures,
    )
    metrics: dict[str, float | None] = {}
    target_warnings: list[str] = []
    for metric, target in DOCUMENTED_PARITY_TARGETS.items():
        raw = qa.get(metric)
        value = (
            float(raw)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool)
            else None
        )
        metrics[metric] = value
        _expect(
            value is not None and 0.0 <= value <= 1.0,
            f"{label}: QA metric {metric} is missing or invalid",
            failures,
        )
        if value is not None and value < target:
            target_warnings.append(
                f"{label}: {metric} {value:.3f} is below the documented "
                f"parity target {target:.2f}"
            )
    warnings.extend(target_warnings)

    consistency_raw = qa.get("mask_xor_f1")
    if consistency_raw is None:
        consistency_raw = qa.get("change_polygon_f1")
        if consistency_raw is not None:
            warnings.append(
                f"{label}: legacy change_polygon_f1 receipt was interpreted "
                "as mask_xor_f1; this circular consistency signal is not accuracy"
            )
    consistency_value = (
        float(consistency_raw)
        if isinstance(consistency_raw, (int, float))
        and not isinstance(consistency_raw, bool)
        else None
    )
    metrics["mask_xor_f1"] = consistency_value
    _expect(
        consistency_value is not None and 0.0 <= consistency_value <= 1.0,
        f"{label}: QA consistency metric mask_xor_f1 is missing or invalid",
        failures,
    )

    summary_change_counts = qa.get("change_counts")
    normalized_summary_change_counts = (
        {
            str(key): value
            for key, value in summary_change_counts.items()
            if isinstance(value, int)
            and not isinstance(value, bool)
            and value != 0
        }
        if isinstance(summary_change_counts, dict)
        else None
    )
    _expect(
        normalized_summary_change_counts == change_counts,
        f"{label}: summary change counts do not match change.geojson",
        failures,
    )
    mesh_stats = qa.get("mesh_stats")
    _expect(
        isinstance(mesh_stats, dict)
        and isinstance(mesh_stats.get("count"), int)
        and mesh_stats["count"] > 0,
        f"{label}: mesh QA count is missing or empty",
        failures,
    )

    performance = summary.get("performance")
    _expect(
        isinstance(performance, dict)
        and isinstance(performance.get("total_runtime_seconds"), (int, float))
        and not isinstance(performance.get("total_runtime_seconds"), bool)
        and performance["total_runtime_seconds"] > 0,
        f"{label}: total runtime receipt is missing or invalid",
        failures,
    )
    timings = (
        performance.get("stage_timings_seconds")
        if isinstance(performance, dict)
        else None
    )
    _expect(
        isinstance(timings, dict)
        and REQUIRED_STAGES.issubset(timings)
        and all(
            isinstance(timings.get(stage), (int, float))
            and not isinstance(timings.get(stage), bool)
            and timings[stage] >= 0
            for stage in REQUIRED_STAGES
        ),
        f"{label}: stage timing receipt is missing or invalid",
        failures,
    )

    inner_artifacts = summary.get("artifacts")
    if not isinstance(inner_artifacts, dict):
        failures.append(f"{label}: summary artifact manifest is missing")
    else:
        for inner_name, outer_name in (
            ("preview", "preview.png"),
            ("change", "change.geojson"),
            ("mesh", "mesh.ply"),
        ):
            inner = inner_artifacts.get(inner_name)
            outer = outer_artifacts.get(outer_name, {})
            _expect(
                isinstance(inner, dict)
                and inner.get("sha256") == outer.get("sha256")
                and inner.get("size_bytes") == outer.get("size_bytes"),
                f"{label}: summary receipt for {outer_name} does not match run metadata",
                failures,
            )
    return {
        "metrics": metrics,
        "quality_scope": (
            "advisory pipeline parity evidence; not current accuracy, "
            "seller intent, or transaction probability"
        ),
        "target_status": (
            "meets_documented_parity_targets"
            if not target_warnings
            else "below_documented_parity_targets"
        ),
        "total_runtime_seconds": (
            round(float(performance["total_runtime_seconds"]), 3)
            if isinstance(performance, dict)
            and isinstance(performance.get("total_runtime_seconds"), (int, float))
            else None
        ),
    }, warnings


def verify_demo_artifacts(
    *,
    api_base: str,
    web_origin: str,
    timeout: float,
) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    receipts: list[dict[str, Any]] = []
    api_base = api_base.rstrip("/")
    web_origin = web_origin.rstrip("/")

    try:
        featured_result = _request(
            f"{api_base}/v1/demo/featured",
            timeout=timeout,
            accept="application/json",
        )
    except RuntimeError as exc:
        failures.append(f"featured demos: request failed: {exc}")
        entries: list[dict[str, Any]] = []
    else:
        featured = _json_object(
            featured_result,
            label="featured demos",
            failures=failures,
        )
        entries = flatten_featured(featured, failures=failures)

    for ordinal, entry in enumerate(entries, start=1):
        label = f"demo {ordinal}"
        run_id = str(entry.get("run_id") or "").strip()
        if not run_id:
            continue
        try:
            run_result = _request(
                f"{api_base}/v1/demo/runs/{quote(run_id, safe='')}",
                timeout=timeout,
                accept="application/json",
            )
        except RuntimeError as exc:
            failures.append(f"{label}: run detail request failed: {exc}")
            continue
        run = _json_object(run_result, label=label, failures=failures)
        artifacts = validate_run(
            run,
            expected_run_id=run_id,
            ordinal=ordinal,
            failures=failures,
        )
        delivered: dict[str, bytes] = {}
        artifact_receipts: list[dict[str, Any]] = []
        for name, expected_type in REQUIRED_ARTIFACTS.items():
            metadata = artifacts.get(name)
            if metadata is None:
                continue
            path = str(metadata.get("signed_url") or "")
            if not path.startswith("/"):
                continue
            try:
                result = _request(
                    f"{api_base}{path}",
                    timeout=timeout,
                    accept=expected_type,
                    origin=web_origin,
                )
            except RuntimeError as exc:
                failures.append(f"{label} {name}: request failed: {exc}")
                continue
            delivered[name] = result.body
            artifact_receipts.append(
                validate_artifact_delivery(
                    name=name,
                    metadata=metadata,
                    result=result,
                    web_origin=web_origin,
                    label=label,
                    failures=failures,
                )
            )

        shape: dict[str, Any] = {}
        if "preview.png" in delivered:
            shape["preview"] = validate_png(
                delivered["preview.png"],
                label=f"{label} preview.png",
                failures=failures,
            )
        change_counts: dict[str, int] = {}
        if "change.geojson" in delivered:
            _, change_counts = validate_geojson(
                delivered["change.geojson"],
                label=f"{label} change.geojson",
                failures=failures,
            )
            shape["change_feature_count"] = sum(change_counts.values())
        if "mesh.ply" in delivered:
            shape["mesh"] = validate_ply(
                delivered["mesh.ply"],
                label=f"{label} mesh.ply",
                failures=failures,
            )
        quality: dict[str, Any] = {}
        if "run_summary.json" in delivered:
            quality, demo_warnings = validate_summary(
                delivered["run_summary.json"],
                label=f"{label} run_summary.json",
                outer_artifacts=artifacts,
                change_counts=change_counts,
                failures=failures,
            )
            warnings.extend(demo_warnings)
        receipts.append(
            {
                "demo_index": ordinal,
                "artifact_count": len(artifact_receipts),
                "artifacts": artifact_receipts,
                "shape": shape,
                "quality": quality,
            }
        )

    artifact_count = sum(receipt["artifact_count"] for receipt in receipts)
    below_target = any(
        receipt.get("quality", {}).get("target_status")
        == "below_documented_parity_targets"
        for receipt in receipts
    )
    report = {
        "schema_version": "citylens/demo-artifact-verification@v1",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "api_host": urlparse(api_base).netloc,
        "web_host": urlparse(web_origin).netloc,
        "passed": not failures,
        "demo_count": len(receipts),
        "required_artifact_count": len(entries) * len(REQUIRED_ARTIFACTS),
        "verified_artifact_count": artifact_count,
        "quality_target_status": (
            "unavailable"
            if not receipts
            else (
                "advisory_below_documented_parity_targets"
                if below_target
                else "meets_documented_parity_targets"
            )
        ),
        "quality_scope": (
            "Pipeline parity metrics are advisory and are not current "
            "real-world accuracy, seller intent, or transaction probability."
        ),
        "failures": failures,
        "warnings": warnings,
        "demos": receipts,
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify all curated production demo artifacts"
    )
    parser.add_argument(
        "--api-base",
        default="https://api.citylens.dev",
        help="Deployed CityLens API origin",
    )
    parser.add_argument(
        "--web-origin",
        default="https://www.citylens.dev",
        help="Browser origin whose CORS delivery contract must pass",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--output",
        default="demo-artifact-verification.json",
    )
    args = parser.parse_args(argv)
    report = verify_demo_artifacts(
        api_base=args.api_base,
        web_origin=args.web_origin,
        timeout=args.timeout,
    )
    Path(args.output).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "demo artifact verification: "
        f"{'passed' if report['passed'] else 'failed'} · "
        f"{report['demo_count']} demos · "
        f"{report['verified_artifact_count']}/"
        f"{report['required_artifact_count']} artifacts · "
        f"quality {report['quality_target_status']}",
        file=sys.stderr,
    )
    for warning in report["warnings"]:
        print(f"warning: {warning}", file=sys.stderr)
    for failure in report["failures"]:
        print(f"failure: {failure}", file=sys.stderr)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REQUIRED_ARTIFACTS = ("preview.png", "change.geojson", "mesh.ply", "run_summary.json")


@dataclass(frozen=True)
class DemoAddress:
    category: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str]
    orthophoto_path: str | None = None
    baseline_path: str | None = None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _http_json(method: str, url: str, *, api_key: str, body: dict | None = None, timeout_s: float = 30.0) -> Any:
    headers = {
        "Accept": "application/json",
        "X-API-Key": api_key,
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(url, method=method, headers=headers, data=data)

    try:
        with urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            ct = (resp.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return json.loads(raw) if raw else None
            # Some backends might return just a run_id string.
            return raw
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        raise RuntimeError(f"HTTP {e.code} calling {url}: {err_body or e.reason}") from e
    except URLError as e:
        raise RuntimeError(f"Network error calling {url}: {e}") from e


def _normalize_run_id(create_resp: Any) -> str:
    if isinstance(create_resp, str):
        rid = create_resp.strip()
        if rid:
            return rid

    if isinstance(create_resp, dict):
        for k in ("run_id", "runId", "id"):
            v = create_resp.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    raise RuntimeError(f"Could not determine run_id from response: {create_resp!r}")


def _normalize_artifacts(raw: Any) -> dict[str, dict[str, Any]]:
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = []
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            if "name" not in value:
                value = {"name": str(key), **value}
            items.append(value)
    else:
        items = []

    out: dict[str, dict[str, Any]] = {}
    for artifact in items:
        if not isinstance(artifact, dict):
            continue
        name = str(artifact.get("name") or "").strip()
        if not name:
            continue
        out[name] = artifact
    return out


def _artifact_fetch_url(artifact: dict[str, Any]) -> str | None:
    for key in ("signed_url", "url"):
        value = artifact.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _probe_url(url: str, *, timeout_s: float = 30.0) -> None:
    req = Request(url, headers={"Accept": "*/*"})
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            resp.read(1)
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        raise RuntimeError(f"HTTP {e.code} fetching artifact {url}: {err_body or e.reason}") from e
    except URLError as e:
        raise RuntimeError(f"Network error fetching artifact {url}: {e}") from e


def _fetch_json_url(url: str, *, timeout_s: float = 30.0) -> Any:
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        raise RuntimeError(f"HTTP {e.code} fetching artifact {url}: {err_body or e.reason}") from e
    except URLError as e:
        raise RuntimeError(f"Network error fetching artifact {url}: {e}") from e


def _validate_summary_json(summary: Any, *, run_id: str) -> None:
    if not isinstance(summary, dict):
        raise RuntimeError(f"run_summary.json for run_id={run_id} did not return a JSON object")
    if not isinstance(summary.get("qa"), dict):
        raise RuntimeError(f"run_summary.json for run_id={run_id} is missing a qa object")
    if not isinstance(summary.get("performance"), dict):
        raise RuntimeError(f"run_summary.json for run_id={run_id} is missing a performance object")


def _validate_completed_run(run: Any, *, run_id: str) -> None:
    if not isinstance(run, dict):
        raise RuntimeError(f"Expected dict run response for run_id={run_id}, got {type(run).__name__}")

    artifacts = _normalize_artifacts(run.get("artifacts"))
    missing = [name for name in REQUIRED_ARTIFACTS if name not in artifacts]
    if missing:
        raise RuntimeError(
            f"Run {run_id} is missing required artifacts: {', '.join(missing)}"
        )

    for artifact_name in REQUIRED_ARTIFACTS:
        fetch_url = _artifact_fetch_url(artifacts[artifact_name])
        if not fetch_url:
            raise RuntimeError(f"Run {run_id} is missing a fetchable URL for {artifact_name}")
        if artifact_name != "run_summary.json":
            _probe_url(fetch_url)

    summary_url = _artifact_fetch_url(artifacts["run_summary.json"])
    summary = _fetch_json_url(summary_url)
    _validate_summary_json(summary, run_id=run_id)


def _load_addresses(path: Path) -> list[DemoAddress]:
    raw = _read_json(path)
    if not isinstance(raw, list):
        raise RuntimeError("demo_addresses.json must be a JSON list")

    out: list[DemoAddress] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        out.append(
            DemoAddress(
                category=str(item.get("category") or "Featured").strip() or "Featured",
                label=str(item.get("label") or "").strip(),
                address=str(item.get("address") or "").strip(),
                imagery_year=int(item.get("imagery_year") or 2024),
                baseline_year=int(item.get("baseline_year") or 2017),
                segmentation_backend=str(item.get("segmentation_backend") or "sam2").strip() or "sam2",
                outputs=[str(x) for x in (item.get("outputs") or [])] if isinstance(item.get("outputs"), list) else ["previews", "change", "mesh"],
                orthophoto_path=(str(item.get("orthophoto_path")).strip() or None) if item.get("orthophoto_path") is not None else None,
                baseline_path=(str(item.get("baseline_path")).strip() or None) if item.get("baseline_path") is not None else None,
            )
        )

    out = [d for d in out if d.label and d.address]
    if not out:
        raise RuntimeError("No valid entries found in demo_addresses.json")
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Precompute Citylens demo runs and write deploy/demo_runs.json")
    parser.add_argument(
        "--api-base",
        default=os.getenv("CITYLENS_API_BASE", "http://localhost:8000").rstrip("/"),
        help="Base URL for engine API (default: $CITYLENS_API_BASE or http://localhost:8000)",
    )
    parser.add_argument(
        "--admin-api-key",
        default=os.getenv("CITYLENS_ADMIN_API_KEY", ""),
        help="Admin API key to call POST /v1/runs (default: $CITYLENS_ADMIN_API_KEY)",
    )
    parser.add_argument(
        "--addresses",
        default=str(Path(__file__).resolve().parents[1] / "deploy" / "demo_addresses.json"),
        help="Path to deploy/demo_addresses.json",
    )
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "deploy" / "demo_runs.json"),
        help="Path to write deploy/demo_runs.json",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20 * 60.0,
        help="Timeout per run",
    )

    args = parser.parse_args(argv)

    if not args.admin_api_key.strip():
        raise SystemExit("Missing --admin-api-key (or CITYLENS_ADMIN_API_KEY)")

    api_base = str(args.api_base).rstrip("/")
    create_url = f"{api_base}/v1/runs"

    addresses_path = Path(args.addresses)
    out_path = Path(args.out)

    demos = _load_addresses(addresses_path)

    results: list[dict[str, Any]] = []

    for demo in demos:
        payload = {
            "address": demo.address,
            "imagery_year": demo.imagery_year,
            "baseline_year": demo.baseline_year,
            "segmentation_backend": demo.segmentation_backend,
            "outputs": demo.outputs,
        }

        # Optional explicit input paths (useful for Cloud Run demos where inputs are baked into the worker image)
        if demo.orthophoto_path:
            payload["orthophoto_path"] = demo.orthophoto_path
        if demo.baseline_path:
            payload["baseline_path"] = demo.baseline_path

        print(f"Creating run for: {demo.label} ({demo.address})", file=sys.stderr)
        create_resp = _http_json("POST", create_url, api_key=args.admin_api_key, body=payload)
        run_id = _normalize_run_id(create_resp)
        print(f"  run_id={run_id}", file=sys.stderr)

        get_url = f"{api_base}/v1/runs/{run_id}"
        deadline = time.time() + float(args.timeout_seconds)

        while True:
            run = _http_json("GET", get_url, api_key=args.admin_api_key)
            status = (run.get("status") if isinstance(run, dict) else None) or "unknown"
            stage = (run.get("stage") if isinstance(run, dict) else None) or ""
            progress = (run.get("progress") if isinstance(run, dict) else None)

            print(f"  status={status} stage={stage} progress={progress}", file=sys.stderr)

            if status == "succeeded":
                break
            if status == "failed":
                raise RuntimeError(f"Run failed for {demo.label} (run_id={run_id})")

            if time.time() > deadline:
                raise RuntimeError(f"Timed out waiting for run_id={run_id} ({demo.label})")

            time.sleep(float(args.poll_interval_seconds))

        _validate_completed_run(run, run_id=run_id)

        results.append(
            {
                "category": demo.category,
                "run_id": run_id,
                "label": demo.label,
                "address": demo.address,
                "imagery_year": demo.imagery_year,
                "baseline_year": demo.baseline_year,
                "segmentation_backend": demo.segmentation_backend,
                "outputs": demo.outputs,
            }
        )

    _write_json(out_path, {"runs": results})
    print(f"Wrote {out_path} with {len(results)} demo runs", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

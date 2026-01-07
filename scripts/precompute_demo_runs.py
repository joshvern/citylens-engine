#!/usr/bin/env python3
"""Precompute demo runs and write deploy/demo_runs.json.

This script calls the normal authenticated API endpoints:
- POST /v1/runs (requires X-API-Key)
- GET /v1/runs/{run_id} until succeeded/failed

It never talks to Firestore or GCS directly; it uses the public API.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class AddressEntry:
    category: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str]


def _http_json(method: str, url: str, *, headers: dict[str, str], body: dict[str, Any] | None = None) -> Any:
    data: bytes | None = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {**headers, "Content-Type": "application/json"}

    req = urllib.request.Request(url=url, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} for {method} {url}: {raw}") from e


def _normalize_segmentation_backend(raw: str) -> str:
    v = (raw or "").strip().lower()
    if v in {"sam2_prompt", "sam2.1", "sam2_1"}:
        return "sam2"
    return v or "sam2"


def _load_addresses(path: Path) -> list[AddressEntry]:
    obj = json.loads(path.read_text(encoding="utf-8"))

    def coerce_entry(category: str, e: dict[str, Any]) -> AddressEntry:
        label = str(e.get("label") or e.get("address") or "").strip()
        address = str(e.get("address") or "").strip()
        if not address:
            raise ValueError(f"Missing address in entry under category '{category}'")

        imagery_year = int(e.get("imagery_year") or 2024)
        baseline_year = int(e.get("baseline_year") or 2017)
        segmentation_backend = _normalize_segmentation_backend(str(e.get("segmentation_backend") or "sam2"))
        outputs = e.get("outputs")
        if isinstance(outputs, list) and outputs:
            outputs_list = [str(x) for x in outputs]
        else:
            outputs_list = ["previews", "change", "mesh"]

        return AddressEntry(
            category=str(category),
            label=label,
            address=address,
            imagery_year=imagery_year,
            baseline_year=baseline_year,
            segmentation_backend=segmentation_backend,
            outputs=outputs_list,
        )

    out: list[AddressEntry] = []

    # Supported formats:
    # 1) {"groups": {"Cat": [..], ...}}
    # 2) {"Cat": [..], ...}
    # 3) [{..}, {..}] (category defaults to "Demo")
    if isinstance(obj, dict) and isinstance(obj.get("groups"), dict):
        groups = obj["groups"]
        for cat, entries in groups.items():
            if not isinstance(entries, list):
                continue
            for e in entries:
                if isinstance(e, dict):
                    out.append(coerce_entry(str(cat), e))
        return out

    if isinstance(obj, dict):
        for cat, entries in obj.items():
            if cat == "defaults":
                continue
            if not isinstance(entries, list):
                continue
            for e in entries:
                if isinstance(e, dict):
                    out.append(coerce_entry(str(cat), e))
        if out:
            return out

    if isinstance(obj, list):
        for e in obj:
            if isinstance(e, dict):
                out.append(coerce_entry("Demo", e))
        return out

    raise ValueError(f"Unsupported addresses JSON format in {path}")


def _poll_run(
    *,
    api_base: str,
    run_id: str,
    api_key: str,
    poll_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    url = f"{api_base}/v1/runs/{urllib.parse.quote(run_id)}"

    last_report_at = 0.0

    while True:
        run = _http_json("GET", url, headers={"X-API-Key": api_key})
        if isinstance(run, dict):
            status = str(run.get("status") or "")
            now = time.time()
            if now - last_report_at >= 30:
                stage = str(run.get("stage") or "")
                progress = run.get("progress")
                exec_id = str(run.get("execution_id") or "")
                prog_str = f"{progress}%" if progress is not None else "?%"
                extra = []
                if stage:
                    extra.append(f"stage={stage}")
                if exec_id:
                    extra.append(f"execution_id={exec_id}")
                suffix = (" (" + ", ".join(extra) + ")") if extra else ""
                print(f"  .. status={status or 'unknown'} progress={prog_str}{suffix}")
                last_report_at = now
            if status in {"succeeded", "failed"}:
                return run

        if time.time() >= deadline:
            raise TimeoutError(f"Timed out waiting for run {run_id} to finish")

        time.sleep(poll_seconds)


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base", required=True, help="Example: https://<service>.run.app")
    parser.add_argument(
        "--admin-api-key",
        default="",
        help="API key used for authenticated /v1/runs (optional; defaults to CITYLENS_ADMIN_API_KEY or first CITYLENS_API_KEYS entry)",
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
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=30 * 60.0)
    args = parser.parse_args(list(argv))

    api_base = str(args.api_base).rstrip("/")

    api_key = str(args.admin_api_key).strip()
    if not api_key:
        api_key = str((os.environ.get("CITYLENS_ADMIN_API_KEY") or "")).strip()
    if not api_key:
        keys_raw = str((os.environ.get("CITYLENS_API_KEYS") or "")).strip()
        first = keys_raw.split(",", 1)[0].strip() if keys_raw else ""
        api_key = first
    if not api_key:
        raise RuntimeError(
            "Missing API key: pass --admin-api-key or set CITYLENS_ADMIN_API_KEY or CITYLENS_API_KEYS"
        )

    addresses_path = Path(args.addresses)
    out_path = Path(args.out)

    entries = _load_addresses(addresses_path)
    if not entries:
        print(f"No demo addresses found in {addresses_path}", file=sys.stderr)
        return 2

    runs_out: list[dict[str, Any]] = []

    for idx, entry in enumerate(entries, start=1):
        print(f"[{idx}/{len(entries)}] Creating run for {entry.label} ({entry.address})")

        create_url = f"{api_base}/v1/runs"
        req_body: dict[str, Any] = {
            "address": entry.address,
            "imagery_year": entry.imagery_year,
            "baseline_year": entry.baseline_year,
            "segmentation_backend": entry.segmentation_backend,
            "outputs": entry.outputs,
        }

        created = _http_json("POST", create_url, headers={"X-API-Key": api_key}, body=req_body)
        if not isinstance(created, dict) or not created.get("run_id"):
            raise RuntimeError(f"Unexpected create_run response: {created}")

        run_id = str(created["run_id"])
        print(f"  -> run_id={run_id}. Polling...")

        final = _poll_run(
            api_base=api_base,
            run_id=run_id,
            api_key=api_key,
            poll_seconds=float(args.poll_seconds),
            timeout_seconds=float(args.timeout_seconds),
        )

        status = str(final.get("status") or "")
        if status != "succeeded":
            raise RuntimeError(f"Run {run_id} finished with status={status}: {final.get('error')}")

        runs_out.append(
            {
                "run_id": run_id,
                "category": entry.category,
                "label": entry.label,
                "address": entry.address,
                "imagery_year": entry.imagery_year,
                "baseline_year": entry.baseline_year,
                "segmentation_backend": entry.segmentation_backend,
                "outputs": ["preview.png", "change.geojson", "mesh.ply", "run_summary.json"],
            }
        )

    payload = {"runs": runs_out}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote allowlist: {out_path} ({len(runs_out)} runs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

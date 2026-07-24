#!/usr/bin/env python3
"""Print a privacy-preserving aggregate parcel product-adoption report."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
API_ROOT = ROOT / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from google.cloud import firestore  # noqa: E402

from app.services.product_adoption import build_product_adoption_report  # noqa: E402


def _default_project() -> str | None:
    configured = os.getenv("GOOGLE_CLOUD_PROJECT")
    if configured:
        return configured
    result = subprocess.run(
        ["gcloud", "config", "get-value", "project"],
        check=False,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value != "(unset)" else None


def _read_rows(client: firestore.Client) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in client.collection_group("product_usage_days").stream():
        row = snapshot.to_dict() or {}
        user_ref = snapshot.reference.parent.parent
        row["_user_id"] = user_ref.id if user_ref is not None else ""
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=_default_project())
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")

    report = build_product_adoption_report(
        _read_rows(firestore.Client(project=args.project)),
        days=args.days,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

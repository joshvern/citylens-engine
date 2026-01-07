from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import Depends

from ..models.schemas import DemoRunFeatured


@dataclass(frozen=True)
class DemoRunEntry:
    run_id: str
    category: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str]


class DemoRegistry:
    def __init__(self, json_path: Path) -> None:
        self._json_path = json_path
        self._cache_mtime: float | None = None
        self._cache: dict[str, DemoRunEntry] = {}
        self._cache_featured: dict[str, list[DemoRunFeatured]] = {}

    @property
    def json_path(self) -> Path:
        return self._json_path

    def _load(self) -> None:
        # Fail closed if allowlist is missing.
        if not self._json_path.exists():
            self._cache_mtime = None
            self._cache = {}
            self._cache_featured = {}
            return

        mtime = self._json_path.stat().st_mtime
        if self._cache_mtime == mtime:
            return

        obj = json.loads(self._json_path.read_text(encoding="utf-8"))
        runs: list[dict[str, Any]] = []

        if isinstance(obj, dict) and isinstance(obj.get("runs"), list):
            runs = [r for r in obj["runs"] if isinstance(r, dict)]
        elif isinstance(obj, dict):
            # Also accept {"Category": [..], ...}
            for cat, entries in obj.items():
                if isinstance(entries, list):
                    for r in entries:
                        if isinstance(r, dict):
                            r = {**r, "category": r.get("category") or cat}
                            runs.append(r)

        new_cache: dict[str, DemoRunEntry] = {}
        new_featured: dict[str, list[DemoRunFeatured]] = {}

        for r in runs:
            run_id = str(r.get("run_id") or "").strip()
            if not run_id:
                continue

            category = str(r.get("category") or "Demo")
            entry = DemoRunEntry(
                run_id=run_id,
                category=category,
                label=str(r.get("label") or run_id),
                address=str(r.get("address") or ""),
                imagery_year=int(r.get("imagery_year") or 0),
                baseline_year=int(r.get("baseline_year") or 0),
                segmentation_backend=str(r.get("segmentation_backend") or ""),
                outputs=[str(x) for x in (r.get("outputs") or []) if str(x)],
            )

            new_cache[run_id] = entry
            new_featured.setdefault(category, []).append(
                DemoRunFeatured(
                    run_id=entry.run_id,
                    label=entry.label,
                    address=entry.address,
                    imagery_year=entry.imagery_year,
                    baseline_year=entry.baseline_year,
                    segmentation_backend=entry.segmentation_backend,
                    outputs=entry.outputs,
                )
            )

        self._cache_mtime = mtime
        self._cache = new_cache
        self._cache_featured = new_featured

    def is_allowed(self, run_id: str) -> bool:
        self._load()
        return str(run_id) in self._cache

    def featured(self) -> dict[str, list[DemoRunFeatured]]:
        self._load()
        return self._cache_featured

    def get(self, run_id: str) -> DemoRunEntry | None:
        self._load()
        return self._cache.get(str(run_id))


def get_demo_registry() -> DemoRegistry:
    default_path = os.getenv("CITYLENS_DEMO_ALLOWLIST_PATH", "/app/deploy/demo_runs.json")
    return DemoRegistry(Path(default_path))

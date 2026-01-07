from __future__ import annotations

import json
<<<<<<< HEAD
import os
=======
>>>>>>> 40da1628b40164ed42e14c918f81e26d62c1320f
from dataclasses import dataclass
from pathlib import Path
from typing import Any

<<<<<<< HEAD
from fastapi import Depends

from ..models.schemas import DemoRunFeatured


@dataclass(frozen=True)
class DemoRunEntry:
    run_id: str
    category: str
=======

@dataclass(frozen=True)
class DemoRunMeta:
    run_id: str
>>>>>>> 40da1628b40164ed42e14c918f81e26d62c1320f
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str]
<<<<<<< HEAD


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
=======
    category: str


class DemoRegistry:
    def __init__(self, *, json_path: str) -> None:
        self._path = Path(json_path)
        self._mtime_ns: int | None = None
        self._runs_by_id: dict[str, DemoRunMeta] = {}
        self._featured_by_category: dict[str, list[DemoRunMeta]] = {}

    def _load(self) -> None:
        try:
            stat = self._path.stat()
        except FileNotFoundError:
            # Fail closed: no allowlisted demo runs.
            self._runs_by_id = {}
            self._featured_by_category = {}
            self._mtime_ns = None
            return

        if self._mtime_ns is not None and stat.st_mtime_ns == self._mtime_ns:
            return

        raw = json.loads(self._path.read_text(encoding="utf-8"))
        runs_raw = raw.get("runs", [])
        if not isinstance(runs_raw, list):
            runs_raw = []

        runs_by_id: dict[str, DemoRunMeta] = {}
        featured_by_category: dict[str, list[DemoRunMeta]] = {}

        for item in runs_raw:
            if not isinstance(item, dict):
                continue

            run_id = str(item.get("run_id") or "").strip()
            if not run_id:
                continue

            label = str(item.get("label") or "").strip()
            address = str(item.get("address") or "").strip()
            category = str(item.get("category") or "Featured").strip() or "Featured"

            imagery_year = int(item.get("imagery_year") or 0)
            baseline_year = int(item.get("baseline_year") or 0)
            segmentation_backend = str(item.get("segmentation_backend") or "").strip()

            outputs_raw = item.get("outputs", [])
            outputs: list[str] = []
            if isinstance(outputs_raw, list):
                outputs = [str(x) for x in outputs_raw if isinstance(x, (str, int, float))]
            elif isinstance(outputs_raw, dict):
                # Allow either list or map in file; expose as list of filenames.
                outputs = [str(k) for k in outputs_raw.keys()]

            meta = DemoRunMeta(
                run_id=run_id,
                label=label,
                address=address,
                imagery_year=imagery_year,
                baseline_year=baseline_year,
                segmentation_backend=segmentation_backend,
                outputs=outputs,
                category=category,
            )

            runs_by_id[run_id] = meta
            featured_by_category.setdefault(category, []).append(meta)

        for cat in featured_by_category:
            featured_by_category[cat].sort(key=lambda m: (m.label.lower(), m.run_id))

        self._runs_by_id = runs_by_id
        self._featured_by_category = featured_by_category
        self._mtime_ns = stat.st_mtime_ns

    def get(self, run_id: str) -> DemoRunMeta | None:
        self._load()
        return self._runs_by_id.get(run_id)

    def featured(self) -> dict[str, list[DemoRunMeta]]:
        self._load()
        # Return a shallow copy to prevent accidental mutation.
        return {k: list(v) for k, v in self._featured_by_category.items()}


def load_demo_registry_from_settings_path(path: str) -> DemoRegistry:
    return DemoRegistry(json_path=path)
>>>>>>> 40da1628b40164ed42e14c918f81e26d62c1320f

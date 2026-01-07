from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DemoRunMeta:
    run_id: str
    label: str
    address: str
    imagery_year: int
    baseline_year: int
    segmentation_backend: str
    outputs: list[str]
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

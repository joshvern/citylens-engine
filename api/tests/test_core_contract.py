from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


def test_core_imports() -> None:
    from citylens_core.models import CitylensRequest
    from citylens_core.pipeline import run_citylens

    assert CitylensRequest is not None
    assert callable(run_citylens)


def test_api_uses_citylensrequest_type() -> None:
    from citylens_core.models import CitylensRequest
    from app.routes import runs as runs_routes

    assert runs_routes.CitylensRequest is CitylensRequest

    # Round-trip: request dict must validate
    req = CitylensRequest.model_validate({"address": "1 Market St"})
    assert req.address


def test_core_standard_artifact_filenames(monkeypatch) -> None:
    import citylens_core.pipeline as pl

    # Avoid real work/network: make stages no-op.
    monkeypatch.setattr(pl, "stage_resolve", lambda req, wd, ctx, summary: ctx)
    monkeypatch.setattr(pl, "stage_fetch", lambda req, wd, ctx, summary: ctx)
    monkeypatch.setattr(pl, "stage_segment", lambda req, wd, ctx, summary: ctx)
    monkeypatch.setattr(pl, "stage_change", lambda req, wd, ctx, summary: ctx)
    monkeypatch.setattr(pl, "stage_reconstruct", lambda req, wd, ctx, summary: ctx)
    monkeypatch.setattr(pl, "stage_render", lambda req, wd, ctx, summary: ctx)

    from citylens_core.models import CitylensRequest

    with TemporaryDirectory() as d:
        work_dir = Path(d)
        req = CitylensRequest.model_validate({"address": "x"})
        out = pl.run_citylens(req, work_dir)

        expected = {"preview.png", "change.geojson", "mesh.ply", "run_summary.json"}
        names = {Path(p).name for p in out.values()}

        assert expected.issubset(names)

        for p in out.values():
            assert Path(p).exists()

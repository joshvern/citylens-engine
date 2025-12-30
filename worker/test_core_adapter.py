from __future__ import annotations


def test_core_importable() -> None:
    from services.core_adapter import CitylensRequest, run_citylens  # noqa: F401

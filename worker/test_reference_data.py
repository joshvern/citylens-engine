from __future__ import annotations

from pathlib import Path

from services.reference_data import _discover_gdb_path


def test_discover_gdb_path_finds_extracted_dataset(tmp_path: Path) -> None:
    dest_dir = tmp_path / "New York_Building_Footprints"
    dest_dir.mkdir(parents=True)
    gdb_path = dest_dir / "New_York_Building_Footprints.gdb"
    gdb_path.mkdir()

    assert _discover_gdb_path(dest_dir) == gdb_path

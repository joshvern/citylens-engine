from __future__ import annotations

import tarfile
from pathlib import Path
from unittest.mock import MagicMock

from services import reference_data
from services.reference_data import _discover_gdb_path, ensure_nyc_county_footprints


def test_discover_gdb_path_finds_extracted_dataset(tmp_path: Path) -> None:
    dest_dir = tmp_path / "New York_Building_Footprints"
    dest_dir.mkdir(parents=True)
    gdb_path = dest_dir / "New_York_Building_Footprints.gdb"
    gdb_path.mkdir()

    assert _discover_gdb_path(dest_dir) == gdb_path


class _FakeBlob:
    def __init__(self, storage: dict[str, bytes], object_name: str) -> None:
        self._storage = storage
        self._name = object_name

    def exists(self) -> bool:
        return self._name in self._storage

    def download_to_filename(self, path: str) -> None:
        Path(path).write_bytes(self._storage[self._name])

    def upload_from_filename(self, path: str) -> None:
        self._storage[self._name] = Path(path).read_bytes()


class _FakeBucket:
    def __init__(self, storage: dict[str, bytes]) -> None:
        self._storage = storage

    def blob(self, object_name: str) -> _FakeBlob:
        return _FakeBlob(self._storage, object_name)


class _FakeGcsClient:
    def __init__(self) -> None:
        self.storage: dict[str, bytes] = {}

    def bucket(self, name: str) -> _FakeBucket:  # noqa: ARG002
        return _FakeBucket(self.storage)


def _build_tar_for_gdb(tmp_path: Path, gdb_name: str) -> bytes:
    """Build an in-memory tarball that contains a fake .gdb directory."""
    staging = tmp_path / "stage"
    staging.mkdir(exist_ok=True)
    gdb = staging / gdb_name
    gdb.mkdir()
    (gdb / "a00000001.gdbtable").write_bytes(b"fake-gdb-data")

    tar_path = tmp_path / "out.tar.gz"
    with tarfile.open(str(tar_path), "w:gz") as tf:
        tf.add(str(gdb), arcname=gdb.name)
    data = tar_path.read_bytes()
    tar_path.unlink()
    return data


def test_gcs_cache_hit_restores_gdb_without_hitting_network(tmp_path: Path, monkeypatch) -> None:
    # Pre-populate GCS with a single county's tarball; only Bronx is configured.
    client = _FakeGcsClient()
    bronx_tar = _build_tar_for_gdb(tmp_path, "Bronx_Building_Footprints.gdb")
    client.storage["reference-data/nyc-footprints/Bronx.tar.gz"] = bronx_tar

    # Network download must NOT fire when the cache hits.
    def _fail_download(url: str, dest_path: Path) -> None:  # noqa: ARG001
        raise AssertionError(
            "cache hit should not trigger HTTP download, but got: " + url
        )

    monkeypatch.setattr(reference_data, "_download", _fail_download)

    result = ensure_nyc_county_footprints(
        data_dir=tmp_path / "ref",
        gcs_client=client,
        gcs_bucket="test-bucket",
        urls={"Bronx": "https://example.test/bronx.zip"},
    )

    assert "Bronx" in result
    gdb = result["Bronx"]
    assert gdb.exists()
    assert gdb.name.endswith(".gdb")
    assert (gdb / "a00000001.gdbtable").read_bytes() == b"fake-gdb-data"


def test_gcs_cache_miss_triggers_http_download_and_uploads(tmp_path: Path, monkeypatch) -> None:
    client = _FakeGcsClient()  # empty

    download_calls: list[str] = []

    def _fake_download(url: str, dest_path: Path) -> None:
        download_calls.append(url)
        # Produce a trivial ZIP that contains an empty .gdb directory entry.
        import zipfile

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(dest_path), "w") as zf:
            zf.writestr(
                "Queens_Building_Footprints.gdb/a00000001.gdbtable",
                b"real-bytes",
            )

    monkeypatch.setattr(reference_data, "_download", _fake_download)

    result = ensure_nyc_county_footprints(
        data_dir=tmp_path / "ref",
        gcs_client=client,
        gcs_bucket="test-bucket",
        urls={"Queens": "https://example.test/queens.zip"},
    )

    # HTTP download must fire on cache miss.
    assert download_calls == ["https://example.test/queens.zip"]

    # Extracted GDB is returned.
    gdb = result["Queens"]
    assert gdb.exists()
    assert gdb.name == "Queens_Building_Footprints.gdb"

    # Tarball is uploaded to GCS for the next cold-start.
    assert "reference-data/nyc-footprints/Queens.tar.gz" in client.storage


def test_already_extracted_gdb_is_reused(tmp_path: Path, monkeypatch) -> None:
    # Pre-populate local disk with an extracted .gdb — neither GCS nor HTTP
    # should be touched.
    dest_dir = tmp_path / "ref" / "Kings_Building_Footprints"
    gdb_path = dest_dir / "Kings_Building_Footprints.gdb"
    gdb_path.mkdir(parents=True)

    client = MagicMock()

    def _fail(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("must not download or hit GCS when local copy exists")

    monkeypatch.setattr(reference_data, "_download", _fail)

    result = ensure_nyc_county_footprints(
        data_dir=tmp_path / "ref",
        gcs_client=client,
        gcs_bucket="test-bucket",
        urls={"Kings": "https://example.test/kings.zip"},
    )

    assert result["Kings"] == gdb_path
    client.bucket.assert_not_called()


def test_no_gcs_client_falls_back_to_original_http_path(tmp_path: Path, monkeypatch) -> None:
    download_calls: list[str] = []

    def _fake_download(url: str, dest_path: Path) -> None:
        download_calls.append(url)
        import zipfile

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(dest_path), "w") as zf:
            zf.writestr(
                "Richmond_Building_Footprints.gdb/a00000001.gdbtable",
                b"x",
            )

    monkeypatch.setattr(reference_data, "_download", _fake_download)

    result = ensure_nyc_county_footprints(
        data_dir=tmp_path / "ref",
        urls={"Richmond": "https://example.test/richmond.zip"},
    )

    assert download_calls == ["https://example.test/richmond.zip"]
    assert result["Richmond"].exists()

from __future__ import annotations

import hashlib
from pathlib import Path

from services.gcs_artifacts import GcsArtifacts


class FakeBlob:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str | None]] = []

    def upload_from_filename(
        self,
        filename: str,
        *,
        content_type: str | None = None,
    ) -> None:
        self.uploads.append((filename, content_type))


class FakeBucket:
    def __init__(self, blob: FakeBlob) -> None:
        self._blob = blob

    def blob(self, _object_name: str) -> FakeBlob:
        return self._blob


class FakeClient:
    def __init__(self, blob: FakeBlob) -> None:
        self._blob = blob

    def bucket(self, _bucket_name: str) -> FakeBucket:
        return FakeBucket(self._blob)


def test_upload_sets_contract_media_type_and_integrity_receipt(
    tmp_path: Path,
) -> None:
    local_path = tmp_path / "change.geojson"
    payload = b'{"type":"FeatureCollection","features":[]}'
    local_path.write_bytes(payload)
    blob = FakeBlob()
    gcs = GcsArtifacts(bucket="test-bucket", client=FakeClient(blob))

    gcs_uri, size_bytes, sha256 = gcs.upload(
        local_path=local_path,
        object_name="runs/run-1/change.geojson",
    )

    assert blob.uploads == [
        (str(local_path), "application/geo+json"),
    ]
    assert gcs_uri == "gs://test-bucket/runs/run-1/change.geojson"
    assert size_bytes == len(payload)
    assert sha256 == hashlib.sha256(payload).hexdigest()

from __future__ import annotations

import hashlib
from pathlib import Path

from google.cloud import storage


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class GcsArtifacts:
    def __init__(self, *, bucket: str, client: storage.Client | None = None) -> None:
        self.client = client or storage.Client()
        self.bucket_name = bucket

    def upload(self, *, local_path: Path, object_name: str) -> tuple[str, int, str]:
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(str(local_path))

        size = int(local_path.stat().st_size)
        sha256 = sha256_file(local_path)
        gcs_uri = f"gs://{self.bucket_name}/{object_name}"
        return gcs_uri, size, sha256

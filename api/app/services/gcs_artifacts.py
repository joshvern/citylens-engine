from __future__ import annotations

from datetime import timedelta

from google.cloud import storage


class GcsArtifacts:
    def __init__(self, *, bucket: str, client: storage.Client | None = None) -> None:
        self.client = client or storage.Client()
        self.bucket_name = bucket

    def signed_url(self, *, object_name: str, ttl_seconds: int) -> str:
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(object_name)
        return blob.generate_signed_url(version="v4", expiration=timedelta(seconds=ttl_seconds), method="GET")

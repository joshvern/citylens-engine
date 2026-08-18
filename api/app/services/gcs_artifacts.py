from __future__ import annotations

import threading
from datetime import timedelta
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

import google.auth
from google.api_core import exceptions as gexc
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
from google.cloud import storage

from .retry import retry_transient

_METADATA_SA_EMAIL_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
)

# Process-level storage client. `storage.Client()` performs credential
# resolution on construction, so building one per request adds latency and
# connection churn; a single client is thread-safe and reusable.
_shared_client_lock = threading.Lock()
_shared_client: storage.Client | None = None


def _shared_storage_client() -> storage.Client:
    global _shared_client
    if _shared_client is None:
        with _shared_client_lock:
            if _shared_client is None:
                _shared_client = storage.Client()
    return _shared_client


def reset_shared_storage_client() -> None:
    """Test hook: drop the cached process-level client."""
    global _shared_client
    with _shared_client_lock:
        _shared_client = None


def _metadata_service_account_email(*, timeout_seconds: float = 2.0) -> str | None:
    try:
        req = UrlRequest(_METADATA_SA_EMAIL_URL, headers={"Metadata-Flavor": "Google"})
        with urlopen(req, timeout=timeout_seconds) as resp:
            return resp.read().decode("utf-8").strip() or None
    except Exception:
        return None


def _service_account_email_from_credentials(credentials: object) -> str | None:
    email = getattr(credentials, "service_account_email", None)
    if isinstance(email, str) and email and email != "default":
        return email
    return _metadata_service_account_email()


class GcsArtifacts:
    def __init__(self, *, bucket: str, client: storage.Client | None = None) -> None:
        self.client = client or _shared_storage_client()
        self.bucket_name = bucket

    def download_bytes(self, *, object_name: str) -> tuple[bytes, str | None]:
        def _op() -> tuple[bytes, str | None]:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)
            # Single round trip: the download response's headers populate
            # blob.content_type, so no exists()/reload() precheck is needed.
            # A missing object surfaces as NotFound, preserved as the
            # FileNotFoundError contract callers already handle.
            try:
                payload = blob.download_as_bytes()
            except gexc.NotFound as exc:
                raise FileNotFoundError(object_name) from exc
            return payload, blob.content_type

        return retry_transient(_op)

    def signed_url(self, *, object_name: str, ttl_seconds: int) -> str:
        def _op() -> str:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(object_name)

            expiration = timedelta(seconds=ttl_seconds)
            api_access_endpoint = "https://storage.googleapis.com"
            try:
                # Works when running with a JSON service-account key file.
                return blob.generate_signed_url(
                    version="v4",
                    expiration=expiration,
                    method="GET",
                    api_access_endpoint=api_access_endpoint,
                )
            except Exception as base_exc:
                # Cloud Run typically uses metadata/ADC credentials which cannot sign bytes locally.
                # Use IAMCredentials-backed signing via impersonated credentials.
                try:
                    source_credentials, _ = google.auth.default()
                    service_account_email = _service_account_email_from_credentials(
                        source_credentials
                    )
                    if not service_account_email:
                        raise base_exc

                    # Lifetime must be <= 3600; keep it reasonably close to the URL TTL.
                    lifetime = max(60, min(int(ttl_seconds), 3600))
                    signing_credentials = impersonated_credentials.Credentials(
                        source_credentials=source_credentials,
                        target_principal=service_account_email,
                        target_scopes=["https://www.googleapis.com/auth/devstorage.read_only"],
                        lifetime=lifetime,
                    )
                    signing_credentials.refresh(Request())

                    return blob.generate_signed_url(
                        version="v4",
                        expiration=expiration,
                        method="GET",
                        api_access_endpoint=api_access_endpoint,
                        credentials=signing_credentials,
                    )
                except Exception:
                    raise base_exc

        return retry_transient(_op)

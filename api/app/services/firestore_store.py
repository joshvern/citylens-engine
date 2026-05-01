from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import firestore

from .retry import retry_transient


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def identity_id_for(provider: str, subject: str) -> str:
    return hashlib.sha256(f"{provider}:{subject}".encode("utf-8")).hexdigest()


# Programmatic user API keys are prefixed so the auth dependency can
# route them to a DB lookup without trying JWKS verification first.
USER_API_KEY_PREFIX = "clk_live_"
# Number of plaintext bytes; encoded as URL-safe base64 (~43 chars after
# stripping padding). Total visible key length ~ len(prefix) + 43 = 52.
USER_API_KEY_BYTES = 32


def _hash_api_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def is_user_api_key(token: str) -> bool:
    return isinstance(token, str) and token.startswith(USER_API_KEY_PREFIX)


class MonthlyQuotaExceeded(Exception):
    def __init__(self, *, runs_used: int, monthly_run_limit: int, month_key: str) -> None:
        super().__init__(
            f"Monthly quota exceeded: {runs_used}/{monthly_run_limit} for {month_key}"
        )
        self.runs_used = runs_used
        self.monthly_run_limit = monthly_run_limit
        self.month_key = month_key


class FirestoreStore:
    def __init__(
        self,
        *,
        project_id: str,
        runs_collection: str = "runs",
        users_collection: str = "users",
        auth_identities_collection: str = "auth_identities",
        usage_months_collection: str = "usage_months",
        api_keys_index_collection: str = "api_keys_by_hash",
        client: firestore.Client | None = None,
    ) -> None:
        self.client = client or firestore.Client(project=project_id)
        self.runs_collection = runs_collection
        self.users_collection = users_collection
        self.auth_identities_collection = auth_identities_collection
        self.usage_months_collection = usage_months_collection
        self.api_keys_index_collection = api_keys_index_collection

    # ---------- Identity ----------

    def get_or_create_user_by_identity(
        self,
        *,
        provider: str,
        subject: str,
        email: Optional[str],
        email_verified: bool,
        is_admin_override: bool,
    ) -> dict[str, Any]:
        ident_id = identity_id_for(provider, subject)

        def _op() -> dict[str, Any]:
            now = utcnow()
            ident_ref = self.client.collection(self.auth_identities_collection).document(ident_id)
            ident_snap = ident_ref.get()
            if ident_snap.exists:
                ident_doc = ident_snap.to_dict() or {}
                app_user_id = str(ident_doc.get("app_user_id") or "")
            else:
                app_user_id = ""

            if not app_user_id:
                app_user_id = uuid.uuid4().hex

            user_ref = self.client.collection(self.users_collection).document(app_user_id)
            user_snap = user_ref.get()

            if not user_snap.exists:
                plan_type = "admin" if is_admin_override else "free"
                user_doc: dict[str, Any] = {
                    "user_id": app_user_id,
                    "email": email,
                    "email_verified": bool(email_verified),
                    "plan_type": plan_type,
                    "is_admin": bool(is_admin_override),
                    "created_at": now,
                    "updated_at": now,
                    "last_login_at": now,
                    "monthly_run_limit": None,
                    "max_concurrent_runs": None,
                    "auth_provider_last": provider,
                    "auth_subject_last": subject,
                }
                user_ref.set(user_doc)
            else:
                user_doc = user_snap.to_dict() or {}
                patch: dict[str, Any] = {
                    "email": email if email is not None else user_doc.get("email"),
                    "email_verified": bool(email_verified)
                    if email is not None
                    else bool(user_doc.get("email_verified")),
                    "last_login_at": now,
                    "updated_at": now,
                    "auth_provider_last": provider,
                    "auth_subject_last": subject,
                }
                if is_admin_override:
                    patch["is_admin"] = True
                    patch["plan_type"] = "admin"
                user_ref.set(patch, merge=True)
                user_doc = {**user_doc, **patch}

            ident_patch = {
                "identity_id": ident_id,
                "app_user_id": app_user_id,
                "auth_provider": provider,
                "auth_subject": subject,
                "email": email,
                "email_verified": bool(email_verified),
                "updated_at": now,
            }
            if not ident_snap.exists:
                ident_patch["created_at"] = now
            ident_ref.set(ident_patch, merge=True)

            return user_doc

        return retry_transient(_op)

    def get_user(self, app_user_id: str) -> Optional[dict[str, Any]]:
        def _op() -> Optional[dict[str, Any]]:
            snap = self.client.collection(self.users_collection).document(app_user_id).get()
            if not snap.exists:
                return None
            return snap.to_dict() or None

        return retry_transient(_op)

    def get_admin_user_for_api_key(self, api_key_hash: str) -> dict[str, Any]:
        app_user_id = f"admin_{api_key_hash[:24]}"

        def _op() -> dict[str, Any]:
            ref = self.client.collection(self.users_collection).document(app_user_id)
            snap = ref.get()
            if snap.exists:
                doc = snap.to_dict() or {}
                if not doc.get("is_admin"):
                    ref.set(
                        {"is_admin": True, "plan_type": "admin", "updated_at": utcnow()},
                        merge=True,
                    )
                    doc["is_admin"] = True
                    doc["plan_type"] = "admin"
                return doc
            now = utcnow()
            doc = {
                "user_id": app_user_id,
                "email": None,
                "email_verified": False,
                "plan_type": "admin",
                "is_admin": True,
                "created_at": now,
                "updated_at": now,
                "last_login_at": now,
                "monthly_run_limit": None,
                "max_concurrent_runs": None,
                "auth_provider_last": "admin_api_key",
                "auth_subject_last": api_key_hash,
            }
            ref.set(doc)
            return doc

        return retry_transient(_op)

    # ---------- Runs ----------

    def create_run(self, *, user_id: str, request_dict: dict[str, Any]) -> dict[str, Any]:
        run_id = uuid.uuid4().hex

        def _op() -> dict[str, Any]:
            now = utcnow()
            doc = {
                "run_id": run_id,
                "user_id": user_id,
                "status": "queued",
                "stage": "queued",
                "progress": 0,
                "request": request_dict,
                "error": None,
                "execution_id": None,
                "created_at": now,
                "updated_at": now,
            }
            self.client.collection(self.runs_collection).document(run_id).set(doc)
            return doc

        return retry_transient(_op)

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        def _op() -> Optional[dict[str, Any]]:
            snap = self.client.collection(self.runs_collection).document(run_id).get()
            if not snap.exists:
                return None
            return snap.to_dict() or None

        return retry_transient(_op)

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        def _op() -> None:
            patch_local = dict(patch)
            patch_local["updated_at"] = utcnow()
            self.client.collection(self.runs_collection).document(run_id).set(
                patch_local,
                merge=True,
            )

        retry_transient(_op)

    def refund_run_quota_if_failed(self, run_id: str) -> bool:
        """Refund the monthly run quota for a failed run, idempotently.

        Called from API read paths so failed runs naturally refund the user's
        monthly counter — pipelines that fail outside the user's control
        (LiDAR coverage, worker timeout, trigger failure) shouldn't burn quota.

        Returns True if a refund was applied, False otherwise (run not failed,
        or already refunded). Sets `quota_refunded=True` on the run doc to
        prevent double-refund.
        """

        ref = self.client.collection(self.runs_collection).document(run_id)

        @firestore.transactional  # type: ignore[misc]
        def _txn(transaction) -> bool:
            snap = ref.get(transaction=transaction)
            if not snap.exists:
                return False
            data = snap.to_dict() or {}
            if str(data.get("status") or "") != "failed":
                return False
            if data.get("quota_refunded") is True:
                return False

            user_id = str(data.get("user_id") or "")
            created_at = data.get("created_at")
            if not user_id or not isinstance(created_at, datetime):
                return False
            created_utc = created_at.astimezone(timezone.utc)
            month_key = f"{created_utc.year:04d}-{created_utc.month:02d}"

            usage_ref = self._usage_doc_ref(app_user_id=user_id, month_key=month_key)
            usage_snap = usage_ref.get(transaction=transaction)
            if usage_snap.exists:
                usage = usage_snap.to_dict() or {}
                runs_used = int(usage.get("runs_used", 0) or 0)
                new_used = max(0, runs_used - 1)
                transaction.set(
                    usage_ref,
                    {"runs_used": new_used, "updated_at": utcnow()},
                    merge=True,
                )

            transaction.set(
                ref,
                {"quota_refunded": True, "updated_at": utcnow()},
                merge=True,
            )
            return True

        def _op() -> bool:
            transaction = self.client.transaction()
            return _txn(transaction)

        return retry_transient(_op)

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        def _op() -> list[dict[str, Any]]:
            col = (
                self.client.collection(self.runs_collection)
                .document(run_id)
                .collection("artifacts")
            )
            return [snap.to_dict() or {} for snap in col.stream()]

        return retry_transient(_op)

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        self.update_run(run_id, {"execution_id": execution_id})

    def mark_failed(self, run_id: str, error: dict[str, Any] | str) -> None:
        payload = (
            error
            if isinstance(error, dict)
            else {
                "code": "UNKNOWN",
                "message": str(error),
                "stage": "failed",
                "traceback_summary": [],
            }
        )
        self.update_run(
            run_id,
            {
                "status": "failed",
                "stage": "failed",
                "error": payload,
                "progress": 100,
            },
        )

    def list_runs(
        self,
        *,
        user_id: str,
        limit: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        def _op() -> tuple[list[dict[str, Any]], str | None]:
            query = (
                self.client.collection(self.runs_collection)
                .where("user_id", "==", user_id)
                .order_by("created_at", direction=firestore.Query.DESCENDING)
                .order_by("run_id", direction=firestore.Query.DESCENDING)
            )
            if cursor:
                created_at, run_id = _decode_list_cursor(cursor)
                query = query.start_after([created_at, run_id])

            docs = list(query.limit(int(limit) + 1).stream())
            next_cursor: str | None = None
            if len(docs) > int(limit):
                next_doc = docs[int(limit) - 1].to_dict() or {}
                next_cursor = _encode_list_cursor(next_doc)
                docs = docs[: int(limit)]

            return [snap.to_dict() or {} for snap in docs], next_cursor

        return retry_transient(_op)

    # ---------- Quotas helpers ----------

    def count_user_concurrent_runs(self, *, user_id: str) -> int:
        def _op() -> int:
            q = (
                self.client.collection(self.runs_collection)
                .where("user_id", "==", user_id)
                .where("status", "in", ["queued", "running"])
            )
            return sum(1 for _ in q.stream())

        return retry_transient(_op)

    # ---------- Monthly usage (transactional) ----------

    def _usage_doc_ref(self, *, app_user_id: str, month_key: str):
        doc_id = f"{app_user_id}_{month_key}"
        return self.client.collection(self.usage_months_collection).document(doc_id)

    def get_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        def _op() -> int:
            snap = self._usage_doc_ref(app_user_id=app_user_id, month_key=month_key).get()
            if not snap.exists:
                return 0
            data = snap.to_dict() or {}
            try:
                return int(data.get("runs_used", 0))
            except (TypeError, ValueError):
                return 0

        return retry_transient(_op)

    def try_increment_monthly_usage(
        self,
        *,
        app_user_id: str,
        month_key: str,
        limit: Optional[int],
    ) -> int:
        """Increment the user's monthly counter atomically.

        Raises MonthlyQuotaExceeded if a finite limit is configured and reached.
        Race-free: a Firestore transaction guarantees that two concurrent writers
        cannot both observe runs_used < limit and increment past the cap.
        """
        ref = self._usage_doc_ref(app_user_id=app_user_id, month_key=month_key)

        @firestore.transactional  # type: ignore[misc]
        def _txn(transaction) -> int:
            snap = ref.get(transaction=transaction)
            now = utcnow()
            if snap.exists:
                data = snap.to_dict() or {}
                runs_used = int(data.get("runs_used", 0) or 0)
            else:
                runs_used = 0
            if limit is not None and runs_used >= limit:
                raise MonthlyQuotaExceeded(
                    runs_used=runs_used,
                    monthly_run_limit=int(limit),
                    month_key=month_key,
                )
            new_used = runs_used + 1
            payload: dict[str, Any] = {
                "app_user_id": app_user_id,
                "month_key": month_key,
                "runs_used": new_used,
                "updated_at": now,
            }
            if not snap.exists:
                payload["created_at"] = now
            transaction.set(ref, payload, merge=True)
            return new_used

        def _op() -> int:
            transaction = self.client.transaction()
            return _txn(transaction)

        return retry_transient(_op)

    # ---------- User API keys ----------
    #
    # Two-document layout per key:
    #   users/{app_user_id}/api_keys/{key_id}  — full record (label,
    #     created_at, last_used_at, revoked_at, key_prefix, key_hash)
    #   api_keys_by_hash/{sha256(plaintext)}   — index pointing back at
    #     {app_user_id, key_id}. Single-read auth lookup.
    # Plaintext is never stored; we hash on create and discard.

    def _user_api_keys_col(self, app_user_id: str):
        return (
            self.client.collection(self.users_collection)
            .document(app_user_id)
            .collection("api_keys")
        )

    def _api_key_index_doc(self, plaintext_hash: str):
        return self.client.collection(self.api_keys_index_collection).document(plaintext_hash)

    def create_api_key(
        self, *, app_user_id: str, label: str
    ) -> tuple[str, str, dict[str, Any]]:
        """Mint a new user API key. Returns (key_id, plaintext, record).

        `plaintext` is shown to the user once and never retrievable
        again. `record` is the metadata persisted in the user
        subcollection (without the hash) — safe to include in the API
        response.
        """
        key_id = uuid.uuid4().hex
        random_part = secrets.token_urlsafe(USER_API_KEY_BYTES).rstrip("=")
        plaintext = f"{USER_API_KEY_PREFIX}{random_part}"
        plaintext_hash = _hash_api_key(plaintext)
        # First few chars of the random part — stable per-key identifier
        # users can recognise in their dashboard without exposing the
        # secret.
        key_prefix = f"{USER_API_KEY_PREFIX}{random_part[:4]}"

        def _op() -> dict[str, Any]:
            now = utcnow()
            record = {
                "key_id": key_id,
                "label": str(label or "").strip()[:128] or "untitled",
                "key_prefix": key_prefix,
                "key_hash": plaintext_hash,
                "created_at": now,
                "last_used_at": None,
                "revoked_at": None,
            }
            user_key_ref = self._user_api_keys_col(app_user_id).document(key_id)
            user_key_ref.set(record)

            index_ref = self._api_key_index_doc(plaintext_hash)
            index_ref.set(
                {
                    "app_user_id": app_user_id,
                    "key_id": key_id,
                    "created_at": now,
                    "revoked_at": None,
                }
            )
            return record

        record = retry_transient(_op)
        return key_id, plaintext, record

    def get_user_id_for_api_key(self, plaintext: str) -> Optional[str]:
        """Resolve a plaintext API key to its owning app_user_id. Returns
        None if the key is unknown or revoked. Best-effort updates
        `last_used_at` on hit."""
        if not is_user_api_key(plaintext):
            return None
        plaintext_hash = _hash_api_key(plaintext)

        def _op() -> Optional[str]:
            index_snap = self._api_key_index_doc(plaintext_hash).get()
            if not index_snap.exists:
                return None
            index_doc = index_snap.to_dict() or {}
            if index_doc.get("revoked_at") is not None:
                return None
            app_user_id = str(index_doc.get("app_user_id") or "")
            key_id = str(index_doc.get("key_id") or "")
            if not app_user_id or not key_id:
                return None

            # Best-effort `last_used_at` update — doesn't block auth on
            # a transient write failure.
            try:
                user_key_ref = self._user_api_keys_col(app_user_id).document(key_id)
                user_key_ref.set({"last_used_at": utcnow()}, merge=True)
            except Exception:
                pass

            return app_user_id

        return retry_transient(_op)

    def list_api_keys(self, *, app_user_id: str) -> list[dict[str, Any]]:
        """Return non-revoked API keys for the user. Hashes are stripped
        before returning so the API response never includes secret
        material."""

        def _op() -> list[dict[str, Any]]:
            docs = self._user_api_keys_col(app_user_id).stream()
            out: list[dict[str, Any]] = []
            for snap in docs:
                data = snap.to_dict() or {}
                if data.get("revoked_at") is not None:
                    continue
                # Drop the hash from the response.
                data.pop("key_hash", None)
                out.append(data)
            out.sort(
                key=lambda d: d.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return out

        return retry_transient(_op)

    def revoke_api_key(self, *, app_user_id: str, key_id: str) -> bool:
        """Mark a key as revoked. Returns False if the key wasn't found
        or was already revoked."""

        def _op() -> bool:
            user_key_ref = self._user_api_keys_col(app_user_id).document(key_id)
            snap = user_key_ref.get()
            if not snap.exists:
                return False
            data = snap.to_dict() or {}
            if data.get("revoked_at") is not None:
                return False
            now = utcnow()
            user_key_ref.set({"revoked_at": now}, merge=True)
            key_hash = str(data.get("key_hash") or "")
            if key_hash:
                self._api_key_index_doc(key_hash).set({"revoked_at": now}, merge=True)
            return True

        return retry_transient(_op)

    def decrement_monthly_usage(self, *, app_user_id: str, month_key: str) -> int:
        ref = self._usage_doc_ref(app_user_id=app_user_id, month_key=month_key)

        @firestore.transactional  # type: ignore[misc]
        def _txn(transaction) -> int:
            snap = ref.get(transaction=transaction)
            if not snap.exists:
                return 0
            data = snap.to_dict() or {}
            runs_used = int(data.get("runs_used", 0) or 0)
            new_used = max(0, runs_used - 1)
            transaction.set(
                ref,
                {"runs_used": new_used, "updated_at": utcnow()},
                merge=True,
            )
            return new_used

        def _op() -> int:
            transaction = self.client.transaction()
            return _txn(transaction)

        return retry_transient(_op)


def _encode_list_cursor(doc: dict[str, Any]) -> str | None:
    created_at = doc.get("created_at")
    run_id = doc.get("run_id")
    if not isinstance(created_at, datetime) or not isinstance(run_id, str):
        return None

    import base64
    import json

    payload = {
        "created_at": created_at.isoformat(),
        "run_id": run_id,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_list_cursor(cursor: str) -> tuple[datetime, str]:
    import base64
    import json

    padded = cursor.encode("ascii")
    padded += b"=" * (-len(padded) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        created_at = datetime.fromisoformat(str(payload["created_at"]))
        run_id = str(payload["run_id"])
        return created_at, run_id
    except Exception as exc:  # pragma: no cover - validated by API route
        raise ValueError("Invalid runs cursor") from exc

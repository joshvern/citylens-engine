from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import firestore

from .retry import retry_transient


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FirestoreStore:
    def __init__(
        self,
        *,
        project_id: str,
        runs_collection: str = "runs",
        users_collection: str = "users",
        client: firestore.Client | None = None,
    ) -> None:
        self.client = client or firestore.Client(project=project_id)
        self.runs_collection = runs_collection
        self.users_collection = users_collection

    # Users
    def get_or_create_user(self, user_id: str) -> dict[str, Any]:
        def _op() -> dict[str, Any]:
            ref = self.client.collection(self.users_collection).document(user_id)
            snap = ref.get()
            if snap.exists:
                return snap.to_dict() or {}

            now = utcnow()
            doc = {
                "user_id": user_id,
                "api_key_hash": user_id,
                "created_at": now,
                "quota_per_day": 10,
                "max_concurrent_runs": 1,
            }
            ref.set(doc)
            return doc

        return retry_transient(_op)

    # Runs
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

    # Quotas helpers
    def count_user_runs_since(self, *, user_id: str, since: datetime) -> int:
        def _op() -> int:
            q = (
                self.client.collection(self.runs_collection)
                .where("user_id", "==", user_id)
                .where("created_at", ">=", since)
            )
            return sum(1 for _ in q.stream())

        return retry_transient(_op)

    def count_user_concurrent_runs(self, *, user_id: str) -> int:
        def _op() -> int:
            q = (
                self.client.collection(self.runs_collection)
                .where("user_id", "==", user_id)
                .where("status", "in", ["queued", "running"])
            )
            return sum(1 for _ in q.stream())

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

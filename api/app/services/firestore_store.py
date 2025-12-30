from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import firestore


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

    # Runs
    def create_run(self, *, user_id: str, request_dict: dict[str, Any]) -> dict[str, Any]:
        run_id = uuid.uuid4().hex
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

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        snap = self.client.collection(self.runs_collection).document(run_id).get()
        if not snap.exists:
            return None
        return snap.to_dict() or None

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        patch = dict(patch)
        patch["updated_at"] = utcnow()
        self.client.collection(self.runs_collection).document(run_id).set(patch, merge=True)

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        col = (
            self.client.collection(self.runs_collection)
            .document(run_id)
            .collection("artifacts")
        )
        return [snap.to_dict() or {} for snap in col.stream()]

    def set_execution_id(self, run_id: str, execution_id: str) -> None:
        self.update_run(run_id, {"execution_id": execution_id})

    def mark_failed(self, run_id: str, error: str) -> None:
        self.update_run(run_id, {"status": "failed", "stage": "failed", "error": error, "progress": 100})

    # Quotas helpers
    def count_user_runs_since(self, *, user_id: str, since: datetime) -> int:
        q = (
            self.client.collection(self.runs_collection)
            .where("user_id", "==", user_id)
            .where("created_at", ">=", since)
        )
        return sum(1 for _ in q.stream())

    def count_user_concurrent_runs(self, *, user_id: str) -> int:
        q = (
            self.client.collection(self.runs_collection)
            .where("user_id", "==", user_id)
            .where("status", "in", ["queued", "running"])
        )
        return sum(1 for _ in q.stream())

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Optional

from google.api_core.exceptions import Forbidden, PermissionDenied
from google.cloud import firestore


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FirestoreStore:
    def __init__(self, *, project_id: str, runs_collection: str = "runs", client: firestore.Client | None = None) -> None:
        self.client = client or firestore.Client(project=project_id)
        self.runs_collection = runs_collection

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        try:
            snap = self.client.collection(self.runs_collection).document(run_id).get()
        except (PermissionDenied, Forbidden) as e:
            # Surface permission errors clearly in logs to debug Firestore IAM issues.
            print(f"Firestore get_run permission error for run {run_id}: {e}", file=sys.stderr)
            raise
        if not snap.exists:
            return None
        return snap.to_dict() or None

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        patch = dict(patch)
        patch["updated_at"] = utcnow()
        try:
            self.client.collection(self.runs_collection).document(run_id).set(patch, merge=True)
        except (PermissionDenied, Forbidden) as e:
            print(f"Firestore update_run permission error for run {run_id}: {e}", file=sys.stderr)
            raise

    def write_artifact(self, *, run_id: str, artifact_id: str, doc: dict[str, Any]) -> None:
        ref = (
            self.client.collection(self.runs_collection)
            .document(run_id)
            .collection("artifacts")
            .document(artifact_id)
        )
        try:
            ref.set(doc)
        except (PermissionDenied, Forbidden) as e:
            print(f"Firestore write_artifact permission error for run {run_id}, artifact {artifact_id}: {e}", file=sys.stderr)
            raise

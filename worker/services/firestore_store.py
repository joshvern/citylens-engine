from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from google.api_core.exceptions import Forbidden, PermissionDenied
from google.cloud import firestore

from .retry import retry_transient

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FirestoreStore:
    def __init__(
        self,
        *,
        project_id: str,
        runs_collection: str = "runs",
        client: firestore.Client | None = None,
    ) -> None:
        self.client = client or firestore.Client(project=project_id)
        self.runs_collection = runs_collection

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        def _op() -> Optional[dict[str, Any]]:
            try:
                snap = self.client.collection(self.runs_collection).document(run_id).get()
            except (PermissionDenied, Forbidden):
                logger.exception("Firestore get_run permission error", extra={"run_id": run_id})
                raise
            if not snap.exists:
                return None
            return snap.to_dict() or None

        return retry_transient(_op)

    # Statuses a delayed execution must never overwrite. A run the stuck-run
    # reconciler already failed AND refunded would otherwise be resurrected
    # to "running" and could finish as succeeded-but-refunded (a free run).
    TERMINAL_STATUSES = ("failed", "succeeded")

    def claim_run_for_processing(
        self, run_id: str
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        """Transactionally claim a run for this worker execution.

        Mirrors the API-side transactional store patterns: the run doc is
        re-read INSIDE the transaction and the "running" claim is written
        only when the run is not already terminal. "queued" is the expected
        state; "running" is also claimed defensively — Cloud Run job retries
        are disabled, but a duplicate execution must not bypass the guard.

        Returns ``(claimed, run_doc)``: ``(True, doc)`` when this execution
        now owns the run; ``(False, doc)`` when the run is terminal (the
        caller must exit cleanly WITHOUT processing); ``(False, None)`` when
        the run does not exist.
        """

        ref = self.client.collection(self.runs_collection).document(run_id)

        @firestore.transactional  # type: ignore[misc]
        def _txn(transaction) -> tuple[bool, Optional[dict[str, Any]]]:
            snap = ref.get(transaction=transaction)
            if not snap.exists:
                return False, None
            data = snap.to_dict() or {}
            if str(data.get("status") or "") in self.TERMINAL_STATUSES:
                return False, data
            transaction.set(
                ref,
                {
                    "status": "running",
                    "stage": "starting",
                    "progress": 1,
                    "error": None,
                    "updated_at": utcnow(),
                },
                merge=True,
            )
            return True, data

        def _op() -> tuple[bool, Optional[dict[str, Any]]]:
            try:
                transaction = self.client.transaction()
                return _txn(transaction)
            except (PermissionDenied, Forbidden):
                logger.exception(
                    "Firestore claim_run permission error",
                    extra={"run_id": run_id},
                )
                raise

        return retry_transient(_op)

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        def _op() -> None:
            patch_local = dict(patch)
            patch_local["updated_at"] = utcnow()
            try:
                self.client.collection(self.runs_collection).document(run_id).set(
                    patch_local,
                    merge=True,
                )
            except (PermissionDenied, Forbidden):
                logger.exception("Firestore update_run permission error", extra={"run_id": run_id})
                raise

        retry_transient(_op)

    def write_artifact(self, *, run_id: str, artifact_id: str, doc: dict[str, Any]) -> None:
        def _op() -> None:
            ref = (
                self.client.collection(self.runs_collection)
                .document(run_id)
                .collection("artifacts")
                .document(artifact_id)
            )
            try:
                ref.set(doc)
            except (PermissionDenied, Forbidden):
                logger.exception(
                    "Firestore write_artifact permission error",
                    extra={"run_id": run_id, "stage": artifact_id},
                )
                raise

        retry_transient(_op)

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

from migration.retry_utils import RetryPolicy, run_with_retry
from migration_v2.config import FirestoreTargetConfig
from migration_v2.models import CanonicalRecord, RoutedSinkRecord
from migration_v2.utils import json_size_bytes, payload_checksum, stable_hash, to_jsonable

LOGGER = logging.getLogger("v2.firestore_sink")


class FirestoreSinkAdapter:
    def __init__(
        self,
        config: FirestoreTargetConfig,
        *,
        retry_policy: RetryPolicy,
        dry_run: bool = False,
    ) -> None:
        from google.cloud import firestore  # Lazy import for optional dependency ergonomics.

        self._client = firestore.Client(project=config.project, database=config.database)
        self._collection_name = config.collection
        self._retry_policy = retry_policy
        self._dry_run = dry_run

    def _doc_id(self, route_key: str) -> str:
        return stable_hash(route_key)

    def preflight_check(self) -> tuple[bool, str]:
        def operation() -> str:
            list(self._client.collection(self._collection_name).limit(1).stream())
            return "OK"

        try:
            run_with_retry(
                operation,
                operation_name="firestore_preflight",
                policy=self._retry_policy,
                logger=LOGGER,
            )
            return True, "Firestore reachable."
        except Exception as exc:  # noqa: BLE001
            return False, f"Firestore preflight failed: {exc}"

    def upsert(self, record: CanonicalRecord) -> None:
        if self._dry_run:
            return

        payload = {
            "routeKey": record.route_key,
            "sourceJob": record.source_job,
            "sourceApi": record.source_api,
            "sourceNamespace": record.source_namespace,
            "sourceKey": record.source_key,
            "payload": record.payload,
            "payloadSizeBytes": record.payload_size_bytes,
            "checksum": record.checksum,
            "eventTs": record.event_ts,
            "updatedAt": datetime.now(timezone.utc),
        }
        doc_ref = self._client.collection(self._collection_name).document(self._doc_id(record.route_key))
        run_with_retry(
            lambda: doc_ref.set(payload, merge=True),
            operation_name=f"firestore_upsert:{record.route_key}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def delete(self, record: CanonicalRecord) -> None:
        if self._dry_run:
            return
        doc_ref = self._client.collection(self._collection_name).document(self._doc_id(record.route_key))
        run_with_retry(
            lambda: doc_ref.delete(),
            operation_name=f"firestore_delete:{record.route_key}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def iter_records(self) -> Iterable[RoutedSinkRecord]:
        collection = self._client.collection(self._collection_name)
        for snapshot in collection.stream():
            raw = snapshot.to_dict() or {}
            payload = to_jsonable(raw.get("payload", {}))
            if not isinstance(payload, dict):
                raise ValueError(
                    f"Firestore document {snapshot.id} in {self._collection_name} has non-object payload."
                )
            computed_checksum = payload_checksum(payload)
            computed_size = json_size_bytes(payload)
            record = CanonicalRecord(
                source_job=str(raw.get("sourceJob", "")),
                source_api=str(raw.get("sourceApi", "")),
                source_namespace=str(raw.get("sourceNamespace", "")),
                source_key=str(raw.get("sourceKey", "")),
                route_key=str(raw.get("routeKey", "")),
                payload=payload,
                payload_size_bytes=computed_size,
                checksum=computed_checksum,
                event_ts=str(raw.get("eventTs", "")),
            )
            yield RoutedSinkRecord(
                destination="firestore",
                record=record,
                stored_checksum=str(raw.get("checksum", "")),
                stored_payload_size_bytes=int(raw.get("payloadSizeBytes", computed_size)),
            )

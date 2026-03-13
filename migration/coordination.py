from __future__ import annotations

import os
import socket
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from migration.control_plane_backend import (
    SpannerControlPlaneBackend,
    is_spanner_control_plane_path,
)
from migration.json_state_backend import JsonStateBackend


LOGGER = logging.getLogger("migration.coordination")


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def default_worker_id() -> str:
    hostname = socket.gethostname() or "unknown-host"
    return f"{hostname}:{os.getpid()}"


def _require_local_backend(backend: JsonStateBackend | None) -> JsonStateBackend:
    if backend is None:
        raise RuntimeError("Local JSON state backend is not configured for this store.")
    return backend


@dataclass(frozen=True)
class LeaseRecord:
    owner_id: str
    lease_expires_at: str
    heartbeat_at: str
    acquired_at: str
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkProgressRecord:
    status: str
    owner_id: str
    updated_at: str
    attempt_count: int = 0
    started_at: str = ""
    completed_at: str = ""
    last_error: str = ""
    metadata: dict[str, str] = field(default_factory=dict)


PROGRESS_STATUS_PENDING = "pending"
PROGRESS_STATUS_RUNNING = "running"
PROGRESS_STATUS_COMPLETED = "completed"
PROGRESS_STATUS_FAILED = "failed"
VALID_PROGRESS_STATUSES = {
    PROGRESS_STATUS_PENDING,
    PROGRESS_STATUS_RUNNING,
    PROGRESS_STATUS_COMPLETED,
    PROGRESS_STATUS_FAILED,
}


class LeaseStore:
    def __init__(self, lease_file: str) -> None:
        self.backend = (
            None
            if is_spanner_control_plane_path(lease_file)
            else JsonStateBackend(lease_file)
        )
        self.spanner = (
            SpannerControlPlaneBackend(lease_file)
            if is_spanner_control_plane_path(lease_file)
            else None
        )

    def _serialize_metadata(self, metadata: dict[str, Any] | None) -> dict[str, str]:
        if not metadata:
            return {}
        return {str(key): str(value) for key, value in metadata.items()}

    def _serialize_record(self, record: LeaseRecord) -> dict[str, Any]:
        return {
            "owner_id": record.owner_id,
            "lease_expires_at": record.lease_expires_at,
            "heartbeat_at": record.heartbeat_at,
            "acquired_at": record.acquired_at,
            "metadata": dict(record.metadata),
        }

    def _deserialize_record(self, raw: Any) -> LeaseRecord | None:
        if not raw:
            return None
        if not isinstance(raw, dict):
            raise ValueError("Lease records must be JSON objects.")
        metadata_raw = raw.get("metadata", {})
        metadata: dict[str, str] = {}
        if isinstance(metadata_raw, dict):
            metadata = {str(key): str(value) for key, value in metadata_raw.items()}
        return LeaseRecord(
            owner_id=str(raw.get("owner_id", "")),
            lease_expires_at=str(raw.get("lease_expires_at", "")),
            heartbeat_at=str(raw.get("heartbeat_at", "")),
            acquired_at=str(raw.get("acquired_at", "")),
            metadata=metadata,
        )

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for lease_key, record_raw in raw.items():
            record = self._deserialize_record(record_raw)
            if record is not None:
                normalized[str(lease_key)] = self._serialize_record(record)
        return normalized

    def get(self, lease_key: str) -> LeaseRecord | None:
        if self.spanner is not None:
            spanner_backend = self.spanner
            row = spanner_backend.get(lease_key)
            return self._deserialize_record(row.payload if row is not None else None)
        backend = _require_local_backend(self.backend)
        current_data = self._normalize_data(backend.read_json_object())
        return self._deserialize_record(current_data.get(lease_key))

    def list_records(self, lease_keys: list[str]) -> dict[str, LeaseRecord]:
        if self.spanner is not None:
            spanner_backend = self.spanner
            rows = spanner_backend.get_many(lease_keys)
            spanner_records: dict[str, LeaseRecord] = {}
            for lease_key, row in rows.items():
                record = self._deserialize_record(row.payload)
                if record is not None:
                    spanner_records[lease_key] = record
            return spanner_records
        backend = _require_local_backend(self.backend)
        current_data = self._normalize_data(backend.read_json_object())
        local_records: dict[str, LeaseRecord] = {}
        for lease_key in lease_keys:
            record = self._deserialize_record(current_data.get(lease_key))
            if record is not None:
                local_records[lease_key] = record
        return local_records

    def _is_expired(self, record: LeaseRecord, now: datetime) -> bool:
        if not record.lease_expires_at:
            return True
        try:
            lease_expires = datetime.fromisoformat(record.lease_expires_at)
        except ValueError:
            return True
        if lease_expires.tzinfo is None:
            lease_expires = lease_expires.replace(tzinfo=UTC)
        return lease_expires <= now

    def try_acquire(
        self,
        lease_key: str,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or utc_now()
        lease_expires = current_time + timedelta(seconds=lease_duration_seconds)
        metadata_value = self._serialize_metadata(metadata)
        result = {"acquired": False}

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, lease_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                if (
                    existing is not None
                    and existing.owner_id != owner_id
                    and not self._is_expired(existing, current_time)
                ):
                    result["acquired"] = False
                    return

                acquired_at = current_time.isoformat()
                if existing is not None and existing.owner_id == owner_id and existing.acquired_at:
                    acquired_at = existing.acquired_at

                record = LeaseRecord(
                    owner_id=owner_id,
                    lease_expires_at=lease_expires.isoformat(),
                    heartbeat_at=current_time.isoformat(),
                    acquired_at=acquired_at,
                    metadata=metadata_value,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=lease_key,
                    payload=self._serialize_record(record),
                    owner_id=owner_id,
                    lease_expires_at=lease_expires,
                )
                result["acquired"] = True

            spanner_backend.run_in_transaction(txn_fn)
            return result["acquired"]

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(lease_key))
            if (
                existing is not None
                and existing.owner_id != owner_id
                and not self._is_expired(existing, current_time)
            ):
                result["acquired"] = False
                return merged

            acquired_at = current_time.isoformat()
            if existing is not None and existing.owner_id == owner_id and existing.acquired_at:
                acquired_at = existing.acquired_at

            merged[lease_key] = self._serialize_record(
                LeaseRecord(
                    owner_id=owner_id,
                    lease_expires_at=lease_expires.isoformat(),
                    heartbeat_at=current_time.isoformat(),
                    acquired_at=acquired_at,
                    metadata=metadata_value,
                )
            )
            result["acquired"] = True
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)
        return result["acquired"]

    def renew(
        self,
        lease_key: str,
        *,
        owner_id: str,
        lease_duration_seconds: int,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or utc_now()
        lease_expires = current_time + timedelta(seconds=lease_duration_seconds)
        metadata_value = self._serialize_metadata(metadata)
        result = {"renewed": False}

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, lease_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                if existing is None or existing.owner_id != owner_id:
                    result["renewed"] = False
                    return

                record = LeaseRecord(
                    owner_id=owner_id,
                    lease_expires_at=lease_expires.isoformat(),
                    heartbeat_at=current_time.isoformat(),
                    acquired_at=existing.acquired_at or current_time.isoformat(),
                    metadata=metadata_value or existing.metadata,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=lease_key,
                    payload=self._serialize_record(record),
                    owner_id=owner_id,
                    lease_expires_at=lease_expires,
                )
                result["renewed"] = True

            spanner_backend.run_in_transaction(txn_fn)
            return result["renewed"]

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(lease_key))
            if existing is None or existing.owner_id != owner_id:
                result["renewed"] = False
                return merged

            merged[lease_key] = self._serialize_record(
                LeaseRecord(
                    owner_id=owner_id,
                    lease_expires_at=lease_expires.isoformat(),
                    heartbeat_at=current_time.isoformat(),
                    acquired_at=existing.acquired_at or current_time.isoformat(),
                    metadata=metadata_value or existing.metadata,
                )
            )
            result["renewed"] = True
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)
        return result["renewed"]

    def release(self, lease_key: str, *, owner_id: str) -> bool:
        result = {"released": False}

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, lease_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                if existing is None or existing.owner_id != owner_id:
                    result["released"] = False
                    return
                spanner_backend.delete(transaction, record_key=lease_key)
                result["released"] = True

            spanner_backend.run_in_transaction(txn_fn)
            return result["released"]

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(lease_key))
            if existing is None or existing.owner_id != owner_id:
                result["released"] = False
                return merged
            del merged[lease_key]
            result["released"] = True
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)
        return result["released"]


class WorkProgressStore:
    def __init__(self, progress_file: str) -> None:
        self.backend = (
            None
            if is_spanner_control_plane_path(progress_file)
            else JsonStateBackend(progress_file)
        )
        self.spanner = (
            SpannerControlPlaneBackend(progress_file)
            if is_spanner_control_plane_path(progress_file)
            else None
        )

    def _serialize_metadata(self, metadata: dict[str, Any] | None) -> dict[str, str]:
        if not metadata:
            return {}
        return {str(key): str(value) for key, value in metadata.items()}

    def _serialize_record(self, record: WorkProgressRecord) -> dict[str, Any]:
        return {
            "status": record.status,
            "owner_id": record.owner_id,
            "updated_at": record.updated_at,
            "attempt_count": record.attempt_count,
            "started_at": record.started_at,
            "completed_at": record.completed_at,
            "last_error": record.last_error,
            "metadata": dict(record.metadata),
        }

    def _deserialize_record(self, raw: Any) -> WorkProgressRecord | None:
        if not raw:
            return None
        if not isinstance(raw, dict):
            raise ValueError("Progress records must be JSON objects.")
        status = str(raw.get("status", "")).lower()
        if status not in VALID_PROGRESS_STATUSES:
            return None
        metadata_raw = raw.get("metadata", {})
        metadata: dict[str, str] = {}
        if isinstance(metadata_raw, dict):
            metadata = {str(key): str(value) for key, value in metadata_raw.items()}
        return WorkProgressRecord(
            status=status,
            owner_id=str(raw.get("owner_id", "")),
            updated_at=str(raw.get("updated_at", "")),
            attempt_count=int(raw.get("attempt_count", 0)),
            started_at=str(raw.get("started_at", "")),
            completed_at=str(raw.get("completed_at", "")),
            last_error=str(raw.get("last_error", "")),
            metadata=metadata,
        )

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for progress_key, record_raw in raw.items():
            record = self._deserialize_record(record_raw)
            if record is not None:
                normalized[str(progress_key)] = self._serialize_record(record)
        return normalized

    def get(self, progress_key: str) -> WorkProgressRecord | None:
        if self.spanner is not None:
            spanner_backend = self.spanner
            row = spanner_backend.get(progress_key)
            return self._deserialize_record(row.payload if row is not None else None)
        backend = _require_local_backend(self.backend)
        current_data = self._normalize_data(backend.read_json_object())
        return self._deserialize_record(current_data.get(progress_key))

    def list_records(self, progress_keys: list[str]) -> dict[str, WorkProgressRecord]:
        if self.spanner is not None:
            spanner_backend = self.spanner
            rows = spanner_backend.get_many(progress_keys)
            spanner_records: dict[str, WorkProgressRecord] = {}
            for progress_key, row in rows.items():
                record = self._deserialize_record(row.payload)
                if record is not None:
                    spanner_records[progress_key] = record
            return spanner_records
        backend = _require_local_backend(self.backend)
        current_data = self._normalize_data(backend.read_json_object())
        records: dict[str, WorkProgressRecord] = {}
        for progress_key in progress_keys:
            record = self._deserialize_record(current_data.get(progress_key))
            if record is not None:
                records[progress_key] = record
        return records

    def list_prefix(self, prefix: str) -> dict[str, WorkProgressRecord]:
        if self.spanner is not None:
            spanner_backend = self.spanner
            rows = spanner_backend.list_prefix(prefix)
            spanner_records: dict[str, WorkProgressRecord] = {}
            for progress_key, row in rows.items():
                record = self._deserialize_record(row.payload)
                if record is not None:
                    spanner_records[progress_key] = record
            return spanner_records
        backend = _require_local_backend(self.backend)
        current_data = self._normalize_data(backend.read_json_object())
        local_records: dict[str, WorkProgressRecord] = {}
        for progress_key, raw in current_data.items():
            if not str(progress_key).startswith(prefix):
                continue
            record = self._deserialize_record(raw)
            if record is not None:
                local_records[str(progress_key)] = record
        return local_records

    def ensure_pending(
        self,
        progress_key: str,
        *,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        current_time = (now or utc_now()).isoformat()
        metadata_value = self._serialize_metadata(metadata)

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, progress_key)
                if existing_row is not None:
                    return
                record = WorkProgressRecord(
                    status=PROGRESS_STATUS_PENDING,
                    owner_id="",
                    updated_at=current_time,
                    attempt_count=0,
                    started_at="",
                    completed_at="",
                    last_error="",
                    metadata=metadata_value,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=progress_key,
                    payload=self._serialize_record(record),
                    status=record.status,
                    owner_id=record.owner_id,
                )

            spanner_backend.run_in_transaction(txn_fn)
            return

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(progress_key))
            if existing is not None:
                return merged
            merged[progress_key] = self._serialize_record(
                WorkProgressRecord(
                    status=PROGRESS_STATUS_PENDING,
                    owner_id="",
                    updated_at=current_time,
                    attempt_count=0,
                    started_at="",
                    completed_at="",
                    last_error="",
                    metadata=metadata_value,
                )
            )
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)

    def mark_running(
        self,
        progress_key: str,
        *,
        owner_id: str,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        current_time = (now or utc_now()).isoformat()
        metadata_value = self._serialize_metadata(metadata)

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, progress_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                attempt_count = 1
                if existing is not None:
                    attempt_count = max(1, existing.attempt_count + 1)
                record = WorkProgressRecord(
                    status=PROGRESS_STATUS_RUNNING,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=attempt_count,
                    started_at=current_time,
                    completed_at="",
                    last_error="",
                    metadata=metadata_value,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=progress_key,
                    payload=self._serialize_record(record),
                    status=record.status,
                    owner_id=record.owner_id,
                )

            spanner_backend.run_in_transaction(txn_fn)
            return

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(progress_key))
            attempt_count = 1
            if existing is not None:
                attempt_count = max(1, existing.attempt_count + 1)
            merged[progress_key] = self._serialize_record(
                WorkProgressRecord(
                    status=PROGRESS_STATUS_RUNNING,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=attempt_count,
                    started_at=current_time,
                    completed_at="",
                    last_error="",
                    metadata=metadata_value,
                )
            )
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)

    def mark_completed(
        self,
        progress_key: str,
        *,
        owner_id: str,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        current_time = (now or utc_now()).isoformat()
        metadata_value = self._serialize_metadata(metadata)

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, progress_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                attempt_count = existing.attempt_count if existing is not None else 1
                started_at = existing.started_at if existing is not None else current_time
                record = WorkProgressRecord(
                    status=PROGRESS_STATUS_COMPLETED,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=max(1, attempt_count),
                    started_at=started_at,
                    completed_at=current_time,
                    last_error="",
                    metadata=metadata_value,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=progress_key,
                    payload=self._serialize_record(record),
                    status=record.status,
                    owner_id=record.owner_id,
                )

            spanner_backend.run_in_transaction(txn_fn)
            return

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(progress_key))
            attempt_count = existing.attempt_count if existing is not None else 1
            started_at = existing.started_at if existing is not None else current_time
            merged[progress_key] = self._serialize_record(
                WorkProgressRecord(
                    status=PROGRESS_STATUS_COMPLETED,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=max(1, attempt_count),
                    started_at=started_at,
                    completed_at=current_time,
                    last_error="",
                    metadata=metadata_value,
                )
            )
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)

    def mark_failed(
        self,
        progress_key: str,
        *,
        owner_id: str,
        error: str,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        current_time = (now or utc_now()).isoformat()
        metadata_value = self._serialize_metadata(metadata)

        if self.spanner is not None:
            spanner_backend = self.spanner

            def txn_fn(transaction: Any) -> None:
                existing_row = spanner_backend.get_with_reader(transaction, progress_key)
                existing = self._deserialize_record(
                    existing_row.payload if existing_row is not None else None
                )
                attempt_count = existing.attempt_count if existing is not None else 1
                started_at = existing.started_at if existing is not None else current_time
                record = WorkProgressRecord(
                    status=PROGRESS_STATUS_FAILED,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=max(1, attempt_count),
                    started_at=started_at,
                    completed_at="",
                    last_error=str(error),
                    metadata=metadata_value,
                )
                spanner_backend.upsert(
                    transaction,
                    record_key=progress_key,
                    payload=self._serialize_record(record),
                    status=record.status,
                    owner_id=record.owner_id,
                )

            spanner_backend.run_in_transaction(txn_fn)
            return

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            merged = self._normalize_data(current_data)
            existing = self._deserialize_record(merged.get(progress_key))
            attempt_count = existing.attempt_count if existing is not None else 1
            started_at = existing.started_at if existing is not None else current_time
            merged[progress_key] = self._serialize_record(
                WorkProgressRecord(
                    status=PROGRESS_STATUS_FAILED,
                    owner_id=owner_id,
                    updated_at=current_time,
                    attempt_count=max(1, attempt_count),
                    started_at=started_at,
                    completed_at="",
                    last_error=str(error),
                    metadata=metadata_value,
                )
            )
            return merged

        backend = _require_local_backend(self.backend)
        backend.merge_write_json_object(merge_fn, ensure_ascii=False)


class WorkCoordinator:
    def __init__(
        self,
        *,
        lease_file: str,
        progress_file: str = "",
        run_id: str = "",
        worker_id: str = "",
        lease_duration_seconds: int = 120,
        heartbeat_interval_seconds: int = 30,
    ) -> None:
        self.enabled = bool(lease_file)
        self.progress_enabled = bool(progress_file)
        self.run_id = str(run_id)
        self.worker_id = worker_id or default_worker_id()
        self.lease_duration_seconds = int(lease_duration_seconds)
        self.heartbeat_interval_seconds = int(heartbeat_interval_seconds)
        self._last_heartbeat: dict[str, datetime] = {}
        self._store = LeaseStore(lease_file) if self.enabled else None
        self._progress_store = WorkProgressStore(progress_file) if self.progress_enabled else None

    def _progress_key(self, work_key: str) -> str:
        if not self.run_id:
            return work_key
        return f"{self.run_id}:{work_key}"

    def seed_manifest(self, work_items: dict[str, dict[str, Any]]) -> None:
        if not self.progress_enabled or self._progress_store is None:
            return
        for work_key, metadata in work_items.items():
            self._progress_store.ensure_pending(
                self._progress_key(work_key),
                metadata=metadata,
            )

    def acquire(self, work_key: str, metadata: dict[str, Any] | None = None) -> bool:
        if not self.enabled or self._store is None:
            return True
        acquired = self._store.try_acquire(
            work_key,
            owner_id=self.worker_id,
            lease_duration_seconds=self.lease_duration_seconds,
            metadata=metadata,
        )
        if acquired:
            self._last_heartbeat[work_key] = utc_now()
        return acquired

    def renew_if_due(
        self,
        work_key: str,
        metadata: dict[str, Any] | None = None,
        *,
        force: bool = False,
    ) -> bool:
        if not self.enabled or self._store is None:
            return True

        current_time = utc_now()
        last_heartbeat = self._last_heartbeat.get(work_key)
        if (
            not force
            and last_heartbeat is not None
            and (current_time - last_heartbeat).total_seconds() < self.heartbeat_interval_seconds
        ):
            return True

        renewed = self._store.renew(
            work_key,
            owner_id=self.worker_id,
            lease_duration_seconds=self.lease_duration_seconds,
            metadata=metadata,
            now=current_time,
        )
        if renewed:
            self._last_heartbeat[work_key] = current_time
        return renewed

    def release(self, work_key: str) -> bool:
        if not self.enabled or self._store is None:
            return True
        self._last_heartbeat.pop(work_key, None)
        return self._store.release(work_key, owner_id=self.worker_id)

    def progress(self, work_key: str) -> WorkProgressRecord | None:
        if not self.progress_enabled or self._progress_store is None:
            return None
        return self._progress_store.get(self._progress_key(work_key))

    def progress_map(self, work_keys: list[str]) -> dict[str, WorkProgressRecord]:
        if not self.progress_enabled or self._progress_store is None:
            return {}
        progress_keys = [self._progress_key(work_key) for work_key in work_keys]
        raw_records = self._progress_store.list_records(progress_keys)
        records: dict[str, WorkProgressRecord] = {}
        for work_key in work_keys:
            progress_key = self._progress_key(work_key)
            if progress_key in raw_records:
                records[work_key] = raw_records[progress_key]
        return records

    def is_completed(self, work_key: str) -> bool:
        record = self.progress(work_key)
        return record is not None and record.status == PROGRESS_STATUS_COMPLETED

    def completed_count(self, work_keys: list[str]) -> int:
        return sum(
            1
            for record in self.progress_map(work_keys).values()
            if record.status == PROGRESS_STATUS_COMPLETED
        )

    def list_progress_records(self) -> dict[str, WorkProgressRecord]:
        if not self.progress_enabled or self._progress_store is None:
            return {}
        prefix = f"{self.run_id}:" if self.run_id else ""
        raw_records = self._progress_store.list_prefix(prefix)
        if not prefix:
            return raw_records
        records: dict[str, WorkProgressRecord] = {}
        for progress_key, record in raw_records.items():
            if progress_key.startswith(prefix):
                records[progress_key[len(prefix) :]] = record
        return records

    def stale_running_keys(
        self,
        work_items: dict[str, dict[str, Any]],
        *,
        now: datetime | None = None,
    ) -> list[str]:
        if (
            not self.enabled
            or not self.progress_enabled
            or self._store is None
            or self._progress_store is None
        ):
            return []
        current_time = now or utc_now()
        progress_map = self.progress_map(list(work_items))
        lease_map = self._store.list_records(list(work_items))
        stale_keys: list[str] = []
        for work_key, progress in progress_map.items():
            if progress.status != PROGRESS_STATUS_RUNNING:
                continue
            lease = lease_map.get(work_key)
            if lease is not None and not self._store._is_expired(lease, current_time):
                continue
            stale_keys.append(work_key)
        return stale_keys

    def reclaim_stale_running(
        self,
        work_items: dict[str, dict[str, Any]],
        *,
        now: datetime | None = None,
    ) -> list[str]:
        if (
            not self.enabled
            or not self.progress_enabled
            or self._store is None
            or self._progress_store is None
        ):
            return []
        current_time = now or utc_now()
        progress_map = self.progress_map(list(work_items))
        reclaimed: list[str] = []
        for work_key in self.stale_running_keys(work_items, now=current_time):
            progress = progress_map[work_key]
            self.mark_failed(
                work_key,
                error=(
                    "stale lease recovered "
                    f"previous_owner={progress.owner_id or 'unknown'} "
                    f"updated_at={progress.updated_at or 'unknown'}"
                ),
                metadata=work_items.get(work_key) or dict(progress.metadata),
            )
            reclaimed.append(work_key)
        return reclaimed

    def claim_next(
        self,
        work_items: dict[str, dict[str, Any]],
    ) -> tuple[str, dict[str, Any]] | None:
        if not work_items:
            return None
        self.seed_manifest(work_items)
        reclaimed = self.reclaim_stale_running(work_items)
        if reclaimed:
            LOGGER.info(
                "Recovered %s stale shard(s) for run_id=%s before claiming new work.",
                len(reclaimed),
                self.run_id or "<none>",
            )
        if self._supports_transactional_claim_next():
            claimed = self._claim_next_transactional(work_items)
            if claimed is not None:
                self._last_heartbeat[claimed[0]] = utc_now()
            return claimed
        work_keys = list(work_items)
        progress_map = self.progress_map(work_keys)
        ordered_keys = self._ordered_work_keys(work_keys, progress_map)
        for work_key in ordered_keys:
            progress = progress_map.get(work_key)
            if progress is not None and progress.status == PROGRESS_STATUS_COMPLETED:
                continue
            metadata = work_items[work_key]
            acquired = self.acquire(work_key, metadata=metadata)
            if not acquired:
                continue
            self.mark_running(work_key, metadata)
            return work_key, metadata
        return None

    def _supports_transactional_claim_next(self) -> bool:
        return bool(
            self.enabled
            and self.progress_enabled
            and self._store is not None
            and self._progress_store is not None
            and self._store.spanner is not None
            and self._progress_store.spanner is not None
            and self._store.spanner.ref.same_table(self._progress_store.spanner.ref)
        )

    def _claim_next_transactional(
        self,
        work_items: dict[str, dict[str, Any]],
    ) -> tuple[str, dict[str, Any]] | None:
        if self._store is None or self._progress_store is None:
            return None
        if self._store.spanner is None or self._progress_store.spanner is None:
            return None
        lease_store = self._store
        progress_store = self._progress_store
        lease_backend = lease_store.spanner
        progress_backend = progress_store.spanner
        if lease_backend is None or progress_backend is None:
            raise RuntimeError("Transactional claim_next requires Spanner-backed control-plane stores.")

        current_time = utc_now()
        lease_expires = current_time + timedelta(seconds=self.lease_duration_seconds)
        metadata_by_progress_key = {
            self._progress_key(work_key): metadata
            for work_key, metadata in work_items.items()
        }
        progress_keys = list(metadata_by_progress_key)
        progress_to_work = {
            self._progress_key(work_key): work_key
            for work_key in work_items
        }
        result: dict[str, tuple[str, dict[str, Any]] | None] = {"claimed": None}

        def txn_fn(transaction: Any) -> None:
            progress_rows = progress_backend._read_records_with_reader(
                transaction,
                progress_keys,
            )
            progress_map: dict[str, WorkProgressRecord] = {}
            for progress_key in progress_keys:
                row = progress_rows.get(progress_key)
                record = progress_store._deserialize_record(
                    row.payload if row is not None else None
                )
                if record is not None:
                    progress_map[progress_to_work[progress_key]] = record

            ordered_keys = self._ordered_work_keys(list(work_items), progress_map)
            for work_key in ordered_keys:
                progress_key = self._progress_key(work_key)
                progress = progress_map.get(work_key)
                if progress is not None and progress.status == PROGRESS_STATUS_COMPLETED:
                    continue

                lease_row = lease_backend.get_with_reader(transaction, work_key)
                lease_record = lease_store._deserialize_record(
                    lease_row.payload if lease_row is not None else None
                )
                if (
                    lease_record is not None
                    and lease_record.owner_id != self.worker_id
                    and not lease_store._is_expired(lease_record, current_time)
                ):
                    continue

                metadata = work_items[work_key]
                acquired_at = current_time.isoformat()
                if (
                    lease_record is not None
                    and lease_record.owner_id == self.worker_id
                    and lease_record.acquired_at
                ):
                    acquired_at = lease_record.acquired_at
                lease_payload = lease_store._serialize_record(
                    LeaseRecord(
                        owner_id=self.worker_id,
                        lease_expires_at=lease_expires.isoformat(),
                        heartbeat_at=current_time.isoformat(),
                        acquired_at=acquired_at,
                        metadata=lease_store._serialize_metadata(metadata),
                    )
                )
                lease_backend.upsert(
                    transaction,
                    record_key=work_key,
                    payload=lease_payload,
                    owner_id=self.worker_id,
                    lease_expires_at=lease_expires,
                )

                attempt_count = 1
                if progress is not None:
                    attempt_count = max(1, progress.attempt_count + 1)
                progress_record = WorkProgressRecord(
                    status=PROGRESS_STATUS_RUNNING,
                    owner_id=self.worker_id,
                    updated_at=current_time.isoformat(),
                    attempt_count=attempt_count,
                    started_at=current_time.isoformat(),
                    completed_at="",
                    last_error="",
                    metadata=progress_store._serialize_metadata(metadata),
                )
                progress_backend.upsert(
                    transaction,
                    record_key=progress_key,
                    payload=progress_store._serialize_record(progress_record),
                    status=progress_record.status,
                    owner_id=progress_record.owner_id,
                )
                result["claimed"] = (work_key, metadata)
                return

        lease_backend.run_in_transaction(txn_fn)
        return result["claimed"]

    def _ordered_work_keys(
        self,
        work_keys: list[str],
        progress_map: dict[str, WorkProgressRecord],
    ) -> list[str]:
        status_rank = {
            PROGRESS_STATUS_FAILED: 0,
            PROGRESS_STATUS_PENDING: 1,
            PROGRESS_STATUS_RUNNING: 2,
            PROGRESS_STATUS_COMPLETED: 3,
        }
        return sorted(
            work_keys,
            key=lambda work_key: (
                status_rank.get(
                    progress_map.get(work_key, WorkProgressRecord(
                        status=PROGRESS_STATUS_PENDING,
                        owner_id="",
                        updated_at="",
                    )).status,
                    99,
                ),
                self._worker_work_rank(work_key),
                work_key,
            ),
        )

    def _worker_work_rank(self, work_key: str) -> int:
        return sum(ord(char) for char in f"{self.worker_id}|{work_key}") % 1024

    def mark_running(self, work_key: str, metadata: dict[str, Any] | None = None) -> None:
        if not self.progress_enabled or self._progress_store is None:
            return
        self._progress_store.mark_running(
            self._progress_key(work_key),
            owner_id=self.worker_id,
            metadata=metadata,
        )

    def mark_completed(self, work_key: str, metadata: dict[str, Any] | None = None) -> None:
        if not self.progress_enabled or self._progress_store is None:
            return
        self._progress_store.mark_completed(
            self._progress_key(work_key),
            owner_id=self.worker_id,
            metadata=metadata,
        )

    def mark_failed(
        self,
        work_key: str,
        *,
        error: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not self.progress_enabled or self._progress_store is None:
            return
        self._progress_store.mark_failed(
            self._progress_key(work_key),
            owner_id=self.worker_id,
            error=error,
            metadata=metadata,
        )

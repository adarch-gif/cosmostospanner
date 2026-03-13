from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from migration.control_plane_backend import (
    SpannerControlPlaneBackend,
    is_spanner_control_plane_path,
)
from migration.json_state_backend import JsonStateBackend


def _require_local_backend(backend: JsonStateBackend | None) -> JsonStateBackend:
    if backend is None:
        raise RuntimeError("Local JSON state backend is not configured for this store.")
    return backend


class JsonStateStore:
    def __init__(self, file_path: str) -> None:
        self.backend = (
            None
            if is_spanner_control_plane_path(file_path)
            else JsonStateBackend(file_path)
        )
        self.spanner = (
            SpannerControlPlaneBackend(file_path)
            if is_spanner_control_plane_path(file_path)
            else None
        )
        self.data: dict[str, Any] = {}
        self._dirty_keys: set[str] = set()
        self._load()

    def _read_file_data(self) -> dict[str, Any]:
        backend = _require_local_backend(self.backend)
        return backend.read_json_object()

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        return dict(raw)

    def _merge_pending_data(
        self,
        current_data: dict[str, Any],
        candidate_data: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(current_data)
        merged.update(candidate_data)
        return merged

    def _load(self) -> None:
        if self.spanner is not None:
            self.data = {}
        else:
            self.data = self._normalize_data(self._read_file_data())
        self._dirty_keys = set()

    def _load_spanner_key(self, key: str) -> None:
        if self.spanner is None or key in self.data:
            return
        row = self.spanner.get(key)
        if row is not None and isinstance(row.payload, dict):
            self.data[key] = self._normalize_data({key: row.payload}).get(key)

    def _list_spanner_data(self) -> dict[str, Any]:
        if self.spanner is None:
            return {}
        rows = self.spanner.list_namespace()
        return self._normalize_data(
            {
                key: row.payload
                for key, row in rows.items()
                if isinstance(row.payload, dict)
            }
        )

    def flush(self) -> None:
        if self.spanner is not None:
            dirty_keys = sorted(self._dirty_keys)
            if not dirty_keys:
                return
            spanner_backend = self.spanner
            persisted: dict[str, Any] = {}

            def txn_fn(transaction: Any) -> None:
                current_rows = spanner_backend._read_records_with_reader(transaction, dirty_keys)
                current_data = self._normalize_data(
                    {
                        key: row.payload
                        for key, row in current_rows.items()
                        if isinstance(row.payload, dict)
                    }
                )
                candidate_data = self._normalize_data(
                    {
                        key: self.data[key]
                        for key in dirty_keys
                        if key in self.data
                    }
                )
                merged = self._merge_pending_data(current_data, candidate_data)
                for key, value in merged.items():
                    spanner_backend.upsert(
                        transaction,
                        record_key=key,
                        payload=value,
                    )
                persisted.update(merged)

            spanner_backend.run_in_transaction(txn_fn)
            self.data.update(persisted)
            self._dirty_keys = set()
            return

        backend = _require_local_backend(self.backend)

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            return self._merge_pending_data(
                self._normalize_data(current_data),
                self.data,
            )

        self.data = backend.merge_write_json_object(merge_fn, ensure_ascii=False)
        self._dirty_keys = set()


@dataclass
class WatermarkCheckpoint:
    watermark: Any = None
    route_keys: list[str] = field(default_factory=list)
    updated_at: str = ""


def compare_watermark_values(left: Any, right: Any) -> int:
    if left is None and right is None:
        return 0
    if left is None:
        return -1
    if right is None:
        return 1
    try:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    except TypeError:
        left_text = str(left)
        right_text = str(right)
        if left_text < right_text:
            return -1
        if left_text > right_text:
            return 1
        return 0


class WatermarkStore(JsonStateStore):
    def _deserialize_checkpoint(self, raw: Any) -> WatermarkCheckpoint:
        if raw is None:
            return WatermarkCheckpoint()
        if isinstance(raw, dict):
            route_keys_raw = raw.get("route_keys", [])
            route_keys = []
            if isinstance(route_keys_raw, list):
                route_keys = sorted({str(item) for item in route_keys_raw})
            return WatermarkCheckpoint(
                watermark=raw.get("watermark"),
                route_keys=route_keys,
                updated_at=str(raw.get("updated_at", "")),
            )
        return WatermarkCheckpoint(watermark=raw)

    def _serialize_checkpoint(self, checkpoint: WatermarkCheckpoint) -> dict[str, Any]:
        return {
            "watermark": checkpoint.watermark,
            "route_keys": sorted({str(route_key) for route_key in checkpoint.route_keys}),
            "updated_at": checkpoint.updated_at,
        }

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for job_name, checkpoint_raw in raw.items():
            normalized[str(job_name)] = self._serialize_checkpoint(
                self._deserialize_checkpoint(checkpoint_raw)
            )
        return normalized

    def _merge_pending_data(
        self,
        current_data: dict[str, Any],
        candidate_data: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(current_data)
        for job_name in set(current_data) | set(candidate_data):
            current_checkpoint = self._deserialize_checkpoint(current_data.get(job_name))
            candidate_checkpoint = self._deserialize_checkpoint(candidate_data.get(job_name))
            comparison = compare_watermark_values(
                candidate_checkpoint.watermark,
                current_checkpoint.watermark,
            )
            if comparison > 0:
                merged[job_name] = self._serialize_checkpoint(candidate_checkpoint)
                continue
            if comparison < 0:
                merged[job_name] = self._serialize_checkpoint(current_checkpoint)
                continue
            merged[job_name] = self._serialize_checkpoint(
                WatermarkCheckpoint(
                    watermark=current_checkpoint.watermark,
                    route_keys=sorted(
                        set(current_checkpoint.route_keys) | set(candidate_checkpoint.route_keys)
                    ),
                    updated_at=max(
                        current_checkpoint.updated_at,
                        candidate_checkpoint.updated_at,
                    ),
                )
            )
        return merged

    def get(self, job_name: str, default: Any = None) -> Any:
        checkpoint = self.get_checkpoint(job_name)
        if checkpoint.watermark is None:
            return default
        return checkpoint.watermark

    def set(self, job_name: str, value: Any) -> None:
        self.set_checkpoint(job_name, WatermarkCheckpoint(watermark=value))

    def get_checkpoint(self, job_name: str) -> WatermarkCheckpoint:
        self._load_spanner_key(job_name)
        return self._deserialize_checkpoint(self.data.get(job_name))

    def set_checkpoint(self, job_name: str, checkpoint: WatermarkCheckpoint) -> None:
        self.data[job_name] = self._serialize_checkpoint(checkpoint)
        self._dirty_keys.add(job_name)


SYNC_STATE_COMPLETE = "complete"
SYNC_STATE_PENDING_CLEANUP = "pending_cleanup"
VALID_SYNC_STATES = {SYNC_STATE_COMPLETE, SYNC_STATE_PENDING_CLEANUP}


@dataclass
class RouteRegistryEntry:
    destination: str
    checksum: str
    payload_size_bytes: int
    updated_at: str
    sync_state: str = SYNC_STATE_COMPLETE
    cleanup_from_destination: str | None = None


class RouteRegistryStore(JsonStateStore):
    def _serialize_entry(self, entry: RouteRegistryEntry) -> dict[str, Any]:
        return {
            "destination": entry.destination,
            "checksum": entry.checksum,
            "payload_size_bytes": entry.payload_size_bytes,
            "updated_at": entry.updated_at,
            "sync_state": entry.sync_state,
            "cleanup_from_destination": entry.cleanup_from_destination,
        }

    def _deserialize_entry(self, raw: Any) -> RouteRegistryEntry | None:
        if not raw:
            return None
        if not isinstance(raw, dict):
            raise ValueError("Route registry entries must be JSON objects.")
        sync_state = str(raw.get("sync_state", SYNC_STATE_COMPLETE))
        if sync_state not in VALID_SYNC_STATES:
            sync_state = SYNC_STATE_COMPLETE
        cleanup_from_destination = raw.get("cleanup_from_destination")
        if cleanup_from_destination is not None:
            cleanup_from_destination = str(cleanup_from_destination)
        return RouteRegistryEntry(
            destination=str(raw.get("destination")),
            checksum=str(raw.get("checksum", "")),
            payload_size_bytes=int(raw.get("payload_size_bytes", 0)),
            updated_at=str(raw.get("updated_at", "")),
            sync_state=sync_state,
            cleanup_from_destination=cleanup_from_destination,
        )

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for route_key, entry_raw in raw.items():
            entry = self._deserialize_entry(entry_raw)
            if entry is not None:
                normalized[str(route_key)] = self._serialize_entry(entry)
        return normalized

    def _merge_pending_data(
        self,
        current_data: dict[str, Any],
        candidate_data: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(current_data)
        for route_key in set(current_data) | set(candidate_data):
            current_entry = self._deserialize_entry(current_data.get(route_key))
            candidate_entry = self._deserialize_entry(candidate_data.get(route_key))
            if candidate_entry is None:
                continue
            if current_entry is None or candidate_entry.updated_at >= current_entry.updated_at:
                merged[route_key] = self._serialize_entry(candidate_entry)
            else:
                merged[route_key] = self._serialize_entry(current_entry)
        return merged

    def get_entry(self, route_key: str) -> RouteRegistryEntry | None:
        self._load_spanner_key(route_key)
        return self._deserialize_entry(self.data.get(route_key))

    def set_entry(self, route_key: str, entry: RouteRegistryEntry) -> None:
        if entry.sync_state not in VALID_SYNC_STATES:
            raise ValueError(f"Invalid registry sync_state: {entry.sync_state}")
        self.data[route_key] = self._serialize_entry(entry)
        self._dirty_keys.add(route_key)

    def items(self) -> list[tuple[str, RouteRegistryEntry]]:
        if self.spanner is not None:
            remote_data = self._list_spanner_data()
            for dirty_key in self._dirty_keys:
                if dirty_key in self.data:
                    remote_data[dirty_key] = self.data[dirty_key]
            spanner_items: list[tuple[str, RouteRegistryEntry]] = []
            for route_key in sorted(remote_data):
                entry = self._deserialize_entry(remote_data.get(route_key))
                if entry is not None:
                    spanner_items.append((route_key, entry))
            return spanner_items

        local_items: list[tuple[str, RouteRegistryEntry]] = []
        for route_key in sorted(self.data):
            entry = self._deserialize_entry(self.data.get(route_key))
            if entry is not None:
                local_items.append((route_key, entry))
        return local_items

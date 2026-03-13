from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from migration.json_state_backend import JsonStateBackend


class JsonStateStore:
    def __init__(self, file_path: str) -> None:
        self.backend = JsonStateBackend(file_path)
        self.data: dict[str, Any] = {}
        self._load()

    def _read_file_data(self) -> dict[str, Any]:
        return self.backend.read_json_object()

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        return dict(raw)

    def _merge_data(self, current_data: dict[str, Any]) -> dict[str, Any]:
        merged = dict(current_data)
        merged.update(self.data)
        return merged

    def _load(self) -> None:
        self.data = self._normalize_data(self._read_file_data())

    def flush(self) -> None:
        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            return self._merge_data(self._normalize_data(current_data))

        self.data = self.backend.merge_write_json_object(merge_fn, ensure_ascii=False)


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

    def _merge_data(self, current_data: dict[str, Any]) -> dict[str, Any]:
        merged = dict(current_data)
        for job_name in set(current_data) | set(self.data):
            current_checkpoint = self._deserialize_checkpoint(current_data.get(job_name))
            candidate_checkpoint = self._deserialize_checkpoint(self.data.get(job_name))
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
        return self._deserialize_checkpoint(self.data.get(job_name))

    def set_checkpoint(self, job_name: str, checkpoint: WatermarkCheckpoint) -> None:
        self.data[job_name] = self._serialize_checkpoint(checkpoint)


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

    def _merge_data(self, current_data: dict[str, Any]) -> dict[str, Any]:
        merged = dict(current_data)
        for route_key in set(current_data) | set(self.data):
            current_entry = self._deserialize_entry(current_data.get(route_key))
            candidate_entry = self._deserialize_entry(self.data.get(route_key))
            if candidate_entry is None:
                continue
            if current_entry is None or candidate_entry.updated_at >= current_entry.updated_at:
                merged[route_key] = self._serialize_entry(candidate_entry)
            else:
                merged[route_key] = self._serialize_entry(current_entry)
        return merged

    def get_entry(self, route_key: str) -> RouteRegistryEntry | None:
        return self._deserialize_entry(self.data.get(route_key))

    def set_entry(self, route_key: str, entry: RouteRegistryEntry) -> None:
        if entry.sync_state not in VALID_SYNC_STATES:
            raise ValueError(f"Invalid registry sync_state: {entry.sync_state}")
        self.data[route_key] = self._serialize_entry(entry)

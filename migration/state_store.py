from __future__ import annotations

from typing import Any

from migration.control_plane_backend import (
    SpannerControlPlaneBackend,
    is_spanner_control_plane_path,
)
from migration.json_state_backend import JsonStateBackend


def _require_local_backend(backend: JsonStateBackend | None) -> JsonStateBackend:
    if backend is None:
        raise RuntimeError("Local watermark state backend is not configured.")
    return backend


class WatermarkStore:
    def __init__(self, state_file: str) -> None:
        self.backend = (
            None
            if is_spanner_control_plane_path(state_file)
            else JsonStateBackend(state_file)
        )
        self.spanner = (
            SpannerControlPlaneBackend(state_file)
            if is_spanner_control_plane_path(state_file)
            else None
        )
        self.data: dict[str, int] = {}
        self._dirty_keys: set[str] = set()
        self._load()

    def _read_file_data(self) -> dict[str, Any]:
        backend = _require_local_backend(self.backend)
        try:
            return backend.read_json_object()
        except ValueError as exc:
            raise ValueError(
                str(exc).replace("State file", "Watermark state file")
            ) from exc

    def _deserialize_value(self, raw: Any) -> int | None:
        if raw is None:
            return None
        if isinstance(raw, dict):
            if "watermark" not in raw:
                return None
            return int(raw.get("watermark", 0))
        return int(raw)

    def _serialize_value(self, value: int) -> dict[str, int]:
        return {"watermark": int(value)}

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, value in raw.items():
            parsed = self._deserialize_value(value)
            if parsed is not None:
                normalized[str(key)] = parsed
        return normalized

    def _merge_data(
        self,
        current_data: dict[str, int],
        candidate_data: dict[str, int],
    ) -> dict[str, int]:
        merged = dict(current_data)
        for container_name, value in candidate_data.items():
            merged[container_name] = max(int(merged.get(container_name, value)), int(value))
        return merged

    def _load(self) -> None:
        if self.spanner is not None:
            self.data = {}
        else:
            self.data = self._normalize_data(self._read_file_data())
        self._dirty_keys = set()

    def _load_spanner_key(self, container_name: str) -> None:
        if self.spanner is None or container_name in self.data:
            return
        row = self.spanner.get(container_name)
        if row is None:
            return
        parsed = self._deserialize_value(row.payload)
        if parsed is not None:
            self.data[container_name] = parsed

    def get(self, container_name: str, default: int = 0) -> int:
        self._load_spanner_key(container_name)
        return int(self.data.get(container_name, default))

    def contains(self, container_name: str) -> bool:
        self._load_spanner_key(container_name)
        return container_name in self.data

    def set(self, container_name: str, value: int) -> None:
        self.data[container_name] = int(value)
        self._dirty_keys.add(container_name)

    def flush(self) -> None:
        if self.spanner is not None:
            dirty_keys = sorted(self._dirty_keys)
            if not dirty_keys:
                return
            spanner_backend = self.spanner
            persisted: dict[str, int] = {}

            def txn_fn(transaction: Any) -> None:
                current_rows = spanner_backend._read_records_with_reader(transaction, dirty_keys)
                current_data = self._normalize_data(
                    {
                        key: row.payload
                        for key, row in current_rows.items()
                        if isinstance(row.payload, dict)
                    }
                )
                candidate_data = {
                    key: int(self.data[key])
                    for key in dirty_keys
                    if key in self.data
                }
                merged = self._merge_data(current_data, candidate_data)
                for key, value in merged.items():
                    spanner_backend.upsert(
                        transaction,
                        record_key=key,
                        payload=self._serialize_value(value),
                    )
                persisted.update(merged)

            spanner_backend.run_in_transaction(txn_fn)
            self.data.update(persisted)
            self._dirty_keys = set()
            return

        backend = _require_local_backend(self.backend)

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            return {
                key: self._serialize_value(value)
                for key, value in self._merge_data(
                    self._normalize_data(current_data),
                    self.data,
                ).items()
            }

        merged = backend.merge_write_json_object(merge_fn)
        self.data = self._normalize_data(merged)
        self._dirty_keys = set()

from __future__ import annotations

from typing import Any

from migration.json_state_backend import JsonStateBackend


class WatermarkStore:
    def __init__(self, state_file: str) -> None:
        self.backend = JsonStateBackend(state_file)
        self.data: dict[str, int] = {}
        self._load()

    def _read_file_data(self) -> dict[str, Any]:
        try:
            return self.backend.read_json_object()
        except ValueError as exc:
            raise ValueError(
                str(exc).replace("State file", "Watermark state file")
            ) from exc

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, int]:
        return {str(key): int(value) for key, value in raw.items()}

    def _merge_data(self, current_data: dict[str, int]) -> dict[str, int]:
        merged = dict(current_data)
        for container_name, value in self.data.items():
            merged[container_name] = max(int(merged.get(container_name, value)), int(value))
        return merged

    def _load(self) -> None:
        self.data = self._normalize_data(self._read_file_data())

    def get(self, container_name: str, default: int = 0) -> int:
        return int(self.data.get(container_name, default))

    def set(self, container_name: str, value: int) -> None:
        self.data[container_name] = int(value)

    def flush(self) -> None:
        def merge_fn(current_data: dict[str, Any]) -> dict[str, int]:
            return self._merge_data(self._normalize_data(current_data))

        self.data = self.backend.merge_write_json_object(merge_fn)

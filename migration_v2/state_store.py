from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class JsonStateStore:
    def __init__(self, file_path: str) -> None:
        self.path = Path(file_path)
        self.data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)


class WatermarkStore(JsonStateStore):
    def get(self, job_name: str, default: Any = None) -> Any:
        return self.data.get(job_name, default)

    def set(self, job_name: str, value: Any) -> None:
        self.data[job_name] = value


@dataclass
class RouteRegistryEntry:
    destination: str
    checksum: str
    payload_size_bytes: int
    updated_at: str


class RouteRegistryStore(JsonStateStore):
    def get_entry(self, route_key: str) -> RouteRegistryEntry | None:
        raw = self.data.get(route_key)
        if not raw:
            return None
        return RouteRegistryEntry(
            destination=str(raw.get("destination")),
            checksum=str(raw.get("checksum", "")),
            payload_size_bytes=int(raw.get("payload_size_bytes", 0)),
            updated_at=str(raw.get("updated_at", "")),
        )

    def set_entry(self, route_key: str, entry: RouteRegistryEntry) -> None:
        self.data[route_key] = {
            "destination": entry.destination,
            "checksum": entry.checksum,
            "payload_size_bytes": entry.payload_size_bytes,
            "updated_at": entry.updated_at,
        }


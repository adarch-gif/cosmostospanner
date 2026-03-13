from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from migration.file_lock import FileLock


class JsonStateStore:
    def __init__(self, file_path: str) -> None:
        self.path = Path(file_path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"State file is corrupted: {self.path}. "
                "Restore a valid JSON document or remove the file to reset."
            ) from exc

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(self.lock_path):
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.path)


class WatermarkStore(JsonStateStore):
    def get(self, job_name: str, default: Any = None) -> Any:
        return self.data.get(job_name, default)

    def set(self, job_name: str, value: Any) -> None:
        self.data[job_name] = value


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
    def get_entry(self, route_key: str) -> RouteRegistryEntry | None:
        raw = self.data.get(route_key)
        if not raw:
            return None
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

    def set_entry(self, route_key: str, entry: RouteRegistryEntry) -> None:
        if entry.sync_state not in VALID_SYNC_STATES:
            raise ValueError(f"Invalid registry sync_state: {entry.sync_state}")
        self.data[route_key] = {
            "destination": entry.destination,
            "checksum": entry.checksum,
            "payload_size_bytes": entry.payload_size_bytes,
            "updated_at": entry.updated_at,
            "sync_state": entry.sync_state,
            "cleanup_from_destination": entry.cleanup_from_destination,
        }

from __future__ import annotations

import json
from pathlib import Path

from migration.file_lock import FileLock


class WatermarkStore:
    def __init__(self, state_file: str) -> None:
        self.path = Path(state_file)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.data: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Watermark state file is corrupted: {self.path}. "
                "Restore a valid JSON document or remove the file to reset."
            ) from exc

    def get(self, container_name: str, default: int = 0) -> int:
        return int(self.data.get(container_name, default))

    def set(self, container_name: str, value: int) -> None:
        self.data[container_name] = int(value)

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(self.lock_path):
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
            tmp.replace(self.path)

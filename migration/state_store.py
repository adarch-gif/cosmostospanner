from __future__ import annotations

import json
from pathlib import Path


class WatermarkStore:
    def __init__(self, state_file: str) -> None:
        self.path = Path(state_file)
        self.data: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, container_name: str, default: int = 0) -> int:
        return int(self.data.get(container_name, default))

    def set(self, container_name: str, value: int) -> None:
        self.data[container_name] = int(value)

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
        tmp.replace(self.path)


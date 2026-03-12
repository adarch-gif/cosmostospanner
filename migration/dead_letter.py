from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("dead_letter")


class DeadLetterSink:
    def __init__(self, file_path: str | None) -> None:
        self._path = Path(file_path) if file_path else None
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        *,
        stage: str,
        mapping_name: str,
        source_container: str,
        target_table: str,
        error: Exception,
        source_document: dict[str, Any] | None = None,
        transformed_row: dict[str, Any] | None = None,
    ) -> None:
        if not self._path:
            return

        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "mapping": mapping_name,
            "source_container": source_container,
            "target_table": target_table,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "source_document": source_document,
            "transformed_row": transformed_row,
        }

        try:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as sink_error:  # noqa: BLE001
            LOGGER.warning("Failed to write dead-letter event: %s", sink_error)


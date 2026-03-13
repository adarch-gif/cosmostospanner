from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class CanonicalRecord:
    source_job: str
    source_api: str
    source_namespace: str
    source_key: str
    route_key: str
    payload: dict[str, Any]
    payload_size_bytes: int
    checksum: str
    event_ts: str
    watermark_value: Any = None


@dataclass(frozen=True)
class RoutedSinkRecord:
    destination: str
    record: CanonicalRecord
    stored_checksum: str
    stored_payload_size_bytes: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


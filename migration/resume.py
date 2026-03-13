from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from migration.control_plane_backend import (
    SpannerControlPlaneBackend,
    is_spanner_control_plane_path,
)
from migration.json_state_backend import JsonStateBackend


def _require_local_backend(backend: JsonStateBackend | None) -> JsonStateBackend:
    if backend is None:
        raise RuntimeError("Local JSON state backend is not configured for reader cursors.")
    return backend


def build_resume_scope(**components: object) -> str:
    payload = json.dumps(components, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StreamResumeState:
    last_source_key: str = ""
    last_watermark: Any = None
    emitted_count: int = 0
    page_start_token: Any = None
    scope: str = ""
    updated_at: str = ""

    @property
    def has_position(self) -> bool:
        return self.emitted_count > 0 and bool(self.last_source_key)

    def reset(self, *, scope: str = "") -> None:
        self.last_source_key = ""
        self.last_watermark = None
        self.emitted_count = 0
        self.page_start_token = None
        self.scope = scope
        self.updated_at = ""

    def ensure_scope(self, scope: str) -> None:
        if self.scope == scope:
            return
        self.reset(scope=scope)

    def set_page_start_token(self, token: Any) -> None:
        self.page_start_token = token

    def mark(self, *, source_key: str, watermark: Any) -> None:
        self.last_source_key = str(source_key)
        self.last_watermark = watermark
        self.emitted_count += 1
        self.updated_at = utc_now_iso()

    def clone(self) -> "StreamResumeState":
        return StreamResumeState(
            last_source_key=self.last_source_key,
            last_watermark=self.last_watermark,
            emitted_count=self.emitted_count,
            page_start_token=self.page_start_token,
            scope=self.scope,
            updated_at=self.updated_at,
        )

    def should_skip(
        self,
        *,
        source_key: str,
        watermark: Any,
        compare_watermarks: Callable[[Any, Any], int],
    ) -> bool:
        if not self.has_position:
            return False
        if self.last_watermark is None:
            return str(source_key) <= self.last_source_key

        comparison = compare_watermarks(watermark, self.last_watermark)
        if comparison < 0:
            return True
        if comparison == 0 and str(source_key) <= self.last_source_key:
            return True
        return False

    def resume_watermark(self, fallback: Any) -> Any:
        if self.last_watermark is None:
            return fallback
        return self.last_watermark


class ReaderCursorStore:
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
        self.data: dict[str, dict[str, Any]] = {}
        self._deleted_keys: set[str] = set()
        self._load()

    def _read_file_data(self) -> dict[str, Any]:
        if self.backend is None:
            return {}
        try:
            return self.backend.read_json_object()
        except ValueError as exc:
            raise ValueError(str(exc).replace("State file", "Reader cursor state file")) from exc

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
        normalized: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if isinstance(value, dict):
                normalized[str(key)] = {
                    "last_source_key": str(value.get("last_source_key", "")),
                    "last_watermark": value.get("last_watermark"),
                    "emitted_count": int(value.get("emitted_count", 0)),
                    "page_start_token": value.get("page_start_token"),
                    "scope": str(value.get("scope", "")),
                    "updated_at": str(value.get("updated_at", "")),
                }
        return normalized

    def _merge_data(self, current_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        merged = dict(current_data)
        for key in self._deleted_keys:
            merged.pop(key, None)
        for key in set(current_data) | set(self.data):
            if key in self._deleted_keys:
                continue
            current_entry = current_data.get(key)
            candidate_entry = self.data.get(key)
            if candidate_entry is None:
                if current_entry is not None:
                    merged[key] = current_entry
                continue
            if current_entry is None or candidate_entry.get("updated_at", "") >= current_entry.get(
                "updated_at", ""
            ):
                merged[key] = candidate_entry
            else:
                merged[key] = current_entry
        return merged

    def _load(self) -> None:
        self.data = self._normalize_data(self._read_file_data())
        self._deleted_keys = set()

    def get(self, key: str) -> StreamResumeState:
        if self.spanner is not None and key not in self._deleted_keys and key not in self.data:
            row = self.spanner.get(key)
            if row is not None and row.payload:
                self.data[key] = self._normalize_data({key: row.payload}).get(key, {})
        raw = self.data.get(key, {})
        return StreamResumeState(
            last_source_key=str(raw.get("last_source_key", "")),
            last_watermark=raw.get("last_watermark"),
            emitted_count=int(raw.get("emitted_count", 0)),
            page_start_token=raw.get("page_start_token"),
            scope=str(raw.get("scope", "")),
            updated_at=str(raw.get("updated_at", "")),
        )

    def set(self, key: str, state: StreamResumeState) -> None:
        self._deleted_keys.discard(key)
        self.data[key] = {
            "last_source_key": state.last_source_key,
            "last_watermark": state.last_watermark,
            "emitted_count": state.emitted_count,
            "page_start_token": state.page_start_token,
            "scope": state.scope,
            "updated_at": state.updated_at or utc_now_iso(),
        }

    def clear(self, key: str) -> None:
        self.data.pop(key, None)
        self._deleted_keys.add(key)

    def flush(self) -> None:
        if self.spanner is not None:
            spanner_backend = self.spanner
            persisted: dict[str, dict[str, Any]] = {}

            def txn_fn(transaction: Any) -> None:
                touched_keys = sorted(set(self.data) | self._deleted_keys)
                current_rows = spanner_backend._read_records_with_reader(
                    transaction,
                    touched_keys,
                )
                current_data = {
                    key: row.payload
                    for key, row in current_rows.items()
                    if isinstance(row.payload, dict)
                }
                merged = self._merge_data(self._normalize_data(current_data))
                for key in sorted(self._deleted_keys):
                    spanner_backend.delete(transaction, record_key=key)
                for key, value in merged.items():
                    spanner_backend.upsert(
                        transaction,
                        record_key=key,
                        payload=value,
                    )
                persisted.update(merged)

            spanner_backend.run_in_transaction(txn_fn)
            self.data = persisted
            self._deleted_keys = set()
            return

        def merge_fn(current_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
            return self._merge_data(self._normalize_data(current_data))

        backend = _require_local_backend(self.backend)
        self.data = backend.merge_write_json_object(merge_fn, ensure_ascii=False)
        self._deleted_keys = set()

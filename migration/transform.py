from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from migration.config import ColumnRule, DeleteRule, TableMapping


@dataclass
class TransformOutput:
    row: dict[str, Any]
    is_delete: bool
    source_ts: int


def _get_nested_value(document: dict[str, Any], path: str | None) -> Any:
    if not path:
        return None

    current: Any = document
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise KeyError(path)
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            idx = int(part)
            if idx < 0 or idx >= len(current):
                raise KeyError(path)
            current = current[idx]
            continue
        raise KeyError(path)
    return current


def _to_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    raise ValueError(f"Cannot convert {value!r} to timestamp.")


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Cannot convert {value!r} to bool.")


def _apply_converter(converter: str, value: Any) -> Any:
    converter = (converter or "identity").strip().lower()

    if converter == "identity":
        return value
    if converter == "string":
        return None if value is None else str(value)
    if converter == "int":
        return None if value is None else int(value)
    if converter == "float":
        return None if value is None else float(value)
    if converter == "bool":
        return _to_bool(value)
    if converter in {"timestamp", "datetime"}:
        return _to_utc_datetime(value)
    if converter == "json_string":
        if value is None:
            return None
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

    raise ValueError(f"Unsupported converter: {converter}")


def _resolve_marker(value: Any) -> Any:
    if value == "__NOW_UTC__":
        return datetime.now(timezone.utc)
    return value


def _extract_column(document: dict[str, Any], rule: ColumnRule) -> Any:
    try:
        raw = _get_nested_value(document, rule.source)
        return _apply_converter(rule.converter, raw)
    except KeyError:
        if rule.default is not None:
            return _resolve_marker(rule.default)
        if rule.required:
            raise ValueError(f"Missing required source field: {rule.source}")
        return None


def _is_delete_event(document: dict[str, Any], delete_rule: DeleteRule | None) -> bool:
    if not delete_rule:
        return False
    try:
        return _get_nested_value(document, delete_rule.field) == delete_rule.equals
    except KeyError:
        return False


def transform_document(document: dict[str, Any], mapping: TableMapping) -> TransformOutput:
    row: dict[str, Any] = {}

    for column_rule in mapping.columns:
        row[column_rule.target] = _extract_column(document, column_rule)

    for target_column, value in mapping.static_columns.items():
        row[target_column] = _resolve_marker(value)

    for key_column in mapping.key_columns:
        if key_column not in row or row[key_column] is None:
            raise ValueError(
                f"Key column {key_column!r} is missing/null after transform for "
                f"{mapping.source_container}->{mapping.target_table}."
            )

    source_ts = int(document.get("_ts", 0) or 0)
    is_delete = _is_delete_event(document, mapping.delete_rule)
    return TransformOutput(row=row, is_delete=is_delete, source_ts=source_ts)


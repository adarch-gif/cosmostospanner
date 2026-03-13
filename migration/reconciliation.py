from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def normalize_reconciliation_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, list):
        return [normalize_reconciliation_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): normalize_reconciliation_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    return value


def canonical_row_subset(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    return {
        column: normalize_reconciliation_value(row.get(column))
        for column in columns
    }


def row_digest(row: dict[str, Any], columns: list[str]) -> str:
    payload = canonical_row_subset(row, columns)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def aggregate_row_digests(row_digests: dict[tuple[Any, ...], str]) -> str:
    aggregate = hashlib.sha256()
    for key_tuple, digest in sorted(
        row_digests.items(),
        key=lambda item: json.dumps(
            normalize_reconciliation_value(list(item[0])),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    ):
        key_text = json.dumps(
            normalize_reconciliation_value(list(key_tuple)),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        aggregate.update(key_text.encode("utf-8"))
        aggregate.update(b"|")
        aggregate.update(digest.encode("ascii"))
        aggregate.update(b"\n")
    return aggregate.hexdigest()

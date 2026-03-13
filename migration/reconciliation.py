from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
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


def key_tuple_text(key_tuple: tuple[Any, ...]) -> str:
    return json.dumps(
        normalize_reconciliation_value(list(key_tuple)),
        ensure_ascii=False,
        separators=(",", ":"),
    )


class DigestAccumulator:
    def __init__(self) -> None:
        self._aggregate = hashlib.sha256()

    def update(self, key_tuple: tuple[Any, ...], digest: str) -> None:
        self.update_text(key_tuple_text(key_tuple), digest)

    def update_text(self, key_text: str, digest: str) -> None:
        self._aggregate.update(key_text.encode("utf-8"))
        self._aggregate.update(b"|")
        self._aggregate.update(digest.encode("ascii"))
        self._aggregate.update(b"\n")

    def hexdigest(self) -> str:
        return self._aggregate.hexdigest()


def aggregate_row_digests(row_digests: dict[tuple[Any, ...], str]) -> str:
    aggregate = DigestAccumulator()
    for key_tuple, digest in sorted(
        row_digests.items(),
        key=lambda item: key_tuple_text(item[0]),
    ):
        aggregate.update(key_tuple, digest)
    return aggregate.hexdigest()


@dataclass(frozen=True)
class ReconciliationSummary:
    source_count: int
    target_count: int
    missing_rows: int
    extra_rows: int
    mismatched_rows: int
    source_checksum: str
    target_checksum: str


class SqliteRowDigestStore:
    def __init__(self, temp_dir: str | None = None) -> None:
        fd, path = tempfile.mkstemp(
            prefix="reconciliation_",
            suffix=".sqlite3",
            dir=temp_dir,
        )
        os.close(fd)
        self._path = path
        self._conn = sqlite3.connect(self._path)
        self._initialize()

    def _initialize(self) -> None:
        self._conn.execute("PRAGMA journal_mode = MEMORY")
        self._conn.execute("PRAGMA synchronous = OFF")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS row_digests (
                key_text TEXT PRIMARY KEY,
                source_digest TEXT,
                target_digest TEXT
            )
            """
        )
        self._conn.commit()

    def __enter__(self) -> "SqliteRowDigestStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()
        if os.path.exists(self._path):
            os.remove(self._path)

    def upsert_source(self, key_tuple: tuple[Any, ...], digest: str) -> None:
        self._conn.execute(
            """
            INSERT INTO row_digests (key_text, source_digest)
            VALUES (?, ?)
            ON CONFLICT(key_text) DO UPDATE SET source_digest = excluded.source_digest
            """,
            (key_tuple_text(key_tuple), digest),
        )

    def upsert_target(self, key_tuple: tuple[Any, ...], digest: str) -> None:
        self._conn.execute(
            """
            INSERT INTO row_digests (key_text, target_digest)
            VALUES (?, ?)
            ON CONFLICT(key_text) DO UPDATE SET target_digest = excluded.target_digest
            """,
            (key_tuple_text(key_tuple), digest),
        )

    def flush(self) -> None:
        self._conn.commit()

    def summarize(self) -> ReconciliationSummary:
        self.flush()
        source_count = self._count_rows(
            "SELECT COUNT(1) FROM row_digests WHERE source_digest IS NOT NULL"
        )
        target_count = self._count_rows(
            "SELECT COUNT(1) FROM row_digests WHERE target_digest IS NOT NULL"
        )
        missing_rows = self._count_rows(
            "SELECT COUNT(1) FROM row_digests WHERE source_digest IS NOT NULL AND target_digest IS NULL"
        )
        extra_rows = self._count_rows(
            "SELECT COUNT(1) FROM row_digests WHERE source_digest IS NULL AND target_digest IS NOT NULL"
        )
        mismatched_rows = self._count_rows(
            "SELECT COUNT(1) FROM row_digests WHERE "
            "source_digest IS NOT NULL AND target_digest IS NOT NULL AND source_digest != target_digest"
        )
        return ReconciliationSummary(
            source_count=source_count,
            target_count=target_count,
            missing_rows=missing_rows,
            extra_rows=extra_rows,
            mismatched_rows=mismatched_rows,
            source_checksum=self._aggregate_checksum("source_digest"),
            target_checksum=self._aggregate_checksum("target_digest"),
        )

    def _count_rows(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row else 0

    def _aggregate_checksum(self, column_name: str) -> str:
        aggregate = DigestAccumulator()
        if column_name == "source_digest":
            sql = (
                "SELECT key_text, source_digest FROM row_digests "
                "WHERE source_digest IS NOT NULL ORDER BY key_text"
            )
        elif column_name == "target_digest":
            sql = (
                "SELECT key_text, target_digest FROM row_digests "
                "WHERE target_digest IS NOT NULL ORDER BY key_text"
            )
        else:
            raise ValueError(f"Unsupported digest column: {column_name}")
        rows = self._conn.execute(sql)
        for key_text, digest in rows:
            aggregate.update_text(str(key_text), str(digest))
        return aggregate.hexdigest()

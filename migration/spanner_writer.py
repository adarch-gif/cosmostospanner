from __future__ import annotations

from typing import Any

from google.cloud import spanner
from google.cloud.spanner_v1 import KeySet

from migration.config import SpannerConfig


class SpannerWriter:
    def __init__(self, config: SpannerConfig, dry_run: bool = False) -> None:
        self._client = spanner.Client(project=config.project)
        self._instance = self._client.instance(config.instance)
        self._database = self._instance.database(config.database)
        self._dry_run = dry_run

    def write_rows(self, table: str, rows: list[dict[str, Any]], mode: str = "upsert") -> int:
        if not rows:
            return 0
        if self._dry_run:
            return len(rows)

        columns = list(rows[0].keys())
        values = [tuple(row[column] for column in columns) for row in rows]

        with self._database.batch() as batch:
            if mode == "insert":
                batch.insert(table=table, columns=columns, values=values)
            elif mode == "update":
                batch.update(table=table, columns=columns, values=values)
            elif mode == "replace":
                batch.replace(table=table, columns=columns, values=values)
            else:
                batch.insert_or_update(table=table, columns=columns, values=values)
        return len(rows)

    def delete_rows(
        self, table: str, key_columns: list[str], key_rows: list[dict[str, Any]]
    ) -> int:
        if not key_rows:
            return 0
        if self._dry_run:
            return len(key_rows)

        keys = [tuple(row[column] for column in key_columns) for row in key_rows]
        keyset = KeySet(keys=keys)

        with self._database.batch() as batch:
            batch.delete(table, keyset)
        return len(key_rows)

    def count_rows(self, table: str) -> int:
        sql = f"SELECT COUNT(1) FROM `{table}`"
        with self._database.snapshot() as snapshot:
            row = next(iter(snapshot.execute_sql(sql)), [0])
        return int(row[0])

    def existing_keys(
        self, table: str, key_columns: list[str], key_rows: list[dict[str, Any]]
    ) -> set[tuple[Any, ...]]:
        if not key_rows:
            return set()

        keys = [tuple(row[column] for column in key_columns) for row in key_rows]
        keyset = KeySet(keys=keys)
        with self._database.snapshot() as snapshot:
            rows = snapshot.read(table=table, columns=key_columns, keyset=keyset)
            return {tuple(row) for row in rows}


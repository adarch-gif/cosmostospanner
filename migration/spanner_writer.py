from __future__ import annotations

import logging
from typing import Any, Iterable

from google.cloud import spanner
from google.cloud.spanner_v1 import KeySet
from google.cloud.spanner_v1 import param_types

from migration.config import SpannerConfig
from migration.retry_utils import RetryPolicy, run_with_retry

LOGGER = logging.getLogger("spanner_writer")


class SpannerWriter:
    def __init__(
        self, config: SpannerConfig, retry_policy: RetryPolicy, dry_run: bool = False
    ) -> None:
        self._client = spanner.Client(project=config.project)
        self._instance = self._client.instance(config.instance)
        self._database = self._instance.database(config.database)
        self._dry_run = dry_run
        self._retry_policy = retry_policy

    def write_rows(self, table: str, rows: list[dict[str, Any]], mode: str = "upsert") -> int:
        if not rows:
            return 0
        if self._dry_run:
            return len(rows)

        columns = list(rows[0].keys())
        values = [tuple(row[column] for column in columns) for row in rows]

        def operation() -> int:
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

        return run_with_retry(
            operation,
            operation_name=f"write_rows:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def delete_rows(
        self, table: str, key_columns: list[str], key_rows: list[dict[str, Any]]
    ) -> int:
        if not key_rows:
            return 0
        if self._dry_run:
            return len(key_rows)

        keys = [tuple(row[column] for column in key_columns) for row in key_rows]
        keyset = KeySet(keys=keys)

        def operation() -> int:
            with self._database.batch() as batch:
                batch.delete(table, keyset)
            return len(key_rows)

        return run_with_retry(
            operation,
            operation_name=f"delete_rows:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def count_rows(self, table: str) -> int:
        def operation() -> int:
            # Table names are validated during config loading before they are interpolated.
            sql = f"SELECT COUNT(1) FROM `{table}`"  # nosec B608
            with self._database.snapshot() as snapshot:
                row = next(iter(snapshot.execute_sql(sql)), [0])
            return int(row[0])

        return run_with_retry(
            operation,
            operation_name=f"count_rows:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def existing_keys(
        self, table: str, key_columns: list[str], key_rows: list[dict[str, Any]]
    ) -> set[tuple[Any, ...]]:
        if not key_rows:
            return set()

        keys = [tuple(row[column] for column in key_columns) for row in key_rows]
        keyset = KeySet(keys=keys)

        def operation() -> set[tuple[Any, ...]]:
            with self._database.snapshot() as snapshot:
                rows = snapshot.read(table=table, columns=key_columns, keyset=keyset)
                return {tuple(row) for row in rows}

        return run_with_retry(
            operation,
            operation_name=f"existing_keys:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def read_rows_by_keys(
        self,
        table: str,
        key_columns: list[str],
        data_columns: list[str],
        key_rows: list[dict[str, Any]],
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        if not key_rows:
            return {}

        selected_columns: list[str] = []
        for column in [*key_columns, *data_columns]:
            if column not in selected_columns:
                selected_columns.append(column)

        keys = [tuple(row[column] for column in key_columns) for row in key_rows]
        keyset = KeySet(keys=keys)

        def operation() -> dict[tuple[Any, ...], dict[str, Any]]:
            output: dict[tuple[Any, ...], dict[str, Any]] = {}
            with self._database.snapshot() as snapshot:
                rows = snapshot.read(table=table, columns=selected_columns, keyset=keyset)
                for row in rows:
                    row_dict = {selected_columns[idx]: row[idx] for idx in range(len(selected_columns))}
                    key_tuple = tuple(row_dict[col] for col in key_columns)
                    output[key_tuple] = row_dict
            return output

        return run_with_retry(
            operation,
            operation_name=f"read_rows_by_keys:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def read_all_rows(
        self,
        table: str,
        key_columns: list[str],
        data_columns: list[str],
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        selected_columns: list[str] = []
        for column in [*key_columns, *data_columns]:
            if column not in selected_columns:
                selected_columns.append(column)

        quoted_columns = ", ".join(f"`{column}`" for column in selected_columns)
        order_clause = ", ".join(f"`{column}`" for column in key_columns)

        def operation() -> dict[tuple[Any, ...], dict[str, Any]]:
            output: dict[tuple[Any, ...], dict[str, Any]] = {}
            # Table and column identifiers are validated during config loading.
            sql = f"SELECT {quoted_columns} FROM `{table}` ORDER BY {order_clause}"  # nosec B608
            with self._database.snapshot() as snapshot:
                rows = snapshot.execute_sql(sql)
                for row in rows:
                    row_dict = {
                        selected_columns[idx]: row[idx]
                        for idx in range(len(selected_columns))
                    }
                    key_tuple = tuple(row_dict[col] for col in key_columns)
                    output[key_tuple] = row_dict
            return output

        return run_with_retry(
            operation,
            operation_name=f"read_all_rows:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def iter_all_rows(
        self,
        table: str,
        key_columns: list[str],
        data_columns: list[str],
    ) -> Iterable[tuple[tuple[Any, ...], dict[str, Any]]]:
        selected_columns: list[str] = []
        for column in [*key_columns, *data_columns]:
            if column not in selected_columns:
                selected_columns.append(column)

        quoted_columns = ", ".join(f"`{column}`" for column in selected_columns)
        order_clause = ", ".join(f"`{column}`" for column in key_columns)
        # Table and column identifiers are validated during config loading.
        sql = f"SELECT {quoted_columns} FROM `{table}` ORDER BY {order_clause}"  # nosec B608
        with self._database.snapshot() as snapshot:
            rows = snapshot.execute_sql(sql)
            for row in rows:
                row_dict = {
                    selected_columns[idx]: row[idx]
                    for idx in range(len(selected_columns))
                }
                key_tuple = tuple(row_dict[col] for col in key_columns)
                yield key_tuple, row_dict

    def table_exists(self, table: str) -> bool:
        def operation() -> bool:
            sql = (
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_NAME = @table_name"
            )
            params = {"table_name": table}
            param_types_map = {"table_name": param_types.STRING}
            with self._database.snapshot() as snapshot:
                row = next(
                    iter(
                        snapshot.execute_sql(
                            sql=sql,
                            params=params,
                            param_types=param_types_map,
                        )
                    ),
                    [0],
                )
            return int(row[0]) > 0

        return run_with_retry(
            operation,
            operation_name=f"table_exists:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def table_columns(self, table: str) -> set[str]:
        def operation() -> set[str]:
            sql = (
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = @table_name"
            )
            params = {"table_name": table}
            param_types_map = {"table_name": param_types.STRING}
            with self._database.snapshot() as snapshot:
                rows = snapshot.execute_sql(sql=sql, params=params, param_types=param_types_map)
                return {str(row[0]) for row in rows}

        return run_with_retry(
            operation,
            operation_name=f"table_columns:{table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

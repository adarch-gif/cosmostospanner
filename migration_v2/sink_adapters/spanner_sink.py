from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from migration.retry_utils import RetryPolicy, run_with_retry
from migration_v2.config import SpannerTargetConfig
from migration_v2.models import CanonicalRecord
from migration_v2.utils import json_dumps

LOGGER = logging.getLogger("v2.spanner_sink")


class SpannerSinkAdapter:
    REQUIRED_COLUMNS = {
        "RouteKey",
        "SourceJob",
        "SourceApi",
        "SourceNamespace",
        "SourceKey",
        "PayloadJson",
        "PayloadSizeBytes",
        "Checksum",
        "EventTs",
        "UpdatedAt",
    }

    def __init__(
        self,
        config: SpannerTargetConfig,
        *,
        retry_policy: RetryPolicy,
        dry_run: bool = False,
    ) -> None:
        from google.cloud import spanner  # Lazy import for optional dependency ergonomics.
        from google.cloud.spanner_v1 import KeySet, param_types

        self._client = spanner.Client(project=config.project)
        self._instance = self._client.instance(config.instance)
        self._database = self._instance.database(config.database)
        self._table = config.table
        self._retry_policy = retry_policy
        self._dry_run = dry_run
        self._param_types = param_types
        self._keyset_cls = KeySet

    def preflight_check(self) -> tuple[bool, str]:
        if not self.table_exists():
            return False, f"Spanner table does not exist: {self._table}"
        columns = self.table_columns()
        missing = sorted(self.REQUIRED_COLUMNS.difference(columns))
        if missing:
            return False, f"Spanner table {self._table} missing columns: {missing}"
        return True, "Spanner table exists with required columns."

    def table_exists(self) -> bool:
        def operation() -> bool:
            sql = (
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_NAME = @table_name"
            )
            params = {"table_name": self._table}
            param_types_map = {"table_name": self._param_types.STRING}
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
            operation_name=f"spanner_table_exists:{self._table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def table_columns(self) -> set[str]:
        def operation() -> set[str]:
            sql = (
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = @table_name"
            )
            params = {"table_name": self._table}
            param_types_map = {"table_name": self._param_types.STRING}
            with self._database.snapshot() as snapshot:
                rows = snapshot.execute_sql(sql=sql, params=params, param_types=param_types_map)
                return {str(row[0]) for row in rows}

        return run_with_retry(
            operation,
            operation_name=f"spanner_table_columns:{self._table}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def upsert(self, record: CanonicalRecord) -> None:
        if self._dry_run:
            return
        columns = [
            "RouteKey",
            "SourceJob",
            "SourceApi",
            "SourceNamespace",
            "SourceKey",
            "PayloadJson",
            "PayloadSizeBytes",
            "Checksum",
            "EventTs",
            "UpdatedAt",
        ]
        values = [
            (
                record.route_key,
                record.source_job,
                record.source_api,
                record.source_namespace,
                record.source_key,
                json_dumps(record.payload),
                int(record.payload_size_bytes),
                record.checksum,
                record.event_ts,
                datetime.now(timezone.utc),
            )
        ]

        def operation() -> None:
            with self._database.batch() as batch:
                batch.insert_or_update(
                    table=self._table,
                    columns=columns,
                    values=values,
                )

        run_with_retry(
            operation,
            operation_name=f"spanner_upsert:{record.route_key}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

    def delete(self, record: CanonicalRecord) -> None:
        if self._dry_run:
            return
        keyset = self._keyset_cls(keys=[(record.route_key,)])

        def operation() -> None:
            with self._database.batch() as batch:
                batch.delete(self._table, keyset)

        run_with_retry(
            operation,
            operation_name=f"spanner_delete:{record.route_key}",
            policy=self._retry_policy,
            logger=LOGGER,
        )

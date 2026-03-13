from __future__ import annotations

import sys
from types import ModuleType
from pathlib import Path

from migration.config import (
    ColumnRule,
    CosmosConfig,
    MigrationConfig,
    RuntimeConfig,
    SpannerConfig,
    TableMapping,
)

azure_module = ModuleType("azure")
cosmos_module = ModuleType("azure.cosmos")
cosmos_module.CosmosClient = object
azure_module.cosmos = cosmos_module
sys.modules.setdefault("azure", azure_module)
sys.modules.setdefault("azure.cosmos", cosmos_module)

google_module = ModuleType("google")
google_cloud_module = ModuleType("google.cloud")
google_spanner_module = ModuleType("google.cloud.spanner")
google_spanner_module.Client = object
google_spanner_v1_module = ModuleType("google.cloud.spanner_v1")
google_spanner_v1_module.KeySet = object
google_spanner_v1_module.param_types = object
google_cloud_module.spanner = google_spanner_module
google_module.cloud = google_cloud_module
sys.modules.setdefault("google", google_module)
sys.modules.setdefault("google.cloud", google_cloud_module)
sys.modules.setdefault("google.cloud.spanner", google_spanner_module)
sys.modules.setdefault("google.cloud.spanner_v1", google_spanner_v1_module)

from scripts import backfill


class _FakeReader:
    def __init__(self, documents: list[dict[str, object]]) -> None:
        self._documents = documents

    def iter_documents(self, **_: object) -> list[dict[str, object]]:
        return list(self._documents)


class _FakeWriter:
    def __init__(self, *, fail_ids: set[str] | None = None) -> None:
        self.fail_ids = fail_ids or set()

    def write_rows(self, table: str, rows: list[dict[str, object]], mode: str) -> int:
        del table, mode
        for row in rows:
            if str(row["user_id"]) in self.fail_ids:
                raise RuntimeError("simulated write failure")
        return len(rows)

    def delete_rows(
        self,
        table: str,
        key_columns: list[str],
        key_rows: list[dict[str, object]],
    ) -> int:
        del table, key_columns
        return len(key_rows)


class _FakeWatermarks:
    def __init__(self, initial: int = 0) -> None:
        self.current = initial
        self.flush_count = 0

    def get(self, container_name: str, default: int = 0) -> int:
        del container_name
        return self.current if self.current else default

    def set(self, container_name: str, value: int) -> None:
        del container_name
        self.current = value

    def flush(self) -> None:
        self.flush_count += 1


class _FakeDeadLetter:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def write(self, **kwargs: object) -> None:
        self.events.append(kwargs)


def _config(error_mode: str = "fail") -> MigrationConfig:
    return MigrationConfig(
        source=CosmosConfig(endpoint="https://example", key="k", database="db"),
        target=SpannerConfig(project="proj", instance="inst", database="db"),
        runtime=RuntimeConfig(
            batch_size=2,
            query_page_size=100,
            error_mode=error_mode,
            watermark_state_file=str(Path("state") / "watermarks.json"),
        ),
        mappings=[
            TableMapping(
                source_container="users",
                target_table="Users",
                key_columns=["user_id"],
                columns=[
                    ColumnRule(target="user_id", source="id", converter="string", required=True),
                    ColumnRule(target="email", source="email", converter="string"),
                ],
            )
        ],
    )


def test_incremental_success_advances_watermark() -> None:
    config = _config()
    mapping = config.mappings[0]
    reader = _FakeReader(
        [
            {"id": "u1", "email": "u1@example.com", "_ts": 10},
            {"id": "u2", "email": "u2@example.com", "_ts": 12},
        ]
    )
    writer = _FakeWriter()
    watermarks = _FakeWatermarks(initial=5)
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        incremental=True,
        since_ts=None,
    )

    assert stats["rows_failed"] == 0
    assert stats["docs_failed"] == 0
    assert stats["max_success_ts"] == 12
    assert watermarks.current == 12
    assert watermarks.flush_count == 1


def test_incremental_failures_hold_watermark_in_place() -> None:
    config = _config(error_mode="skip")
    mapping = config.mappings[0]
    reader = _FakeReader(
        [
            {"id": "u1", "email": "u1@example.com", "_ts": 10},
            {"id": "u2", "email": "u2@example.com", "_ts": 12},
        ]
    )
    writer = _FakeWriter(fail_ids={"u2"})
    watermarks = _FakeWatermarks(initial=5)
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        incremental=True,
        since_ts=None,
    )

    assert stats["rows_failed"] == 1
    assert stats["max_success_ts"] == 10
    assert watermarks.current == 5
    assert watermarks.flush_count == 0
    assert len(dead_letter.events) == 1

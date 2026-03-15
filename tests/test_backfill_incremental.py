from __future__ import annotations

import sys
from types import ModuleType
from types import SimpleNamespace
from pathlib import Path

import pytest

from migration.resume import StreamResumeState
from migration.config import (
    ColumnRule,
    CosmosConfig,
    MigrationConfig,
    RuntimeConfig,
    SpannerConfig,
    TableMapping,
)
from migration.sharding import stable_shard_for_text

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
        self.calls: list[dict[str, object]] = []

    def iter_documents(self, **_: object) -> list[dict[str, object]]:
        self.calls.append(dict(_))
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
    def __init__(self, initial: dict[str, int] | None = None) -> None:
        self.values = dict(initial or {})
        self.flush_count = 0

    def get(self, container_name: str, default: int = 0) -> int:
        return self.values.get(container_name, default)

    def contains(self, container_name: str) -> bool:
        return container_name in self.values

    def set(self, container_name: str, value: int) -> None:
        self.values[container_name] = value

    def flush(self) -> None:
        self.flush_count += 1


class _FakeDeadLetter:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def write(self, **kwargs: object) -> None:
        self.events.append(kwargs)


class _FakeCursorStore:
    def __init__(self, initial: StreamResumeState | None = None) -> None:
        self.state = initial or StreamResumeState()
        self.flush_count = 0
        self.cleared_keys: list[str] = []

    def get(self, key: str) -> StreamResumeState:
        del key
        return self.state.clone()

    def set(self, key: str, state: StreamResumeState) -> None:
        del key
        self.state = state.clone()

    def clear(self, key: str) -> None:
        self.cleared_keys.append(key)
        self.state = StreamResumeState()

    def flush(self) -> None:
        self.flush_count += 1


class _FakeCoordinator:
    def __init__(
        self,
        *,
        renew_results: list[bool] | None = None,
        completed_keys: set[str] | None = None,
    ) -> None:
        self.renew_results = list(renew_results or [True])
        self.calls = 0
        self.completed_keys = completed_keys or set()

    def renew_if_due(self, work_key: str, metadata: dict[str, object]) -> bool:
        del work_key, metadata
        index = min(self.calls, len(self.renew_results) - 1)
        self.calls += 1
        return self.renew_results[index]

    def is_completed(self, work_key: str) -> bool:
        return work_key in self.completed_keys


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
    watermarks = _FakeWatermarks(initial={"v1:users->Users": 5})
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=None,
        incremental=True,
        since_ts=None,
    )

    assert stats["rows_failed"] == 0
    assert stats["docs_failed"] == 0
    assert stats["max_success_ts"] == 12
    assert watermarks.values["v1:users->Users"] == 12
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
    watermarks = _FakeWatermarks(initial={"v1:users->Users": 5})
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=None,
        incremental=True,
        since_ts=None,
    )

    assert stats["rows_failed"] == 1
    assert stats["max_success_ts"] == 10
    assert watermarks.values["v1:users->Users"] == 5
    assert watermarks.flush_count == 0
    assert len(dead_letter.events) == 1


def test_incremental_reads_legacy_watermark_and_writes_mapping_scoped_checkpoint() -> None:
    config = _config()
    mapping = config.mappings[0]
    reader = _FakeReader(
        [
            {"id": "u1", "email": "u1@example.com", "_ts": 10},
            {"id": "u2", "email": "u2@example.com", "_ts": 12},
        ]
    )
    writer = _FakeWriter()
    watermarks = _FakeWatermarks(initial={"users": 7})
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=None,
        incremental=True,
        since_ts=None,
    )

    assert stats["max_success_ts"] == 12
    assert reader.calls[0]["parameters"] == [{"name": "@last_ts", "value": 2}]
    assert watermarks.values["users"] == 7
    assert watermarks.values["v1:users->Users"] == 12


def test_process_mapping_raises_when_distributed_lease_is_lost() -> None:
    config = _config()
    mapping = config.mappings[0]
    reader = _FakeReader([{"id": "u1", "email": "u1@example.com", "_ts": 10}])
    writer = _FakeWriter()
    watermarks = _FakeWatermarks()
    dead_letter = _FakeDeadLetter()
    coordinator = _FakeCoordinator(renew_results=[False])

    with pytest.raises(RuntimeError, match="Lost distributed lease"):
        backfill._process_mapping(
            config=config,
            mapping=mapping,
            reader=reader,
            writer=writer,
            watermarks=watermarks,
            dead_letter=dead_letter,
            metrics=None,
            cursor_store=None,
            incremental=False,
            since_ts=None,
            coordinator=coordinator,
            work_key="v1:users->Users",
        )


def test_process_mapping_filters_records_by_client_hash_shard() -> None:
    config = _config()
    mapping = config.mappings[0]
    mapping.shard_count = 2
    mapping.shard_mode = "client_hash"
    mapping.shard_key_source = "id"
    documents = [
        {"id": "u1", "email": "u1@example.com", "_ts": 10},
        {"id": "u2", "email": "u2@example.com", "_ts": 12},
        {"id": "u3", "email": "u3@example.com", "_ts": 15},
    ]
    target_shard = 0
    expected_docs = [
        doc for doc in documents if stable_shard_for_text(str(doc["id"]), mapping.shard_count) == target_shard
    ]
    reader = _FakeReader(documents)
    writer = _FakeWriter()
    watermarks = _FakeWatermarks()
    dead_letter = _FakeDeadLetter()

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=None,
        incremental=False,
        since_ts=None,
        shard_index=target_shard,
    )

    assert stats["docs_seen"] == len(expected_docs)
    assert stats["rows_upserted"] == len(expected_docs)
    assert stats["max_ts_seen"] == max((int(doc["_ts"]) for doc in expected_docs), default=0)


def test_should_skip_completed_work_only_for_full_runs() -> None:
    coordinator = _FakeCoordinator(completed_keys={"v1:users->Users:shard=0"})

    assert backfill._should_skip_completed_work(
        coordinator,
        incremental=False,
        work_key="v1:users->Users:shard=0",
    )
    assert not backfill._should_skip_completed_work(
        coordinator,
        incremental=True,
        work_key="v1:users->Users:shard=0",
    )


def test_build_full_run_work_plan_includes_all_selected_mappings() -> None:
    config = _config()
    orders_mapping = TableMapping(
        source_container="orders",
        target_table="Orders",
        key_columns=["order_id"],
        columns=[
            ColumnRule(target="order_id", source="id", converter="string", required=True),
        ],
    )
    config.mappings.append(orders_mapping)
    config.mappings[0].shard_count = 2
    coordinator = SimpleNamespace(worker_id="worker-a")

    work_items, work_context = backfill._build_full_run_work_plan(config.mappings, coordinator)

    assert set(work_items) == {
        "v1:users->Users:shard=0",
        "v1:users->Users:shard=1",
        "v1:orders->Orders",
    }
    assert work_context["v1:orders->Orders"][0].target_table == "Orders"
    assert work_context["v1:users->Users:shard=1"][1] == 1


def test_process_mapping_persists_cursor_only_after_successful_writes() -> None:
    config = _config(error_mode="skip")
    mapping = config.mappings[0]
    reader = _FakeReader(
        [
            {"id": "u1", "email": "u1@example.com", "_ts": 10},
            {"id": "u2", "email": "u2@example.com", "_ts": 12},
        ]
    )
    writer = _FakeWriter(fail_ids={"u2"})
    watermarks = _FakeWatermarks(initial={"v1:users->Users": 5})
    dead_letter = _FakeDeadLetter()
    cursor_store = _FakeCursorStore()

    def iter_documents(**kwargs: object):
        resume_state = kwargs.get("resume_state")
        assert isinstance(resume_state, StreamResumeState)
        for document in reader._documents:
            resume_state.mark(
                source_key=str(document["id"]),
                watermark=document["_ts"],
            )
            yield document

    reader.iter_documents = iter_documents  # type: ignore[assignment]

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=cursor_store,
        incremental=True,
        since_ts=None,
    )

    assert stats["rows_failed"] == 1
    assert cursor_store.state.last_source_key == "u1"
    assert cursor_store.state.last_watermark == 10
    assert cursor_store.cleared_keys == []


def test_process_mapping_clears_cursor_after_successful_full_run() -> None:
    config = _config()
    mapping = config.mappings[0]
    reader = _FakeReader([{"id": "u1", "email": "u1@example.com", "_ts": 10}])
    writer = _FakeWriter()
    watermarks = _FakeWatermarks()
    dead_letter = _FakeDeadLetter()
    cursor_store = _FakeCursorStore()

    def iter_documents(**kwargs: object):
        resume_state = kwargs.get("resume_state")
        assert isinstance(resume_state, StreamResumeState)
        for document in reader._documents:
            resume_state.mark(
                source_key=str(document["id"]),
                watermark=document["_ts"],
            )
            yield document

    reader.iter_documents = iter_documents  # type: ignore[assignment]

    stats = backfill._process_mapping(
        config=config,
        mapping=mapping,
        reader=reader,
        writer=writer,
        watermarks=watermarks,
        dead_letter=dead_letter,
        metrics=None,
        cursor_store=cursor_store,
        incremental=False,
        since_ts=None,
    )

    assert stats["rows_failed"] == 0
    assert cursor_store.cleared_keys == ["v1:users->Users"]

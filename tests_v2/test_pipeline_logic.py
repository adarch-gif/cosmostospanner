from __future__ import annotations

from types import SimpleNamespace

import pytest

from migration.resume import StreamResumeState
from migration.sharding import stable_shard_for_text
from migration_v2.config import MongoJobConfig, RoutingConfig
from migration_v2.models import CanonicalRecord
from migration_v2.pipeline import JobStats, V2MigrationPipeline
from migration_v2.router import SizeRouter
from migration_v2.state_store import (
    SYNC_STATE_COMPLETE,
    SYNC_STATE_PENDING_CLEANUP,
    RouteRegistryEntry,
    RouteRegistryStore,
    WatermarkCheckpoint,
    WatermarkStore,
)


class _FakeSink:
    def __init__(self) -> None:
        self.upserts: list[str] = []
        self.deletes: list[str] = []
        self.fail_delete_count = 0
        self.fail_upsert_count = 0

    def upsert(self, record: CanonicalRecord) -> None:
        if self.fail_upsert_count > 0:
            self.fail_upsert_count -= 1
            raise RuntimeError("simulated upsert failure")
        self.upserts.append(record.route_key)

    def delete(self, record: CanonicalRecord) -> None:
        if self.fail_delete_count > 0:
            self.fail_delete_count -= 1
            raise RuntimeError("simulated delete failure")
        self.deletes.append(record.route_key)


class _FakeCoordinator:
    def __init__(
        self,
        *,
        acquire_result: bool = True,
        renew_result: bool = True,
        allowed_keys: set[str] | None = None,
        worker_id: str = "worker-1",
        completed_keys: set[str] | None = None,
        progress_enabled: bool = False,
    ) -> None:
        self.acquire_result = acquire_result
        self.renew_result = renew_result
        self.allowed_keys = allowed_keys
        self.worker_id = worker_id
        self.completed_keys = completed_keys or set()
        self.progress_enabled = progress_enabled or bool(completed_keys)
        self.acquired: list[str] = []
        self.released: list[str] = []
        self.running: list[str] = []
        self.completed: list[str] = []
        self.failed: list[str] = []
        self.claim_requests: list[list[str]] = []

    def acquire(self, work_key: str, metadata: dict[str, object]) -> bool:
        del metadata
        self.acquired.append(work_key)
        if self.allowed_keys is not None:
            return work_key in self.allowed_keys
        return self.acquire_result

    def claim_next(
        self,
        work_items: dict[str, dict[str, object]],
    ) -> tuple[str, dict[str, object]] | None:
        self.claim_requests.append(list(work_items))
        for work_key, metadata in work_items.items():
            if work_key in self.completed_keys:
                continue
            if self.allowed_keys is not None and work_key not in self.allowed_keys:
                continue
            self.running.append(work_key)
            return work_key, metadata
        return None

    def renew_if_due(self, work_key: str, metadata: dict[str, object]) -> bool:
        del work_key, metadata
        return self.renew_result

    def release(self, work_key: str) -> bool:
        self.released.append(work_key)
        return True

    def is_completed(self, work_key: str) -> bool:
        return work_key in self.completed_keys

    def completed_count(self, work_keys: list[str]) -> int:
        return sum(1 for work_key in work_keys if work_key in self.completed_keys)

    def mark_running(self, work_key: str, metadata: dict[str, object]) -> None:
        del metadata
        self.running.append(work_key)

    def mark_completed(self, work_key: str, metadata: dict[str, object]) -> None:
        del metadata
        self.completed.append(work_key)

    def mark_failed(self, work_key: str, *, error: str, metadata: dict[str, object]) -> None:
        del error, metadata
        self.failed.append(work_key)


class _FakeCursorStore:
    def __init__(self) -> None:
        self.state_by_key: dict[str, StreamResumeState] = {}
        self.flush_count = 0
        self.cleared: list[str] = []

    def get(self, key: str) -> StreamResumeState:
        return self.state_by_key.get(key, StreamResumeState()).clone()

    def set(self, key: str, state: StreamResumeState) -> None:
        self.state_by_key[key] = state.clone()

    def clear(self, key: str) -> None:
        self.cleared.append(key)
        self.state_by_key.pop(key, None)

    def flush(self) -> None:
        self.flush_count += 1


def _record(size: int, checksum: str = "c1") -> CanonicalRecord:
    payload = {"id": "1", "v": "x" * 10}
    return CanonicalRecord(
        source_job="mongo_users",
        source_api="mongodb",
        source_namespace="mongodb.appdb.users",
        source_key="1",
        route_key="mongodb.appdb.users|1",
        payload=payload,
        payload_size_bytes=size,
        checksum=checksum,
        event_ts="2026-03-12T00:00:00+00:00",
        watermark_value=1,
    )


def test_apply_route_moves_between_sinks(tmp_path) -> None:
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(runtime=SimpleNamespace(error_mode="fail"))
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()

    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    stats = JobStats()

    small = _record(size=500, checksum="a")
    pipeline._apply_route(job, small, stats)
    assert stats.firestore_writes == 1
    assert pipeline.firestore_sink.upserts == ["mongodb.appdb.users|1"]

    large = _record(size=2000, checksum="b")
    pipeline._apply_route(job, large, stats)
    assert stats.spanner_writes == 1
    assert stats.moved_records == 1
    assert pipeline.spanner_sink.upserts == ["mongodb.appdb.users|1"]
    assert pipeline.firestore_sink.deletes == ["mongodb.appdb.users|1"]


def test_apply_route_skips_unchanged_checksum(tmp_path) -> None:
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(runtime=SimpleNamespace(error_mode="fail"))
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    stats = JobStats()
    rec = _record(size=500, checksum="same")
    pipeline._apply_route(job, rec, stats)
    pipeline._apply_route(job, rec, stats)
    assert stats.unchanged_skipped == 1
    assert len(pipeline.firestore_sink.upserts) == 1


def test_apply_route_keeps_pending_cleanup_when_delete_fails_in_skip_mode(tmp_path) -> None:
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(runtime=SimpleNamespace(error_mode="skip"))
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()

    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    stats = JobStats()

    first = _record(size=500, checksum="a")
    pipeline._apply_route(job, first, stats)

    pipeline.firestore_sink.fail_delete_count = 1
    second = _record(size=2_000, checksum="b")
    pipeline._apply_route(job, second, stats)

    entry = pipeline.registry.get_entry(second.route_key)
    assert entry is not None
    assert entry.destination == "spanner"
    assert entry.sync_state == SYNC_STATE_PENDING_CLEANUP
    assert entry.cleanup_from_destination == "firestore"
    assert stats.cleanup_failed == 1


def test_apply_route_reconciles_pending_cleanup_then_skips_unchanged(tmp_path) -> None:
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(runtime=SimpleNamespace(error_mode="fail"))
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    stats = JobStats()
    rec = _record(size=2_000, checksum="same")
    pipeline.registry.set_entry(
        rec.route_key,
        RouteRegistryEntry(
            destination="spanner",
            checksum="same",
            payload_size_bytes=2_000,
            updated_at="2026-03-12T00:00:00+00:00",
            sync_state=SYNC_STATE_PENDING_CLEANUP,
            cleanup_from_destination="firestore",
        ),
    )
    pipeline.registry.flush()

    pipeline._apply_route(job, rec, stats)
    entry = pipeline.registry.get_entry(rec.route_key)
    assert entry is not None
    assert entry.sync_state == SYNC_STATE_COMPLETE
    assert entry.cleanup_from_destination is None
    assert stats.moved_records == 1
    assert stats.unchanged_skipped == 1
    assert pipeline.spanner_sink.upserts == []


def test_run_incremental_skips_checkpointed_records_and_advances_checkpoint(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="incremental",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.watermarks.set_checkpoint(
        job.name,
        WatermarkCheckpoint(
            watermark=100,
            route_keys=["mongodb.appdb.users|1"],
            updated_at="2026-03-12T00:00:00+00:00",
        ),
    )
    pipeline.watermarks.flush()
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline._source_iter = lambda *_args, **_kwargs: iter(
        [
            _record(size=500, checksum="c1"),
            CanonicalRecord(
                source_job="mongo_users",
                source_api="mongodb",
                source_namespace="mongodb.appdb.users",
                source_key="2",
                route_key="mongodb.appdb.users|2",
                payload={"id": "2"},
                payload_size_bytes=500,
                checksum="c2",
                event_ts="2026-03-12T00:00:00+00:00",
                watermark_value=100,
            ),
            CanonicalRecord(
                source_job="mongo_users",
                source_api="mongodb",
                source_namespace="mongodb.appdb.users",
                source_key="3",
                route_key="mongodb.appdb.users|3",
                payload={"id": "3"},
                payload_size_bytes=500,
                checksum="c3",
                event_ts="2026-03-12T00:00:01+00:00",
                watermark_value=101,
            ),
        ]
    )

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]
    checkpoint = pipeline.watermarks.get_checkpoint(job.name)

    assert stats.checkpoint_skipped == 1
    assert checkpoint.watermark == 101
    assert checkpoint.route_keys == ["mongodb.appdb.users|3"]


def test_run_incremental_holds_checkpoint_when_failures_occur(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="skip",
            mode="incremental",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.watermarks.set_checkpoint(
        job.name,
        WatermarkCheckpoint(
            watermark=100,
            route_keys=["mongodb.appdb.users|1"],
            updated_at="2026-03-12T00:00:00+00:00",
        ),
    )
    pipeline.watermarks.flush()
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.firestore_sink.fail_upsert_count = 1
    pipeline._source_iter = lambda *_args, **_kwargs: iter(
        [
            CanonicalRecord(
                source_job="mongo_users",
                source_api="mongodb",
                source_namespace="mongodb.appdb.users",
                source_key="2",
                route_key="mongodb.appdb.users|2",
                payload={"id": "2"},
                payload_size_bytes=500,
                checksum="c2",
                event_ts="2026-03-12T00:00:00+00:00",
                watermark_value=100,
            ),
            CanonicalRecord(
                source_job="mongo_users",
                source_api="mongodb",
                source_namespace="mongodb.appdb.users",
                source_key="3",
                route_key="mongodb.appdb.users|3",
                payload={"id": "3"},
                payload_size_bytes=500,
                checksum="c3",
                event_ts="2026-03-12T00:00:01+00:00",
                watermark_value=101,
            ),
        ]
    )

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]
    checkpoint = pipeline.watermarks.get_checkpoint(job.name)

    assert stats.failed_records == 1
    assert checkpoint.watermark == 100
    assert checkpoint.route_keys == ["mongodb.appdb.users|1"]


def test_run_skips_job_when_lease_is_held_elsewhere(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="full",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.coordinator = _FakeCoordinator(acquire_result=False)
    pipeline._source_iter = lambda *_args, **_kwargs: iter([_record(size=500)])

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]

    assert stats.docs_seen == 0
    assert stats.lease_conflicts == 1
    assert pipeline.coordinator.acquired == ["v2:mongo_users"]


def test_run_raises_when_distributed_lease_is_lost(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="full",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.coordinator = _FakeCoordinator(acquire_result=True, renew_result=False)
    pipeline._source_iter = lambda *_args, **_kwargs: iter([_record(size=500)])

    with pytest.raises(RuntimeError, match="Lost distributed lease"):
        pipeline.run()

    assert pipeline.coordinator.released == ["v2:mongo_users"]


def test_run_client_hash_shards_only_process_owned_shard(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
        shard_count=2,
        shard_mode="client_hash",
    )
    rec1 = CanonicalRecord(
        source_job="mongo_users",
        source_api="mongodb",
        source_namespace="mongodb.appdb.users",
        source_key="1",
        route_key="mongodb.appdb.users|1",
        payload={"id": "1"},
        payload_size_bytes=500,
        checksum="c1",
        event_ts="2026-03-12T00:00:00+00:00",
        watermark_value=10,
    )
    target_shard = stable_shard_for_text(rec1.route_key, job.shard_count)
    other_key = next(
        key
        for key in ("mongodb.appdb.users|2", "mongodb.appdb.users|3", "mongodb.appdb.users|4")
        if stable_shard_for_text(key, job.shard_count) != target_shard
    )
    rec2 = CanonicalRecord(
        source_job="mongo_users",
        source_api="mongodb",
        source_namespace="mongodb.appdb.users",
        source_key=other_key.rsplit("|", maxsplit=1)[1],
        route_key=other_key,
        payload={"id": other_key.rsplit("|", maxsplit=1)[1]},
        payload_size_bytes=500,
        checksum="c2",
        event_ts="2026-03-12T00:00:01+00:00",
        watermark_value=11,
    )

    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="incremental",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.coordinator = _FakeCoordinator(allowed_keys={f"v2:mongo_users:shard={target_shard}"})
    pipeline._source_iter = lambda *_args, **_kwargs: iter([rec1, rec2])

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]
    checkpoint = pipeline.watermarks.get_checkpoint(f"{job.name}:shard={target_shard}")

    assert stats.docs_seen == 1
    assert stats.firestore_writes == 1
    assert checkpoint.watermark == rec1.watermark_value


def test_run_skips_completed_shard_when_progress_tracker_marks_it_done(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="full",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
            run_id="full-20260313",
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.coordinator = _FakeCoordinator(completed_keys={"v2:mongo_users"})
    pipeline._source_iter = lambda *_args, **_kwargs: iter([_record(size=500)])

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]

    assert stats.docs_seen == 0
    assert stats.progress_skips == 1
    assert pipeline.coordinator.acquired == []


def test_full_run_progress_scheduler_claims_across_all_jobs(tmp_path) -> None:
    job1 = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    job2 = MongoJobConfig(
        name="mongo_orders",
        api="mongodb",
        route_namespace="mongodb.appdb.orders",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="orders",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="full",
            flush_state_each_batch=True,
            batch_size=10,
            max_records_per_job={},
            run_id="full-20260313",
        ),
        jobs=[job1, job2],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.coordinator = _FakeCoordinator(progress_enabled=True)
    pipeline._source_iter = lambda *_args, **_kwargs: iter([])

    stats_by_job = pipeline.run()

    assert set(stats_by_job) == {"mongo_users", "mongo_orders"}
    assert pipeline.coordinator.claim_requests
    assert set(pipeline.coordinator.claim_requests[0]) == {"v2:mongo_users", "v2:mongo_orders"}


def test_run_incremental_persists_and_clears_reader_cursor_on_success(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="fail",
            mode="incremental",
            flush_state_each_batch=True,
            batch_size=1,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.reader_cursors = _FakeCursorStore()

    records = [
        CanonicalRecord(
            source_job="mongo_users",
            source_api="mongodb",
            source_namespace="mongodb.appdb.users",
            source_key="2",
            route_key="mongodb.appdb.users|2",
            payload={"id": "2"},
            payload_size_bytes=500,
            checksum="c2",
            event_ts="2026-03-12T00:00:00+00:00",
            watermark_value=100,
        ),
        CanonicalRecord(
            source_job="mongo_users",
            source_api="mongodb",
            source_namespace="mongodb.appdb.users",
            source_key="3",
            route_key="mongodb.appdb.users|3",
            payload={"id": "3"},
            payload_size_bytes=500,
            checksum="c3",
            event_ts="2026-03-12T00:00:01+00:00",
            watermark_value=101,
        ),
    ]

    def source_iter(*_args, **kwargs):
        resume_state = kwargs.get("resume_state")
        assert isinstance(resume_state, StreamResumeState)
        for record in records:
            resume_state.mark(source_key=record.source_key, watermark=record.watermark_value)
            yield record

    pipeline._source_iter = source_iter

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]

    assert stats.docs_seen == 2
    assert pipeline.reader_cursors.flush_count >= 1
    assert pipeline.reader_cursors.cleared == ["v2:mongo_users"]


def test_run_incremental_keeps_last_safe_reader_cursor_on_failure(tmp_path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://x",
        database="appdb",
        collection="users",
    )
    pipeline = V2MigrationPipeline.__new__(V2MigrationPipeline)
    pipeline.config = SimpleNamespace(
        runtime=SimpleNamespace(
            error_mode="skip",
            mode="incremental",
            flush_state_each_batch=True,
            batch_size=1,
            max_records_per_job={},
        ),
        jobs=[job],
    )
    pipeline.router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1000,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        )
    )
    pipeline.registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    pipeline.watermarks = WatermarkStore(str(tmp_path / "watermarks.json"))
    pipeline.dead_letter = SimpleNamespace(write=lambda **_: None)
    pipeline.firestore_sink = _FakeSink()
    pipeline.spanner_sink = _FakeSink()
    pipeline.firestore_sink.fail_upsert_count = 1
    pipeline.reader_cursors = _FakeCursorStore()

    records = [
        CanonicalRecord(
            source_job="mongo_users",
            source_api="mongodb",
            source_namespace="mongodb.appdb.users",
            source_key="2",
            route_key="mongodb.appdb.users|2",
            payload={"id": "2"},
            payload_size_bytes=500,
            checksum="c2",
            event_ts="2026-03-12T00:00:00+00:00",
            watermark_value=100,
        ),
        CanonicalRecord(
            source_job="mongo_users",
            source_api="mongodb",
            source_namespace="mongodb.appdb.users",
            source_key="3",
            route_key="mongodb.appdb.users|3",
            payload={"id": "3"},
            payload_size_bytes=500,
            checksum="c3",
            event_ts="2026-03-12T00:00:01+00:00",
            watermark_value=101,
        ),
    ]

    def source_iter(*_args, **kwargs):
        resume_state = kwargs.get("resume_state")
        assert isinstance(resume_state, StreamResumeState)
        for record in records:
            resume_state.mark(source_key=record.source_key, watermark=record.watermark_value)
            yield record

    pipeline._source_iter = source_iter

    stats_by_job = pipeline.run()
    stats = stats_by_job[job.name]

    assert stats.failed_records == 1
    saved = pipeline.reader_cursors.state_by_key["v2:mongo_users"]
    assert saved.last_source_key == "3"
    assert pipeline.reader_cursors.cleared == []

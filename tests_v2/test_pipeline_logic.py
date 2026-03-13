from __future__ import annotations

from types import SimpleNamespace

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

from __future__ import annotations

from types import SimpleNamespace

from migration_v2.config import MongoJobConfig, RoutingConfig
from migration_v2.models import CanonicalRecord
from migration_v2.pipeline import JobStats, V2MigrationPipeline
from migration_v2.router import SizeRouter
from migration_v2.state_store import RouteRegistryStore


class _FakeSink:
    def __init__(self) -> None:
        self.upserts: list[str] = []
        self.deletes: list[str] = []

    def upsert(self, record: CanonicalRecord) -> None:
        self.upserts.append(record.route_key)

    def delete(self, record: CanonicalRecord) -> None:
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


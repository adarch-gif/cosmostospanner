from __future__ import annotations

from pathlib import Path

from migration_v2.config import (
    CassandraJobConfig,
    FirestoreTargetConfig,
    MongoJobConfig,
    PipelineV2Config,
    RoutingConfig,
    RuntimeV2Config,
    SpannerTargetConfig,
)
from migration_v2.models import CanonicalRecord, RoutedSinkRecord
from migration_v2.reconciliation import SqliteRouteDigestStore, V2ReconciliationRunner
from migration_v2.state_store import (
    SYNC_STATE_COMPLETE,
    SYNC_STATE_PENDING_CLEANUP,
    RouteRegistryEntry,
    RouteRegistryStore,
)


class _FakeMongoSource:
    def __init__(self, records_by_shard: dict[int | None, list[CanonicalRecord]]) -> None:
        self.records_by_shard = records_by_shard

    def iter_records(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
        shard_index: int | None = None,
        shard_count: int = 1,
    ):
        del job, mode, watermark, max_records, shard_count
        yield from self.records_by_shard.get(shard_index, [])


class _EmptyCassandraSource:
    def iter_records(self, *_args, **_kwargs):
        return iter(())


class _FakeSink:
    def __init__(self, records: list[RoutedSinkRecord]) -> None:
        self.records = records

    def iter_records(self):
        yield from self.records


def _source_record(
    route_key: str,
    *,
    source_key: str,
    payload_size_bytes: int,
    checksum: str,
    event_ts: str = "2026-03-13T00:00:00+00:00",
) -> CanonicalRecord:
    return CanonicalRecord(
        source_job="mongo_users",
        source_api="mongodb",
        source_namespace="mongo.users",
        source_key=source_key,
        route_key=route_key,
        payload={"id": source_key},
        payload_size_bytes=payload_size_bytes,
        checksum=checksum,
        event_ts=event_ts,
    )


def _sink_record(
    destination: str,
    route_key: str,
    *,
    source_key: str,
    checksum: str,
    stored_checksum: str | None = None,
    payload_size_bytes: int = 10,
    stored_payload_size_bytes: int | None = None,
    event_ts: str = "2026-03-13T00:00:00+00:00",
) -> RoutedSinkRecord:
    record = CanonicalRecord(
        source_job="mongo_users",
        source_api="mongodb",
        source_namespace="mongo.users",
        source_key=source_key,
        route_key=route_key,
        payload={"id": source_key},
        payload_size_bytes=payload_size_bytes,
        checksum=checksum,
        event_ts=event_ts,
    )
    return RoutedSinkRecord(
        destination=destination,
        record=record,
        stored_checksum=stored_checksum or checksum,
        stored_payload_size_bytes=stored_payload_size_bytes or payload_size_bytes,
    )


def _runtime(tmp_path: Path) -> RuntimeV2Config:
    return RuntimeV2Config(
        log_level="INFO",
        route_registry_file=str(tmp_path / "registry.json"),
        retry_attempts=1,
        retry_initial_delay_seconds=0.0,
        retry_max_delay_seconds=0.0,
        retry_backoff_multiplier=1.0,
        retry_jitter_seconds=0.0,
    )


def test_sqlite_route_digest_store_reports_exact_mismatch_classes() -> None:
    with SqliteRouteDigestStore() as store:
        store.upsert_expected("mongo.users|1", route_digest="a" * 64, target_digest="b" * 64)
        store.upsert_expected("mongo.users|2", route_digest="c" * 64, target_digest="d" * 64)
        store.upsert_registry(
            "mongo.users|1",
            registry_digest="a" * 64,
            sync_state=SYNC_STATE_COMPLETE,
        )
        store.upsert_registry(
            "mongo.users|2",
            registry_digest="e" * 64,
            sync_state=SYNC_STATE_PENDING_CLEANUP,
        )
        store.upsert_registry(
            "mongo.users|3",
            registry_digest="f" * 64,
            sync_state=SYNC_STATE_COMPLETE,
        )
        store.upsert_target("mongo.users|1", target_digest="b" * 64)
        store.upsert_target("mongo.users|2", target_digest="f" * 64)
        store.upsert_target("mongo.users|4", target_digest="0" * 64)

        summary = store.summarize(source_rejected_rows=1, target_metadata_mismatches=2)

    assert summary.source_count == 2
    assert summary.registry_count == 3
    assert summary.target_count == 3
    assert summary.source_rejected_rows == 1
    assert summary.missing_registry_rows == 0
    assert summary.extra_registry_rows == 1
    assert summary.mismatched_registry_rows == 1
    assert summary.missing_target_rows == 0
    assert summary.extra_target_rows == 1
    assert summary.mismatched_target_rows == 1
    assert summary.pending_cleanup_rows == 1
    assert summary.target_metadata_mismatches == 2


def test_reconciliation_runner_classifies_registry_target_and_metadata_issues(tmp_path: Path) -> None:
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongo.users",
        key_fields=["id"],
        connection_string="mongodb://example",
        database="app",
        collection="users",
        shard_count=2,
        shard_mode="query_template",
        source_query='{"shard": {{SHARD_INDEX}}}',
    )
    config = PipelineV2Config(
        runtime=_runtime(tmp_path),
        routing=RoutingConfig(
            firestore_lt_bytes=100,
            spanner_max_payload_bytes=10_000,
            payload_size_overhead_bytes=0,
        ),
        firestore_target=FirestoreTargetConfig(project="p"),
        spanner_target=SpannerTargetConfig(project="p", instance="i", database="d", table="t"),
        jobs=[job],
    )

    registry = RouteRegistryStore(str(tmp_path / "registry.json"))
    registry.set_entry(
        "mongo.users|1",
        RouteRegistryEntry(
            destination="firestore",
            checksum="a" * 64,
            payload_size_bytes=10,
            updated_at="2026-03-13T00:00:00+00:00",
            sync_state=SYNC_STATE_COMPLETE,
        ),
    )
    registry.set_entry(
        "mongo.users|2",
        RouteRegistryEntry(
            destination="spanner",
            checksum="wrong",
            payload_size_bytes=500,
            updated_at="2026-03-13T00:00:00+00:00",
            sync_state=SYNC_STATE_PENDING_CLEANUP,
        ),
    )
    registry.flush()

    runner = V2ReconciliationRunner(
        config,
        registry=registry,
        mongo_source=_FakeMongoSource(
            {
                0: [
                    _source_record(
                        "mongo.users|1",
                        source_key="1",
                        payload_size_bytes=10,
                        checksum="a" * 64,
                    )
                ],
                1: [
                    _source_record(
                        "mongo.users|2",
                        source_key="2",
                        payload_size_bytes=500,
                        checksum="b" * 64,
                    ),
                    _source_record(
                        "mongo.users|3",
                        source_key="3",
                        payload_size_bytes=20_000,
                        checksum="c" * 64,
                    ),
                ],
            }
        ),
        cassandra_source=_EmptyCassandraSource(),
        firestore_sink=_FakeSink(
            [
                _sink_record(
                    "firestore",
                    "mongo.users|1",
                    source_key="1",
                    checksum="a" * 64,
                    payload_size_bytes=10,
                ),
                _sink_record(
                    "firestore",
                    "mongo.users|extra",
                    source_key="extra",
                    checksum="d" * 64,
                    payload_size_bytes=10,
                ),
            ]
        ),
        spanner_sink=_FakeSink(
            [
                _sink_record(
                    "spanner",
                    "mongo.users|2",
                    source_key="2",
                    checksum="b" * 64,
                    stored_checksum="mismatch",
                    payload_size_bytes=500,
                )
            ]
        ),
    )

    summaries = runner.validate()
    summary = summaries[job.name]

    assert summary.source_count == 2
    assert summary.registry_count == 2
    assert summary.target_count == 3
    assert summary.source_rejected_rows == 1
    assert summary.missing_registry_rows == 0
    assert summary.extra_registry_rows == 0
    assert summary.mismatched_registry_rows == 1
    assert summary.missing_target_rows == 0
    assert summary.extra_target_rows == 1
    assert summary.mismatched_target_rows == 0
    assert summary.pending_cleanup_rows == 1
    assert summary.target_metadata_mismatches == 1

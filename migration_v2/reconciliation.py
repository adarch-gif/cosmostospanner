from __future__ import annotations

import os
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import Iterable, Protocol

from migration.reconciliation import DigestAccumulator, row_digest
from migration.retry_utils import RetryPolicy
from migration.sharding import stable_shard_for_text
from migration_v2.config import CassandraJobConfig, MongoJobConfig, PipelineV2Config
from migration_v2.models import CanonicalRecord, RoutedSinkRecord
from migration_v2.router import RouteDestination, SizeRouter
from migration_v2.sink_adapters.firestore_sink import FirestoreSinkAdapter
from migration_v2.sink_adapters.spanner_sink import SpannerSinkAdapter
from migration_v2.source_adapters.base import SourceAdapter
from migration_v2.source_adapters.cassandra_cosmos import CassandraCosmosSourceAdapter
from migration_v2.source_adapters.mongo_cosmos import MongoCosmosSourceAdapter
from migration_v2.state_store import (
    SYNC_STATE_PENDING_CLEANUP,
    RouteRegistryEntry,
    RouteRegistryStore,
)

ROUTE_COMPARE_COLUMNS = [
    "destination",
    "checksum",
    "payload_size_bytes",
]

TARGET_COMPARE_COLUMNS = [
    "destination",
    "source_job",
    "source_api",
    "source_namespace",
    "source_key",
    "checksum",
    "payload_size_bytes",
    "event_ts",
]


class SinkReader(Protocol):
    def iter_records(self) -> Iterable[RoutedSinkRecord]:
        ...


def _expected_route_row(record: CanonicalRecord, destination: str) -> dict[str, object]:
    return {
        "destination": destination,
        "checksum": record.checksum,
        "payload_size_bytes": record.payload_size_bytes,
    }


def _expected_target_row(record: CanonicalRecord, destination: str) -> dict[str, object]:
    return {
        "destination": destination,
        "source_job": record.source_job,
        "source_api": record.source_api,
        "source_namespace": record.source_namespace,
        "source_key": record.source_key,
        "checksum": record.checksum,
        "payload_size_bytes": record.payload_size_bytes,
        "event_ts": record.event_ts,
    }


def _registry_row(entry: RouteRegistryEntry) -> dict[str, object]:
    return {
        "destination": entry.destination,
        "checksum": entry.checksum,
        "payload_size_bytes": entry.payload_size_bytes,
    }


def _target_row(record: RoutedSinkRecord) -> dict[str, object]:
    return {
        "destination": record.destination,
        "source_job": record.record.source_job,
        "source_api": record.record.source_api,
        "source_namespace": record.record.source_namespace,
        "source_key": record.record.source_key,
        "checksum": record.record.checksum,
        "payload_size_bytes": record.record.payload_size_bytes,
        "event_ts": record.record.event_ts,
    }


@dataclass(frozen=True)
class RoutedValidationSummary:
    source_count: int
    registry_count: int
    target_count: int
    source_rejected_rows: int
    missing_registry_rows: int
    extra_registry_rows: int
    mismatched_registry_rows: int
    missing_target_rows: int
    extra_target_rows: int
    mismatched_target_rows: int
    pending_cleanup_rows: int
    target_metadata_mismatches: int
    source_route_checksum: str
    registry_checksum: str
    source_target_checksum: str
    target_checksum: str


class SqliteRouteDigestStore:
    def __init__(self, temp_dir: str | None = None) -> None:
        fd, path = tempfile.mkstemp(
            prefix="v2_reconciliation_",
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
            CREATE TABLE IF NOT EXISTS route_digests (
                key_text TEXT PRIMARY KEY,
                expected_route_digest TEXT,
                expected_target_digest TEXT,
                registry_digest TEXT,
                target_digest TEXT,
                registry_sync_state TEXT
            )
            """
        )
        self._conn.commit()

    def __enter__(self) -> "SqliteRouteDigestStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()
        if os.path.exists(self._path):
            os.remove(self._path)

    def upsert_expected(
        self,
        route_key: str,
        *,
        route_digest: str,
        target_digest: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO route_digests (key_text, expected_route_digest, expected_target_digest)
            VALUES (?, ?, ?)
            ON CONFLICT(key_text) DO UPDATE SET
                expected_route_digest = excluded.expected_route_digest,
                expected_target_digest = excluded.expected_target_digest
            """,
            (route_key, route_digest, target_digest),
        )

    def upsert_registry(
        self,
        route_key: str,
        *,
        registry_digest: str,
        sync_state: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO route_digests (key_text, registry_digest, registry_sync_state)
            VALUES (?, ?, ?)
            ON CONFLICT(key_text) DO UPDATE SET
                registry_digest = excluded.registry_digest,
                registry_sync_state = excluded.registry_sync_state
            """,
            (route_key, registry_digest, sync_state),
        )

    def upsert_target(self, route_key: str, *, target_digest: str) -> None:
        self._conn.execute(
            """
            INSERT INTO route_digests (key_text, target_digest)
            VALUES (?, ?)
            ON CONFLICT(key_text) DO UPDATE SET
                target_digest = excluded.target_digest
            """,
            (route_key, target_digest),
        )

    def flush(self) -> None:
        self._conn.commit()

    def summarize(
        self,
        *,
        source_rejected_rows: int = 0,
        target_metadata_mismatches: int = 0,
    ) -> RoutedValidationSummary:
        self.flush()
        return RoutedValidationSummary(
            source_count=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE expected_target_digest IS NOT NULL"
            ),
            registry_count=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE registry_digest IS NOT NULL"
            ),
            target_count=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE target_digest IS NOT NULL"
            ),
            source_rejected_rows=source_rejected_rows,
            missing_registry_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_route_digest IS NOT NULL AND registry_digest IS NULL"
            ),
            extra_registry_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_route_digest IS NULL AND registry_digest IS NOT NULL"
            ),
            mismatched_registry_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_route_digest IS NOT NULL AND registry_digest IS NOT NULL AND "
                "expected_route_digest != registry_digest"
            ),
            missing_target_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_target_digest IS NOT NULL AND target_digest IS NULL"
            ),
            extra_target_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_target_digest IS NULL AND target_digest IS NOT NULL"
            ),
            mismatched_target_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE "
                "expected_target_digest IS NOT NULL AND target_digest IS NOT NULL AND "
                "expected_target_digest != target_digest"
            ),
            pending_cleanup_rows=self._count_rows(
                "SELECT COUNT(1) FROM route_digests WHERE registry_sync_state = ?",
                (SYNC_STATE_PENDING_CLEANUP,),
            ),
            target_metadata_mismatches=target_metadata_mismatches,
            source_route_checksum=self._aggregate_checksum("expected_route_digest"),
            registry_checksum=self._aggregate_checksum("registry_digest"),
            source_target_checksum=self._aggregate_checksum("expected_target_digest"),
            target_checksum=self._aggregate_checksum("target_digest"),
        )

    def _count_rows(self, sql: str, params: tuple[object, ...] = ()) -> int:
        row = self._conn.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def _aggregate_checksum(self, column_name: str) -> str:
        aggregate = DigestAccumulator()
        allowed_columns = {
            "expected_route_digest",
            "registry_digest",
            "expected_target_digest",
            "target_digest",
        }
        if column_name not in allowed_columns:
            raise ValueError(f"Unsupported digest column: {column_name}")
        sql = (
            f"SELECT key_text, {column_name} FROM route_digests "  # nosec B608
            f"WHERE {column_name} IS NOT NULL ORDER BY key_text"
        )
        rows = self._conn.execute(sql)
        for key_text, digest in rows:
            aggregate.update_text(str(key_text), str(digest))
        return aggregate.hexdigest()


class V2ReconciliationRunner:
    def __init__(
        self,
        config: PipelineV2Config,
        *,
        router: SizeRouter | None = None,
        registry: RouteRegistryStore | None = None,
        firestore_sink: SinkReader | None = None,
        spanner_sink: SinkReader | None = None,
        mongo_source: SourceAdapter | None = None,
        cassandra_source: SourceAdapter | None = None,
    ) -> None:
        self.config = config
        self.router = router or SizeRouter(config.routing)
        self.registry = registry or RouteRegistryStore(config.runtime.route_registry_file)
        retry_policy = RetryPolicy.from_runtime(config.runtime)
        self.firestore_sink = firestore_sink or FirestoreSinkAdapter(
            config.firestore_target,
            retry_policy=retry_policy,
            dry_run=False,
        )
        self.spanner_sink = spanner_sink or SpannerSinkAdapter(
            config.spanner_target,
            retry_policy=retry_policy,
            dry_run=False,
        )
        self.mongo_source = mongo_source or MongoCosmosSourceAdapter(retry_policy=retry_policy)
        self.cassandra_source = cassandra_source or CassandraCosmosSourceAdapter(
            retry_policy=retry_policy
        )

    def _selected_jobs(
        self,
        job_names: list[str] | None = None,
    ) -> list[MongoJobConfig | CassandraJobConfig]:
        enabled_jobs = [job for job in self.config.jobs if job.enabled]
        if not job_names:
            return enabled_jobs
        allow = set(job_names)
        return [job for job in enabled_jobs if job.name in allow]

    def _source_iter(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        *,
        shard_index: int | None = None,
    ) -> Iterable[CanonicalRecord]:
        if isinstance(job, MongoJobConfig):
            return self.mongo_source.iter_records(
                job,
                mode="full",
                watermark=None,
                max_records=None,
                shard_index=shard_index,
                shard_count=job.shard_count,
            )
        return self.cassandra_source.iter_records(
            job,
            mode="full",
            watermark=None,
            max_records=None,
            shard_index=shard_index,
            shard_count=job.shard_count,
        )

    def _iter_job_source_records(
        self,
        job: MongoJobConfig | CassandraJobConfig,
    ) -> Iterable[CanonicalRecord]:
        if job.shard_count <= 1:
            yield from self._source_iter(job, shard_index=None)
            return

        for shard_index in range(job.shard_count):
            for record in self._source_iter(job, shard_index=shard_index):
                if (
                    job.shard_mode == "client_hash"
                    and stable_shard_for_text(record.route_key, job.shard_count) != shard_index
                ):
                    continue
                yield record

    def _namespace_prefixes(
        self,
        jobs: list[MongoJobConfig | CassandraJobConfig],
    ) -> dict[str, str]:
        return {f"{job.route_namespace}|": job.name for job in jobs}

    def _job_name_for_route_key(
        self,
        route_key: str,
        namespace_prefixes: dict[str, str],
    ) -> str | None:
        for prefix in sorted(namespace_prefixes, key=len, reverse=True):
            if route_key.startswith(prefix):
                return namespace_prefixes[prefix]
        return None

    def validate(
        self,
        *,
        job_names: list[str] | None = None,
    ) -> dict[str, RoutedValidationSummary]:
        jobs = self._selected_jobs(job_names)
        if not jobs:
            raise ValueError("No v2 jobs selected. Check job filters or enabled flags.")

        namespace_prefixes = self._namespace_prefixes(jobs)
        stores = {job.name: SqliteRouteDigestStore() for job in jobs}
        source_rejected_rows = {job.name: 0 for job in jobs}
        target_metadata_mismatches = {job.name: 0 for job in jobs}

        try:
            for job in jobs:
                store = stores[job.name]
                for record in self._iter_job_source_records(job):
                    decision = self.router.decide(record.payload_size_bytes)
                    if decision.destination == RouteDestination.REJECT:
                        source_rejected_rows[job.name] += 1
                        continue
                    destination = decision.destination.value
                    store.upsert_expected(
                        record.route_key,
                        route_digest=row_digest(
                            _expected_route_row(record, destination),
                            ROUTE_COMPARE_COLUMNS,
                        ),
                        target_digest=row_digest(
                            _expected_target_row(record, destination),
                            TARGET_COMPARE_COLUMNS,
                        ),
                    )

            for route_key, entry in self.registry.items():
                job_name = self._job_name_for_route_key(route_key, namespace_prefixes)
                if not job_name:
                    continue
                stores[job_name].upsert_registry(
                    route_key,
                    registry_digest=row_digest(_registry_row(entry), ROUTE_COMPARE_COLUMNS),
                    sync_state=entry.sync_state,
                )

            for sink_record in self.firestore_sink.iter_records():
                job_name = self._job_name_for_route_key(
                    sink_record.record.route_key,
                    namespace_prefixes,
                )
                if not job_name:
                    continue
                if (
                    sink_record.stored_checksum != sink_record.record.checksum
                    or sink_record.stored_payload_size_bytes != sink_record.record.payload_size_bytes
                ):
                    target_metadata_mismatches[job_name] += 1
                stores[job_name].upsert_target(
                    sink_record.record.route_key,
                    target_digest=row_digest(_target_row(sink_record), TARGET_COMPARE_COLUMNS),
                )

            for sink_record in self.spanner_sink.iter_records():
                job_name = self._job_name_for_route_key(
                    sink_record.record.route_key,
                    namespace_prefixes,
                )
                if not job_name:
                    continue
                if (
                    sink_record.stored_checksum != sink_record.record.checksum
                    or sink_record.stored_payload_size_bytes != sink_record.record.payload_size_bytes
                ):
                    target_metadata_mismatches[job_name] += 1
                stores[job_name].upsert_target(
                    sink_record.record.route_key,
                    target_digest=row_digest(_target_row(sink_record), TARGET_COMPARE_COLUMNS),
                )

            return {
                job.name: stores[job.name].summarize(
                    source_rejected_rows=source_rejected_rows[job.name],
                    target_metadata_mismatches=target_metadata_mismatches[job.name],
                )
                for job in jobs
            }
        finally:
            for store in stores.values():
                store.close()

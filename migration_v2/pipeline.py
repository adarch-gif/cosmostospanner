from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable

from migration.dead_letter import DeadLetterSink
from migration.retry_utils import RetryPolicy
from migration_v2.config import (
    CassandraJobConfig,
    MongoJobConfig,
    PipelineV2Config,
)
from migration_v2.models import CanonicalRecord, utc_now_iso
from migration_v2.router import RouteDestination, SizeRouter
from migration_v2.sink_adapters.firestore_sink import FirestoreSinkAdapter
from migration_v2.sink_adapters.spanner_sink import SpannerSinkAdapter
from migration_v2.source_adapters.cassandra_cosmos import CassandraCosmosSourceAdapter
from migration_v2.source_adapters.mongo_cosmos import MongoCosmosSourceAdapter
from migration_v2.state_store import RouteRegistryEntry, RouteRegistryStore, WatermarkStore

LOGGER = logging.getLogger("v2.pipeline")


@dataclass
class JobStats:
    docs_seen: int = 0
    firestore_writes: int = 0
    spanner_writes: int = 0
    moved_records: int = 0
    unchanged_skipped: int = 0
    rejected_records: int = 0
    failed_records: int = 0
    cleanup_failed: int = 0
    max_watermark: Any = None


class V2MigrationPipeline:
    def __init__(self, config: PipelineV2Config) -> None:
        self.config = config
        self.router = SizeRouter(config.routing)
        self.watermarks = WatermarkStore(config.runtime.state_file)
        self.registry = RouteRegistryStore(config.runtime.route_registry_file)
        self.dead_letter = DeadLetterSink(config.runtime.dlq_file_path)

        retry_policy = RetryPolicy.from_runtime(config.runtime)
        self.firestore_sink = FirestoreSinkAdapter(
            config.firestore_target,
            retry_policy=retry_policy,
            dry_run=config.runtime.dry_run,
        )
        self.spanner_sink = SpannerSinkAdapter(
            config.spanner_target,
            retry_policy=retry_policy,
            dry_run=config.runtime.dry_run,
        )
        self.mongo_source = MongoCosmosSourceAdapter()
        self.cassandra_source = CassandraCosmosSourceAdapter()

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
        watermark: Any,
        max_records: int | None,
    ) -> Iterable[CanonicalRecord]:
        if isinstance(job, MongoJobConfig):
            return self.mongo_source.iter_records(
                job,
                mode=self.config.runtime.mode,
                watermark=watermark,
                max_records=max_records,
            )
        return self.cassandra_source.iter_records(
            job,
            mode=self.config.runtime.mode,
            watermark=watermark,
            max_records=max_records,
        )

    def _is_newer_watermark(self, candidate: Any, current: Any) -> bool:
        if candidate is None:
            return False
        if current is None:
            return True
        try:
            return candidate > current
        except TypeError:
            return str(candidate) > str(current)

    def _write_dead_letter(
        self,
        *,
        stage: str,
        job_name: str,
        source_namespace: str,
        error: Exception,
        record: CanonicalRecord | None = None,
    ) -> None:
        self.dead_letter.write(
            stage=stage,
            mapping_name=job_name,
            source_container=source_namespace,
            target_table="firestore|spanner",
            error=error,
            source_document=record.payload if record else None,
        )

    def _apply_route(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        record: CanonicalRecord,
        stats: JobStats,
    ) -> None:
        decision = self.router.decide(record.payload_size_bytes)
        if decision.destination == RouteDestination.REJECT:
            stats.rejected_records += 1
            error = ValueError(
                f"Route rejected for {record.route_key}: {decision.reason}"
            )
            if self.config.runtime.error_mode == "skip":
                self._write_dead_letter(
                    stage="route_reject",
                    job_name=job.name,
                    source_namespace=record.source_namespace,
                    error=error,
                    record=record,
                )
                return
            raise error

        previous = self.registry.get_entry(record.route_key)
        new_destination = decision.destination.value
        if previous and previous.destination == new_destination and previous.checksum == record.checksum:
            stats.unchanged_skipped += 1
            return

        destination_sink = (
            self.firestore_sink
            if decision.destination == RouteDestination.FIRESTORE
            else self.spanner_sink
        )
        old_sink = None
        if previous and previous.destination != new_destination:
            old_sink = (
                self.firestore_sink
                if previous.destination == RouteDestination.FIRESTORE.value
                else self.spanner_sink
            )

        try:
            destination_sink.upsert(record)
        except Exception as exc:  # noqa: BLE001
            stats.failed_records += 1
            if self.config.runtime.error_mode == "skip":
                self._write_dead_letter(
                    stage="route_write",
                    job_name=job.name,
                    source_namespace=record.source_namespace,
                    error=exc,
                    record=record,
                )
                return
            raise

        if decision.destination == RouteDestination.FIRESTORE:
            stats.firestore_writes += 1
        else:
            stats.spanner_writes += 1

        if old_sink:
            try:
                old_sink.delete(record)
                stats.moved_records += 1
            except Exception as exc:  # noqa: BLE001
                stats.cleanup_failed += 1
                if self.config.runtime.error_mode == "skip":
                    self._write_dead_letter(
                        stage="route_cleanup",
                        job_name=job.name,
                        source_namespace=record.source_namespace,
                        error=exc,
                        record=record,
                    )
                    return
                raise

        self.registry.set_entry(
            record.route_key,
            RouteRegistryEntry(
                destination=new_destination,
                checksum=record.checksum,
                payload_size_bytes=record.payload_size_bytes,
                updated_at=utc_now_iso(),
            ),
        )

    def run(self, *, job_names: list[str] | None = None) -> dict[str, JobStats]:
        jobs = self._selected_jobs(job_names)
        if not jobs:
            raise ValueError("No v2 jobs selected. Check job filters or enabled flags.")

        all_stats: dict[str, JobStats] = {}
        for job in jobs:
            watermark = (
                self.watermarks.get(job.name)
                if self.config.runtime.mode == "incremental"
                else None
            )
            max_records = self.config.runtime.max_records_per_job.get(job.name)
            stats = JobStats(max_watermark=watermark)
            LOGGER.info(
                "Starting v2 job %s api=%s mode=%s watermark=%s",
                job.name,
                job.api,
                self.config.runtime.mode,
                watermark,
            )
            for record in self._source_iter(job, watermark=watermark, max_records=max_records):
                stats.docs_seen += 1
                self._apply_route(job, record, stats)
                if self._is_newer_watermark(record.watermark_value, stats.max_watermark):
                    stats.max_watermark = record.watermark_value
                if (
                    self.config.runtime.flush_state_each_batch
                    and stats.docs_seen % self.config.runtime.batch_size == 0
                ):
                    if self.config.runtime.mode == "incremental" and stats.max_watermark is not None:
                        self.watermarks.set(job.name, stats.max_watermark)
                    self.registry.flush()
                    self.watermarks.flush()

            if self.config.runtime.mode == "incremental" and stats.max_watermark is not None:
                self.watermarks.set(job.name, stats.max_watermark)
            self.registry.flush()
            self.watermarks.flush()
            LOGGER.info(
                "Completed v2 job %s | seen=%s firestore=%s spanner=%s moved=%s unchanged=%s rejected=%s failed=%s cleanup_failed=%s watermark=%s",
                job.name,
                stats.docs_seen,
                stats.firestore_writes,
                stats.spanner_writes,
                stats.moved_records,
                stats.unchanged_skipped,
                stats.rejected_records,
                stats.failed_records,
                stats.cleanup_failed,
                stats.max_watermark,
            )
            all_stats[job.name] = stats

        return all_stats

    def preflight(self, *, job_names: list[str] | None = None, check_sources: bool = True) -> list[str]:
        issues: list[str] = []
        ok, msg = self.firestore_sink.preflight_check()
        if not ok:
            issues.append(msg)
        ok, msg = self.spanner_sink.preflight_check()
        if not ok:
            issues.append(msg)

        jobs = self._selected_jobs(job_names)
        if check_sources:
            for job in jobs:
                try:
                    iterator = self._source_iter(job, watermark=None, max_records=1)
                    next(iter(iterator), None)
                except Exception as exc:  # noqa: BLE001
                    issues.append(f"Source preflight failed for job {job.name}: {exc}")
        return issues


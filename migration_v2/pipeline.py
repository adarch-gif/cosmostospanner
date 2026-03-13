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
from migration_v2.state_store import (
    SYNC_STATE_COMPLETE,
    SYNC_STATE_PENDING_CLEANUP,
    RouteRegistryEntry,
    RouteRegistryStore,
    WatermarkCheckpoint,
    WatermarkStore,
    compare_watermark_values,
)

LOGGER = logging.getLogger("v2.pipeline")


@dataclass
class JobStats:
    docs_seen: int = 0
    firestore_writes: int = 0
    spanner_writes: int = 0
    moved_records: int = 0
    unchanged_skipped: int = 0
    checkpoint_skipped: int = 0
    out_of_order_records: int = 0
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
        return compare_watermark_values(candidate, current) > 0

    def _checkpoint_contains(
        self,
        checkpoint: WatermarkCheckpoint,
        record: CanonicalRecord,
    ) -> bool:
        if checkpoint.watermark is None or record.watermark_value is None:
            return False
        comparison = compare_watermark_values(record.watermark_value, checkpoint.watermark)
        if comparison < 0:
            return True
        if comparison == 0 and record.route_key in checkpoint.route_keys:
            return True
        return False

    def _advance_checkpoint(
        self,
        checkpoint: WatermarkCheckpoint,
        record: CanonicalRecord,
    ) -> WatermarkCheckpoint:
        if record.watermark_value is None:
            return checkpoint
        if checkpoint.watermark is None:
            return WatermarkCheckpoint(
                watermark=record.watermark_value,
                route_keys=[record.route_key],
                updated_at=utc_now_iso(),
            )
        comparison = compare_watermark_values(record.watermark_value, checkpoint.watermark)
        if comparison > 0:
            return WatermarkCheckpoint(
                watermark=record.watermark_value,
                route_keys=[record.route_key],
                updated_at=utc_now_iso(),
            )
        if comparison == 0:
            route_keys = sorted(set(checkpoint.route_keys) | {record.route_key})
            return WatermarkCheckpoint(
                watermark=checkpoint.watermark,
                route_keys=route_keys,
                updated_at=utc_now_iso(),
            )
        return checkpoint

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

    def _sink_for_destination(self, destination: str) -> FirestoreSinkAdapter | SpannerSinkAdapter:
        if destination == RouteDestination.FIRESTORE.value:
            return self.firestore_sink
        if destination == RouteDestination.SPANNER.value:
            return self.spanner_sink
        raise ValueError(f"Unsupported destination in route registry: {destination}")

    def _finalize_pending_cleanup(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        record: CanonicalRecord,
        previous: RouteRegistryEntry,
        stats: JobStats,
    ) -> bool:
        if previous.sync_state != SYNC_STATE_PENDING_CLEANUP:
            return True

        cleanup_from = previous.cleanup_from_destination
        if not cleanup_from:
            LOGGER.warning(
                "Route registry entry %s has pending_cleanup without cleanup_from_destination. "
                "Marking as complete.",
                record.route_key,
            )
            self.registry.set_entry(
                record.route_key,
                RouteRegistryEntry(
                    destination=previous.destination,
                    checksum=previous.checksum,
                    payload_size_bytes=previous.payload_size_bytes,
                    updated_at=utc_now_iso(),
                    sync_state=SYNC_STATE_COMPLETE,
                    cleanup_from_destination=None,
                ),
            )
            self.registry.flush()
            return True

        cleanup_sink = self._sink_for_destination(cleanup_from)
        try:
            cleanup_sink.delete(record)
            stats.moved_records += 1
        except Exception as exc:  # noqa: BLE001
            stats.cleanup_failed += 1
            if self.config.runtime.error_mode == "skip":
                self._write_dead_letter(
                    stage="route_cleanup_reconcile",
                    job_name=job.name,
                    source_namespace=record.source_namespace,
                    error=exc,
                    record=record,
                )
                return False
            raise

        self.registry.set_entry(
            record.route_key,
            RouteRegistryEntry(
                destination=previous.destination,
                checksum=previous.checksum,
                payload_size_bytes=previous.payload_size_bytes,
                updated_at=utc_now_iso(),
                sync_state=SYNC_STATE_COMPLETE,
                cleanup_from_destination=None,
            ),
        )
        self.registry.flush()
        return True

    def _apply_route(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        record: CanonicalRecord,
        stats: JobStats,
    ) -> bool:
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
                return False
            raise error

        previous = self.registry.get_entry(record.route_key)
        if previous and previous.sync_state == SYNC_STATE_PENDING_CLEANUP:
            cleanup_ok = self._finalize_pending_cleanup(job, record, previous, stats)
            if not cleanup_ok:
                return False
            previous = self.registry.get_entry(record.route_key)

        new_destination = decision.destination.value
        if (
            previous
            and previous.sync_state == SYNC_STATE_COMPLETE
            and previous.destination == new_destination
            and previous.checksum == record.checksum
        ):
            stats.unchanged_skipped += 1
            return True

        destination_sink = self._sink_for_destination(new_destination)
        old_sink = None
        old_destination = None
        if previous and previous.destination != new_destination:
            old_destination = previous.destination
            old_sink = self._sink_for_destination(old_destination)

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
                return False
            raise

        if decision.destination == RouteDestination.FIRESTORE:
            stats.firestore_writes += 1
        else:
            stats.spanner_writes += 1

        if old_sink:
            # Persist move-intent before cleanup so interrupted runs can reconcile deterministically.
            self.registry.set_entry(
                record.route_key,
                RouteRegistryEntry(
                    destination=new_destination,
                    checksum=record.checksum,
                    payload_size_bytes=record.payload_size_bytes,
                    updated_at=utc_now_iso(),
                    sync_state=SYNC_STATE_PENDING_CLEANUP,
                    cleanup_from_destination=old_destination,
                ),
            )
            self.registry.flush()
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
                    return False
                raise

        self.registry.set_entry(
            record.route_key,
            RouteRegistryEntry(
                destination=new_destination,
                checksum=record.checksum,
                payload_size_bytes=record.payload_size_bytes,
                updated_at=utc_now_iso(),
                sync_state=SYNC_STATE_COMPLETE,
                cleanup_from_destination=None,
            ),
        )
        if old_sink:
            self.registry.flush()
        return True

    def run(self, *, job_names: list[str] | None = None) -> dict[str, JobStats]:
        jobs = self._selected_jobs(job_names)
        if not jobs:
            raise ValueError("No v2 jobs selected. Check job filters or enabled flags.")

        all_stats: dict[str, JobStats] = {}
        for job in jobs:
            stored_checkpoint = (
                self.watermarks.get_checkpoint(job.name)
                if self.config.runtime.mode == "incremental"
                else WatermarkCheckpoint()
            )
            max_records = self.config.runtime.max_records_per_job.get(job.name)
            stats = JobStats(max_watermark=stored_checkpoint.watermark)
            candidate_checkpoint = WatermarkCheckpoint(
                watermark=stored_checkpoint.watermark,
                route_keys=list(stored_checkpoint.route_keys),
                updated_at=stored_checkpoint.updated_at,
            )
            checkpoint_blocked = False
            last_seen_watermark = stored_checkpoint.watermark
            LOGGER.info(
                "Starting v2 job %s api=%s mode=%s watermark=%s",
                job.name,
                job.api,
                self.config.runtime.mode,
                stored_checkpoint.watermark,
            )
            for record in self._source_iter(
                job,
                watermark=stored_checkpoint.watermark,
                max_records=max_records,
            ):
                stats.docs_seen += 1
                if self.config.runtime.mode == "incremental":
                    if self._checkpoint_contains(stored_checkpoint, record):
                        stats.checkpoint_skipped += 1
                        continue
                    if compare_watermark_values(record.watermark_value, last_seen_watermark) < 0:
                        stats.out_of_order_records += 1
                    if self._is_newer_watermark(record.watermark_value, last_seen_watermark):
                        last_seen_watermark = record.watermark_value
                processed_successfully = self._apply_route(job, record, stats)
                if not processed_successfully:
                    checkpoint_blocked = True
                    continue
                if self.config.runtime.mode == "incremental":
                    candidate_checkpoint = self._advance_checkpoint(candidate_checkpoint, record)
                if self._is_newer_watermark(record.watermark_value, stats.max_watermark):
                    stats.max_watermark = record.watermark_value
                if (
                    self.config.runtime.flush_state_each_batch
                    and stats.docs_seen % self.config.runtime.batch_size == 0
                ):
                    self.registry.flush()
            if self.config.runtime.mode == "incremental":
                if stats.out_of_order_records > 0:
                    checkpoint_blocked = True
                    LOGGER.warning(
                        "Checkpoint not advanced for v2 job %s because out-of-order incremental "
                        "records were observed (%s). Ensure source results are ordered by %s.",
                        job.name,
                        stats.out_of_order_records,
                        job.incremental_field,
                    )
                if checkpoint_blocked:
                    LOGGER.warning(
                        "Checkpoint not advanced for v2 job %s because record-level issues occurred "
                        "(failed=%s rejected=%s cleanup_failed=%s).",
                        job.name,
                        stats.failed_records,
                        stats.rejected_records,
                        stats.cleanup_failed,
                    )
                elif candidate_checkpoint.watermark is not None:
                    self.watermarks.set_checkpoint(job.name, candidate_checkpoint)
                    stats.max_watermark = candidate_checkpoint.watermark
            self.registry.flush()
            self.watermarks.flush()
            LOGGER.info(
                "Completed v2 job %s | seen=%s firestore=%s spanner=%s moved=%s unchanged=%s checkpoint_skipped=%s rejected=%s failed=%s cleanup_failed=%s out_of_order=%s watermark=%s",
                job.name,
                stats.docs_seen,
                stats.firestore_writes,
                stats.spanner_writes,
                stats.moved_records,
                stats.unchanged_skipped,
                stats.checkpoint_skipped,
                stats.rejected_records,
                stats.failed_records,
                stats.cleanup_failed,
                stats.out_of_order_records,
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

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable

from migration.dead_letter import DeadLetterSink
from migration.coordination import WorkCoordinator
from migration.resume import ReaderCursorStore, StreamResumeState
from migration.retry_utils import RetryPolicy
from migration.sharding import shard_execution_order, stable_shard_for_text
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
    lease_conflicts: int = 0
    progress_skips: int = 0
    max_watermark: Any = None


class V2MigrationPipeline:
    def __init__(self, config: PipelineV2Config) -> None:
        self.config = config
        self.router = SizeRouter(config.routing)
        self.watermarks = WatermarkStore(config.runtime.state_file)
        self.registry = RouteRegistryStore(config.runtime.route_registry_file)
        self.dead_letter = DeadLetterSink(config.runtime.dlq_file_path)
        self.reader_cursors = (
            ReaderCursorStore(config.runtime.reader_cursor_state_file)
            if config.runtime.reader_cursor_state_file
            else None
        )

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
        self.mongo_source = MongoCosmosSourceAdapter(retry_policy=retry_policy)
        self.cassandra_source = CassandraCosmosSourceAdapter(retry_policy=retry_policy)
        self.coordinator = (
            WorkCoordinator(
                lease_file=config.runtime.lease_file,
                progress_file=config.runtime.progress_file,
                run_id=config.runtime.run_id,
                worker_id=config.runtime.worker_id,
                lease_duration_seconds=config.runtime.lease_duration_seconds,
                heartbeat_interval_seconds=config.runtime.heartbeat_interval_seconds,
            )
            if config.runtime.lease_file or config.runtime.progress_file
            else None
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
        watermark: Any,
        max_records: int | None,
        shard_index: int | None = None,
        shard_count: int = 1,
        resume_state: StreamResumeState | None = None,
    ) -> Iterable[CanonicalRecord]:
        if isinstance(job, MongoJobConfig):
            return self.mongo_source.iter_records(
                job,
                mode=self.config.runtime.mode,
                watermark=watermark,
                max_records=max_records,
                shard_index=shard_index,
                shard_count=shard_count,
                resume_state=resume_state,
            )
        return self.cassandra_source.iter_records(
            job,
            mode=self.config.runtime.mode,
            watermark=watermark,
            max_records=max_records,
            shard_index=shard_index,
            shard_count=shard_count,
            resume_state=resume_state,
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

    def _job_work_key(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        shard_index: int | None = None,
    ) -> str:
        base = f"v2:{job.name}"
        if shard_index is None or job.shard_count <= 1:
            return base
        return f"{base}:shard={shard_index}"

    def _checkpoint_key(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        shard_index: int | None = None,
    ) -> str:
        if shard_index is None or job.shard_count <= 1:
            return job.name
        return f"{job.name}:shard={shard_index}"

    def _job_work_metadata(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        *,
        shard_index: int,
    ) -> dict[str, object]:
        return {
            "job_name": job.name,
            "api": job.api,
            "mode": self.config.runtime.mode,
            "shard_index": shard_index,
            "shard_count": job.shard_count,
        }

    def _build_full_run_work_plan(
        self,
        jobs: list[MongoJobConfig | CassandraJobConfig],
    ) -> tuple[
        dict[str, dict[str, object]],
        dict[str, tuple[MongoJobConfig | CassandraJobConfig, int, int | None]],
        dict[str, JobStats],
    ]:
        coordinator = getattr(self, "coordinator", None)
        work_items: dict[str, dict[str, object]] = {}
        work_context: dict[str, tuple[MongoJobConfig | CassandraJobConfig, int, int | None]] = {}
        all_stats: dict[str, JobStats] = {job.name: JobStats() for job in jobs}
        for job in jobs:
            shard_indices = list(range(job.shard_count))
            if coordinator:
                shard_indices = shard_execution_order(
                    self._job_work_key(job),
                    coordinator.worker_id,
                    job.shard_count,
                )
            max_records = self.config.runtime.max_records_per_job.get(job.name)
            for shard_index in shard_indices:
                work_key = self._job_work_key(job, shard_index)
                work_items[work_key] = self._job_work_metadata(job, shard_index=shard_index)
                work_context[work_key] = (job, shard_index, max_records)
        return work_items, work_context, all_stats

    def _log_job_summary(self, job: MongoJobConfig | CassandraJobConfig, stats: JobStats) -> None:
        LOGGER.info(
            "Completed v2 job %s | seen=%s firestore=%s spanner=%s moved=%s unchanged=%s checkpoint_skipped=%s rejected=%s failed=%s cleanup_failed=%s lease_conflicts=%s progress_skips=%s out_of_order=%s watermark=%s",
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
            stats.lease_conflicts,
            stats.progress_skips,
            stats.out_of_order_records,
            stats.max_watermark,
        )

    def _run_full_progress_shard(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        *,
        shard_index: int,
        work_key: str,
        max_records: int | None,
        stats: JobStats,
        coordinator: WorkCoordinator,
    ) -> None:
        reader_cursors = getattr(self, "reader_cursors", None)
        source_resume_state = (
            reader_cursors.get(work_key) if reader_cursors else None
        )
        safe_resume_state = (
            source_resume_state.clone() if source_resume_state else None
        )
        cursor_blocked = False
        shard_start_docs_seen = stats.docs_seen
        shard_start_firestore = stats.firestore_writes
        shard_start_spanner = stats.spanner_writes
        shard_start_failed = stats.failed_records
        shard_start_rejected = stats.rejected_records
        shard_start_cleanup_failed = stats.cleanup_failed
        shard_start_out_of_order = stats.out_of_order_records
        LOGGER.info(
            "Starting v2 job %s api=%s shard=%s/%s mode=%s watermark=%s",
            job.name,
            job.api,
            shard_index,
            job.shard_count,
            self.config.runtime.mode,
            None,
        )
        try:
            for record in self._source_iter(
                job,
                watermark=None,
                max_records=max_records,
                shard_index=shard_index if job.shard_count > 1 else None,
                shard_count=job.shard_count,
                resume_state=source_resume_state,
            ):
                renewed = coordinator.renew_if_due(
                    work_key,
                    metadata={
                        "job_name": job.name,
                        "api": job.api,
                        "phase": "run",
                        "shard_index": shard_index,
                    },
                )
                if not renewed:
                    raise RuntimeError(
                        f"Lost distributed lease for v2 job {job.name} shard {shard_index}"
                    )
                if not self._record_matches_shard(
                    job,
                    record,
                    shard_index if job.shard_count > 1 else None,
                ):
                    continue
                stats.docs_seen += 1
                processed_successfully = self._apply_route(job, record, stats)
                if not processed_successfully:
                    continue
                if source_resume_state is not None:
                    safe_resume_state = source_resume_state.clone()
                if self._is_newer_watermark(record.watermark_value, stats.max_watermark):
                    stats.max_watermark = record.watermark_value
                if (
                    self.config.runtime.flush_state_each_batch
                    and stats.docs_seen % self.config.runtime.batch_size == 0
                ):
                    self.registry.flush()
                    if not cursor_blocked:
                        self._persist_reader_cursor(work_key, safe_resume_state)
            shard_failed = stats.failed_records - shard_start_failed
            shard_rejected = stats.rejected_records - shard_start_rejected
            shard_cleanup_failed = stats.cleanup_failed - shard_start_cleanup_failed
            shard_out_of_order = stats.out_of_order_records - shard_start_out_of_order
            if (
                shard_failed == 0
                and shard_rejected == 0
                and shard_cleanup_failed == 0
                and shard_out_of_order == 0
            ):
                coordinator.mark_completed(
                    work_key,
                    metadata={
                        "job_name": job.name,
                        "api": job.api,
                        "docs_seen": stats.docs_seen - shard_start_docs_seen,
                        "firestore_writes": stats.firestore_writes - shard_start_firestore,
                        "spanner_writes": stats.spanner_writes - shard_start_spanner,
                    },
                )
            else:
                coordinator.mark_failed(
                    work_key,
                    error=(
                        f"failed={shard_failed} "
                        f"rejected={shard_rejected} "
                        f"cleanup_failed={shard_cleanup_failed} "
                        f"out_of_order={shard_out_of_order}"
                    ),
                    metadata={
                        "job_name": job.name,
                        "api": job.api,
                        "docs_seen": stats.docs_seen - shard_start_docs_seen,
                    },
                )
            self.registry.flush()
            self.watermarks.flush()
            if not cursor_blocked:
                self._persist_reader_cursor(work_key, safe_resume_state)
            if (
                shard_failed == 0
                and shard_rejected == 0
                and shard_cleanup_failed == 0
                and shard_out_of_order == 0
            ):
                self._clear_reader_cursor(work_key)
        except Exception as exc:
            coordinator.mark_failed(
                work_key,
                error=str(exc),
                metadata={
                    "job_name": job.name,
                    "api": job.api,
                    "shard_index": shard_index,
                    "shard_count": job.shard_count,
                },
            )
            raise

    def _run_full_progress_scheduler(
        self,
        jobs: list[MongoJobConfig | CassandraJobConfig],
        coordinator: WorkCoordinator,
    ) -> dict[str, JobStats]:
        work_items, work_context, all_stats = self._build_full_run_work_plan(jobs)
        for job in jobs:
            job_work_keys = [
                work_key
                for work_key, (candidate_job, _shard_index, _max_records) in work_context.items()
                if candidate_job is job
            ]
            all_stats[job.name].progress_skips += coordinator.completed_count(job_work_keys)
        pending_work_items = {
            work_key: metadata
            for work_key, metadata in work_items.items()
            if not coordinator.is_completed(work_key)
        }
        while pending_work_items:
            claim = coordinator.claim_next(pending_work_items)
            if claim is None:
                break
            work_key, _metadata = claim
            job, shard_index, max_records = work_context[work_key]
            try:
                self._run_full_progress_shard(
                    job,
                    shard_index=shard_index,
                    work_key=work_key,
                    max_records=max_records,
                    stats=all_stats[job.name],
                    coordinator=coordinator,
                )
            finally:
                coordinator.release(work_key)
                pending_work_items.pop(work_key, None)
        for job in jobs:
            self._log_job_summary(job, all_stats[job.name])
        return all_stats

    def _record_matches_shard(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        record: CanonicalRecord,
        shard_index: int | None,
    ) -> bool:
        if shard_index is None or job.shard_count <= 1:
            return True
        if job.shard_mode != "client_hash":
            return True
        return stable_shard_for_text(record.route_key, job.shard_count) == shard_index

    def _persist_reader_cursor(self, work_key: str, state: StreamResumeState | None) -> None:
        reader_cursors = getattr(self, "reader_cursors", None)
        if reader_cursors is None or state is None or not state.has_position:
            return
        reader_cursors.set(work_key, state.clone())
        reader_cursors.flush()

    def _clear_reader_cursor(self, work_key: str) -> None:
        reader_cursors = getattr(self, "reader_cursors", None)
        if reader_cursors is None:
            return
        reader_cursors.clear(work_key)
        reader_cursors.flush()

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

        coordinator = getattr(self, "coordinator", None)
        if (
            coordinator
            and self.config.runtime.mode == "full"
            and getattr(coordinator, "progress_enabled", False)
        ):
            return self._run_full_progress_scheduler(jobs, coordinator)

        all_stats: dict[str, JobStats] = {}
        for job in jobs:
            stats = JobStats()
            shard_indices = list(range(job.shard_count))
            if coordinator:
                shard_indices = shard_execution_order(
                    self._job_work_key(job),
                    coordinator.worker_id,
                    job.shard_count,
                )
            max_records = self.config.runtime.max_records_per_job.get(job.name)
            work_items = {
                self._job_work_key(job, shard_index): self._job_work_metadata(
                    job,
                    shard_index=shard_index,
                )
                for shard_index in shard_indices
            }
            for shard_index in shard_indices:
                work_key = self._job_work_key(job, shard_index)
                if (
                    coordinator
                    and self.config.runtime.mode == "full"
                    and coordinator.is_completed(work_key)
                ):
                    stats.progress_skips += 1
                    LOGGER.info(
                        "Skipping v2 job %s shard=%s/%s because the shard is already marked completed for run_id=%s.",
                        job.name,
                        shard_index,
                        job.shard_count,
                        self.config.runtime.run_id,
                    )
                    continue
                checkpoint_key = self._checkpoint_key(
                    job,
                    shard_index if job.shard_count > 1 else None,
                )
                stored_checkpoint = (
                    self.watermarks.get_checkpoint(checkpoint_key)
                    if self.config.runtime.mode == "incremental"
                    else WatermarkCheckpoint()
                )
                if self._is_newer_watermark(stored_checkpoint.watermark, stats.max_watermark):
                    stats.max_watermark = stored_checkpoint.watermark
                if coordinator:
                    acquired = coordinator.acquire(
                        work_key,
                        metadata=work_items[work_key],
                    )
                    if not acquired:
                        stats.lease_conflicts += 1
                        LOGGER.info(
                            "Skipping v2 job %s shard=%s/%s because lease is held by another worker.",
                            job.name,
                            shard_index,
                            job.shard_count,
                        )
                        continue
                    coordinator.mark_running(
                        work_key,
                        metadata=work_items[work_key],
                    )
                candidate_checkpoint = WatermarkCheckpoint(
                    watermark=stored_checkpoint.watermark,
                    route_keys=list(stored_checkpoint.route_keys),
                    updated_at=stored_checkpoint.updated_at,
                )
                reader_cursors = getattr(self, "reader_cursors", None)
                source_resume_state = (
                    reader_cursors.get(work_key) if reader_cursors else None
                )
                safe_resume_state = (
                    source_resume_state.clone() if source_resume_state else None
                )
                shard_start_docs_seen = stats.docs_seen
                shard_start_firestore = stats.firestore_writes
                shard_start_spanner = stats.spanner_writes
                shard_start_failed = stats.failed_records
                shard_start_rejected = stats.rejected_records
                shard_start_cleanup_failed = stats.cleanup_failed
                shard_start_out_of_order = stats.out_of_order_records
                checkpoint_blocked = False
                cursor_blocked = False
                last_seen_watermark = stored_checkpoint.watermark
                LOGGER.info(
                    "Starting v2 job %s api=%s shard=%s/%s mode=%s watermark=%s",
                    job.name,
                    job.api,
                    shard_index,
                    job.shard_count,
                    self.config.runtime.mode,
                    stored_checkpoint.watermark,
                )
                try:
                    for record in self._source_iter(
                        job,
                        watermark=stored_checkpoint.watermark,
                        max_records=max_records,
                        shard_index=shard_index if job.shard_count > 1 else None,
                        shard_count=job.shard_count,
                        resume_state=source_resume_state,
                    ):
                        if coordinator:
                            renewed = coordinator.renew_if_due(
                                work_key,
                                metadata={
                                    "job_name": job.name,
                                    "api": job.api,
                                    "phase": "run",
                                    "shard_index": shard_index,
                                },
                            )
                            if not renewed:
                                raise RuntimeError(
                                    f"Lost distributed lease for v2 job {job.name} shard {shard_index}"
                                )
                        if not self._record_matches_shard(
                            job,
                            record,
                            shard_index if job.shard_count > 1 else None,
                        ):
                            continue
                        stats.docs_seen += 1
                        if self.config.runtime.mode == "incremental":
                            if self._checkpoint_contains(stored_checkpoint, record):
                                stats.checkpoint_skipped += 1
                                if source_resume_state is not None:
                                    safe_resume_state = source_resume_state.clone()
                                continue
                            if compare_watermark_values(record.watermark_value, last_seen_watermark) < 0:
                                stats.out_of_order_records += 1
                                cursor_blocked = True
                            if self._is_newer_watermark(record.watermark_value, last_seen_watermark):
                                last_seen_watermark = record.watermark_value
                        processed_successfully = self._apply_route(job, record, stats)
                        if not processed_successfully:
                            checkpoint_blocked = True
                            continue
                        if source_resume_state is not None and not cursor_blocked:
                            safe_resume_state = source_resume_state.clone()
                        if self.config.runtime.mode == "incremental":
                            candidate_checkpoint = self._advance_checkpoint(candidate_checkpoint, record)
                        if self._is_newer_watermark(record.watermark_value, stats.max_watermark):
                            stats.max_watermark = record.watermark_value
                        if (
                            self.config.runtime.flush_state_each_batch
                            and stats.docs_seen % self.config.runtime.batch_size == 0
                        ):
                            self.registry.flush()
                            if not cursor_blocked:
                                self._persist_reader_cursor(work_key, safe_resume_state)
                    if self.config.runtime.mode == "incremental":
                        if stats.out_of_order_records > 0:
                            checkpoint_blocked = True
                            LOGGER.warning(
                                "Checkpoint not advanced for v2 job %s shard=%s/%s because out-of-order incremental "
                                "records were observed (%s). Ensure source results are ordered by %s.",
                                job.name,
                                shard_index,
                                job.shard_count,
                                stats.out_of_order_records,
                                job.incremental_field,
                            )
                        if checkpoint_blocked:
                            LOGGER.warning(
                                "Checkpoint not advanced for v2 job %s shard=%s/%s because record-level issues occurred "
                                "(failed=%s rejected=%s cleanup_failed=%s).",
                                job.name,
                                shard_index,
                                job.shard_count,
                                stats.failed_records,
                                stats.rejected_records,
                                stats.cleanup_failed,
                            )
                        elif candidate_checkpoint.watermark is not None:
                            self.watermarks.set_checkpoint(checkpoint_key, candidate_checkpoint)
                            if self._is_newer_watermark(candidate_checkpoint.watermark, stats.max_watermark):
                                stats.max_watermark = candidate_checkpoint.watermark
                    if coordinator and self.config.runtime.mode == "full":
                        shard_failed = stats.failed_records - shard_start_failed
                        shard_rejected = stats.rejected_records - shard_start_rejected
                        shard_cleanup_failed = stats.cleanup_failed - shard_start_cleanup_failed
                        shard_out_of_order = stats.out_of_order_records - shard_start_out_of_order
                        if (
                            shard_failed == 0
                            and shard_rejected == 0
                            and shard_cleanup_failed == 0
                            and shard_out_of_order == 0
                        ):
                            coordinator.mark_completed(
                                work_key,
                                metadata={
                                    "job_name": job.name,
                                    "api": job.api,
                                    "docs_seen": stats.docs_seen - shard_start_docs_seen,
                                    "firestore_writes": stats.firestore_writes - shard_start_firestore,
                                    "spanner_writes": stats.spanner_writes - shard_start_spanner,
                                },
                            )
                        else:
                            coordinator.mark_failed(
                                work_key,
                                error=(
                                    f"failed={shard_failed} "
                                    f"rejected={shard_rejected} "
                                    f"cleanup_failed={shard_cleanup_failed} "
                                    f"out_of_order={shard_out_of_order}"
                                ),
                                metadata={
                                    "job_name": job.name,
                                    "api": job.api,
                                    "docs_seen": stats.docs_seen - shard_start_docs_seen,
                                },
                            )
                    self.registry.flush()
                    self.watermarks.flush()
                    if not cursor_blocked:
                        self._persist_reader_cursor(work_key, safe_resume_state)
                    shard_failed = stats.failed_records - shard_start_failed
                    shard_rejected = stats.rejected_records - shard_start_rejected
                    shard_cleanup_failed = stats.cleanup_failed - shard_start_cleanup_failed
                    shard_out_of_order = stats.out_of_order_records - shard_start_out_of_order
                    if (
                        shard_failed == 0
                        and shard_rejected == 0
                        and shard_cleanup_failed == 0
                        and shard_out_of_order == 0
                        and not checkpoint_blocked
                    ):
                        self._clear_reader_cursor(work_key)
                except Exception as exc:
                    if coordinator and self.config.runtime.mode == "full":
                        coordinator.mark_failed(
                            work_key,
                            error=str(exc),
                            metadata={
                                "job_name": job.name,
                                "api": job.api,
                                "shard_index": shard_index,
                                "shard_count": job.shard_count,
                            },
                        )
                    raise
                finally:
                    if coordinator:
                        coordinator.release(work_key)
            self._log_job_summary(job, stats)
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

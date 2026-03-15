from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from time import monotonic
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.config import MigrationConfig, TableMapping, load_config
from migration.coordination import WorkCoordinator
from migration.cosmos_reader import CosmosReader
from migration.dead_letter import DeadLetterSink
from migration.logging_utils import build_log_context, configure_logging
from migration.metrics import MetricsCollector
from migration.release_gate import (
    enforce_stage_rehearsal_or_raise,
    logical_fingerprint_v1,
)
from migration.resume import ReaderCursorStore, StreamResumeState
from migration.retry_utils import RetryPolicy
from migration.sharding import (
    apply_shard_placeholders,
    shard_execution_order,
    stable_shard_for_text,
)
from migration.spanner_writer import SpannerWriter
from migration.state_store import WatermarkStore
from migration.transform import TransformOutput, transform_document

LOGGER = logging.getLogger("backfill")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate Azure Cosmos DB data to Google Cloud Spanner."
    )
    parser.add_argument("--config", required=True, help="Path to migration YAML config.")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use watermark-based incremental pull from Cosmos DB.",
    )
    parser.add_argument(
        "--container",
        action="append",
        dest="containers",
        help="Optional source container filter. Can be repeated.",
    )
    parser.add_argument(
        "--since-ts",
        type=int,
        default=None,
        help="Override watermark timestamp (Cosmos _ts epoch seconds).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and transform documents but skip Spanner writes.",
    )
    return parser.parse_args()


def _selected_mappings(config: MigrationConfig, containers: list[str] | None) -> list[TableMapping]:
    if not containers:
        return config.mappings
    container_set = set(containers)
    return [m for m in config.mappings if m.source_container in container_set]


def _query_for_mapping(
    mapping: TableMapping,
    incremental: bool,
    watermark: int,
    overlap_seconds: int,
    *,
    shard_index: int | None = None,
    shard_count: int = 1,
) -> tuple[str, list[dict[str, Any]]]:
    if not incremental:
        query = mapping.source_query
        if mapping.shard_mode == "query_template" and shard_index is not None and shard_count > 1:
            query = apply_shard_placeholders(
                query,
                shard_index=shard_index,
                shard_count=shard_count,
            )
        return query, []

    start_ts = max(0, watermark - overlap_seconds)
    query = mapping.incremental_query or "SELECT * FROM c WHERE c._ts > @last_ts"
    if mapping.shard_mode == "query_template" and shard_index is not None and shard_count > 1:
        query = apply_shard_placeholders(
            query,
            shard_index=shard_index,
            shard_count=shard_count,
        )
    parameters = [{"name": "@last_ts", "value": start_ts}]
    return query, parameters


def _mapping_work_key(mapping: TableMapping, shard_index: int | None = None) -> str:
    base = f"v1:{mapping.source_container}->{mapping.target_table}"
    if shard_index is None or mapping.shard_count <= 1:
        return base
    return f"{base}:shard={shard_index}"


def _watermark_key(mapping: TableMapping, shard_index: int | None = None) -> str:
    return _mapping_work_key(mapping, shard_index)


def _legacy_watermark_key(mapping: TableMapping, shard_index: int | None = None) -> str:
    base = mapping.source_container
    if shard_index is None or mapping.shard_count <= 1:
        return base
    return f"{base}:shard={shard_index}"


def _load_starting_watermark(
    watermarks: WatermarkStore,
    mapping: TableMapping,
    shard_index: int | None = None,
) -> int:
    watermark_key = _watermark_key(mapping, shard_index)
    if watermarks.contains(watermark_key):
        return watermarks.get(watermark_key, 0)

    legacy_key = _legacy_watermark_key(mapping, shard_index)
    if legacy_key != watermark_key and watermarks.contains(legacy_key):
        legacy_value = watermarks.get(legacy_key, 0)
        LOGGER.info(
            "Using legacy watermark key %s for %s -> %s shard=%s; future checkpoints will be written to %s.",
            legacy_key,
            mapping.source_container,
            mapping.target_table,
            shard_index if shard_index is not None else "none",
            watermark_key,
        )
        return legacy_value
    return 0


def _mapping_metric_labels(
    mapping: TableMapping,
    *,
    incremental: bool,
    shard_index: int | None,
) -> dict[str, str]:
    labels = {
        "source_container": mapping.source_container,
        "target_table": mapping.target_table,
        "mode": "incremental" if incremental else "full",
    }
    if shard_index is not None and mapping.shard_count > 1:
        labels["shard"] = str(shard_index)
    return labels


def _publish_mapping_metrics(
    metrics: MetricsCollector | None,
    *,
    mapping: TableMapping,
    counters: dict[str, int],
    incremental: bool,
    shard_index: int | None,
    resume_watermark: int,
    duration_seconds: float | None = None,
) -> None:
    if metrics is None:
        return

    labels = _mapping_metric_labels(
        mapping,
        incremental=incremental,
        shard_index=shard_index,
    )
    metrics.gauge(
        "migration_v1_docs_seen",
        counters["docs_seen"],
        labels=labels,
        description="Current number of source documents seen for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_rows_upserted",
        counters["rows_upserted"],
        labels=labels,
        description="Current number of rows upserted for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_rows_deleted",
        counters["rows_deleted"],
        labels=labels,
        description="Current number of rows deleted for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_rows_failed",
        counters["rows_failed"],
        labels=labels,
        description="Current number of row-level failures for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_docs_failed",
        counters["docs_failed"],
        labels=labels,
        description="Current number of document-level failures for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_max_source_watermark",
        counters["max_ts_seen"],
        labels=labels,
        description="Highest source watermark observed for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_max_success_watermark",
        counters["max_success_ts"],
        labels=labels,
        description="Highest successfully committed watermark for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_resume_watermark",
        resume_watermark,
        labels=labels,
        description="Starting resume watermark used for the v1 mapping.",
    )
    metrics.gauge(
        "migration_v1_watermark_lag_seconds",
        max(0, counters["max_ts_seen"] - counters["max_success_ts"]),
        labels=labels,
        description="Observed watermark lag between seen and successfully committed records.",
    )
    if duration_seconds is not None:
        metrics.gauge(
            "migration_v1_mapping_duration_seconds",
            duration_seconds,
            labels=labels,
            description="Elapsed runtime for the current v1 mapping execution.",
        )


def _get_nested_value(document: dict[str, Any], path: str) -> Any:
    current: Any = document
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise KeyError(path)
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            idx = int(part)
            if idx < 0 or idx >= len(current):
                raise KeyError(path)
            current = current[idx]
            continue
        raise KeyError(path)
    return current


def _document_matches_shard(
    document: dict[str, Any],
    mapping: TableMapping,
    shard_index: int | None,
) -> bool:
    if shard_index is None or mapping.shard_count <= 1:
        return True
    if mapping.shard_mode != "client_hash":
        return True
    shard_key_source = mapping.shard_key_source or "id"
    shard_value = _get_nested_value(document, shard_key_source)
    return stable_shard_for_text(str(shard_value), mapping.shard_count) == shard_index


def _flush_upserts(
    writer: SpannerWriter,
    mapping: TableMapping,
    buffer: list[tuple[TransformOutput, StreamResumeState]],
    counters: dict[str, int],
    *,
    error_mode: str,
    dead_letter: DeadLetterSink,
) -> tuple[int, StreamResumeState | None]:
    if not buffer:
        return 0, None
    rows = [item.row for item, _ in buffer]
    try:
        counters["rows_upserted"] += writer.write_rows(mapping.target_table, rows, mapping.mode)
        return max(item.source_ts for item, _ in buffer), buffer[-1][1].clone()
    except Exception as exc:  # noqa: BLE001
        if error_mode != "skip":
            raise
        LOGGER.warning(
            "Batch upsert failed for %s -> %s; falling back to row writes: %s",
            mapping.source_container,
            mapping.target_table,
            exc,
        )
        max_success_ts = 0
        successful_resume_state: StreamResumeState | None = None
        for item, resume_state in buffer:
            try:
                counters["rows_upserted"] += writer.write_rows(
                    mapping.target_table,
                    [item.row],
                    mapping.mode,
                )
                max_success_ts = max(max_success_ts, item.source_ts)
                successful_resume_state = resume_state.clone()
            except Exception as row_exc:  # noqa: BLE001
                counters["rows_failed"] += 1
                dead_letter.write(
                    stage="write_upsert",
                    mapping_name=f"{mapping.source_container}->{mapping.target_table}",
                    source_container=mapping.source_container,
                    target_table=mapping.target_table,
                    error=row_exc,
                    transformed_row=item.row,
                )
        return max_success_ts, successful_resume_state
    finally:
        buffer.clear()


def _flush_deletes(
    writer: SpannerWriter,
    mapping: TableMapping,
    buffer: list[tuple[TransformOutput, StreamResumeState]],
    counters: dict[str, int],
    *,
    error_mode: str,
    dead_letter: DeadLetterSink,
) -> tuple[int, StreamResumeState | None]:
    if not buffer:
        return 0, None
    key_rows = [item.row for item, _ in buffer]
    try:
        counters["rows_deleted"] += writer.delete_rows(
            mapping.target_table,
            mapping.key_columns,
            key_rows,
        )
        return max(item.source_ts for item, _ in buffer), buffer[-1][1].clone()
    except Exception as exc:  # noqa: BLE001
        if error_mode != "skip":
            raise
        LOGGER.warning(
            "Batch delete failed for %s -> %s; falling back to key deletes: %s",
            mapping.source_container,
            mapping.target_table,
            exc,
        )
        max_success_ts = 0
        successful_resume_state: StreamResumeState | None = None
        for item, resume_state in buffer:
            try:
                counters["rows_deleted"] += writer.delete_rows(
                    mapping.target_table,
                    mapping.key_columns,
                    [item.row],
                )
                max_success_ts = max(max_success_ts, item.source_ts)
                successful_resume_state = resume_state.clone()
            except Exception as row_exc:  # noqa: BLE001
                counters["rows_failed"] += 1
                dead_letter.write(
                    stage="delete",
                    mapping_name=f"{mapping.source_container}->{mapping.target_table}",
                    source_container=mapping.source_container,
                    target_table=mapping.target_table,
                    error=row_exc,
                    transformed_row=item.row,
                )
        return max_success_ts, successful_resume_state
    finally:
        buffer.clear()


def _process_mapping(
    config: MigrationConfig,
    mapping: TableMapping,
    reader: CosmosReader,
    writer: SpannerWriter,
    watermarks: WatermarkStore,
    dead_letter: DeadLetterSink,
    metrics: MetricsCollector | None,
    cursor_store: ReaderCursorStore | None,
    incremental: bool,
    since_ts: int | None,
    coordinator: WorkCoordinator | None = None,
    work_key: str | None = None,
    shard_index: int | None = None,
) -> dict[str, int]:
    started_at = monotonic()
    counters = {
        "docs_seen": 0,
        "rows_upserted": 0,
        "rows_deleted": 0,
        "rows_failed": 0,
        "docs_failed": 0,
        "max_ts_seen": 0,
        "max_success_ts": 0,
    }
    upsert_buffer: list[tuple[TransformOutput, StreamResumeState]] = []
    delete_buffer: list[tuple[TransformOutput, StreamResumeState]] = []

    watermark_key = _watermark_key(mapping, shard_index)
    last_watermark = (
        since_ts
        if since_ts is not None
        else _load_starting_watermark(watermarks, mapping, shard_index)
    )
    query, params = _query_for_mapping(
        mapping=mapping,
        incremental=incremental,
        watermark=last_watermark,
        overlap_seconds=config.runtime.watermark_overlap_seconds,
        shard_index=shard_index,
        shard_count=mapping.shard_count,
    )

    max_docs = config.runtime.max_docs_per_container.get(mapping.source_container)
    cursor_key = work_key or _mapping_work_key(mapping, shard_index)
    resume_state = cursor_store.get(cursor_key) if cursor_store else None
    safe_resume_state = resume_state.clone() if resume_state else None

    def persist_reader_cursor(state: StreamResumeState | None) -> None:
        if cursor_store is None or state is None or not state.has_position:
            return
        cursor_store.set(cursor_key, state.clone())
        cursor_store.flush()

    def clear_reader_cursor() -> None:
        if cursor_store is None:
            return
        cursor_store.clear(cursor_key)
        cursor_store.flush()

    LOGGER.info(
        "Running mapping %s -> %s (incremental=%s, watermark=%s, watermark_key=%s, query=%s)",
        mapping.source_container,
        mapping.target_table,
        incremental,
        last_watermark,
        watermark_key,
        query,
    )

    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=query,
        parameters=params,
        page_size=config.runtime.query_page_size,
        max_docs=max_docs,
        resume_state=resume_state,
    ):
        if coordinator and work_key:
            renewed = coordinator.renew_if_due(
                work_key,
                metadata={
                    "source_container": mapping.source_container,
                    "target_table": mapping.target_table,
                    "phase": "process_mapping",
                },
            )
            if not renewed:
                raise RuntimeError(
                    f"Lost distributed lease for mapping {mapping.source_container}->{mapping.target_table}"
                )
        if not _document_matches_shard(document, mapping, shard_index):
            continue
        counters["docs_seen"] += 1
        counters["max_ts_seen"] = max(counters["max_ts_seen"], int(document.get("_ts", 0) or 0))
        try:
            result: TransformOutput = transform_document(document, mapping)
        except Exception as exc:  # noqa: BLE001
            counters["docs_failed"] += 1
            if config.runtime.error_mode == "skip":
                dead_letter.write(
                    stage="transform",
                    mapping_name=f"{mapping.source_container}->{mapping.target_table}",
                    source_container=mapping.source_container,
                    target_table=mapping.target_table,
                    error=exc,
                    source_document=document,
                )
                LOGGER.warning(
                    "Skipping doc due to transform error in %s: %s",
                    mapping.source_container,
                    exc,
                )
                continue
            raise

        if result.is_delete:
            delete_buffer.append((result, resume_state.clone() if resume_state else StreamResumeState()))
            if len(delete_buffer) >= config.runtime.batch_size:
                batch_max_success_ts, batch_resume_state = _flush_deletes(
                        writer,
                        mapping,
                        delete_buffer,
                        counters,
                        error_mode=config.runtime.error_mode,
                        dead_letter=dead_letter,
                    )
                counters["max_success_ts"] = max(counters["max_success_ts"], batch_max_success_ts)
                if batch_resume_state is not None:
                    safe_resume_state = batch_resume_state
                    persist_reader_cursor(safe_resume_state)
                _publish_mapping_metrics(
                    metrics,
                    mapping=mapping,
                    counters=counters,
                    incremental=incremental,
                    shard_index=shard_index,
                    resume_watermark=last_watermark,
                )
                if metrics is not None:
                    metrics.flush()
        else:
            upsert_buffer.append((result, resume_state.clone() if resume_state else StreamResumeState()))
            if len(upsert_buffer) >= config.runtime.batch_size:
                batch_max_success_ts, batch_resume_state = _flush_upserts(
                        writer,
                        mapping,
                        upsert_buffer,
                        counters,
                        error_mode=config.runtime.error_mode,
                        dead_letter=dead_letter,
                    )
                counters["max_success_ts"] = max(counters["max_success_ts"], batch_max_success_ts)
                if batch_resume_state is not None:
                    safe_resume_state = batch_resume_state
                    persist_reader_cursor(safe_resume_state)
                _publish_mapping_metrics(
                    metrics,
                    mapping=mapping,
                    counters=counters,
                    incremental=incremental,
                    shard_index=shard_index,
                    resume_watermark=last_watermark,
                )
                if metrics is not None:
                    metrics.flush()

    batch_max_success_ts, batch_resume_state = _flush_upserts(
        writer,
        mapping,
        upsert_buffer,
        counters,
        error_mode=config.runtime.error_mode,
        dead_letter=dead_letter,
    )
    counters["max_success_ts"] = max(counters["max_success_ts"], batch_max_success_ts)
    if batch_resume_state is not None:
        safe_resume_state = batch_resume_state
        persist_reader_cursor(safe_resume_state)

    batch_max_success_ts, batch_resume_state = _flush_deletes(
        writer,
        mapping,
        delete_buffer,
        counters,
        error_mode=config.runtime.error_mode,
        dead_letter=dead_letter,
    )
    counters["max_success_ts"] = max(counters["max_success_ts"], batch_max_success_ts)
    if batch_resume_state is not None:
        safe_resume_state = batch_resume_state
        persist_reader_cursor(safe_resume_state)

    if incremental and counters["max_success_ts"] > 0:
        if counters["rows_failed"] > 0 or counters["docs_failed"] > 0:
            LOGGER.warning(
                "Checkpoint not advanced for %s -> %s because failures occurred "
                "(row_failures=%s doc_failures=%s).",
                mapping.source_container,
                mapping.target_table,
                counters["rows_failed"],
                counters["docs_failed"],
            )
        else:
            watermarks.set(watermark_key, counters["max_success_ts"])
            if config.runtime.flush_watermark_each_mapping:
                watermarks.flush()
                clear_reader_cursor()
    elif counters["rows_failed"] == 0 and counters["docs_failed"] == 0:
        clear_reader_cursor()

    _publish_mapping_metrics(
        metrics,
        mapping=mapping,
        counters=counters,
        incremental=incremental,
        shard_index=shard_index,
        resume_watermark=last_watermark,
        duration_seconds=monotonic() - started_at,
    )
    if metrics is not None:
        metrics.flush()
    return counters


def _should_skip_completed_work(
    coordinator: WorkCoordinator | None,
    *,
    incremental: bool,
    work_key: str,
) -> bool:
    if coordinator is None or incremental:
        return False
    return coordinator.is_completed(work_key)


def _mapping_work_metadata(
    mapping: TableMapping,
    *,
    incremental: bool,
    shard_index: int,
) -> dict[str, object]:
    return {
        "source_container": mapping.source_container,
        "target_table": mapping.target_table,
        "mode": "incremental" if incremental else "full",
        "shard_index": shard_index,
        "shard_count": mapping.shard_count,
    }


def _build_full_run_work_plan(
    mappings: list[TableMapping],
    coordinator: WorkCoordinator,
) -> tuple[dict[str, dict[str, object]], dict[str, tuple[TableMapping, int]]]:
    work_items: dict[str, dict[str, object]] = {}
    work_context: dict[str, tuple[TableMapping, int]] = {}
    for mapping in mappings:
        shard_indices = shard_execution_order(
            _mapping_work_key(mapping),
            coordinator.worker_id,
            mapping.shard_count,
        )
        for shard_index in shard_indices:
            work_key = _mapping_work_key(mapping, shard_index)
            metadata = _mapping_work_metadata(
                mapping,
                incremental=False,
                shard_index=shard_index,
            )
            work_items[work_key] = metadata
            work_context[work_key] = (mapping, shard_index)
    return work_items, work_context


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    if args.dry_run:
        config.runtime.dry_run = True
    log_context = build_log_context(
        pipeline="v1",
        deployment_environment=config.runtime.deployment_environment,
        run_id=config.runtime.run_id,
        worker_id=config.runtime.worker_id,
    )
    configure_logging(
        config.runtime.log_level,
        config.runtime.log_format,
        static_fields=log_context,
    )
    metrics = MetricsCollector(
        config.runtime.metrics_file_path,
        output_format=config.runtime.metrics_format,
        static_labels=log_context,
    )

    mappings = _selected_mappings(config, args.containers)
    if not mappings:
        LOGGER.error("No mappings selected. Check --container filters.")
        return 1
    enforce_stage_rehearsal_or_raise(
        runtime=config.runtime,
        pipeline="v1",
        logical_fingerprint=logical_fingerprint_v1(config, mappings),
    )

    retry_policy = RetryPolicy.from_runtime(config.runtime)
    reader = CosmosReader(config.source, retry_policy=retry_policy)
    writer = SpannerWriter(config.target, retry_policy=retry_policy, dry_run=config.runtime.dry_run)
    watermark_store = WatermarkStore(config.runtime.watermark_state_file)
    cursor_store = (
        ReaderCursorStore(config.runtime.reader_cursor_state_file)
        if config.runtime.reader_cursor_state_file
        else None
    )
    dead_letter = DeadLetterSink(config.runtime.dlq_file_path)
    coordinator = (
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

    exit_code = 0
    if coordinator and not args.incremental and getattr(coordinator, "progress_enabled", False):
        work_items, work_context = _build_full_run_work_plan(mappings, coordinator)
        for mapping in mappings:
            mapping_work_keys = [
                work_key
                for work_key, (candidate_mapping, _shard_index) in work_context.items()
                if candidate_mapping is mapping
            ]
            completed_count = coordinator.completed_count(mapping_work_keys)
            if completed_count:
                LOGGER.info(
                    "Skipping %s completed shard(s) for mapping %s -> %s under run_id=%s.",
                    completed_count,
                    mapping.source_container,
                    mapping.target_table,
                    config.runtime.run_id,
                )
        pending_work_items = {
            work_key: metadata
            for work_key, metadata in work_items.items()
            if not coordinator.is_completed(work_key)
        }
        while pending_work_items:
            claim = coordinator.claim_next(pending_work_items)
            if claim is None:
                break
            work_key, metadata = claim
            mapping, shard_index = work_context[work_key]
            try:
                stats = _process_mapping(
                    config=config,
                    mapping=mapping,
                    reader=reader,
                    writer=writer,
                    watermarks=watermark_store,
                    dead_letter=dead_letter,
                    metrics=metrics,
                    cursor_store=cursor_store,
                    incremental=False,
                    since_ts=args.since_ts,
                    coordinator=coordinator,
                    work_key=work_key,
                    shard_index=shard_index if mapping.shard_count > 1 else None,
                )
                if stats["rows_failed"] == 0 and stats["docs_failed"] == 0:
                    coordinator.mark_completed(
                        work_key,
                        metadata={
                            "source_container": mapping.source_container,
                            "target_table": mapping.target_table,
                            "docs_seen": stats["docs_seen"],
                            "rows_upserted": stats["rows_upserted"],
                            "rows_deleted": stats["rows_deleted"],
                            "max_success_ts": stats["max_success_ts"],
                        },
                    )
                else:
                    coordinator.mark_failed(
                        work_key,
                        error=(
                            f"row_failures={stats['rows_failed']} "
                            f"doc_failures={stats['docs_failed']}"
                        ),
                        metadata={
                            "source_container": mapping.source_container,
                            "target_table": mapping.target_table,
                            "docs_seen": stats["docs_seen"],
                        },
                    )
                LOGGER.info(
                    "Completed %s -> %s shard=%s/%s | docs_seen=%s upserted=%s deleted=%s row_failures=%s doc_failures=%s max_ts=%s max_success_ts=%s",
                    mapping.source_container,
                    mapping.target_table,
                    shard_index,
                    mapping.shard_count,
                    stats["docs_seen"],
                    stats["rows_upserted"],
                    stats["rows_deleted"],
                    stats["rows_failed"],
                    stats["docs_failed"],
                    stats["max_ts_seen"],
                    stats["max_success_ts"],
                )
            except Exception as exc:  # noqa: BLE001
                coordinator.mark_failed(
                    work_key,
                    error=str(exc),
                    metadata={
                        "source_container": mapping.source_container,
                        "target_table": mapping.target_table,
                        "shard_index": shard_index,
                        "shard_count": mapping.shard_count,
                    },
                )
                LOGGER.exception(
                    "Mapping failed %s -> %s shard=%s/%s: %s",
                    mapping.source_container,
                    mapping.target_table,
                    shard_index,
                    mapping.shard_count,
                    exc,
                )
                exit_code = 1
                if config.runtime.error_mode != "skip":
                    break
            finally:
                coordinator.release(work_key)
                pending_work_items.pop(work_key, None)
        return exit_code

    for mapping in mappings:
        shard_indices = list(range(mapping.shard_count))
        if coordinator:
            shard_indices = shard_execution_order(
                _mapping_work_key(mapping),
                coordinator.worker_id,
                mapping.shard_count,
            )
        work_items = {
            _mapping_work_key(mapping, shard_index): _mapping_work_metadata(
                mapping,
                incremental=args.incremental,
                shard_index=shard_index,
            )
            for shard_index in shard_indices
        }

        for shard_index in shard_indices:
            work_key = _mapping_work_key(mapping, shard_index)
            if _should_skip_completed_work(
                coordinator,
                incremental=args.incremental,
                work_key=work_key,
            ):
                LOGGER.info(
                    "Skipping mapping %s -> %s shard=%s/%s because the shard is already marked completed for run_id=%s.",
                    mapping.source_container,
                    mapping.target_table,
                    shard_index,
                    mapping.shard_count,
                    config.runtime.run_id,
                )
                continue
            if coordinator:
                acquired = coordinator.acquire(
                    work_key,
                    metadata=work_items[work_key],
                )
                if not acquired:
                    LOGGER.info(
                        "Skipping mapping %s -> %s shard=%s/%s because lease is held by another worker.",
                        mapping.source_container,
                        mapping.target_table,
                        shard_index,
                        mapping.shard_count,
                    )
                    continue
                coordinator.mark_running(
                    work_key,
                    metadata=work_items[work_key],
                )
            try:
                stats = _process_mapping(
                    config=config,
                    mapping=mapping,
                    reader=reader,
                    writer=writer,
                    watermarks=watermark_store,
                    dead_letter=dead_letter,
                    metrics=metrics,
                    cursor_store=cursor_store,
                    incremental=args.incremental,
                    since_ts=args.since_ts,
                    coordinator=coordinator,
                    work_key=work_key,
                    shard_index=shard_index if mapping.shard_count > 1 else None,
                )
                if coordinator and not args.incremental:
                    if stats["rows_failed"] == 0 and stats["docs_failed"] == 0:
                        coordinator.mark_completed(
                            work_key,
                            metadata={
                                "source_container": mapping.source_container,
                                "target_table": mapping.target_table,
                                "docs_seen": stats["docs_seen"],
                                "rows_upserted": stats["rows_upserted"],
                                "rows_deleted": stats["rows_deleted"],
                                "max_success_ts": stats["max_success_ts"],
                            },
                        )
                    else:
                        coordinator.mark_failed(
                            work_key,
                            error=(
                                f"row_failures={stats['rows_failed']} "
                                f"doc_failures={stats['docs_failed']}"
                            ),
                            metadata={
                                "source_container": mapping.source_container,
                                "target_table": mapping.target_table,
                                "docs_seen": stats["docs_seen"],
                            },
                        )
                LOGGER.info(
                    "Completed %s -> %s shard=%s/%s | docs_seen=%s upserted=%s deleted=%s row_failures=%s doc_failures=%s max_ts=%s max_success_ts=%s",
                    mapping.source_container,
                    mapping.target_table,
                    shard_index,
                    mapping.shard_count,
                    stats["docs_seen"],
                    stats["rows_upserted"],
                    stats["rows_deleted"],
                    stats["rows_failed"],
                    stats["docs_failed"],
                    stats["max_ts_seen"],
                    stats["max_success_ts"],
                )
            except Exception as exc:  # noqa: BLE001
                if coordinator and not args.incremental:
                    coordinator.mark_failed(
                        work_key,
                        error=str(exc),
                        metadata={
                            "source_container": mapping.source_container,
                            "target_table": mapping.target_table,
                            "shard_index": shard_index,
                            "shard_count": mapping.shard_count,
                        },
                    )
                LOGGER.exception(
                    "Mapping failed %s -> %s shard=%s/%s: %s",
                    mapping.source_container,
                    mapping.target_table,
                    shard_index,
                    mapping.shard_count,
                    exc,
                )
                exit_code = 1
                if config.runtime.error_mode != "skip":
                    break
            finally:
                if coordinator:
                    coordinator.release(work_key)
        if exit_code != 0 and config.runtime.error_mode != "skip":
            break

    if args.incremental:
        watermark_store.flush()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

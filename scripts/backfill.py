from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.config import MigrationConfig, TableMapping, load_config
from migration.cosmos_reader import CosmosReader
from migration.dead_letter import DeadLetterSink
from migration.logging_utils import configure_logging
from migration.retry_utils import RetryPolicy
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
) -> tuple[str, list[dict[str, Any]]]:
    if not incremental:
        return mapping.source_query, []

    start_ts = max(0, watermark - overlap_seconds)
    query = mapping.incremental_query or "SELECT * FROM c WHERE c._ts > @last_ts"
    parameters = [{"name": "@last_ts", "value": start_ts}]
    return query, parameters


def _flush_upserts(
    writer: SpannerWriter,
    mapping: TableMapping,
    buffer: list[dict[str, Any]],
    counters: dict[str, int],
    *,
    error_mode: str,
    dead_letter: DeadLetterSink,
) -> None:
    if not buffer:
        return
    try:
        counters["rows_upserted"] += writer.write_rows(mapping.target_table, buffer, mapping.mode)
        return
    except Exception as exc:  # noqa: BLE001
        if error_mode != "skip":
            raise
        LOGGER.warning(
            "Batch upsert failed for %s -> %s; falling back to row writes: %s",
            mapping.source_container,
            mapping.target_table,
            exc,
        )
        for row in buffer:
            try:
                counters["rows_upserted"] += writer.write_rows(
                    mapping.target_table,
                    [row],
                    mapping.mode,
                )
            except Exception as row_exc:  # noqa: BLE001
                counters["rows_failed"] += 1
                dead_letter.write(
                    stage="write_upsert",
                    mapping_name=f"{mapping.source_container}->{mapping.target_table}",
                    source_container=mapping.source_container,
                    target_table=mapping.target_table,
                    error=row_exc,
                    transformed_row=row,
                )
    finally:
        buffer.clear()


def _flush_deletes(
    writer: SpannerWriter,
    mapping: TableMapping,
    buffer: list[dict[str, Any]],
    counters: dict[str, int],
    *,
    error_mode: str,
    dead_letter: DeadLetterSink,
) -> None:
    if not buffer:
        return
    try:
        counters["rows_deleted"] += writer.delete_rows(mapping.target_table, mapping.key_columns, buffer)
        return
    except Exception as exc:  # noqa: BLE001
        if error_mode != "skip":
            raise
        LOGGER.warning(
            "Batch delete failed for %s -> %s; falling back to key deletes: %s",
            mapping.source_container,
            mapping.target_table,
            exc,
        )
        for key_row in buffer:
            try:
                counters["rows_deleted"] += writer.delete_rows(
                    mapping.target_table,
                    mapping.key_columns,
                    [key_row],
                )
            except Exception as row_exc:  # noqa: BLE001
                counters["rows_failed"] += 1
                dead_letter.write(
                    stage="delete",
                    mapping_name=f"{mapping.source_container}->{mapping.target_table}",
                    source_container=mapping.source_container,
                    target_table=mapping.target_table,
                    error=row_exc,
                    transformed_row=key_row,
                )
    finally:
        buffer.clear()


def _process_mapping(
    config: MigrationConfig,
    mapping: TableMapping,
    reader: CosmosReader,
    writer: SpannerWriter,
    watermarks: WatermarkStore,
    dead_letter: DeadLetterSink,
    incremental: bool,
    since_ts: int | None,
) -> dict[str, int]:
    counters = {
        "docs_seen": 0,
        "rows_upserted": 0,
        "rows_deleted": 0,
        "rows_failed": 0,
        "docs_failed": 0,
        "max_ts_seen": 0,
    }
    upsert_buffer: list[dict[str, Any]] = []
    delete_buffer: list[dict[str, Any]] = []

    last_watermark = since_ts if since_ts is not None else watermarks.get(mapping.source_container, 0)
    query, params = _query_for_mapping(
        mapping=mapping,
        incremental=incremental,
        watermark=last_watermark,
        overlap_seconds=config.runtime.watermark_overlap_seconds,
    )

    max_docs = config.runtime.max_docs_per_container.get(mapping.source_container)
    LOGGER.info(
        "Running mapping %s -> %s (incremental=%s, watermark=%s, query=%s)",
        mapping.source_container,
        mapping.target_table,
        incremental,
        last_watermark,
        query,
    )

    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=query,
        parameters=params,
        page_size=config.runtime.query_page_size,
        max_docs=max_docs,
    ):
        counters["docs_seen"] += 1
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

        counters["max_ts_seen"] = max(counters["max_ts_seen"], result.source_ts)
        if result.is_delete:
            delete_buffer.append(result.row)
            if len(delete_buffer) >= config.runtime.batch_size:
                _flush_deletes(
                    writer,
                    mapping,
                    delete_buffer,
                    counters,
                    error_mode=config.runtime.error_mode,
                    dead_letter=dead_letter,
                )
        else:
            upsert_buffer.append(result.row)
            if len(upsert_buffer) >= config.runtime.batch_size:
                _flush_upserts(
                    writer,
                    mapping,
                    upsert_buffer,
                    counters,
                    error_mode=config.runtime.error_mode,
                    dead_letter=dead_letter,
                )

    _flush_upserts(
        writer,
        mapping,
        upsert_buffer,
        counters,
        error_mode=config.runtime.error_mode,
        dead_letter=dead_letter,
    )
    _flush_deletes(
        writer,
        mapping,
        delete_buffer,
        counters,
        error_mode=config.runtime.error_mode,
        dead_letter=dead_letter,
    )

    if incremental and counters["max_ts_seen"] > 0:
        watermarks.set(mapping.source_container, counters["max_ts_seen"])
        if config.runtime.flush_watermark_each_mapping:
            watermarks.flush()
    return counters


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    if args.dry_run:
        config.runtime.dry_run = True
    configure_logging(config.runtime.log_level)

    mappings = _selected_mappings(config, args.containers)
    if not mappings:
        LOGGER.error("No mappings selected. Check --container filters.")
        return 1

    retry_policy = RetryPolicy.from_runtime(config.runtime)
    reader = CosmosReader(config.source, retry_policy=retry_policy)
    writer = SpannerWriter(config.target, retry_policy=retry_policy, dry_run=config.runtime.dry_run)
    watermark_store = WatermarkStore(config.runtime.watermark_state_file)
    dead_letter = DeadLetterSink(config.runtime.dlq_file_path)

    exit_code = 0
    for mapping in mappings:
        try:
            stats = _process_mapping(
                config=config,
                mapping=mapping,
                reader=reader,
                writer=writer,
                watermarks=watermark_store,
                dead_letter=dead_letter,
                incremental=args.incremental,
                since_ts=args.since_ts,
            )
            LOGGER.info(
                "Completed %s -> %s | docs_seen=%s upserted=%s deleted=%s row_failures=%s doc_failures=%s max_ts=%s",
                mapping.source_container,
                mapping.target_table,
                stats["docs_seen"],
                stats["rows_upserted"],
                stats["rows_deleted"],
                stats["rows_failed"],
                stats["docs_failed"],
                stats["max_ts_seen"],
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception(
                "Mapping failed %s -> %s: %s",
                mapping.source_container,
                mapping.target_table,
                exc,
            )
            exit_code = 1
            if config.runtime.error_mode != "skip":
                break

    if args.incremental:
        watermark_store.flush()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

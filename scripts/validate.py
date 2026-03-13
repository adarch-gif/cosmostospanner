from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.config import MigrationConfig, TableMapping, load_config
from migration.cosmos_reader import CosmosReader
from migration.logging_utils import configure_logging
from migration.reconciliation import SqliteRowDigestStore, row_digest
from migration.retry_utils import RetryPolicy
from migration.spanner_writer import SpannerWriter
from migration.transform import transform_document

LOGGER = logging.getLogger("validate")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Cosmos-to-Spanner migration.")
    parser.add_argument("--config", required=True, help="Path to migration YAML config.")
    parser.add_argument(
        "--container",
        action="append",
        dest="containers",
        help="Optional source container filter. Can be repeated.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Sample size for key existence checks.",
    )
    parser.add_argument(
        "--compare-values",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Compare sampled row values in addition to key existence checks.",
    )
    parser.add_argument(
        "--reconciliation-mode",
        choices=("sampled", "checksums"),
        default="sampled",
        help="Validation depth. 'sampled' is faster; 'checksums' performs a full row-level reconciliation.",
    )
    return parser.parse_args()


def _selected_mappings(config: MigrationConfig, containers: list[str] | None) -> list[TableMapping]:
    if not containers:
        return config.mappings
    container_set = set(containers)
    return [m for m in config.mappings if m.source_container in container_set]


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return value


def _comparison_columns(mapping: TableMapping) -> list[str]:
    selected_columns: list[str] = []
    for column in mapping.key_columns:
        if column not in selected_columns:
            selected_columns.append(column)
    compare_columns = mapping.validation_columns or [rule.target for rule in mapping.columns]
    for column in [*compare_columns, *mapping.static_columns.keys()]:
        if column not in selected_columns:
            selected_columns.append(column)
    return selected_columns


def _collect_sample_rows(
    reader: CosmosReader, mapping: TableMapping, sample_size: int
) -> tuple[list[dict[str, Any]], int]:
    if sample_size <= 0:
        return [], 0

    sample_rows: list[dict[str, Any]] = []
    transform_failures = 0
    max_scan = sample_size * 5
    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=mapping.source_query,
        page_size=min(100, sample_size),
        max_docs=max_scan,
    ):
        try:
            transformed = transform_document(document, mapping)
        except Exception as exc:  # noqa: BLE001
            transform_failures += 1
            LOGGER.warning(
                "Validation sample transform failed for %s -> %s: %s",
                mapping.source_container,
                mapping.target_table,
                exc,
            )
            continue
        if transformed.is_delete:
            continue
        sample_rows.append(transformed.row)
        if len(sample_rows) >= sample_size:
            break
    return sample_rows, transform_failures


def _count_expected_rows(reader: CosmosReader, mapping: TableMapping) -> tuple[int, int]:
    count = 0
    transform_failures = 0
    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=mapping.source_query,
        page_size=500,
    ):
        try:
            transformed = transform_document(document, mapping)
        except Exception as exc:  # noqa: BLE001
            transform_failures += 1
            LOGGER.warning(
                "Validation count transform failed for %s -> %s: %s",
                mapping.source_container,
                mapping.target_table,
                exc,
            )
            continue
        if transformed.is_delete:
            continue
        count += 1
    return count, transform_failures


def _validate_mapping(
    reader: CosmosReader,
    writer: SpannerWriter,
    mapping: TableMapping,
    sample_size: int,
    compare_values: bool,
    reconciliation_mode: str,
) -> dict[str, Any]:
    if reconciliation_mode == "checksums":
        return _validate_mapping_checksums(reader, writer, mapping)

    cosmos_count, count_transform_failures = _count_expected_rows(reader, mapping)
    spanner_count = writer.count_rows(mapping.target_table)

    sample_rows, sample_transform_failures = _collect_sample_rows(reader, mapping, sample_size)
    sample_keys = [{key: row[key] for key in mapping.key_columns} for row in sample_rows]
    existing = writer.existing_keys(mapping.target_table, mapping.key_columns, sample_keys)
    missing = 0
    for key_row in sample_keys:
        key_tuple = tuple(key_row[column] for column in mapping.key_columns)
        if key_tuple not in existing:
            missing += 1

    sample_value_mismatch_rows = 0
    sample_value_mismatch_fields = 0
    if compare_values and sample_rows:
        compare_columns = mapping.validation_columns or [rule.target for rule in mapping.columns]
        target_rows = writer.read_rows_by_keys(
            table=mapping.target_table,
            key_columns=mapping.key_columns,
            data_columns=compare_columns,
            key_rows=sample_keys,
        )

        for source_row in sample_rows:
            key_tuple = tuple(source_row[column] for column in mapping.key_columns)
            target_row = target_rows.get(key_tuple)
            if not target_row:
                continue
            row_has_mismatch = False
            for column in compare_columns:
                src_value = _normalize_value(source_row.get(column))
                tgt_value = _normalize_value(target_row.get(column))
                if src_value != tgt_value:
                    sample_value_mismatch_fields += 1
                    row_has_mismatch = True
            if row_has_mismatch:
                sample_value_mismatch_rows += 1

    total_transform_failures = count_transform_failures + sample_transform_failures
    return {
        "reconciliation_mode": "sampled",
        "cosmos_count": cosmos_count,
        "spanner_count": spanner_count,
        "count_delta": spanner_count - cosmos_count,
        "sample_size": len(sample_rows),
        "sample_missing": missing,
        "sample_value_mismatch_rows": sample_value_mismatch_rows,
        "sample_value_mismatch_fields": sample_value_mismatch_fields,
        "source_checksum": "",
        "target_checksum": "",
        "full_missing_rows": 0,
        "full_extra_rows": 0,
        "full_mismatched_rows": 0,
        "transform_failures": total_transform_failures,
    }


def _validate_mapping_checksums(
    reader: CosmosReader,
    writer: SpannerWriter,
    mapping: TableMapping,
) -> dict[str, Any]:
    compare_columns = _comparison_columns(mapping)
    transform_failures = 0
    with SqliteRowDigestStore() as digest_store:
        for document in reader.iter_documents(
            container_name=mapping.source_container,
            query=mapping.source_query,
            page_size=500,
        ):
            try:
                transformed = transform_document(document, mapping)
            except Exception as exc:  # noqa: BLE001
                transform_failures += 1
                LOGGER.warning(
                    "Checksum reconciliation transform failed for %s -> %s: %s",
                    mapping.source_container,
                    mapping.target_table,
                    exc,
                )
                continue
            if transformed.is_delete:
                continue
            key_tuple = tuple(transformed.row[column] for column in mapping.key_columns)
            digest_store.upsert_source(
                key_tuple,
                row_digest(transformed.row, compare_columns),
            )

        if hasattr(writer, "iter_all_rows"):
            target_rows = writer.iter_all_rows(
                table=mapping.target_table,
                key_columns=mapping.key_columns,
                data_columns=compare_columns,
            )
        else:
            target_rows = writer.read_all_rows(
                table=mapping.target_table,
                key_columns=mapping.key_columns,
                data_columns=compare_columns,
            ).items()

        for key_tuple, row in target_rows:
            digest_store.upsert_target(key_tuple, row_digest(row, compare_columns))

        summary = digest_store.summarize()

    return {
        "reconciliation_mode": "checksums",
        "cosmos_count": summary.source_count,
        "spanner_count": summary.target_count,
        "count_delta": summary.target_count - summary.source_count,
        "sample_size": 0,
        "sample_missing": 0,
        "sample_value_mismatch_rows": 0,
        "sample_value_mismatch_fields": 0,
        "source_checksum": summary.source_checksum,
        "target_checksum": summary.target_checksum,
        "full_missing_rows": summary.missing_rows,
        "full_extra_rows": summary.extra_rows,
        "full_mismatched_rows": summary.mismatched_rows,
        "transform_failures": transform_failures,
    }


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    configure_logging(config.runtime.log_level)

    mappings = _selected_mappings(config, args.containers)
    if not mappings:
        LOGGER.error("No mappings selected. Check --container filters.")
        return 1

    retry_policy = RetryPolicy.from_runtime(config.runtime)
    reader = CosmosReader(config.source, retry_policy=retry_policy)
    writer = SpannerWriter(config.target, retry_policy=retry_policy, dry_run=False)

    has_failures = False
    for mapping in mappings:
        report = _validate_mapping(
            reader=reader,
            writer=writer,
            mapping=mapping,
            sample_size=args.sample_size,
            compare_values=args.compare_values,
            reconciliation_mode=args.reconciliation_mode,
        )
        if report["reconciliation_mode"] == "checksums":
            LOGGER.info(
                "Validation %s -> %s | mode=checksums cosmos=%s spanner=%s delta=%s missing=%s extra=%s mismatched=%s transform_failures=%s source_checksum=%s target_checksum=%s",
                mapping.source_container,
                mapping.target_table,
                report["cosmos_count"],
                report["spanner_count"],
                report["count_delta"],
                report["full_missing_rows"],
                report["full_extra_rows"],
                report["full_mismatched_rows"],
                report["transform_failures"],
                report["source_checksum"],
                report["target_checksum"],
            )
        else:
            LOGGER.info(
                "Validation %s -> %s | mode=sampled cosmos=%s spanner=%s delta=%s sample=%s missing=%s value_row_mismatch=%s value_field_mismatch=%s transform_failures=%s",
                mapping.source_container,
                mapping.target_table,
                report["cosmos_count"],
                report["spanner_count"],
                report["count_delta"],
                report["sample_size"],
                report["sample_missing"],
                report["sample_value_mismatch_rows"],
                report["sample_value_mismatch_fields"],
                report["transform_failures"],
            )
        if report["reconciliation_mode"] == "checksums":
            has_mapping_failures = (
                report["count_delta"] != 0
                or report["full_missing_rows"] > 0
                or report["full_extra_rows"] > 0
                or report["full_mismatched_rows"] > 0
                or report["transform_failures"] > 0
                or report["source_checksum"] != report["target_checksum"]
            )
        else:
            has_mapping_failures = (
                report["count_delta"] != 0
                or report["sample_missing"] > 0
                or report["sample_value_mismatch_rows"] > 0
                or report["transform_failures"] > 0
            )
        if has_mapping_failures:
            has_failures = True

    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

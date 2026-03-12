from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.config import MigrationConfig, TableMapping, load_config
from migration.cosmos_reader import CosmosReader
from migration.logging_utils import configure_logging
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
    return parser.parse_args()


def _selected_mappings(config: MigrationConfig, containers: list[str] | None) -> list[TableMapping]:
    if not containers:
        return config.mappings
    container_set = set(containers)
    return [m for m in config.mappings if m.source_container in container_set]


def _collect_sample_keys(
    reader: CosmosReader, mapping: TableMapping, sample_size: int
) -> list[dict[str, object]]:
    if sample_size <= 0:
        return []

    sample_keys: list[dict[str, object]] = []
    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=mapping.source_query,
        page_size=min(100, sample_size),
        max_docs=sample_size,
    ):
        transformed = transform_document(document, mapping)
        if transformed.is_delete:
            continue
        sample_keys.append({key: transformed.row[key] for key in mapping.key_columns})
    return sample_keys


def _count_expected_rows(reader: CosmosReader, mapping: TableMapping) -> int:
    count = 0
    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=mapping.source_query,
        page_size=500,
    ):
        transformed = transform_document(document, mapping)
        if transformed.is_delete:
            continue
        count += 1
    return count


def _validate_mapping(
    reader: CosmosReader,
    writer: SpannerWriter,
    mapping: TableMapping,
    sample_size: int,
) -> dict[str, int]:
    cosmos_count = _count_expected_rows(reader, mapping)
    spanner_count = writer.count_rows(mapping.target_table)

    sample_keys = _collect_sample_keys(reader, mapping, sample_size)
    existing = writer.existing_keys(mapping.target_table, mapping.key_columns, sample_keys)
    missing = 0
    for key_row in sample_keys:
        key_tuple = tuple(key_row[column] for column in mapping.key_columns)
        if key_tuple not in existing:
            missing += 1

    return {
        "cosmos_count": cosmos_count,
        "spanner_count": spanner_count,
        "count_delta": spanner_count - cosmos_count,
        "sample_size": len(sample_keys),
        "sample_missing": missing,
    }


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    configure_logging(config.runtime.log_level)

    mappings = _selected_mappings(config, args.containers)
    if not mappings:
        LOGGER.error("No mappings selected. Check --container filters.")
        return 1

    reader = CosmosReader(config.source)
    writer = SpannerWriter(config.target, dry_run=False)

    has_failures = False
    for mapping in mappings:
        report = _validate_mapping(reader, writer, mapping, args.sample_size)
        LOGGER.info(
            "Validation %s -> %s | cosmos=%s spanner=%s delta=%s sample=%s missing=%s",
            mapping.source_container,
            mapping.target_table,
            report["cosmos_count"],
            report["spanner_count"],
            report["count_delta"],
            report["sample_size"],
            report["sample_missing"],
        )
        if report["count_delta"] != 0 or report["sample_missing"] > 0:
            has_failures = True

    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

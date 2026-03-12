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
from migration.retry_utils import RetryPolicy
from migration.spanner_writer import SpannerWriter

LOGGER = logging.getLogger("preflight")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preflight checks for Cosmos DB to Spanner migration config."
    )
    parser.add_argument("--config", required=True, help="Path to migration YAML config.")
    parser.add_argument(
        "--container",
        action="append",
        dest="containers",
        help="Optional source container filter. Can be repeated.",
    )
    parser.add_argument(
        "--check-source",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Check source container accessibility with count query.",
    )
    return parser.parse_args()


def _selected_mappings(config: MigrationConfig, containers: list[str] | None) -> list[TableMapping]:
    if not containers:
        return config.mappings
    container_set = set(containers)
    return [m for m in config.mappings if m.source_container in container_set]


def _check_spanner_schema(writer: SpannerWriter, mapping: TableMapping) -> tuple[bool, str]:
    if not writer.table_exists(mapping.target_table):
        return False, f"Target table does not exist: {mapping.target_table}"

    target_columns = writer.table_columns(mapping.target_table)
    mapped_columns = {rule.target for rule in mapping.columns}
    mapped_columns.update(mapping.static_columns.keys())
    mapped_columns.update(mapping.key_columns)
    missing_columns = sorted(mapped_columns.difference(target_columns))
    if missing_columns:
        return (
            False,
            f"Target table {mapping.target_table} is missing columns: {missing_columns}",
        )
    return True, "OK"


def _check_source_access(reader: CosmosReader, mapping: TableMapping) -> tuple[bool, str]:
    try:
        count = reader.count_documents(mapping.source_container)
    except Exception as exc:  # noqa: BLE001
        return False, f"Unable to query source container {mapping.source_container}: {exc}"
    return True, f"Accessible (count={count})"


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    configure_logging(config.runtime.log_level)
    retry_policy = RetryPolicy.from_runtime(config.runtime)

    mappings = _selected_mappings(config, args.containers)
    if not mappings:
        LOGGER.error("No mappings selected. Check --container filters.")
        return 1

    writer = SpannerWriter(config.target, retry_policy=retry_policy, dry_run=True)
    reader = CosmosReader(config.source, retry_policy=retry_policy)
    failures = 0

    for mapping in mappings:
        ok_schema, msg_schema = _check_spanner_schema(writer, mapping)
        if ok_schema:
            LOGGER.info(
                "Preflight schema OK for %s -> %s: %s",
                mapping.source_container,
                mapping.target_table,
                msg_schema,
            )
        else:
            failures += 1
            LOGGER.error(
                "Preflight schema FAILED for %s -> %s: %s",
                mapping.source_container,
                mapping.target_table,
                msg_schema,
            )

        if args.check_source:
            ok_source, msg_source = _check_source_access(reader, mapping)
            if ok_source:
                LOGGER.info(
                    "Preflight source OK for %s -> %s: %s",
                    mapping.source_container,
                    mapping.target_table,
                    msg_source,
                )
            else:
                failures += 1
                LOGGER.error(
                    "Preflight source FAILED for %s -> %s: %s",
                    mapping.source_container,
                    mapping.target_table,
                    msg_source,
                )

    if failures:
        LOGGER.error("Preflight completed with %s failure(s).", failures)
        return 1
    LOGGER.info("Preflight completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


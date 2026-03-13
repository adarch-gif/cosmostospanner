from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.config import load_config
from migration.cosmos_reader import CosmosReader
from migration.logging_utils import configure_logging
from migration.release_gate import (
    RELEASE_GATE_STATUS_FAILED,
    RELEASE_GATE_STATUS_PASSED,
    ReleaseGateStore,
    build_release_gate_record,
    logical_fingerprint_v1,
    logical_fingerprint_v2,
    make_release_gate_key,
)
from migration.retry_utils import RetryPolicy
from migration.spanner_writer import SpannerWriter
from migration.transform import transform_document
from migration_v2.config import load_v2_config
from migration_v2.pipeline import V2MigrationPipeline
from migration_v2.reconciliation import V2ReconciliationRunner

LOGGER = logging.getLogger("release_gate")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run stage rehearsal checks and write a release-gate attestation."
    )
    parser.add_argument("--gate-file", required=True, help="Shared release-gate state location.")
    parser.add_argument("--scope", required=True, help="Release-gate scope/campaign identifier.")
    parser.add_argument(
        "--environment",
        choices=("stage",),
        default="stage",
        help="Environment label to record in the attestation.",
    )
    parser.add_argument("--v1-config", help="Path to stage v1 YAML config.")
    parser.add_argument(
        "--container",
        action="append",
        dest="containers",
        help="Optional v1 container filter. Can be repeated.",
    )
    parser.add_argument(
        "--v1-reconciliation-mode",
        choices=("sampled", "checksums"),
        default="checksums",
        help="Validation depth for v1 stage rehearsal.",
    )
    parser.add_argument("--v2-config", help="Path to stage v2 YAML config.")
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        help="Optional v2 job filter. Can be repeated.",
    )
    return parser.parse_args()


def _selected_v2_jobs(config, job_names: list[str] | None):
    if not job_names:
        return [job for job in config.jobs if job.enabled]
    allow = set(job_names)
    return [job for job in config.jobs if job.enabled and job.name in allow]


def _selected_v1_mappings(config, containers: list[str] | None):
    if not containers:
        return config.mappings
    allow = set(containers)
    return [mapping for mapping in config.mappings if mapping.source_container in allow]


def _check_v1_spanner_schema(writer: SpannerWriter, mapping) -> bool:
    if not writer.table_exists(mapping.target_table):
        return False
    target_columns = writer.table_columns(mapping.target_table)
    mapped_columns = {rule.target for rule in mapping.columns}
    mapped_columns.update(mapping.static_columns.keys())
    mapped_columns.update(mapping.key_columns)
    return not mapped_columns.difference(target_columns)


def _check_v1_source_access(reader: CosmosReader, mapping) -> bool:
    try:
        reader.count_documents(mapping.source_container)
    except Exception:  # noqa: BLE001
        return False
    return True


def _normalize_value(value: Any) -> Any:
    from datetime import datetime, timezone

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return value


def _comparison_columns(mapping) -> list[str]:
    selected_columns: list[str] = []
    for column in mapping.key_columns:
        if column not in selected_columns:
            selected_columns.append(column)
    compare_columns = mapping.validation_columns or [rule.target for rule in mapping.columns]
    for column in [*compare_columns, *mapping.static_columns.keys()]:
        if column not in selected_columns:
            selected_columns.append(column)
    return selected_columns


def _validate_v1_mapping(
    reader: CosmosReader,
    writer: SpannerWriter,
    mapping,
    reconciliation_mode: str,
) -> dict[str, Any]:
    if reconciliation_mode == "checksums":
        from migration.reconciliation import SqliteRowDigestStore, row_digest

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
                except Exception:  # noqa: BLE001
                    transform_failures += 1
                    continue
                if transformed.is_delete:
                    continue
                key_tuple = tuple(transformed.row[column] for column in mapping.key_columns)
                digest_store.upsert_source(
                    key_tuple,
                    row_digest(transformed.row, compare_columns),
                )

            for key_tuple, row in writer.iter_all_rows(
                table=mapping.target_table,
                key_columns=mapping.key_columns,
                data_columns=compare_columns,
            ):
                digest_store.upsert_target(key_tuple, row_digest(row, compare_columns))

            summary = digest_store.summarize()

        return {
            "reconciliation_mode": "checksums",
            "count_delta": summary.target_count - summary.source_count,
            "full_missing_rows": summary.missing_rows,
            "full_extra_rows": summary.extra_rows,
            "full_mismatched_rows": summary.mismatched_rows,
            "transform_failures": transform_failures,
            "source_checksum": summary.source_checksum,
            "target_checksum": summary.target_checksum,
        }

    sample_rows: list[dict[str, Any]] = []
    transform_failures = 0
    expected_rows = 0
    for document in reader.iter_documents(
        container_name=mapping.source_container,
        query=mapping.source_query,
        page_size=500,
    ):
        try:
            transformed = transform_document(document, mapping)
        except Exception:  # noqa: BLE001
            transform_failures += 1
            continue
        if transformed.is_delete:
            continue
        expected_rows += 1
        if len(sample_rows) < 100:
            sample_rows.append(transformed.row)
    sample_keys = [{key: row[key] for key in mapping.key_columns} for row in sample_rows]
    existing = writer.existing_keys(mapping.target_table, mapping.key_columns, sample_keys)
    missing = 0
    for key_row in sample_keys:
        key_tuple = tuple(key_row[column] for column in mapping.key_columns)
        if key_tuple not in existing:
            missing += 1
    compare_columns = mapping.validation_columns or [rule.target for rule in mapping.columns]
    target_rows = writer.read_rows_by_keys(
        table=mapping.target_table,
        key_columns=mapping.key_columns,
        data_columns=compare_columns,
        key_rows=sample_keys,
    )
    mismatch_rows = 0
    for source_row in sample_rows:
        key_tuple = tuple(source_row[column] for column in mapping.key_columns)
        target_row = target_rows.get(key_tuple)
        if not target_row:
            continue
        row_mismatch = any(
            _normalize_value(source_row.get(column)) != _normalize_value(target_row.get(column))
            for column in compare_columns
        )
        if row_mismatch:
            mismatch_rows += 1
    return {
        "reconciliation_mode": "sampled",
        "count_delta": writer.count_rows(mapping.target_table) - expected_rows,
        "sample_missing": missing,
        "sample_value_mismatch_rows": mismatch_rows,
        "transform_failures": transform_failures,
    }


def _has_v1_validation_issues(report: dict[str, Any]) -> bool:
    if report["reconciliation_mode"] == "checksums":
        return (
            report["count_delta"] != 0
            or report["full_missing_rows"] > 0
            or report["full_extra_rows"] > 0
            or report["full_mismatched_rows"] > 0
            or report["transform_failures"] > 0
            or report["source_checksum"] != report["target_checksum"]
        )
    return (
        report["count_delta"] != 0
        or report["sample_missing"] > 0
        or report["sample_value_mismatch_rows"] > 0
        or report["transform_failures"] > 0
    )


def _run_v1_release_gate(
    config_path: str,
    containers: list[str] | None,
    reconciliation_mode: str,
) -> tuple[bool, str, dict[str, Any], list[str]]:
    config = load_config(config_path)
    if config.runtime.deployment_environment != "stage":
        raise ValueError("v1 release gate config must set runtime.deployment_environment=stage")
    mappings = _selected_v1_mappings(config, containers)
    if not mappings:
        raise ValueError("No v1 mappings selected for stage release gate.")
    retry_policy = RetryPolicy.from_runtime(config.runtime)
    writer = SpannerWriter(config.target, retry_policy=retry_policy, dry_run=False)
    reader = CosmosReader(config.source, retry_policy=retry_policy)

    preflight_failures = 0
    validation_failures = 0
    details: dict[str, Any] = {
        "selected_mappings": len(mappings),
        "reconciliation_mode": reconciliation_mode,
    }

    for mapping in mappings:
        ok_schema = _check_v1_spanner_schema(writer, mapping)
        ok_source = _check_v1_source_access(reader, mapping)
        if not ok_schema or not ok_source:
            preflight_failures += 1
        report = _validate_v1_mapping(
            reader=reader,
            writer=writer,
            mapping=mapping,
            reconciliation_mode=reconciliation_mode,
        )
        if _has_v1_validation_issues(report):
            validation_failures += 1

    details["preflight_failures"] = preflight_failures
    details["validation_failures"] = validation_failures
    success = preflight_failures == 0 and validation_failures == 0
    return success, logical_fingerprint_v1(config, mappings), details, ["preflight", "validation"]


def _has_v2_validation_issues(summary: Any) -> bool:
    return (
        summary.source_rejected_rows > 0
        or summary.pending_cleanup_rows > 0
        or summary.missing_registry_rows > 0
        or summary.extra_registry_rows > 0
        or summary.mismatched_registry_rows > 0
        or summary.missing_target_rows > 0
        or summary.extra_target_rows > 0
        or summary.mismatched_target_rows > 0
        or summary.target_metadata_mismatches > 0
        or summary.source_route_checksum != summary.registry_checksum
        or summary.source_target_checksum != summary.target_checksum
    )


def _run_v2_release_gate(
    config_path: str,
    jobs: list[str] | None,
) -> tuple[bool, str, dict[str, Any], list[str]]:
    config = load_v2_config(config_path)
    if config.runtime.deployment_environment != "stage":
        raise ValueError("v2 release gate config must set runtime.deployment_environment=stage")
    selected_jobs = _selected_v2_jobs(config, jobs)
    if not selected_jobs:
        raise ValueError("No v2 jobs selected for stage release gate.")

    pipeline = V2MigrationPipeline(config)
    preflight_issues = pipeline.preflight(job_names=jobs, check_sources=True)
    summaries = V2ReconciliationRunner(config).validate(job_names=jobs)
    validation_failures = sum(1 for summary in summaries.values() if _has_v2_validation_issues(summary))

    details: dict[str, Any] = {
        "selected_jobs": len(selected_jobs),
        "preflight_failures": len(preflight_issues),
        "validation_failures": validation_failures,
    }
    success = not preflight_issues and validation_failures == 0
    return success, logical_fingerprint_v2(config, selected_jobs), details, ["preflight", "validation"]


def _record_gate_result(
    store: ReleaseGateStore,
    *,
    pipeline: str,
    environment: str,
    scope: str,
    success: bool,
    logical_fingerprint: str,
    checks: list[str],
    details: dict[str, Any],
) -> None:
    store.set(
        make_release_gate_key(pipeline, environment, scope),
        build_release_gate_record(
            pipeline=pipeline,
            environment=environment,
            scope=scope,
            status=RELEASE_GATE_STATUS_PASSED if success else RELEASE_GATE_STATUS_FAILED,
            logical_fingerprint=logical_fingerprint,
            checks=checks if success else [],
            details=details,
        ),
    )


def main() -> int:
    args = parse_args()
    if not args.v1_config and not args.v2_config:
        LOGGER.error("At least one of --v1-config or --v2-config is required.")
        return 1

    configure_logging("INFO")
    store = ReleaseGateStore(args.gate_file)
    has_failures = False

    if args.v1_config:
        try:
            success, fingerprint, details, checks = _run_v1_release_gate(
                args.v1_config,
                args.containers,
                args.v1_reconciliation_mode,
            )
            _record_gate_result(
                store,
                pipeline="v1",
                environment=args.environment,
                scope=args.scope,
                success=success,
                logical_fingerprint=fingerprint,
                checks=checks,
                details=details,
            )
            has_failures = has_failures or not success
            LOGGER.info(
                "Stage release gate v1 | scope=%s success=%s details=%s",
                args.scope,
                success,
                details,
            )
        except Exception as exc:  # noqa: BLE001
            has_failures = True
            _record_gate_result(
                store,
                pipeline="v1",
                environment=args.environment,
                scope=args.scope,
                success=False,
                logical_fingerprint="",
                checks=[],
                details={"error": str(exc)},
            )
            LOGGER.exception("Stage release gate failed for v1: %s", exc)

    if args.v2_config:
        try:
            success, fingerprint, details, checks = _run_v2_release_gate(
                args.v2_config,
                args.jobs,
            )
            _record_gate_result(
                store,
                pipeline="v2",
                environment=args.environment,
                scope=args.scope,
                success=success,
                logical_fingerprint=fingerprint,
                checks=checks,
                details=details,
            )
            has_failures = has_failures or not success
            LOGGER.info(
                "Stage release gate v2 | scope=%s success=%s details=%s",
                args.scope,
                success,
                details,
            )
        except Exception as exc:  # noqa: BLE001
            has_failures = True
            _record_gate_result(
                store,
                pipeline="v2",
                environment=args.environment,
                scope=args.scope,
                success=False,
                logical_fingerprint="",
                checks=[],
                details={"error": str(exc)},
            )
            LOGGER.exception("Stage release gate failed for v2: %s", exc)

    store.flush()
    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

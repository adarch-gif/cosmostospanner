from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.logging_utils import configure_logging
from migration.release_gate import (
    enforce_stage_rehearsal_or_raise,
    logical_fingerprint_v2,
)
from migration_v2.config import load_v2_config
from migration_v2.pipeline import V2MigrationPipeline

LOGGER = logging.getLogger("v2.route_migrate")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run v2 Cosmos (Mongo/Cassandra) size-routed migration to Firestore/Spanner."
    )
    parser.add_argument("--config", required=True, help="Path to v2 YAML config.")
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        help="Optional job filter. Can be repeated.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Override runtime mode to incremental.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and route records without sink writes.",
    )
    parser.add_argument(
        "--allow-partial-success",
        action="store_true",
        help="Return success even if skipped/rejected records exist in error_mode=skip.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_v2_config(args.config)
    if args.incremental:
        config.runtime.mode = "incremental"
    if args.dry_run:
        config.runtime.dry_run = True

    configure_logging(config.runtime.log_level)
    selected_jobs = [
        job
        for job in config.jobs
        if job.enabled and (not args.jobs or job.name in set(args.jobs))
    ]
    if not selected_jobs:
        LOGGER.error("No jobs selected. Check --job filters or enabled flags.")
        return 1
    enforce_stage_rehearsal_or_raise(
        runtime=config.runtime,
        pipeline="v2",
        logical_fingerprint=logical_fingerprint_v2(config, selected_jobs),
    )
    pipeline = V2MigrationPipeline(config)
    stats_by_job = pipeline.run(job_names=args.jobs)

    has_issues = False
    for job_name, stats in stats_by_job.items():
        LOGGER.info(
            "V2 job summary %s | seen=%s firestore=%s spanner=%s moved=%s unchanged=%s checkpoint_skipped=%s rejected=%s failed=%s cleanup_failed=%s lease_conflicts=%s progress_skips=%s out_of_order=%s watermark=%s",
            job_name,
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
        if stats.rejected_records > 0 or stats.failed_records > 0 or stats.cleanup_failed > 0:
            has_issues = True

    if has_issues and not args.allow_partial_success:
        LOGGER.error("V2 migration completed with record-level issues.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

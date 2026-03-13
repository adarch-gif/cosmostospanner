from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.logging_utils import configure_logging
from migration_v2.config import load_v2_config
from migration_v2.reconciliation import V2ReconciliationRunner

LOGGER = logging.getLogger("v2.validate")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate v2 routed migration output across source, route registry, Firestore, and Spanner."
    )
    parser.add_argument("--config", required=True, help="Path to v2 YAML config.")
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        help="Optional job filter. Can be repeated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_v2_config(args.config)
    configure_logging(config.runtime.log_level)

    runner = V2ReconciliationRunner(config)
    summaries = runner.validate(job_names=args.jobs)

    has_issues = False
    for job_name, summary in summaries.items():
        LOGGER.info(
            "V2 validation %s | source=%s registry=%s target=%s rejected_source=%s pending_cleanup=%s "
            "missing_registry=%s extra_registry=%s mismatched_registry=%s missing_target=%s extra_target=%s "
            "mismatched_target=%s target_metadata_mismatches=%s "
            "source_route_checksum=%s registry_checksum=%s source_target_checksum=%s target_checksum=%s",
            job_name,
            summary.source_count,
            summary.registry_count,
            summary.target_count,
            summary.source_rejected_rows,
            summary.pending_cleanup_rows,
            summary.missing_registry_rows,
            summary.extra_registry_rows,
            summary.mismatched_registry_rows,
            summary.missing_target_rows,
            summary.extra_target_rows,
            summary.mismatched_target_rows,
            summary.target_metadata_mismatches,
            summary.source_route_checksum,
            summary.registry_checksum,
            summary.source_target_checksum,
            summary.target_checksum,
        )
        if (
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
        ):
            has_issues = True

    return 1 if has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())

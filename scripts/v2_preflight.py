from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migration.logging_utils import build_log_context, configure_logging
from migration_v2.config import load_v2_config
from migration_v2.pipeline import V2MigrationPipeline

LOGGER = logging.getLogger("v2.preflight")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run preflight checks for v2 multi-API router.")
    parser.add_argument("--config", required=True, help="Path to v2 YAML config.")
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        help="Optional job filter. Can be repeated.",
    )
    parser.add_argument(
        "--check-sources",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Check source connectivity by reading up to one row per selected job.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_v2_config(args.config)
    configure_logging(
        config.runtime.log_level,
        config.runtime.log_format,
        static_fields=build_log_context(
            pipeline="v2-preflight",
            deployment_environment=config.runtime.deployment_environment,
            run_id=config.runtime.run_id,
            worker_id=config.runtime.worker_id,
        ),
    )

    pipeline = V2MigrationPipeline(config)
    issues = pipeline.preflight(job_names=args.jobs, check_sources=args.check_sources)
    if issues:
        for issue in issues:
            LOGGER.error("Preflight issue: %s", issue)
        LOGGER.error("V2 preflight failed with %s issue(s).", len(issues))
        return 1
    LOGGER.info("V2 preflight completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


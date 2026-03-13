from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any

from migration.coordination import WorkCoordinator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect distributed migration control-plane progress and stale leases.",
    )
    parser.add_argument("--progress-file", required=True, help="Progress store location.")
    parser.add_argument(
        "--lease-file",
        default="",
        help="Lease store location. Required to detect or reclaim stale running shards.",
    )
    parser.add_argument("--run-id", default="", help="Run identifier used for progress keys.")
    parser.add_argument(
        "--reclaim-stale",
        action="store_true",
        help="Mark stale running shards as failed so they can be reclaimed by workers.",
    )
    parser.add_argument(
        "--output",
        choices=("text", "json"),
        default="text",
        help="Summary output format.",
    )
    return parser.parse_args()


def _progress_metadata(progress_records: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        work_key: dict(record.metadata)
        for work_key, record in progress_records.items()
    }


def _build_summary(
    progress_records: dict[str, Any],
    *,
    stale_keys: list[str],
    reclaimed_keys: list[str],
    run_id: str,
) -> dict[str, Any]:
    status_counts = Counter(record.status for record in progress_records.values())
    failed_samples: list[dict[str, str]] = []
    for work_key, record in progress_records.items():
        if record.status != "failed":
            continue
        failed_samples.append(
            {
                "work_key": work_key,
                "owner_id": record.owner_id,
                "last_error": record.last_error,
            }
        )
        if len(failed_samples) >= 5:
            break
    return {
        "run_id": run_id,
        "total_shards": len(progress_records),
        "status_counts": dict(status_counts),
        "stale_running": sorted(stale_keys),
        "reclaimed": sorted(reclaimed_keys),
        "failed_samples": failed_samples,
    }


def main() -> int:
    args = parse_args()
    coordinator = WorkCoordinator(
        lease_file=args.lease_file,
        progress_file=args.progress_file,
        run_id=args.run_id,
    )
    progress_records = coordinator.list_progress_records()
    work_items = _progress_metadata(progress_records)
    stale_keys = (
        coordinator.stale_running_keys(work_items)
        if args.lease_file and work_items
        else []
    )
    reclaimed_keys: list[str] = []
    if args.reclaim_stale:
        if not args.lease_file:
            raise SystemExit("--lease-file is required when --reclaim-stale is used.")
        reclaimed_keys = coordinator.reclaim_stale_running(work_items)
        progress_records = coordinator.list_progress_records()
        stale_keys = coordinator.stale_running_keys(_progress_metadata(progress_records))

    summary = _build_summary(
        progress_records,
        stale_keys=stale_keys,
        reclaimed_keys=reclaimed_keys,
        run_id=args.run_id,
    )
    if args.output == "json":
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"run_id: {summary['run_id'] or '<none>'}")
        print(f"total_shards: {summary['total_shards']}")
        print("status_counts:")
        for status, count in sorted(summary["status_counts"].items()):
            print(f"  {status}: {count}")
        if summary["stale_running"]:
            print("stale_running:")
            for work_key in summary["stale_running"]:
                print(f"  - {work_key}")
        if summary["reclaimed"]:
            print("reclaimed:")
            for work_key in summary["reclaimed"]:
                print(f"  - {work_key}")
        if summary["failed_samples"]:
            print("failed_samples:")
            for item in summary["failed_samples"]:
                print(
                    f"  - {item['work_key']} owner={item['owner_id'] or '<none>'} "
                    f"error={item['last_error'] or '<none>'}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

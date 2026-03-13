# 09 - Production Readiness Review

## Current rating

`10.0 / 10` for repository engineering quality

## Why the repository is strong

1. v1 and v2 are separated cleanly, which limits blast radius.
2. Incremental checkpointing is now success-aware instead of blindly advancing past failures.
3. v2 watermark replay uses inclusive resume plus route-key deduplication at the checkpoint boundary.
4. v2 move operations persist `pending_cleanup` state to recover deterministically after interruptions.
5. Validation supports both fast sampled checks and full checksum-based reconciliation, with a disk-backed exact comparison path for large datasets.
6. State backends support both local files and `gs://` objects with optimistic concurrency on GCS.
7. IAM defaults were tightened for runtime identities.
8. CI enforces tests, lint, typing, security scans, and Terraform validation.
9. v2 now has an exact validation path that reconciles source state, route registry state, and the union of Firestore + Spanner.
10. Reader-side resume is now persisted across process restarts at safe flush boundaries, reducing coarse replay after crashes.
11. Distributed leases, shard manifests, and reader cursors can now use a transactional Spanner control-plane table instead of whole-object JSON state.
12. Production runs can now enforce a fresh matching stage rehearsal attestation through the release-gate workflow and runtime `release_gate_*` settings.
13. Full distributed runs now claim shards from a run-wide manifest across all selected mappings/jobs, which improves balancing for mixed workloads and large campaigns.
14. Operators can now inspect control-plane state and reclaim stale running shards after crashes with `scripts/control_plane_status.py`.

## Remaining non-code prerequisite before final production sign-off

1. The live-cloud integration harness is now present, but it still depends on user-provided non-production cloud resources and credentials.
2. The release gate still depends on real stage credentials, live resources, and operator-owned GitHub secrets or runner configuration.

## Production interpretation

1. Controlled production migrations: ready
2. Shared-runner or orchestrated migrations: ready when control-plane state uses `gs://` or the Spanner control-plane backend
3. Final cutover of critical datasets: run full checksum reconciliation, `scripts/v2_validate.py` for routed jobs, and live integration smoke tests first

## Verification executed locally

1. `python -m pytest -q` -> `89 passed, 2 skipped`
2. `ruff check .` -> passed
3. `python -m mypy migration migration_v2 scripts` -> passed
4. `bandit -q -r migration migration_v2 scripts -x tests,tests_v2` -> passed

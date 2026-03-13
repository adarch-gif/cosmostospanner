# 08 - Operations and SRE Guidance

## 1. Core operational controls

1. Run preflight before each migration run.
2. Use dry-run in non-production before the first write.
3. Keep state and DLQ artifacts in durable storage.
4. Alert on failed, rejected, and cleanup-failed record counts.
5. Monitor watermark progression and route-registry sync state.

## 2. Metrics to monitor

1. Source docs read per second
2. Sink writes per second for Firestore and Spanner
3. Retry counts and backoff events
4. DLQ events by stage and error type
5. Watermark progression by job or container
6. Validation mismatch trends
7. Pending cleanup backlog in the v2 route registry

## 3. Recommended deployment pattern

1. Provision infrastructure via Terraform per environment.
2. Run in `dev`, then `stage`, then `prod`.
3. Keep v1 and v2 cutovers independent.
4. Run a canary job or container subset before the full migration.

## 4. Incident playbook

1. Stop the active migration job.
2. Inspect DLQ, recent logs, and state objects.
3. Fix the config, schema, or IAM issue.
4. Re-run preflight.
5. Resume from the stored checkpoint.

## 5. v2 route consistency behavior

For cross-sink moves between Firestore and Spanner:

1. The new destination is written first.
2. The registry is marked `pending_cleanup`.
3. The old destination delete is attempted.
4. On success, the registry is marked `complete`.
5. On failure in `error_mode=skip`, the entry remains `pending_cleanup` and is retried on later runs.

## 6. Incremental checkpoint behavior

1. v1 advances the watermark only after successful writes.
2. v2 replays inclusively from the last watermark and deduplicates by `route_key` at the watermark boundary.
3. If v2 detects out-of-order incremental records or record-level failures, it holds the checkpoint in place instead of advancing it.

## 7. State backend guidance

1. Local state files use lock-file guarded atomic writes.
2. `gs://` state objects use GCS generation preconditions for optimistic concurrency.
3. Prefer `gs://` paths for shared runners, orchestrated jobs, and production cutovers.

## 8. Rollback strategy

v1:

1. Route traffic back to the Cosmos-backed path if needed.
2. Stop v1 migration jobs.
3. Preserve state, DLQ, and validation outputs for RCA.

v2:

1. Stop v2 jobs.
2. Preserve route registry and watermark snapshots.
3. If routing rules changed, update config and replay.
4. If entries remain `pending_cleanup`, rerun incremental jobs until backlog reaches zero.

## 9. Suggested SLO targets

1. Preflight success rate: `100%` before a scheduled run
2. DLQ rate during steady-state incremental sync: `< 0.1%`
3. Pending cleanup backlog at cutover: `0`
4. Post-migration checksum reconciliation on critical datasets: `0` mismatched rows

## 10. Change management guidance

1. Treat routing thresholds and checkpoint storage paths as change-controlled config.
2. Version config files in git.
3. Require PR review for Terraform and migration config changes.
4. Run integration smoke tests after Terraform changes and before cutover.

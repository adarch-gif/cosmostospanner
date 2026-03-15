# 08 - Operations and SRE Guidance

This document is now the short SRE summary. The canonical operational reference is `docs/RUNBOOK.md`.

Read these together:

1. `docs/RUNBOOK.md`
2. `docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`
3. `docs/TROUBLESHOOTING.md`
4. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`

## 1. Core operational controls

1. Run preflight before each migration run.
2. Use dry-run in non-production before the first write.
3. Keep state and DLQ artifacts in durable storage.
4. Alert on failed, rejected, and cleanup-failed record counts.
5. Monitor watermark progression and route-registry sync state.
6. If using persisted reader cursors, store them on the same durable backend as the other state files.

## 2. Metrics to monitor

1. Source docs read per second
2. Sink writes per second for Firestore and Spanner
3. Retry counts and backoff events
4. DLQ events by stage and error type
5. Watermark progression by job or container
6. Validation mismatch trends
7. Pending cleanup backlog in the v2 route registry
8. Source stream restart counts and reader resume warnings
9. Local temporary disk usage during full checksum reconciliation
10. v2 validation mismatch counts across source, route registry, and sinks
11. Reader cursor age and restart-resume events for long-running source scans

## 3. Recommended deployment pattern

1. Provision infrastructure via Terraform per environment.
2. Run in `dev`, then `stage`, then `prod`.
3. Keep v1 and v2 cutovers independent.
4. Run a canary job or container subset before the full migration.
5. For multi-runner execution, configure a shared `lease_file` so workers can claim mappings or jobs without overlap.
6. For large containers or jobs, enable `shard_count > 1` and assign runners against the same shared lease/state backend.
7. Prefer query-level sharding over client-side hash filtering when the source API can push partition predicates down.
8. For distributed full runs, add `progress_file` plus a unique `run_id` so reruns can skip shards already marked `completed`.
9. With `progress_file` enabled, workers claim shards from a shared manifest and naturally focus on `failed`/`pending` work before completed shards.
10. For the highest concurrency, use Spanner-backed `lease_file`, `progress_file`, and `reader_cursor_state_file` locations and create the table from [spanner_control_plane.sql](C:/Users/ados1/cosmos-to-spanner-migration/infra/ddl/spanner_control_plane.sql).
11. For mixed workloads, the scheduler now claims from a run-wide manifest across all selected mappings/jobs instead of draining one mapping/job at a time.

## 4. Incident playbook

1. Stop the active migration job.
2. Inspect DLQ, recent logs, and state objects.
3. Fix the config, schema, or IAM issue.
4. Re-run preflight.
5. Run `scripts/v2_validate.py` if the incident involves routed v2 data consistency.
6. Run `scripts/control_plane_status.py --progress-file ... --lease-file ... --run-id ...` to inspect shard state and identify stale running shards.
7. If a runner crashed and left stale `running` work behind, rerun `scripts/control_plane_status.py` with `--reclaim-stale`.
8. Resume from the stored checkpoint.

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
3. Prefer `gs://` paths or the Spanner control-plane backend for shared runners, orchestrated jobs, and production cutovers.
4. Leases are separate from checkpoints: use `runtime.lease_file` to coordinate runner ownership and `runtime.watermark_state_file` / `runtime.state_file` / `runtime.route_registry_file` to persist progress.
5. Set `runtime.heartbeat_interval_seconds` low enough that crashed workers release ownership quickly, but lower than `runtime.lease_duration_seconds`.
6. Use `runtime.progress_file` only for run-scoped full migrations. Do not reuse the same `run_id` for unrelated full reruns.
7. `runtime.reader_cursor_state_file` is a restart-optimization layer. It must never be treated as the primary correctness checkpoint.
8. If you use Spanner for control-plane state, keep `lease_file` and `progress_file` on the same table with different `namespace` values so `claim_next` can update ownership and progress in one transaction.
9. For the largest runs, also move `watermark_state_file`, `state_file`, and `route_registry_file` onto that table with separate namespaces so checkpoints and registry state are row-based instead of whole-object state.
10. For prod cutovers that must be blocked until stage rehearsal passes, configure `release_gate_file`, `release_gate_scope`, and `require_stage_rehearsal_for_prod: true`.
11. If workers can crash mid-run, use `scripts/control_plane_status.py` to convert stale `running` shards back into reclaimable failed work before restarting large campaigns.

## 8. Shard execution guidance

1. `client_hash` sharding is deterministic and safe, but every runner still scans the source stream and filters non-owned records locally.
2. `query_template` sharding is the preferred mode for very large datasets because it reduces duplicated source reads.
3. Use shard-specific checkpoints automatically created by the runtime for `shard_count > 1`.
4. Keep shard definitions stable for the duration of a run. Changing `shard_count` mid-run changes work ownership and checkpoint keys.
5. When scaling workers up or down, drain or restart the run cleanly rather than changing shard settings in place.
6. Progress state is tracked per shard as `running`, `failed`, or `completed`. A rerun with the same `run_id` will skip shards already marked `completed`.
7. A shard left in `running` after its lease expires can be reclaimed explicitly through `scripts/control_plane_status.py --reclaim-stale`.

## 9. Source resume behavior

1. v1 Cosmos SQL reads resume from Azure continuation tokens after transient page failures inside a running job.
2. v2 Mongo and Cassandra reads reopen the source query and resume from the last emitted `(watermark, source_key)` boundary.
3. Mongo resume is deterministic because the adapter re-sorts by incremental field and key fields.
4. Cassandra resume assumes a stable source query order across retries; use query patterns with predictable ordering when possible.
5. If `reader_cursor_state_file` is configured, the runtime persists the last safe reader boundary after successful state/write flushes so a later process restart can resume more precisely.
6. Reader-side resume does not change write checkpoints. Watermarks still advance only after successful writes.
7. Release-gate attestations are separate from runtime checkpoints. They prove stage rehearsal completed; they are not replay state.

## 10. Rollback strategy

v1:

1. Route traffic back to the Cosmos-backed path if needed.
2. Stop v1 migration jobs.
3. Preserve state, DLQ, and validation outputs for RCA.

v2:

1. Stop v2 jobs.
2. Preserve route registry and watermark snapshots.
3. If routing rules changed, update config and replay.
4. If entries remain `pending_cleanup`, rerun incremental jobs until backlog reaches zero.
5. Re-run `scripts/v2_validate.py` before resuming cutover.

## 11. Suggested SLO targets

1. Preflight success rate: `100%` before a scheduled run
2. DLQ rate during steady-state incremental sync: `< 0.1%`
3. Pending cleanup backlog at cutover: `0`
4. Post-migration checksum reconciliation on critical datasets: `0` mismatched rows
5. v2 routed validation on critical datasets: `0` missing/mismatched registry rows, `0` missing/mismatched target rows
6. Reader cursor backlog on active jobs: should not stall indefinitely relative to the primary checkpoint/watermark

## 12. Change management guidance

1. Treat routing thresholds and checkpoint storage paths as change-controlled config.
2. Treat lease settings and worker identity strategy as change-controlled config for distributed runs.
3. Treat shard configuration as change-controlled config for distributed runs.
4. Version config files in git.
5. Require PR review for Terraform and migration config changes.
6. Run integration smoke tests after Terraform changes and before cutover.

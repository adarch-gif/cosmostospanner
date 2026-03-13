# 08 - Operations and SRE Guidance

## 1. Core operational controls

1. Run preflight before each migration run.
2. Use dry-run in non-prod before first write.
3. Keep state and DLQ artifacts in durable storage.
4. Alert on failed/rejected record counts.
5. Monitor route-registry sync state and clear pending cleanups.

## 2. Metrics to monitor

1. Source docs read/sec
2. Sink writes/sec (Firestore + Spanner)
3. Retry counts and backoff events
4. DLQ events by stage/error type
5. Watermark progression by job
6. Validation mismatch trends
7. Route registry states:
   - `complete`
   - `pending_cleanup`

## 3. Recommended deployment pattern

1. Provision infra via Terraform per env.
2. Run in `dev`, then `stage`, then `prod`.
3. Keep v1 and v2 cutovers independent.
4. Run canary job/container subset before full run.

## 4. Incident playbook (summary)

1. Stop active migration job.
2. Inspect DLQ and recent logs.
3. Fix config/schema issue.
4. Re-run preflight.
5. Resume incremental run from watermark state.

## 5. v2 route consistency behavior

For cross-sink moves (Firestore <-> Spanner), v2 now uses persistent move state:

1. Upsert is written to the new destination first.
2. Registry is marked `pending_cleanup` with previous destination metadata.
3. Old destination delete is attempted.
4. On success, registry is marked `complete`.
5. On failure in `error_mode=skip`, entry stays `pending_cleanup` and is retried on later runs.
## 6. Rollback strategy

### v1

1. Route traffic back to Cosmos source-backed path if needed.
2. Stop v1 migration jobs.
3. Preserve state + logs for RCA.

### v2

1. Stop v2 jobs.
2. Keep route registry and watermark snapshots.
3. If routing rule changed, update config and replay.
4. If entries are `pending_cleanup`, rerun incremental until cleanup backlog is zero.

## 7. State store safety guarantees

1. Watermark and registry flushes use lock-file guarded atomic writes.
2. Corrupted JSON state files fail fast with explicit error messages.
3. Run only one writer per state file path unless you provide external coordination.

## 8. Change management guidance

1. Treat routing thresholds as change-controlled config.
2. Version config files in git.
3. Require PR review for Terraform + migration config changes.

# 08 - Operations and SRE Guidance

## 1. Core operational controls

1. Run preflight before each migration run.
2. Use dry-run in non-prod before first write.
3. Keep state and DLQ artifacts in durable storage.
4. Alert on failed/rejected record counts.

## 2. Metrics to monitor

1. Source docs read/sec
2. Sink writes/sec (Firestore + Spanner)
3. Retry counts and backoff events
4. DLQ events by stage/error type
5. Watermark progression by job
6. Validation mismatch trends

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

## 5. Rollback strategy

### v1

1. Route traffic back to Cosmos source-backed path if needed.
2. Stop v1 migration jobs.
3. Preserve state + logs for RCA.

### v2

1. Stop v2 jobs.
2. Keep route registry and watermark snapshots.
3. If routing rule changed, update config and replay.

## 6. Change management guidance

1. Treat routing thresholds as change-controlled config.
2. Version config files in git.
3. Require PR review for Terraform + migration config changes.


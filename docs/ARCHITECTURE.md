# Architecture Guide

## Purpose

This project migrates data from Azure Cosmos DB (SQL API) to Google Cloud Spanner using a configurable pipeline.

The code supports:

- Full backfill.
- Watermark-based incremental sync.
- Preflight source/target checks.
- Reconciliation validation (sampled or full checksum mode).
- Retry/backoff and dead-letter handling.

## High-level design

The project is intentionally simple and script-driven:

- `scripts/preflight.py`: validates target schema and source accessibility before migration.
- `scripts/backfill.py`: orchestrates read -> transform -> write.
- `scripts/validate.py`: compares expected source rows to target rows and checks sampled keys.
- `migration/config.py`: loads and validates YAML config.
- `migration/cosmos_reader.py`: reads documents from Cosmos containers.
- `migration/transform.py`: maps source fields to target columns and applies converters.
- `migration/spanner_writer.py`: writes upserts/deletes and performs validation reads in Spanner.
- `migration/retry_utils.py`: bounded exponential backoff with jitter.
- `migration/dead_letter.py`: dead-letter JSONL sink for skipped/failed records.
- `migration/state_store.py`: stores per-container watermark state (`_ts`) for incremental runs.

## Data flow

### Full backfill flow

1. Load config and initialize clients.
2. For each mapping, execute `source_query`.
3. Transform each document to a Spanner row.
4. Buffer rows and write in batches (`batch_size`).
5. Optionally map tombstone docs to delete operations.
6. On batch write failure in skip mode, fallback to row-level writes/deletes and emit DLQ entries.
7. Emit counters and completion summary.

### Incremental flow

1. Read last watermark from `runtime.watermark_state_file`.
2. Execute `incremental_query` (default: `_ts > @last_ts` with overlap window).
3. Upsert/delete transformed rows in batches.
4. Track `max_ts_seen` and persist a new watermark only after successful writes.
5. Optionally flush watermark state after each mapping.

## Idempotency model

- Default write mode is `upsert` (`insert_or_update`) for rerunnable jobs.
- Incremental overlap window intentionally re-reads a small window to reduce boundary misses.
- Re-reads are safe for upsert paths.

## Consistency and deletion model

- This pipeline reads current query results from Cosmos containers.
- Hard deletes in Cosmos are not detectable unless delete events are materialized as tombstone fields/documents.
- For delete handling, configure `delete_rule` and ensure key columns are available.

## Runtime and scaling model

- Single-process, single-threaded execution.
- Throughput tuning knobs:
  - `runtime.query_page_size`
  - `runtime.batch_size`
  - mapping-specific `runtime.max_docs_per_container`
- Scale-out is expected through orchestration (multiple jobs, sharded mappings, or time-based partitioning).
- Transient errors are retried based on runtime retry policy.

## Security model

- Secrets are read from environment variables (recommended) or inline config.
- Cosmos key should be supplied via `source.key_env`.
- GCP auth relies on Application Default Credentials.
- Watermark state can use either a local file or a `gs://` object and should be protected with least privilege.
- Dead-letter file can contain source records; protect access similarly to production data.

## Limitations to understand before production

- Full live-cloud integration tests still depend on user-provided resources and credentials.
- Incremental mode is watermark polling, not true cross-cloud CDC stream replication.

# Cosmos DB -> Cloud Spanner Migration Toolkit

This project provides a configurable migration framework for:

1. One-time bulk backfill from Azure Cosmos DB to Google Cloud Spanner.
2. Watermark-based incremental sync (using Cosmos `_ts`).
3. Validation checks (row counts + sample key existence).

All mapping behavior is YAML-driven.

## Documentation index

- `docs/ARCHITECTURE.md`: internal design and data flow.
- `docs/CONFIG_REFERENCE.md`: complete YAML config specification.
- `docs/RUNBOOK.md`: end-to-end execution runbook (setup to cutover/rollback).
- `docs/TROUBLESHOOTING.md`: common issues and fixes.
- `docs/SENIOR_REVIEW.md`: engineering readiness assessment and score.

## Folder structure

- `config/migration.example.yaml`: end-to-end config template.
- `scripts/backfill.py`: backfill and incremental sync runner.
- `scripts/validate.py`: post-load validation runner.
- `migration/`: shared modules for config, read, transform, write, and watermark state.

## Migration plan (production rollout)

### Phase 0: Discovery and target design

- Inventory all Cosmos containers, partition keys, RU consumption, document sizes, and growth rates.
- Classify access patterns: point reads, range scans, fan-out queries, write hot-spots.
- Define Spanner primary keys and interleaving strategy based on query patterns and cardinality.
- Define typed column model (avoid storing everything as JSON unless justified).
- Define rules for nested arrays/objects, nullability, and default values.
- Establish migration SLOs: RPO, RTO, max outage window, and acceptable data drift.

### Phase 1: Environment and schema readiness

- Create GCP project, Spanner instance, and target database.
- Create Spanner DDL for all target tables and indexes.
- Set up service accounts, IAM, and secrets (Cosmos key + GCP auth).
- Run this toolkit in `--dry-run` mode against sample data.
- Validate type conversions and key mapping behavior.

### Phase 2: Initial backfill

- Execute full backfill for each mapping (container -> table).
- Tune `batch_size` and `query_page_size` using observed throughput and commit latency.
- Capture baseline metrics: docs/sec, rows/sec, retry rates, and failure counts.
- Run `scripts/validate.py` and resolve mismatches.

### Phase 3: Incremental catch-up

- Run `backfill.py --incremental` in repeated intervals.
- Persist `_ts` watermarks in `runtime.watermark_state_file`.
- Use overlap window (`watermark_overlap_seconds`) to reduce missed events near boundaries.
- If source deletes are represented with tombstones, configure `delete_rule`.
- Continue until source-target lag is consistently within your RPO.

### Phase 4: Cutover

- Freeze high-risk schema changes and non-essential writes.
- Option A (preferred): dual-write app layer to Cosmos + Spanner for a short period.
- Option B: brief write freeze, run final incremental sync, then switch reads/writes to Spanner.
- Execute validation immediately after cutover.
- Keep rollback path ready (traffic switch back to Cosmos if severe issue).

### Phase 5: Post-cutover hardening

- Monitor p95/p99 latency, error rates, and key query plans in Spanner.
- Compare business KPIs between old/new paths.
- Keep Cosmos in read-only fallback mode for defined retention period.
- Decommission Cosmos only after acceptance window closes.

## Quick start

1. Install dependencies.

```powershell
cd C:\Users\ados1\cosmos-to-spanner-migration
python -m pip install -r requirements.txt
```

2. Copy config template and set values.

```powershell
Copy-Item .\config\migration.example.yaml .\config\migration.yaml
$env:COSMOS_KEY = "<cosmos-primary-key>"
```

3. Dry-run.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

4. Full backfill.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

5. Incremental sync.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

6. Validate.

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
```

## Config highlights

- `mappings[]`: one entry per container -> Spanner table.
- `columns[]`: source path and conversion rules for each target column.
- `static_columns`: fixed values or runtime marker (`__NOW_UTC__`).
- `key_columns`: required for idempotent upsert and delete support.
- `mode`: `upsert`, `insert`, `update`, or `replace`.
- `delete_rule`: optional tombstone mapping.
- `incremental_query`: defaults to `_ts > @last_ts` if omitted.

## Recommended production extensions

- Add retry/backoff around Cosmos and Spanner calls.
- Add dead-letter logging for failed documents.
- Add audit table for per-batch checksums and run IDs.
- Use orchestration (Cloud Run Jobs, Airflow, or GitHub Actions) for scheduling.
- Add canary validations on critical entities before each environment cutover.

## Current implementation limits

- No built-in retry/backoff yet for transient network/service failures.
- No dead-letter queue for skipped records.
- Validation focuses on counts/key-existence, not full row value checksums.
- Watermark file is local state and should not be shared by concurrent writers.

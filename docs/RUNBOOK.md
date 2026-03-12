# End-to-End Runbook

This runbook is designed so a new engineer can operate this project without prior context.

## 1. Prerequisites

### Access and permissions

- Read access to source Cosmos DB account/database/containers.
- Write access to target Cloud Spanner database.
- Ability to create/read local watermark state file.

### Software

- Python 3.10+ recommended.
- `pip`.
- Git.

### Credentials

- Cosmos key available via environment variable, for example:
  - `COSMOS_KEY`
- GCP authentication via Application Default Credentials:
  - `GOOGLE_APPLICATION_CREDENTIALS` or workload identity.

## 2. Clone and install

```powershell
git clone https://github.com/adarch-gif/cosmostospanner.git
cd cosmostospanner
python -m pip install -r requirements.txt
```

## 3. Prepare config

```powershell
Copy-Item .\config\migration.example.yaml .\config\migration.yaml
```

Update `config/migration.yaml`:

- Set `source.endpoint`, `source.database`.
- Set `target.project`, `target.instance`, `target.database`.
- Define one or more `mappings`.

Set secret:

```powershell
$env:COSMOS_KEY = "<cosmos-primary-key>"
```

## 4. Prepare target schema

Before migration, ensure Spanner tables/indexes exist and match mapping output:

- Key columns exist and types are compatible.
- All mapped columns exist in target tables.
- Required indexes for critical read paths are pre-created.

## 5. Preflight checklist

- Validate local config syntax.
- Confirm credentials are loaded.
- Confirm network path to Cosmos and Spanner.
- Confirm Spanner write quota and expected throughput.
- Confirm `watermark_state_file` path is writable.
- Confirm `dlq_file_path` path is writable.

Run automated preflight checks:

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

## 6. Dry-run

Dry-run tests extraction and transformation without writing to Spanner.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

Exit criteria:

- Script exits `0`.
- No unexpected transform failures.
- Mapping counters and log flow look correct.

## 7. Initial backfill

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

For controlled rollout, backfill by container:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --container users
```

## 8. Validation after backfill

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
```

Interpretation:

- `count_delta = 0`, `sample_missing = 0`, and `sample_value_mismatch_rows = 0` indicates strong sample parity.
- Non-zero deltas/mismatches require investigation before cutover.

## 9. Incremental catch-up

Run incremental periodically until lag is acceptable:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

Optional forced watermark start:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental --since-ts 1700000000
```

## 10. Cutover procedure

### Recommended

1. Freeze non-essential schema changes.
2. Run final incremental sync.
3. Run validation.
4. Switch read traffic to Spanner.
5. Switch write traffic to Spanner (or dual-write period then disable Cosmos writes).
6. Monitor error rates, latency, and data correctness.

### Acceptance criteria

- No P1/P2 migration incidents.
- Validation checks pass on critical containers.
- Application KPIs are stable after switch.

## 11. Rollback plan

Rollback trigger examples:

- Sustained correctness mismatch.
- Spanner latency/error SLO breach.
- Critical write path regression.

Rollback steps:

1. Route reads/writes back to Cosmos.
2. Stop migration incrementals.
3. Preserve logs, watermark file, and validation outputs for root cause analysis.
4. Fix issue.
5. Re-run targeted backfill/incremental and re-validate before next cutover.

## 12. Observability recommendations

Track these for each run:

- Docs read/sec from Cosmos.
- Rows written/sec to Spanner.
- Batch commit latency.
- Failed docs count and top exception classes.
- DLQ event count and top `stage`/`error_type`.
- Watermark progression by container.
- Validation delta trends.

## 13. Operational guardrails

- Run only one writer job per mapping unless you add coordination.
- Keep config under version control.
- Treat watermark file as operational state (backup it).
- Prefer `upsert` mode for rerunnable migrations.
- Use `error_mode=fail` in production for first run, then `skip` with DLQ if required for continuity.

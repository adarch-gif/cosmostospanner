# Cosmos DB -> Cloud Spanner Migration Toolkit

This project provides a configurable migration framework for:

1. One-time bulk backfill from Azure Cosmos DB to Google Cloud Spanner.
2. Watermark-based incremental sync (using Cosmos `_ts`).
3. Validation checks (sampled or full checksum-based reconciliation).
4. Preflight schema/access checks before migration.
5. Retry/backoff and dead-letter logging for operational resiliency.
6. Security hardening: identifier validation, least-privilege IAM defaults, distributed-safe state options, and CI security scans.

It now includes a separate **v2 multi-API router** for Cosmos MongoDB API and Cassandra API:

- `< 1 MiB` payloads route to Firestore.
- `>= 1 MiB` payloads route to Spanner up to a safer configured cap (`8 MiB` by default).
- Route registry tracks document destination to support deterministic moves.
- Incremental resume uses inclusive watermark replay plus route-key deduplication to avoid boundary misses.
- v2 implementation is isolated from v1 so requirements can evolve independently.

All mapping behavior is YAML-driven.

## Numbered docs (read in order)

1. `docs/00_START_HERE.md`
2. `docs/01_REPO_OVERVIEW.md`
3. `docs/02_ARCHITECTURE_AND_DATA_FLOW.md`
4. `docs/03_V1_SQL_API_QUICKSTART.md`
5. `docs/04_V2_MULTIAPI_ROUTER_QUICKSTART.md`
6. `docs/05_TERRAFORM_IAC_GUIDE.md`
7. `docs/06_CONFIG_PARAMETERS_AND_SECRETS.md`
8. `docs/07_CODEBASE_STRUCTURE.md`
9. `docs/08_OPERATIONS_AND_SRE.md`
10. `docs/09_PRODUCTION_READINESS_REVIEW.md`
11. `docs/10_INTEGRATION_TESTING.md`

## Additional deep-dive docs

- `docs/ARCHITECTURE.md`
- `docs/CONFIG_REFERENCE.md`
- `docs/RUNBOOK.md`
- `docs/GO_LIVE_CHECKLIST.md`
- `docs/TROUBLESHOOTING.md`
- `docs/SENIOR_REVIEW.md`
- `docs/V2_MULTIAPI_ROUTING.md`
- `infra/terraform/README.md`

## Folder structure

- `config/migration.example.yaml`: end-to-end config template.
- `config/v2.multiapi-routing.example.yaml`: v2 multi-API routing config template.
- `scripts/preflight.py`: source/target readiness checks.
- `scripts/backfill.py`: backfill and incremental sync runner.
- `scripts/validate.py`: post-load validation runner.
- `scripts/v2_preflight.py`: v2 source/target readiness checks.
- `scripts/v2_route_migrate.py`: v2 Mongo/Cassandra size-routed migration runner.
- `scripts/bootstrap_env.ps1`: env bootstrap + Terraform init/plan/apply/destroy helper.
- `migration/`: shared modules for config, read, transform, write, and watermark state.
- `migration_v2/`: v2 source adapters, sinks, router, and pipeline orchestration.
- `tests/`: unit tests for config parsing, transforms, and retry behavior.
- `tests_v2/`: unit tests for v2 router/config/pipeline logic.
- `.github/workflows/ci.yml`: GitHub Actions workflow running test suite on push/PR.
- `pyproject.toml` + `mypy.ini`: lint/type policy used by CI quality gates.
- `infra/terraform/`: Terraform module and stack roots for v1 and v2 infrastructure.
- `infra/terraform/envs/`: environment wrappers (`dev`, `stage`, `prod`) to deploy v1+v2 together.

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

4. Preflight (recommended before first write).

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

5. Full backfill.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

6. Incremental sync.

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

7. Validate (includes value comparison by default).

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
```

For high-assurance cutover, run full checksum reconciliation:

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

8. Run tests.

```powershell
python -m pytest -q
```

9. Run quality/security checks (same guardrails used in CI).

```powershell
ruff check .
python -m mypy migration migration_v2 scripts
bandit -q -r migration migration_v2 scripts -x tests,tests_v2
python -m pip_audit -r requirements.txt
```

## V2 quick start (Mongo/Cassandra with dynamic Firestore/Spanner routing)

1. Copy v2 config template and set values.

```powershell
Copy-Item .\config\v2.multiapi-routing.example.yaml .\config\v2.multiapi-routing.yaml
```

2. Run v2 preflight.

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

3. Run full v2 migration.

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

4. Run incremental v2 migration.

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

## Terraform quick start (env wrappers)

```powershell
# From repo root
.\scripts\bootstrap_env.ps1 -Environment dev -Action plan
```

See `docs/05_TERRAFORM_IAC_GUIDE.md` for full parameter/flow details.

## Config highlights

- `mappings[]`: one entry per container -> Spanner table.
- `columns[]`: source path and conversion rules for each target column.
- `static_columns`: fixed values or runtime marker (`__NOW_UTC__`).
- `key_columns`: required for idempotent upsert and delete support.
- `mode`: `upsert`, `insert`, `update`, or `replace`.
- `delete_rule`: optional tombstone mapping.
- `incremental_query`: defaults to `_ts > @last_ts` if omitted.
- `validation_columns`: optional explicit column list for value-level validation.
- `watermark_state_file`, `state_file`, `route_registry_file`: can be local paths or `gs://bucket/object` paths.
- `retry_*`: retry policy for Cosmos and Spanner operations.
- `dlq_file_path`: JSONL file path for skipped/failed records.

## Recommended production extensions

- Add audit table for per-batch checksums and run IDs.
- Use orchestration (Cloud Run Jobs, Airflow, or GitHub Actions) for scheduling.
- Add canary validations on critical entities before each environment cutover.
- Add integration tests against ephemeral Cosmos/Spanner test environments.

## Current implementation limits

- Incremental replication is watermark-based, not full CDC/change-stream semantics.
- Integration tests against live cloud resources require provisioned credentials and opt-in execution.
- Distributed execution is safest when state files use `gs://` objects or another externally coordinated backend.

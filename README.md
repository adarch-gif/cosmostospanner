# Cosmos DB -> Cloud Spanner Migration Toolkit

This project provides a configurable migration framework for:

1. One-time bulk backfill from Azure Cosmos DB to Google Cloud Spanner.
2. Watermark-based incremental sync (using Cosmos `_ts`).
3. Validation checks (sampled or full checksum-based reconciliation with disk-backed large-dataset support).
4. Preflight schema/access checks before migration.
5. Retry/backoff and dead-letter logging for operational resiliency.
6. Security hardening: identifier validation, least-privilege IAM defaults, distributed-safe state options, and CI security scans.
7. Shard-aware execution for large mappings and jobs, including per-shard checkpoints and shared lease coordination.
8. Exact routed validation for v2 across source, route registry, Firestore, and Spanner.
9. Persisted reader cursors so source scans can resume across process restarts at the last safe boundary.

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
12. `docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`

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
- `scripts/v2_validate.py`: v2 exact routed validation runner.
- `scripts/release_gate.py`: stage rehearsal attestation and production release-gate runner.
- `scripts/control_plane_status.py`: distributed run progress summary and stale-shard recovery helper.
- `scripts/bootstrap_env.ps1`: env bootstrap + Terraform init/plan/apply/destroy helper.
- `migration/`: shared modules for config, read, transform, write, and watermark state.
- `migration_v2/`: v2 source adapters, sinks, router, reconciliation, and pipeline orchestration.
- `tests/`: unit tests for config parsing, transforms, and retry behavior.
- `tests_v2/`: unit tests for v2 router/config/pipeline logic.
- `.github/workflows/ci.yml`: GitHub Actions workflow running test suite on push/PR.
- `.github/workflows/stage-release-gate.yml`: manual workflow for writing stage rehearsal attestations.
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

For high-assurance cutover, run full checksum reconciliation. The repository now uses a disk-backed reconciliation store so checksum mode stays exact without requiring the full source/target keyspace in memory:

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

5. Run exact v2 validation before cutover.

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
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
- `watermark_state_file`, `state_file`, `route_registry_file`: can be local paths, `gs://bucket/object` paths, or `spanner://<project>/<instance>/<database>/<table>?namespace=<name>` control-plane locations.
- `reader_cursor_state_file`: optional persisted source cursor state for crash/restart-aware reader resume; supports local paths, `gs://`, and `spanner://<project>/<instance>/<database>/<table>?namespace=<name>`.
- `lease_file`: optional shared lease state for multi-runner coordination; supports local paths, `gs://`, and `spanner://...` control-plane tables.
- `progress_file`, `run_id`: optional run-scoped shard progress tracking for distributed full runs; use a unique `run_id` per migration campaign. For transactional shard claims, point `progress_file` and `lease_file` at the same Spanner control-plane table with different namespaces.
- `deployment_environment`, `release_gate_file`, `release_gate_scope`, `release_gate_max_age_hours`, `require_stage_rehearsal_for_prod`: optional production gate controls that block prod execution until a fresh matching stage attestation exists.
- `worker_id`, `lease_duration_seconds`, `heartbeat_interval_seconds`: control distributed lease ownership and heartbeats.
- `shard_count`, `shard_mode`, `shard_key_source`: control shard-based parallelization for large mappings and jobs.
- `{{SHARD_INDEX}}`, `{{SHARD_COUNT}}`: available in query-template sharding mode for source-side partitioning.
- `retry_*`: retry policy for Cosmos, Spanner, and reader-side stream restarts after transient mid-stream source failures.
- `dlq_file_path`: JSONL file path for skipped/failed records.

## Recommended production extensions

- Add audit table for per-batch checksums and run IDs.
- Use orchestration (Cloud Run Jobs, Airflow, or GitHub Actions) for scheduling.
- Add canary validations on critical entities before each environment cutover.
- Add integration tests against ephemeral Cosmos/Spanner test environments.

## Current implementation limits

- Incremental replication is watermark-based, not full CDC/change-stream semantics.
- Integration tests against live cloud resources require provisioned credentials and opt-in execution.
- Distributed execution is safest when lease, progress, and reader-cursor state use `gs://` objects or the Spanner control-plane backend.
- Multi-runner execution requires a shared `lease_file` plus shared state paths; otherwise runners can still overlap on the same work.
- For resumable distributed full runs, pair `lease_file` with `progress_file` and a unique `run_id` so completed shards are skipped on reruns.
- When `progress_file` is configured, workers claim shards from a shared run manifest instead of probing every shard blindly.
- Full distributed runs now claim work from a run-wide manifest across all selected mappings/jobs, which improves balancing across mixed workloads.
- For Spanner-backed coordination, create the control-plane table from [infra/ddl/spanner_control_plane.sql](C:/Users/ados1/cosmos-to-spanner-migration/infra/ddl/spanner_control_plane.sql) and use distinct namespaces for leases, progress, and reader cursors.
- Use [scripts/control_plane_status.py](C:/Users/ados1/cosmos-to-spanner-migration/scripts/control_plane_status.py) to summarize progress state and reclaim stale running shards after runner crashes.
- For enforced stage rehearsal before prod, use [scripts/release_gate.py](C:/Users/ados1/cosmos-to-spanner-migration/scripts/release_gate.py) and the runtime `release_gate_*` settings described in [11_RELEASE_GATE_AND_STAGE_REHEARSAL.md](C:/Users/ados1/cosmos-to-spanner-migration/docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md).
- `client_hash` sharding improves ownership isolation but can still duplicate source scans across workers; prefer `query_template` sharding for very large datasets.
- Cosmos SQL source reads resume by continuation token after transient page failures.
- Mongo and Cassandra source reads reopen from the last emitted position and skip already-emitted records; this is safest when the source query order is stable.
- If `reader_cursor_state_file` is configured, the runtime persists the last safe source boundary after successful flushes so later process restarts can resume more precisely.
- Full checksum reconciliation is exact and bounded-memory, but it uses local temporary disk while comparing very large datasets.
- v2 exact validation streams Firestore, Spanner, and the configured route-registry backend; for the largest runs, prefer a Spanner-backed `route_registry_file` instead of an object-backed registry.
- Reader cursor state is a restart hint, not a replacement for checkpoints. Clear the configured cursor state file if you intentionally want to restart a full run from the beginning.

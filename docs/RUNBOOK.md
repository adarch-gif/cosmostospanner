# Operations Runbook

This document is the canonical operations manual for the `cosmos-to-spanner-migration` repository. It is written for engineers, operators, SREs, and technical leads who need to run the repository safely in development, stage, or production. The architecture document explains how the system is designed. This runbook explains how to operate it without guessing.

Read this document together with:

1. `docs/ARCHITECTURE.md`
2. `docs/CONFIG_REFERENCE.md`
3. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
4. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
5. `docs/TROUBLESHOOTING.md`
6. `docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`

## Table of Contents

1. Operating philosophy
2. Roles and responsibilities
3. Environment model
4. Preconditions and access
5. Local setup and dependency preparation
6. Control-plane backend selection
7. v1 configuration preparation
8. v2 configuration preparation
9. Secrets and authentication
10. Preflight runbook
11. Dry-run and canary runbook
12. v1 full backfill runbook
13. v2 full migration runbook
14. Distributed full-run orchestration
15. v1 incremental catch-up
16. v2 incremental catch-up
17. Validation and reconciliation
18. Stage rehearsal and release gates
19. Cutover planning
20. Cutover execution
21. Rollback and recovery
22. Incident response playbooks
23. Observability and alerting guidance
24. Control-plane maintenance
25. Disaster recovery and resume strategy
26. Change management and release discipline
27. Command catalog
28. FAQ

## 1. Operating Philosophy

This repository should be operated as migration infrastructure, not as a convenience script collection. That means the operator mindset matters as much as the command syntax.

The rules of engagement are:

1. correctness beats speed
2. replay safety beats cosmetic throughput
3. stage rehearsal beats production improvisation
4. explicit state beats human memory
5. validation evidence beats intuition

If you are forced to choose between a faster path and a more observable or replay-safe path, choose the replay-safe path.

The repository was specifically hardened to survive reruns, worker failure, and cutover pressure. Those protections only help if operators use the features as intended.

In practical terms, that means:

1. do not run production migrations with ad hoc local-only state
2. do not treat a reader cursor as a primary checkpoint
3. do not enable distributed runners without shared lease and progress state
4. do not use skip mode casually without a DLQ review plan
5. do not start a production campaign if validation, release-gate, or rollback planning is still vague

This runbook is long because those mistakes are common and expensive.

## 2. Roles and Responsibilities

The repository can be operated by a small team, but the responsibilities should still be explicit.

### 2.1 Migration owner

The migration owner is accountable for the end-to-end campaign. This role decides:

1. which configs and mappings or jobs are in scope
2. what the `run_id` is for distributed full runs
3. when stage rehearsal is considered complete
4. what validation evidence is required before cutover
5. whether rollback should be triggered during an incident

### 2.2 Operator or release engineer

The operator executes the runs and ensures that:

1. credentials are present
2. config values match the intended environment
3. state locations are writable and shared where required
4. logs and metrics are being emitted
5. progress and lease state are monitored during execution

### 2.3 SRE or platform engineer

This role is responsible for:

1. infrastructure availability
2. service accounts and IAM
3. target capacity planning
4. log and metrics collection
5. incident escalation support

### 2.4 Reviewer or approver

This role reviews:

1. configuration changes
2. Terraform changes
3. release-gate evidence
4. go-live criteria

Small teams may collapse these roles, but the responsibilities themselves should not disappear.

## 3. Environment Model

The expected environment progression is:

1. `dev`
2. `stage`
3. `prod`

This is not just naming. The runtime and release-gate logic depend on environment labeling.

### 3.1 Dev

Use dev for:

1. unit-level experiments
2. config validation
3. local or low-risk dry-runs
4. target-schema smoke tests

Recommended posture:

1. local JSON state is acceptable
2. text logs are fine
3. distributed coordination is usually unnecessary
4. dry-run mode should be used early and often

### 3.2 Stage

Use stage for:

1. live-cloud rehearsal
2. integration verification
3. realistic performance tuning
4. validation rehearsal
5. release-gate attestation generation

Recommended posture:

1. use shared durable state
2. use JSON logs if stage observability tooling supports them
3. exercise exact validation on representative workloads
4. use the same logical workload shape that production will use

### 3.3 Prod

Use prod only when:

1. the target schema is approved
2. the stage rehearsal is complete
3. release-gate configuration is present if enforced
4. rollback ownership is clear
5. control-plane backends are durable and shared
6. monitoring and alerting are active

Recommended posture:

1. prefer Spanner-backed control-plane state for distributed campaigns
2. use structured logging
3. emit metrics snapshots for external collection
4. turn on `require_stage_rehearsal_for_prod` for high-risk campaigns

## 4. Preconditions and Access

Before any operational run, verify access and permissions explicitly.

### 4.1 Source prerequisites

For v1:

1. read access to the Cosmos SQL API account
2. access to the configured database and containers
3. the `COSMOS_KEY` or equivalent configured environment secret

For v2:

1. access to Cosmos MongoDB API connection string and any database/collection pairs in scope
2. access to Cosmos Cassandra contact points, credentials, keyspaces, and tables if Cassandra jobs are enabled

### 4.2 Target prerequisites

For Spanner:

1. the project exists
2. the instance exists
3. the database exists
4. the target tables exist
5. the service account can read and write the relevant resources

For Firestore in v2:

1. the database exists
2. the collection namespace is approved
3. the runtime identity has read and write access

### 4.3 Control-plane prerequisites

If you are using:

1. local state, verify the directory exists and is writable
2. `gs://` state, verify bucket access and object write permissions
3. `spanner://` control-plane state, verify the control-plane table exists and permissions allow row reads and writes

### 4.4 Operational prerequisites

Before a real campaign starts, verify:

1. who is on point during execution
2. where logs will be collected
3. where metrics snapshots will be scraped from
4. where DLQ artifacts will be stored
5. how rollback will be executed if needed

These are operational prerequisites, not paperwork.

## 5. Local Setup and Dependency Preparation

### 5.1 Clone and install

Use the repository root:

```powershell
cd C:\Users\ados1\cosmos-to-spanner-migration
python -m pip install -r requirements.txt
```

For local quality gates:

```powershell
python -m pip install -r requirements-dev.txt
```

### 5.2 Verify toolchain

Confirm:

1. Python is available
2. dependencies install without resolution failures
3. local authentication to GCP works if you are testing live-cloud access
4. required environment variables are present

### 5.3 Run baseline quality checks before operational changes

Even for config-only changes, it is good practice to confirm the repo is green:

```powershell
python -m pytest -q
ruff check .
python -m mypy migration migration_v2 scripts
bandit -q -r migration migration_v2 scripts -x tests,tests_v2
python -m pip_audit -r requirements.txt
```

This does not replace preflight, but it reduces the chance that you are operating from a broken local tree.

### 5.4 Operator pre-run checklist

Before opening any serious migration session, walk through this quick checklist:

1. confirm you are in the repository root you intend to use
2. confirm the branch or commit is the approved one for the campaign
3. confirm the config file path you are about to use
4. confirm the environment variables in the shell match the environment you intend to target
5. confirm local notes or runbooks for the campaign are open and current

This is basic operational hygiene, but it prevents a surprising number of avoidable errors.

### 5.5 Common operator mistakes to avoid

The most common mistakes in real migration windows are not exotic. They are basic execution errors under time pressure.

Avoid:

1. running a prod config from a shell that still has dev credentials loaded
2. reusing an old terminal session with stale environment variables
3. pointing at the wrong config file because of copy-paste from previous notes
4. assuming a warning is expected without checking whether it is new for this campaign
5. deleting local state or artifacts before the evidence is reviewed

These sound simple, but disciplined execution is part of what makes the system production-grade in practice.

### 5.6 Session recordkeeping

For stage and production work, keep a simple running session log with:

1. start and end times
2. commands executed
3. config files used
4. notable warnings or anomalies
5. validation results
6. any manual interventions

This log does not need to be elaborate. Even a concise campaign note is enough. The point is to avoid reconstructing critical events from memory after the fact.

## 6. Control-Plane Backend Selection

Choosing the state backend is an operational decision with correctness implications.

### 6.1 Local JSON

Use local JSON only when all of the following are true:

1. one runner is executing
2. the run is low-risk or local
3. restart safety requirements are modest
4. you do not need multi-runner coordination

Do not use local JSON for a distributed production campaign.

### 6.2 `gs://` object state

Use GCS-backed state when:

1. you need durable shared state
2. you may have more than one runner, but the concurrency profile is moderate
3. you want an easier upgrade path from local state without provisioning the full Spanner control plane

Recommended objects to move off local disk first:

1. watermarks
2. reader cursors
3. route registry
4. leases
5. progress manifests

### 6.3 `spanner://` control-plane state

Use Spanner-backed state when:

1. multiple workers will run concurrently
2. state durability is critical
3. reclaiming stale work matters
4. you want row-level concurrency rather than whole-object rewrites

Recommended production pattern:

1. keep leases and progress in the same control-plane table under different namespaces
2. keep v1 and v2 checkpoints in separate namespaces
3. store reader cursors in their own namespaces
4. store release-gate attestations in a dedicated namespace

### 6.4 State selection matrix

Use this rule set:

1. local development: local JSON
2. single-runner stage: GCS or Spanner
3. production single-runner: GCS minimum, Spanner preferred
4. production distributed run: Spanner preferred

### 6.5 State paths that matter

For v1, relevant runtime state paths are:

1. `watermark_state_file`
2. `reader_cursor_state_file`
3. `lease_file`
4. `progress_file`
5. `release_gate_file`
6. `dlq_file_path`
7. `metrics_file_path`

For v2, relevant runtime state paths are:

1. `state_file`
2. `route_registry_file`
3. `reader_cursor_state_file`
4. `lease_file`
5. `progress_file`
6. `release_gate_file`
7. `dlq_file_path`
8. `metrics_file_path`

Treat these as change-controlled configuration in production.

## 7. v1 Configuration Preparation

### 7.1 Start from the example

```powershell
Copy-Item .\config\migration.example.yaml .\config\migration.yaml
```

Do not start from memory. Start from the example file because it exposes the current supported runtime fields.

### 7.2 Populate `source`

Confirm:

1. `endpoint` matches the Cosmos account
2. `key_env` points to the actual environment variable that will hold the secret
3. `database` is correct

### 7.3 Populate `target`

Confirm:

1. `project` is correct
2. `instance` is correct
3. `database` is correct

### 7.4 Populate `runtime`

At minimum, decide:

1. `deployment_environment`
2. `batch_size`
3. `query_page_size`
4. `log_format`
5. `watermark_state_file`
6. `error_mode`
7. `dlq_file_path`

For production or distributed runs, also decide:

1. `reader_cursor_state_file`
2. `lease_file`
3. `progress_file`
4. `run_id`
5. `release_gate_file`
6. `release_gate_scope`
7. `require_stage_rehearsal_for_prod`
8. `metrics_file_path`
9. `metrics_format`

### 7.5 Populate mappings

For each mapping, review:

1. `source_container`
2. `target_table`
3. `key_columns`
4. `mode`
5. `source_query`
6. `incremental_query`
7. `shard_count`
8. `shard_mode`
9. `shard_key_source` if client-hash sharding is used
10. `columns`
11. `static_columns`
12. `validation_columns`
13. `delete_rule`

### 7.6 v1 configuration review checklist

Before any live run, answer yes to all of the following:

1. Does every target column exist in the target table?
2. Are `key_columns` correct for idempotent upserts?
3. Does the incremental query use the intended watermark field?
4. If sharding is enabled, is the shard strategy stable for the whole campaign?
5. Are state paths shared and durable if more than one runner will be used?
6. Is `run_id` set if `progress_file` is configured?
7. Are release-gate settings present if stage rehearsal is required for production?

## 8. v2 Configuration Preparation

### 8.1 Start from the example

```powershell
Copy-Item .\config\v2.multiapi-routing.example.yaml .\config\v2.multiapi-routing.yaml
```

### 8.2 Populate `runtime`

Decide:

1. `deployment_environment`
2. `mode`
3. `batch_size`
4. `error_mode`
5. `log_format`
6. `state_file`
7. `route_registry_file`
8. `dlq_file_path`
9. `metrics_file_path`
10. `metrics_format`

For distributed or production runs, also decide:

1. `reader_cursor_state_file`
2. `lease_file`
3. `progress_file`
4. `run_id`
5. `release_gate_file`
6. `release_gate_scope`
7. `require_stage_rehearsal_for_prod`

### 8.3 Populate `routing`

Review:

1. `firestore_lt_bytes`
2. `spanner_max_payload_bytes`
3. `payload_size_overhead_bytes`

Do not casually change routing thresholds in the middle of a campaign. Changing thresholds changes the expected destination of records and can create a large route-move event set.

### 8.4 Populate `targets`

For Firestore:

1. confirm project
2. confirm database
3. confirm collection

For Spanner:

1. confirm project
2. confirm instance
3. confirm database
4. confirm target table

### 8.5 Populate jobs

For each job, confirm:

1. `name`
2. `api`
3. `enabled`
4. source connection settings
5. `route_namespace`
6. `key_fields`
7. `source_query`
8. `incremental_field`
9. `page_size`
10. `shard_count`
11. `shard_mode`

### 8.6 v2 configuration review checklist

Before a live run, answer yes to all of the following:

1. Are route namespaces stable and unique?
2. Are `key_fields` sufficient to define a stable route key?
3. Does the incremental field match the real update semantics of the source?
4. Are routing thresholds approved for the campaign?
5. Is the route-registry location durable and shared?
6. If distributed full runs will be used, are `lease_file`, `progress_file`, and `run_id` configured?
7. Is there a plan for reviewing `pending_cleanup` backlog before cutover?

## 9. Secrets and Authentication

### 9.1 v1 required secrets

Typical environment variables:

```powershell
$env:COSMOS_KEY = "<cosmos-sql-key>"
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\gcp-sa.json"
```

### 9.2 v2 required secrets

Typical environment variables:

```powershell
$env:COSMOS_MONGO_CONNECTION_STRING = "<mongo-connection-string>"
$env:COSMOS_CASSANDRA_USERNAME = "<cassandra-username>"
$env:COSMOS_CASSANDRA_PASSWORD = "<cassandra-password>"
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\gcp-sa.json"
```

### 9.3 Authentication checklist

Before every live run, confirm:

1. source secrets are present in the current shell or runtime environment
2. GCP ADC or service-account auth works
3. the service account has both target-data and control-plane permissions
4. any secret rotation during the campaign is understood and scheduled

### 9.4 Principle of least privilege

For production:

1. source credentials should be read-only where possible
2. target credentials should be scoped only to the required databases and tables
3. control-plane writes should be allowed only to the needed namespaces or tables
4. release-gate writing should be limited to authorized operators or workflows

## 10. Preflight Runbook

Preflight is not optional for serious runs. It is the last cheap point to catch config, access, and schema errors.

### 10.1 Run v1 preflight

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

### 10.2 Run v2 preflight

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

### 10.3 What preflight should verify operationally

Even if the scripts pass, manually confirm:

1. the intended environment is correct
2. the correct config file is being used
3. the target resources match the release scope
4. state locations point at the intended environment and not a leftover dev path

### 10.4 Preflight exit criteria

Do not continue to dry-run or write-bearing execution unless:

1. preflight exits zero
2. no target-schema mismatch remains unresolved
3. source and target access are both healthy
4. required control-plane locations are accessible

### 10.5 Common preflight failures

Common failure classes include:

1. missing or wrong credentials
2. target table missing
3. mapped column missing
4. route target misconfiguration
5. invalid state path
6. environment mismatch

Resolve them before moving on.

### 10.6 Detailed preflight checklist for change windows

For real change windows, preflight should be treated as a structured checklist rather than a single command invocation. Use the script, then verify the following manually.

#### Source-readiness checks

1. Confirm the source account, database, and workload scope are exactly the ones in the cutover plan.
2. Confirm the incremental watermark field is correct for the datasets in scope.
3. Confirm any source-side query-template sharding placeholders render to the intended predicates.
4. Confirm there is no source-side schema or workload freeze conflict that would invalidate the migration assumptions.

#### Target-readiness checks

1. Confirm target tables exist and are writable.
2. Confirm expected indexes or query-supporting structures are already present.
3. Confirm the target service account can read metadata and perform mutations.
4. Confirm no parallel maintenance or schema activity is happening on the target that could distort the run.

#### Control-plane checks

1. Confirm state backends point to the right environment.
2. Confirm old campaign state will not be accidentally reused.
3. Confirm `run_id` is correct for distributed full runs.
4. Confirm release-gate scope matches the planned campaign scope.

#### Observability checks

1. Confirm log format matches what operators expect to consume.
2. Confirm metrics output path is writable.
3. Confirm log and metrics collectors, if any, are active.
4. Confirm the team knows where to read DLQ output if skip mode is enabled.

#### Human checks

1. Confirm the operator on call is actually present.
2. Confirm rollback ownership is clear.
3. Confirm any production change ticket or approval dependency is satisfied.
4. Confirm the team knows what conditions stop the run.

Preflight is the cheapest time to discover a wrong path, wrong environment, or wrong scope. Treat it accordingly.

## 11. Dry-Run and Canary Runbook

Dry-run and canary execution exist to reduce risk before a full write-bearing campaign.

### 11.1 v1 dry-run

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

Use dry-run to verify:

1. source read throughput is acceptable
2. transformation rules behave as expected
3. logging and metrics are wired correctly
4. no unexpected transform failures appear

### 11.2 v2 dry-run

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

Use dry-run to verify:

1. source adapters can read
2. route decisions make sense
3. oversized payload rejection behavior is understood
4. route-registry semantics are observable even though sink writes are suppressed

### 11.3 Canary strategy

A canary is a write-bearing run over a narrow workload. Recommended canary scopes include:

1. one v1 container
2. one or two v2 jobs
3. a single shard of a larger workload
4. a capped record count in a controlled environment

For v1, container-scoped canary:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --container users
```

For v2, job-scoped canary:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

### 11.4 Dry-run and canary exit criteria

Only move to full runs when:

1. expected mappings or jobs are selected
2. logs are clean enough to explain every warning or error
3. DLQ is empty or fully understood
4. metrics show realistic source and write behavior
5. post-canary validation is successful

## 12. v1 Full Backfill Runbook

### 12.1 When to use a full backfill

Use a full backfill when:

1. migrating historical data for the first time
2. rerunning a mapping after a schema or transformation change
3. recovering a target table after a prior failed campaign

### 12.2 Basic v1 full backfill command

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

### 12.3 Container-scoped execution

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --container users
```

Use container scoping when:

1. you want to stage workload rollout
2. you need targeted recovery
3. you are rehearsing on a subset before the full campaign

### 12.4 What to watch during the run

Monitor:

1. document read rate
2. row upsert and delete counts
3. transform failures
4. DLQ count
5. maximum source watermark seen
6. maximum successful watermark committed
7. mapping duration

### 12.5 Full backfill operator checklist

While the run is active:

1. confirm the right config path is logged
2. confirm the expected mappings are being processed
3. confirm the target write mode is the intended one
4. confirm batch sizes are not causing write instability
5. confirm state files or control-plane records are being updated

### 12.6 End-of-run checklist

On completion:

1. confirm the process exits zero
2. confirm no critical failures were hidden in logs
3. inspect metrics or summary logs for abnormal failure counts
4. confirm reader cursors are cleared for completed work where expected
5. schedule validation immediately

### 12.7 v1 performance tuning guidance

v1 tuning should be done conservatively. The purpose is not to maximize benchmark numbers. The purpose is to complete the migration within the required window without compromising stability.

Primary knobs:

1. `batch_size`
2. `query_page_size`
3. shard count
4. worker count if distributed execution is enabled

Tuning guidance:

1. Increase `query_page_size` only if source reads remain stable and do not cause unacceptable memory or transient retry behavior.
2. Increase `batch_size` only if target write latency remains acceptable and error rates do not climb.
3. Increase shard count only when the source query or shard strategy can support the additional concurrency safely.
4. Increase worker count only when shared control-plane state, monitoring, and target capacity are ready for it.

Anti-patterns:

1. raising all knobs at once
2. changing shard topology during a running campaign
3. interpreting temporary throughput spikes as sustainable capacity
4. accepting DLQ growth as an acceptable cost of speed

The safe tuning loop is:

1. change one lever
2. run a representative canary
3. inspect logs, metrics, and validation
4. decide whether the gain is worth the new risk profile

### 12.8 v1 checkpoint audit checklist

When v1 progress looks suspicious, audit the checkpoint model directly.

Check:

1. the current mapping-scoped watermark key
2. whether a legacy container-scoped key still exists and is being used as fallback
3. whether shard-specific keys exist for sharded mappings
4. whether reader cursor state is newer than the primary watermark in a way that would be expected after the last flush
5. whether max-success watermark metrics are consistent with stored checkpoint values

Questions to ask:

1. Is the mapping identity stable?
2. Did the mapping definition change since the checkpoint was written?
3. Did an operator override the starting watermark with `--since-ts`?
4. Did the run crash before the next safe flush boundary?

This kind of audit is what separates real checkpoint debugging from guesswork.

## 13. v2 Full Migration Runbook

### 13.1 When to use a full v2 run

Use a full v2 run when:

1. migrating a routed workload for the first time
2. rebuilding a route-registry-backed dataset
3. replaying after a route-threshold or job-definition change

### 13.2 Basic v2 full migration command

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

### 13.3 Job-scoped execution

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

### 13.4 What to watch during the run

Monitor:

1. docs seen
2. Firestore writes
3. Spanner writes
4. moved records
5. unchanged skips
6. rejected records
7. failed records
8. cleanup failures
9. checkpoint skips
10. out-of-order records

### 13.5 Special v2 concerns

v2 operators must pay attention to:

1. records rejected for size
2. route moves across sinks
3. `pending_cleanup` backlog
4. exact route-registry consistency

These concerns do not exist in the same form in v1.

### 13.6 End-of-run checklist

On completion:

1. confirm the process exits zero or the error counts are intentionally tolerated
2. inspect route-registry state
3. confirm `pending_cleanup` backlog is understood
4. confirm metrics show sane sink-write distribution
5. run exact validation on critical jobs

### 13.7 v2 route-registry audit checklist

When investigating v2 behavior, the route registry is usually the first durable truth source after the logs.

Audit:

1. how many entries are `complete`
2. how many entries are `pending_cleanup`
3. whether `cleanup_from_destination` is set for `pending_cleanup` entries
4. whether checksums in the registry match the source records being replayed
5. whether destination distribution aligns with the expected routing thresholds

Questions to ask:

1. Did route thresholds change mid-campaign?
2. Did one sink become unavailable during a large move event?
3. Are unchanged records being skipped as expected?
4. Is the registry telling a coherent story about current ownership?

### 13.8 v2 route-move operational guidance

Moves between Firestore and Spanner are legitimate events, but they deserve operator attention.

When move rates are high:

1. confirm a config or data-shape change actually explains them
2. check cleanup failure counts
3. inspect whether the target sink mix still matches expectations
4. confirm exact validation after the move wave settles

Large move events often indicate one of three things:

1. expected data growth
2. a route-threshold change
3. a bug or misconfiguration in payload sizing or job definitions

Do not assume high move counts are harmless until you have explained them.

## 14. Distributed Full-Run Orchestration

Distributed full runs should be treated as campaigns, not simple commands.

### 14.1 When to use distributed execution

Use distributed execution when:

1. one runner cannot finish in the required window
2. shard-level parallelism is safe and useful
3. the operational team can monitor control-plane state actively

Do not use distributed execution just because it sounds more advanced.

### 14.2 Required configuration for distributed runs

For v1:

1. configure `lease_file`
2. configure `progress_file`
3. set `run_id`
4. set shard counts deliberately
5. ensure state backends are shared

For v2:

1. configure `lease_file`
2. configure `progress_file`
3. set `run_id`
4. set job shard counts deliberately
5. ensure the route registry and checkpoints are shared

### 14.3 Launch model

The usual pattern is:

1. validate config once
2. start multiple workers against the same config
3. let the coordinator distribute work through leases and progress manifests
4. watch progress until all shards are complete or failed

### 14.4 Progress inspection

Use:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>"
```

This should be part of normal distributed operations, not only incident response.

### 14.5 Reclaim stale work

If a worker crashed and left stale `running` work behind:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>" --reclaim-stale
```

Use this only when you have verified that the original worker is gone or should no longer own the work.

### 14.6 Distributed-run safety rules

Do not:

1. change `shard_count` mid-run
2. reuse `run_id` for a different campaign
3. point a new campaign at an old progress manifest accidentally
4. use local-only state with more than one runner

These are the fastest ways to create confusing or unsafe behavior.

### 14.7 Operating cadence for long distributed campaigns

For long-running distributed campaigns, establish a regular operating cadence instead of watching the run passively.

Suggested cadence:

1. every 15 to 30 minutes, inspect progress status counts
2. every 15 to 30 minutes, inspect failure, rejection, and cleanup metrics
3. after any worker restart, recheck lease and stale-work state
4. at each material milestone, capture a brief operator note about progress and anomalies

What to ask at each check:

1. Are completed shards increasing?
2. Are failures isolated or systemic?
3. Are any shards stuck in `running` longer than expected?
4. Is the target staying healthy under current concurrency?
5. In v2, is cleanup debt shrinking or accumulating?

This cadence matters because distributed failures often become expensive only after they sit unnoticed for a while.

## 15. v1 Incremental Catch-Up

Incremental catch-up is the bridge between initial backfill and cutover.

### 15.1 Basic command

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

### 15.2 Explicit watermark override

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental --since-ts 1700000000
```

Use `--since-ts` carefully. It overrides the normal starting watermark and should be used only when you have a clear replay or recovery reason.

### 15.3 Incremental operating goals

During incremental phase, the main goals are:

1. reduce source-target lag
2. preserve boundary correctness
3. detect delete events if configured
4. prepare for final cutover

### 15.4 Metrics that matter most

Watch:

1. resume watermark
2. max source watermark seen
3. max success watermark committed
4. watermark lag
5. row failures and DLQ events

### 15.5 Incremental stop conditions

Pause or investigate if:

1. watermark lag stops decreasing
2. failure rates rise materially
3. target write latency becomes unstable
4. validation drift increases between incremental cycles

## 16. v2 Incremental Catch-Up

### 16.1 Basic command

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

### 16.2 v2 incremental semantics to remember

v2 incrementals:

1. replay inclusively from the last watermark
2. deduplicate via route keys at the watermark boundary
3. hold checkpoint advancement if failures or out-of-order conditions occur
4. may revisit `pending_cleanup` entries and reconcile them

These are features, not bugs.

### 16.3 Metrics that matter most

Watch:

1. checkpoint-skipped counts
2. out-of-order counts
3. rejected record counts
4. failed record counts
5. cleanup failures
6. pending cleanup backlog

### 16.4 Incremental stop conditions

Pause or investigate if:

1. checkpoint advancement is repeatedly blocked
2. pending cleanup backlog grows instead of shrinking
3. oversized rejection counts are larger than expected
4. exact validation begins to drift

## 17. Validation and Reconciliation

Validation is a separate runbook because it should be treated as a distinct operation, not an afterthought.

### 17.1 v1 sampled validation

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
```

Use sampled validation when:

1. you need fast feedback
2. you are in early rehearsal
3. you are validating non-critical or low-risk mappings

### 17.2 v1 checksum reconciliation

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

Use checksum mode when:

1. preparing for cutover
2. validating critical datasets
3. closing a high-risk incident

### 17.3 v2 exact validation

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

Use exact validation when:

1. validating routed jobs before cutover
2. verifying route-registry consistency after cleanup issues
3. closing a v2 incident involving sink divergence

### 17.4 Validation triage

If validation fails:

1. determine whether the issue is count-only, value mismatch, or missing records
2. determine whether the mismatch is systematic or isolated
3. check logs, DLQ, and checkpoint behavior for the matching run
4. decide whether targeted replay, mapping fix, or rollback is appropriate

### 17.5 Validation evidence checklist

For critical workloads, collect:

1. command used
2. config version or hash
3. environment
4. workload scope
5. validation output
6. date and operator

This turns validation into auditable evidence rather than tribal memory.

### 17.6 Validation decision tree

Use this triage decision tree when validation fails.

#### Case A: counts differ but sampled values mostly match

Likely causes:

1. skipped deletes
2. incomplete replay
3. stale target rows from earlier campaigns

Actions:

1. inspect delete-rule behavior
2. inspect incremental coverage
3. inspect whether target cleanup is required

#### Case B: counts match but value mismatches exist

Likely causes:

1. conversion logic differences
2. static-column expectations not reflected in validation
3. field extraction bugs

Actions:

1. isolate specific rows
2. compare transform output to target rows
3. determine whether the mismatch is systematic or isolated

#### Case C: v2 registry matches source but sinks diverge

Likely causes:

1. interrupted sink writes
2. cleanup failures
3. stale sink state

Actions:

1. inspect `pending_cleanup`
2. inspect sink-specific write failures
3. rerun exact validation after targeted replay

#### Case D: everything drifts after a config change

Likely causes:

1. the config change altered workload identity
2. route thresholds changed
3. shard layout changed
4. mappings or job definitions drifted

Actions:

1. compare config versions
2. decide whether the prior campaign should be abandoned or resumed
3. treat the new run as a new change-controlled event

## 18. Stage Rehearsal and Release Gates

Stage rehearsal is the bridge between technical testing and production trust.

### 18.1 What stage rehearsal should include

A serious stage rehearsal includes:

1. preflight
2. representative migration execution
3. validation
4. operational monitoring
5. rollback rehearsal or at least rollback readiness review

### 18.2 Write release-gate attestations

Use:

```powershell
python .\scripts\release_gate.py `
  --gate-file "<release-gate-path>" `
  --scope "<campaign-scope>" `
  --v1-config "<stage-v1-config>" `
  --v2-config "<stage-v2-config>"
```

You can scope v1 containers or v2 jobs using repeated flags if needed.

### 18.3 What a passing release gate means

A passing release gate means:

1. the selected stage workload passed preflight
2. the selected stage workload passed validation
3. the attestation was recorded under the expected scope
4. the logical workload fingerprint matches the stage run

It does not mean business sign-off is complete. It means the technical rehearsal requirement has been met.

### 18.4 Production enforcement

To force production to respect release-gate state:

1. set `deployment_environment: prod`
2. set `release_gate_file`
3. set `release_gate_scope`
4. set `require_stage_rehearsal_for_prod: true`
5. optionally tune `release_gate_max_age_hours`

### 18.5 Release-gate checklist

Before production:

1. verify the scope string matches the intended campaign
2. verify the attestation is fresh enough
3. verify the stage configs used a matching workload definition
4. verify the release-gate store is readable from the production runtime

## 19. Cutover Planning

Cutover is a business event, not just a technical task. Plan it explicitly.

### 19.1 Decide the cutover strategy

Common options:

1. read cutover first, then write cutover
2. dual-write period, then read cutover
3. freeze writes briefly, run final incremental, then switch all traffic

The repository supports these patterns by reducing migration lag and providing validation evidence. It does not choose the application strategy for you.

### 19.2 Define acceptance criteria

Before cutover day, define:

1. acceptable lag before the final switch
2. acceptable validation posture for each critical dataset
3. who can approve go or no-go
4. what conditions trigger rollback

### 19.3 Freeze change where appropriate

Recommended:

1. freeze non-essential schema changes
2. freeze migration-config changes late in the campaign
3. freeze route-threshold changes during final v2 convergence

### 19.4 Capacity and dependency review

Confirm:

1. target Spanner capacity is adequate
2. Firestore quotas are not at risk
3. source-side query load is acceptable
4. monitoring is staffed
5. incident contacts are available

### 19.5 Cutover readiness checklist

Do not proceed unless:

1. recent incremental runs are stable
2. validation results are acceptable
3. release gate is passed if required
4. rollback is rehearsed or operationally clear
5. on-call coverage is defined

### 19.6 Cutover command-center preparation

Before the window opens, set up a lightweight command-center operating model.

Define:

1. one decision owner
2. one operator running the commands
3. one person watching application and target health
4. one person tracking validation and evidence

Prepare:

1. the exact configs to be used
2. the exact commands to be run
3. the rollback commands and owners
4. log and metrics links
5. target contact points for source, target, and platform escalation

This reduces the most common cutover failure mode: too many people improvising simultaneously.

### 19.7 Cutover go or no-go questions

Immediately before cutover, ask:

1. Are the most recent incremental runs stable?
2. Is validation good enough for the data classes in scope?
3. Is any DLQ debt unresolved in a way that affects critical correctness?
4. In v2, is `pending_cleanup` backlog acceptable and explained?
5. Is release-gate evidence current and matching?
6. Is rollback still executable right now?

If any answer is weak or unclear, the default should be no-go until clarified.

## 20. Cutover Execution

### 20.1 Typical cutover flow

1. announce change window start
2. confirm source and target health
3. run final incremental sync
4. run final validation or a focused validation set
5. switch reads
6. switch writes or enable the pre-decided application path
7. monitor closely

### 20.2 Final incremental commands

For v1:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

For v2:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

### 20.3 Post-switch monitoring

Immediately after switch, watch:

1. application error rate
2. target latency
3. validation spot checks
4. DLQ growth
5. v2 pending cleanup backlog
6. user-visible KPIs if available

### 20.4 Cutover completion checklist

A cutover should not be declared complete until:

1. the application path is stable
2. no critical validation failure remains open
3. no severe target performance issue remains open
4. any residual DLQ or cleanup debt has an explicit disposition

### 20.5 Detailed cutover sequence by operating style

#### Style A: final incremental plus traffic switch

Use when:

1. a short quiet window is available
2. dual-write is not being used

Sequence:

1. pause or minimize source writes if the application plan requires it
2. run final incrementals
3. run focused validation
4. switch reads
5. switch writes
6. monitor

#### Style B: read switch first, write switch later

Use when:

1. read traffic can be isolated from write traffic
2. the team wants to de-risk the write switch separately

Sequence:

1. run final incrementals
2. switch reads
3. monitor read correctness and target load
4. switch writes later under controlled observation

#### Style C: dual-write window

Use when:

1. the application stack supports it safely
2. the team wants a longer confidence window

Sequence:

1. enable dual-write
2. keep incrementals running as needed
3. validate parity
4. switch reads fully to target
5. disable legacy writes after confidence is sufficient

The repository supports all three styles by reducing lag and providing validation, but the application-layer control logic still belongs to the wider cutover plan.

### 20.6 Post-cutover stabilization period

Do not treat the traffic switch as the end of the event. The first stabilization period matters just as much.

Recommended stabilization checks:

1. compare high-level application KPIs before and after switch
2. run spot validations on critical entities
3. check p95 and p99 latency on the target
4. check for elevated error classes in application logs
5. confirm no unexpected growth in migration DLQ or cleanup backlog

Recommended stabilization posture:

1. keep the migration team on standby
2. avoid unrelated schema or config changes
3. keep the source system available for fallback during the agreed observation window

### 20.7 Declaring success

A migration should be declared successful only when:

1. the application is stable on the target path
2. validation evidence is accepted
3. no hidden cleanup or replay debt remains that would undermine trust
4. the rollback threshold period has been passed or formally waived

## 21. Rollback and Recovery

Rollback is not failure. Rollback is a controlled safety mechanism.

### 21.1 Rollback triggers

Examples:

1. sustained correctness mismatch
2. sustained target latency or error SLO violation
3. critical application regression after traffic switch
4. unresolved route divergence in v2

### 21.2 Rollback steps

1. route traffic back to the source-backed or previous path
2. stop migration incrementals if they would interfere with diagnosis
3. preserve logs, metrics, state snapshots, and validation outputs
4. record the exact trigger and decision time
5. begin root cause analysis

### 21.3 What to preserve for RCA

Keep:

1. config files used
2. DLQ artifacts
3. metrics snapshots
4. release-gate state if relevant
5. control-plane status outputs
6. validation outputs

### 21.4 Recovery after rollback

Recovery usually follows this sequence:

1. identify whether the issue was config, schema, target, source, or code
2. correct the issue
3. rerun preflight
4. rerun targeted migration or incremental catch-up
5. rerun validation
6. plan the next cutover attempt

### 21.5 Rollback decision framework

Rollback is often delayed because teams treat it as an emotional failure instead of an engineering decision. Use a simple framework.

Rollback immediately if:

1. production correctness is in doubt and cannot be resolved quickly
2. user-visible failures are severe and persistent
3. target instability threatens a wider platform incident

Stabilize first and investigate if:

1. the issue is limited to observability noise
2. the issue is recoverable without risking correctness
3. the business impact is low and the diagnosis is fast

The critical question is not "Can we probably make it work?" The critical question is "Are we still within the agreed risk envelope?"

### 21.6 Roll-forward versus rollback

Sometimes the right answer is not a full rollback but a controlled roll-forward. That is only appropriate when:

1. the issue is fully understood
2. the fix is small and low-risk
3. correctness is still trusted
4. the rollback path is still preserved if the roll-forward fails

Do not use roll-forward as a euphemism for panic patching.

## 22. Incident Response Playbooks

This section is organized by symptom rather than by module because that is how incidents arrive.

### 22.1 Symptom: source reads fail immediately

Likely causes:

1. credentials missing or rotated
2. endpoint wrong
3. network or firewall issue
4. source account throttling or service issue

Response:

1. verify environment variables
2. rerun preflight
3. confirm the source endpoint and database
4. inspect retry logs
5. pause the campaign if the source platform is degraded

### 22.2 Symptom: target writes fail

Likely causes:

1. schema mismatch
2. permission issue
3. quota or throughput pressure
4. target platform incident

Response:

1. inspect the exact error class in logs or DLQ
2. verify target schema
3. verify IAM
4. reduce pressure if needed by lowering concurrency or batch size
5. rerun only after the cause is understood

### 22.3 Symptom: distributed workers appear stuck

Likely causes:

1. stale running shards
2. lease renewal problems
3. progress state mismatch
4. workers not sharing the same state backend

Response:

1. run `control_plane_status.py`
2. verify all workers are using the same state paths
3. reclaim stale shards if the original worker is gone
4. restart cleanly if necessary

### 22.4 Symptom: v2 pending cleanup backlog rises

Likely causes:

1. delete failures in the old sink
2. unstable connectivity to one sink
3. overaggressive route-threshold change causing many moves

Response:

1. inspect cleanup errors
2. verify sink-specific permissions
3. rerun incrementals to reconcile backlog if the cause is fixed
4. do not cut over while backlog is poorly understood

### 22.5 Symptom: checkpoint does not advance

For v1, likely causes:

1. repeated record-level failures
2. failed batch writes
3. no newer source watermark being observed

For v2, likely causes:

1. blocked checkpoint due to failures
2. out-of-order records
3. unresolved cleanup failures

Response:

1. inspect failure counters
2. inspect logs around the last flush boundary
3. resolve the underlying cause before forcing new watermark logic

### 22.6 Symptom: validation fails

Likely causes:

1. transform logic mismatch
2. target write failures hidden by skip mode
3. stale or wrong config
4. incomplete rerun after partial recovery
5. route-registry inconsistency in v2

Response:

1. determine the mismatch class
2. isolate workload scope
3. examine DLQ and logs for that scope
4. decide whether targeted replay or full rerun is required

### 22.7 Symptom: release gate blocks production unexpectedly

Likely causes:

1. wrong scope string
2. attestation expired
3. logical workload fingerprint changed
4. production config cannot read the release-gate store

Response:

1. verify the scope
2. inspect the attestation age
3. compare the current workload to the staged workload
4. rerun or rewrite the stage release-gate step if needed

Do not disable the gate casually just to get through a deadline. If the gate is failing, it is signaling a real mismatch or missing evidence.

### 22.8 Symptom: DLQ stays empty but the target still looks wrong

Likely causes:

1. a systematic transform issue that does not throw exceptions
2. incorrect mapping or validation column definitions
3. stale target state from an earlier run

Response:

1. compare a few source records with their transformed rows directly
2. verify mapping rules and static columns
3. run checksum or exact validation depending on pipeline

### 22.9 Symptom: the run completes too quickly

This sounds positive, but it can be a red flag.

Likely causes:

1. wrong workload scope
2. job or mapping filter accidentally enabled
3. all work skipped due to old progress state
4. source query returning far fewer rows than expected

Response:

1. confirm the selected mappings or jobs
2. inspect progress skips
3. inspect existing progress manifest state
4. confirm source counts independently if needed

### 22.10 Symptom: one shard repeatedly fails while others succeed

Likely causes:

1. shard-specific data issue
2. hot partition or skew
3. malformed query-template rendering for that shard
4. a target write hotspot driven by that shard's keys

Response:

1. isolate logs for the shard
2. verify shard-specific query rendering
3. inspect representative records from the shard
4. decide whether shard-local replay or broader config correction is needed

### 22.11 Incident severity framing

Use a simple severity lens:

1. correctness risk to production data or cutover trust is high severity
2. target instability that blocks writes is high severity
3. recoverable lag with stable correctness is medium severity
4. local-only tooling or observability inconvenience is lower severity

This matters because migration incidents can look noisy without all being equally important.

### 22.12 Incident communication and handoff

During a migration incident, technical work is only half the job. Clear communication prevents secondary damage.

For every serious incident, capture:

1. start time
2. affected workload scope
3. whether correctness is at risk
4. whether production traffic is already switched
5. what the current safe action is: continue, pause, or rollback

When handing off between engineers or shifts, include:

1. exact command history
2. last known checkpoint or control-plane state
3. last known validation status
4. DLQ and cleanup backlog status
5. what has already been ruled out
6. what the next operator should verify first

This prevents the common incident pattern where each person restarts diagnosis from zero and unknowingly repeats risky actions.

### 22.13 Evidence-first incident closure

Do not close a migration incident based only on the run "looking normal again." Close it when:

1. the cause is understood well enough to explain
2. the corrective action is documented
3. validation confirms correctness where needed
4. any remaining operational debt has an owner and timeline

Migration incidents are especially dangerous when the surface symptoms disappear but the underlying data issue is never disproven.

## 23. Observability and Alerting Guidance

The repository emits logs and metrics, but the operating team must decide how to consume them.

### 23.1 Logging recommendations

For local use:

1. text logs are fine
2. console output is usually enough

For stage and prod:

1. prefer JSON logs
2. include run id and worker id
3. centralize logs if possible

### 23.2 Metrics recommendations

If you configure `metrics_file_path`, decide whether to use:

1. `prometheus` format for textfile collector style integrations
2. `json` for custom collectors or artifact capture

### 23.3 Minimal alert set

At minimum, alert on:

1. repeated run failure
2. DLQ growth above threshold
3. v2 pending cleanup backlog above threshold
4. watermark lag stall
5. stale running shard count above threshold

### 23.4 Dashboard suggestions

Useful dashboard panels include:

1. documents seen over time
2. writes by sink over time
3. failures and DLQ over time
4. watermark lag
5. pending cleanup backlog
6. shard status counts by run id

### 23.5 How to interpret alert combinations

Alerts are most useful in combinations rather than isolation.

Examples:

1. high watermark lag plus low failure count suggests throughput pressure rather than correctness failure
2. high cleanup backlog plus high cleanup failures points to sink-delete problems in v2
3. high progress skips during a rerun may be expected if the same `run_id` is being resumed
4. high lease conflicts plus low overall progress suggests too many workers or mis-scoped work ownership
5. validation failure plus low or zero DLQ may indicate a systematic transform or config issue rather than record-level exceptions

This style of interpretation matters because migration telemetry is highly contextual.

### 23.6 Suggested operational thresholds

Thresholds will vary by environment, but a reasonable starting posture is:

1. DLQ growth in production should be near zero unless explicitly tolerated
2. v2 `pending_cleanup` backlog should trend to zero before cutover
3. repeated stale-shard reclamation should be treated as a reliability signal, not routine background noise
4. watermark lag should trend downward across incremental cycles

These are not hardcoded runtime thresholds. They are operating heuristics.

## 24. Control-Plane Maintenance

The control plane is operational data. Treat it carefully.

### 24.1 Snapshot control-plane state before major events

Before:

1. production cutover
2. rollback
3. reclaiming stale shards
4. route-threshold changes

capture current state or ensure it is durably retained.

### 24.2 Do not manually edit state unless unavoidable

Manual edits to:

1. watermark stores
2. route registries
3. progress manifests
4. lease state

should be exceptional. Use runtime flows and status tools where possible.

### 24.3 If manual state intervention is unavoidable

Follow this discipline:

1. stop active workers
2. snapshot current state
3. write down the exact reason and desired effect
4. make the smallest possible change
5. rerun validation or targeted checks immediately after

### 24.4 Control-plane hygiene rules

1. do not reuse old `run_id` values casually
2. do not mix stage and prod state namespaces
3. do not let old state paths linger ambiguously in config
4. do not assume a reader cursor can replace a checkpoint

### 24.5 When to inspect control-plane state directly

Direct inspection is appropriate when:

1. a distributed run is stuck
2. a rerun is skipping too much or too little work
3. checkpoint advancement is not behaving as expected
4. release-gate enforcement is failing unexpectedly
5. v2 cleanup backlog is not shrinking

The goal of direct inspection is to answer a specific question. Do not browse control-plane state aimlessly in production and start editing because something "looks odd."

## 25. Disaster Recovery and Resume Strategy

The repository is designed for resume, but DR still needs a plan.

### 25.1 If a single worker crashes

Expected response:

1. inspect logs
2. confirm whether the lease expired or the process is gone
3. reclaim stale work if necessary
4. restart the run with the same config and state

### 25.2 If a host or environment is lost

If control-plane state is durable and shared, a new host can continue with:

1. the same config
2. the same state backends
3. the same `run_id` for distributed full runs if continuing the same campaign

### 25.3 If state storage is lost or corrupted

Response depends on what was lost:

1. if only reader cursors were lost, correctness remains recoverable through primary checkpoints
2. if checkpoints were lost, replay scope increases and may require careful restart planning
3. if route registry was lost in v2, exact routed state may require rebuild or deeper recovery steps

This is why durable control-plane storage matters.

### 25.4 DR recommendations

For serious production campaigns:

1. use durable shared control-plane storage
2. retain logs and metrics centrally
3. store configs in version control
4. keep release-gate and validation artifacts

### 25.5 Recovery drills worth rehearsing

If the team has time for only a few rehearsals, rehearse these:

1. resume a v1 incremental after a forced process crash
2. resume a v2 incremental with a deliberately introduced `pending_cleanup` entry
3. reclaim stale distributed shards and continue the campaign
4. validate a failed stage release gate and repair the workflow

These rehearsals test the features that most directly justify the architecture.

### 25.6 Recovery mindset

When recovering, avoid two common mistakes:

1. wiping state first and asking questions later
2. rerunning blindly from scratch without understanding what state is still durable

The correct mindset is:

1. inspect current truth
2. preserve evidence
3. choose the smallest safe replay scope
4. validate again after recovery

## 26. Change Management and Release Discipline

Migration campaigns fail when last-minute config drift is treated casually.

### 26.1 Change-controlled items

Treat the following as controlled changes:

1. mapping definitions
2. v2 job definitions
3. routing thresholds
4. state paths
5. shard counts
6. batch sizes for critical runs
7. release-gate scope and requirements

### 26.2 Review expectations

At minimum:

1. review config changes in pull requests
2. review Terraform changes in pull requests
3. review cutover runbooks before the change window

### 26.3 Late-stage freeze

As cutover approaches, freeze:

1. non-essential config changes
2. non-essential schema changes
3. non-essential infrastructure changes

This reduces the chance that the final campaign is invalidated by avoidable drift.

### 26.4 Recommended review package for a production campaign

Before approving a production campaign, the reviewer should be able to inspect:

1. the exact v1 and or v2 config files
2. the exact state backend locations
3. the planned `run_id` if distributed full runs are involved
4. the stage validation evidence
5. the release-gate scope and status
6. the rollback plan
7. the monitoring plan

This is a much better review package than a generic "we already tested it."

### 26.5 Anti-patterns in change management

Avoid:

1. editing configs directly on an operator workstation minutes before cutover without review
2. changing route thresholds during the final convergence window
3. changing shard strategy after progress state already exists
4. mixing stage and prod state paths
5. copying and pasting commands from old campaign notes without checking scope and environment

These are mundane mistakes, but they create disproportionate risk.

### 26.6 Post-campaign review and closeout

A migration campaign is not operationally complete when the commands stop running. It is complete when the campaign memory has been captured and the system is left in a clean, defensible state.

Closeout should include:

1. final validation summary
2. final release-gate or approval artifact references
3. final command log or execution record
4. any remaining known issues or accepted debt
5. disposition of source system retention and rollback window

Recommended post-campaign questions:

1. Which settings worked well and should become the next default?
2. Which alerts were most useful and which were noisy?
3. Did any manual intervention occur that should be turned into tooling?
4. Did the team discover any ambiguity in the runbook or architecture docs?
5. Did the control-plane design behave as expected under real pressure?

This review is important because migration tooling improves fastest when the team captures operational learning immediately, not months later.

### 26.7 Documentation updates after incidents or cutovers

If a campaign exposed a gap, update the documentation while the evidence is fresh.

Good triggers for a documentation update:

1. an operator had to infer behavior from code during an incident
2. a control-plane recovery step was unclear
3. release-gate behavior surprised the team
4. a validation mismatch required a repeated custom explanation
5. a route-threshold or shard-strategy change exposed hidden assumptions

The best documentation in migration systems is written by people who just had to use it under pressure.

## 27. Command Catalog

### 27.1 v1 commands

Preflight:

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

Dry-run:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

Full backfill:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

Incremental:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

Validation:

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

### 27.2 v2 commands

Preflight:

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

Dry-run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

Full run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

Incremental:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Exact validation:

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

### 27.3 Shared commands

Release gate:

```powershell
python .\scripts\release_gate.py --gate-file "<path>" --scope "<scope>" --v1-config "<stage-v1-config>" --v2-config "<stage-v2-config>"
```

Control-plane status:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>"
```

Reclaim stale:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>" --reclaim-stale
```

### 27.4 Command usage patterns and guardrails

The command catalog is small, but the way commands are used matters.

Guidance:

1. run preflight before the first write-bearing run in any environment
2. prefer container or job scoping for canaries instead of inventing temporary config variants
3. use the same config file path consistently within a campaign
4. keep command history for production windows
5. record when `--since-ts` overrides are used and why

For distributed campaigns:

1. document the exact `run_id`
2. document the shared state paths
3. document how many workers were launched
4. document any stale-shard reclamation events

For validation:

1. record whether the mode was sampled, checksums, or exact routed validation
2. record the sample size when using sampled mode
3. attach outputs to the campaign notes when possible

### 27.5 Example production operating sequence

An example high-discipline production sequence for v1 might be:

1. run preflight
2. run a final canary or small confirmation incremental if needed
3. run the main incremental convergence cycle
4. run checksum validation on critical mappings
5. confirm release gate if enforced
6. execute cutover
7. monitor stabilization

An example high-discipline production sequence for v2 might be:

1. run preflight
2. run final incremental convergence
3. inspect `pending_cleanup` backlog
4. run exact routed validation on critical jobs
5. confirm release gate if enforced
6. execute cutover
7. monitor route consistency and cleanup state during stabilization

These examples are not hard rules, but they show the intended rhythm of operations.

### 27.6 What not to automate blindly

Some steps are good automation targets:

1. preflight
2. migrations
3. validation
4. release-gate writing
5. metrics and log collection

Some steps should retain an explicit human decision point:

1. final go or no-go
2. rollback trigger
3. manual control-plane intervention
4. approval of significant route-threshold changes

The repository is automation-friendly, but high-stakes migration governance still benefits from deliberate human checkpoints.

## 28. FAQ

### Do I need reader cursors?

Not for correctness. They are an efficiency and restart-precision feature. The primary checkpoint is still the correctness boundary.

### Can I run distributed jobs with local JSON state?

You technically can misconfigure it that way, but you should not. Distributed execution requires shared durable state.

### Should I use skip mode in production?

Only with intention. Skip mode is useful, but only if you have a disciplined DLQ review and replay plan.

### Do I need exact validation for every run?

Not every run. But you should use high-assurance validation for critical workloads and before cutover.

### Can I change shard counts mid-run?

Do not do that. Shard counts are part of workload identity and checkpointing semantics.

### When should I prefer Spanner-backed control-plane state?

Whenever distributed execution, durability, or strong operational safety matters. That usually means stage for serious rehearsal and production for real campaigns.

### What matters most during cutover?

Three things:

1. recent incremental stability
2. validation evidence
3. rollback clarity

If any of those are weak, the migration is not really ready.

### What is the most dangerous operational misconception?

The most dangerous misconception is thinking that because the pipeline is replay-safe, operators can be sloppy. Replay-safe does not mean consequence-free. It means the system gives you a safe recovery path if you preserve state and make deliberate decisions.

### When is it safe to clean up old state?

Only after:

1. the campaign is complete
2. validation evidence is retained
3. rollback is no longer needed
4. any incident RCA window has closed

### Should old source systems be decommissioned immediately after cutover?

Usually no. Keep the source available through the agreed observation window so rollback remains practical if a latent issue appears.

### What should be documented after a campaign?

At minimum:

1. commands run
2. config versions
3. validation results
4. release-gate evidence
5. incidents and resolutions
6. final go-live decision and timestamp

That documentation is part of the platform memory for the next campaign.

### How much manual state editing is too much?

If you find yourself repeatedly editing checkpoint, route-registry, or progress state by hand, stop and treat that as a design or tooling problem. Manual state edits should be rare, explicit, and reversible. Frequent manual edits mean the operating model is weaker than it should be.

### What is the right default when the team is unsure?

Pause, gather evidence, and prefer the safer replay path. The repository is designed to support deliberate recovery. It is not designed to make panic decisions safe.

### When should the team stop and escalate?

Stop and escalate when:

1. correctness is uncertain and cannot be proven quickly
2. a proposed manual state edit would affect multiple mappings, jobs, or shards
3. a production gate is being bypassed for schedule reasons
4. rollback is being debated without a clear risk statement

Those are not moments for isolated operator judgment. They are moments for explicit engineering review.

### What is the best way to use this runbook during an active event?

Use it as a decision aid, not just as reading material. Before a major phase:

1. review the relevant section
2. confirm the inputs and commands
3. confirm the stop conditions
4. confirm who owns the next decision

During incidents, do not try to absorb the entire document. Jump to the symptom, confirm the likely causes, preserve evidence, and then decide whether to continue, pause, or roll back.

### What should a new operator do first?

Read the architecture guide, then this runbook, then the relevant config file. After that, run preflight in the intended environment before attempting any write-bearing command. That sequence gives the fastest safe path to situational awareness.

Then confirm who owns rollback, because that answer should never be unclear once a migration run starts.

Clarity before commands is part of safe execution.

That discipline is what turns tooling into a reliable operational system.

It also prevents avoidable confusion later.

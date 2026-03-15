# 19 - Architecture Reference

This document is the canonical architecture reference for the `cosmos-to-spanner-migration` repository. It explains what the system is trying to achieve, why the codebase is split into two pipelines, how the distributed control plane works, how checkpoints and reader cursors differ, how idempotency is preserved, how data integrity is validated, and where the code is designed to be extended safely.

If you are new to the repository, read this guide together with:

1. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
2. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
3. `docs/20_OPERATIONS_RUNBOOK.md`
4. `docs/21_CONFIG_REFERENCE.md`
5. `docs/07_CODEBASE_STRUCTURE.md`

This guide is intentionally long. The repository is no longer a simple one-shot migration script. It now contains two distinct migration engines, a shared control-plane abstraction, distributed coordination, release-gate enforcement, structured observability, and multiple correctness layers. Short summaries are useful as entry points, but they are not enough to operate, extend, or review the system at Principal level.

## Table of Contents

1. System purpose and problem statement
2. Architectural goals and non-goals
3. High-level system shape
4. Why the repository contains two pipelines
5. Shared design principles
6. Repository structure and runtime boundaries
7. v1 architecture deep dive
8. v2 architecture deep dive
9. Shared control plane and state backends
10. Distributed execution model
11. Idempotency, replay, and checkpoint design
12. Reader cursor design and crash recovery
13. Data integrity and validation strategy
14. Observability model
15. Security, safety, and release governance
16. Infrastructure and deployment topology
17. Extensibility model
18. Key design decisions and trade-offs
19. Limitations and operational caveats
20. Reading order for maintainers
21. Glossary

## 1. System Purpose and Problem Statement

At a glance, this repository migrates data from Azure Cosmos DB into Google Cloud data stores. That description is correct but incomplete. The real problem is not "copy documents from A to B." The real problem is "move live production data across clouds with correctness, restartability, and controlled cutover behavior."

In practice, a cloud-to-cloud migration fails when one of the following is true:

1. The pipeline cannot be rerun safely.
2. The cutover plan depends on operator memory instead of machine-readable state.
3. Incremental runs can miss or duplicate records at the checkpoint boundary.
4. Long-running scans cannot resume cleanly after process or worker failure.
5. Validation is either too shallow to trust or too expensive to run at production scale.
6. Distributed workers compete for the same work and overwrite each other's progress.
7. The pipeline itself has no internal model of route ownership, cleanup state, or replay safety.

This repository addresses those failure modes directly.

The system therefore has five primary business objectives:

1. Move existing data during a full backfill.
2. Continue catching up while the source system is still live.
3. Provide enough evidence to support a controlled cutover.
4. Survive partial failures, retries, reruns, and worker crashes without silent corruption.
5. Support environments ranging from local development to production-scale distributed migration campaigns.

The architecture is built around those objectives, not around a simplistic ETL pattern.

## 2. Architectural Goals and Non-Goals

### 2.1 Architectural goals

The architecture aims to satisfy the following technical properties.

#### Correctness before convenience

The design prioritizes replay safety, deterministic ownership, and explicit state transitions over minimal code volume. That is why the repository includes watermarks, route registries, leases, progress manifests, reader cursors, release gates, and reconciliation tools instead of assuming that a single script run will finish successfully.

#### Idempotent reruns

Both pipelines are designed so that rerunning the same workload is safe. In v1, upserts and watermark advancement semantics make repeated runs safe. In v2, destination routing plus route-registry state makes repeated runs safe even when documents move between Firestore and Spanner.

#### Explicit state

Progress is not inferred from logs alone. The system persists:

1. primary checkpoints
2. optional reader cursors
3. distributed leases
4. full-run progress manifests
5. v2 route registry entries
6. release-gate attestations

The architecture treats state as a first-class control plane.

#### Scalable from laptop to distributed execution

The same logical abstractions can be backed by local JSON, GCS objects, or a Spanner control-plane table. That allows the exact same migration logic to work in:

1. local developer testing
2. single-runner staging
3. orchestrated multi-runner production campaigns

#### YAML-driven behavior

The migration logic is controlled by configuration instead of hardcoded mapping logic. This lowers the cost of adding mappings, jobs, sharding rules, release gates, and state locations without forking the engine.

#### Inspectable and auditable behavior

The repository includes preflight checks, validation flows, stage-release gates, structured logs, and metrics snapshots so operators can reason about what happened and why.

### 2.2 Non-goals

Equally important are the things the repository intentionally does not try to be.

#### It is not a general CDC platform

The system supports backfill and watermark-based incremental replay. It is not trying to be a universal, low-latency change-data-capture service with strict ordering guarantees across all source APIs.

#### It is not a general-purpose orchestration framework

The repository includes distributed coordination primitives, but it does not try to replace Airflow, Argo, or a workflow scheduler. It assumes an external scheduler may invoke jobs, while the repository owns workload-level correctness and coordination.

#### It is not an arbitrary transformation engine

The v1 pipeline supports practical mapping and conversion rules, but it is intentionally narrower than a full data-processing platform such as Spark. The design optimizes for migration correctness and operational clarity, not arbitrary transformation expressiveness.

#### It is not a universal target abstraction

The repository supports specific targets that are relevant to the migration problem:

1. Cloud Spanner for structured and larger records
2. Firestore for smaller routed documents in v2

That target scope is deliberate.

## 3. High-Level System Shape

The repository should be visualized as a layered system, not as a single script.

Layer 1 is the source layer:

1. Cosmos SQL API for v1
2. Cosmos MongoDB API for v2
3. Cosmos Cassandra API for v2

Layer 2 is the execution layer:

1. `scripts/preflight.py`
2. `scripts/backfill.py`
3. `scripts/validate.py`
4. `scripts/v2_preflight.py`
5. `scripts/v2_route_migrate.py`
6. `scripts/v2_validate.py`
7. `scripts/release_gate.py`
8. `scripts/control_plane_status.py`

Layer 3 is the pipeline-runtime layer:

1. v1 modules under `migration/`
2. v2 modules under `migration_v2/`

Layer 4 is the shared control plane:

1. watermarks
2. reader cursors
3. leases
4. progress manifests
5. route registry
6. release-gate state

Layer 5 is the target layer:

1. Cloud Spanner
2. Firestore
3. DLQ files
4. metrics snapshots
5. logs
6. validation artifacts

Layer 6 is the infrastructure and release layer:

1. Terraform
2. GitHub Actions CI
3. stage-release-gate workflow

The reason to think in layers is simple: many issues that appear to be "pipeline bugs" are actually control-plane or operational-design problems. For example:

1. duplicated work is usually a lease or progress issue
2. skipped records are usually checkpoint-boundary or resume-state issues
3. unsafe production execution is usually a release-governance issue
4. unexplained backlog is usually an observability gap

The layered model makes it easier to isolate the category of problem before debugging the code.

## 4. Why the Repository Contains Two Pipelines

One of the first questions a new engineer asks is why there are two pipelines instead of a single shared framework with pluggable adapters. The answer is architectural honesty.

The v1 and v2 problems overlap only at a high level. Their runtime assumptions are materially different.

### 4.1 v1 problem shape

v1 migrates data from Cosmos SQL API into Spanner using mapping-driven transformation rules. The system assumes:

1. a source query model that is SQL-like
2. a target table model that is explicitly columnar
3. a one-direction destination flow into Spanner
4. transformation logic centered on column extraction and type conversion
5. watermark advancement tied to successful table writes

This is a mapping-centric pipeline.

### 4.2 v2 problem shape

v2 migrates records from Cosmos Mongo or Cassandra sources into either Firestore or Spanner depending on payload size. The system assumes:

1. canonical source records rather than direct table mappings
2. payload-aware routing decisions
3. records may move between sinks over time
4. the runtime must track the authoritative current destination
5. cleanup of the previous destination is part of correctness
6. validation must reconcile source, registry, and two targets

This is a routing-centric pipeline.

### 4.3 Why not force a shared abstraction?

Trying to merge both into a single generic engine would likely produce an abstraction that is too vague to be safe:

1. v1 needs row transformation and key-column awareness
2. v2 needs route ownership and move semantics
3. v1 watermarks are mapping-scoped
4. v2 checkpoints are watermark plus route-key boundary state
5. v1 validation is source-to-target
6. v2 validation is source-to-registry-to-union-of-sinks

The repository therefore shares infrastructure where the behavior is genuinely shared, and separates pipelines where the semantics differ materially.

This is the right architectural split.

## 5. Shared Design Principles

Although the runtime code is split, both pipelines are designed around a common set of engineering principles.

### 5.1 Config-first execution

Operational behavior is described in YAML. This includes:

1. source endpoints and credentials
2. targets
3. runtime retries
4. checkpoint locations
5. lease and progress backends
6. release-gate locations
7. mappings or jobs
8. sharding parameters
9. metrics and logging output

The practical benefit is that environment-specific behavior can change without changing code.

### 5.2 Safe by replay

The pipelines assume failures will happen. Therefore:

1. writes are designed to be replay-safe
2. checkpoints advance only after successful work
3. resume state is persisted separately from primary checkpoints
4. state backends support atomic or optimistic writes

### 5.3 Distributed coordination is optional, not mandatory

The same logical pipeline can run:

1. without leases or progress tracking for local use
2. with leases only for cooperative worker ownership
3. with leases and progress manifests for distributed full runs

This keeps the local developer path simple while still supporting production scale.

### 5.4 Validation is a product feature, not an afterthought

The repository does not treat validation as manual SQL after the migration. It includes explicit validation runners because migration correctness must be proven, not assumed.

### 5.5 Operational state is externalized

State is not hidden inside process memory or transient log streams. The system externalizes replay state so a later run or another worker can continue safely.

### 5.6 Safety checks are shift-left and runtime-enforced

The repository includes both:

1. build-time quality gates such as tests, linting, typing, Bandit, and pip-audit
2. runtime gates such as preflight, release-gate attestations, and config validation

That combination matters because many migration failures are operationally induced, not purely code induced.

## 6. Repository Structure and Runtime Boundaries

The repository layout is intentionally organized by responsibility.

### 6.1 `config/`

This folder contains example YAML files for v1 and v2. These serve two roles:

1. bootstrapping new environments
2. documenting the supported runtime surface area

The example configs are important architecture artifacts because they reveal which behaviors are intended to be operator-controlled rather than hardcoded.

### 6.2 `migration/`

This package contains the v1 runtime plus shared infrastructure used by both pipelines. Key modules include:

1. `config.py`
2. `cosmos_reader.py`
3. `transform.py`
4. `spanner_writer.py`
5. `state_store.py`
6. `resume.py`
7. `coordination.py`
8. `release_gate.py`
9. `metrics.py`
10. `logging_utils.py`
11. `retry_utils.py`
12. `dead_letter.py`
13. `control_plane_backend.py`
14. `sharding.py`
15. `reconciliation.py`

### 6.3 `migration_v2/`

This package contains v2-specific runtime logic:

1. `config.py`
2. `models.py`
3. `router.py`
4. `pipeline.py`
5. `state_store.py`
6. `reconciliation.py`
7. `source_adapters/`
8. `sink_adapters/`

### 6.4 `scripts/`

This folder exposes the operational entrypoints. The scripts are intentionally thin. Their job is to:

1. parse command-line flags
2. load config
3. bootstrap logging
4. invoke the appropriate runtime
5. return meaningful exit codes

The deeper logic stays in packages so it can be tested and reasoned about.

### 6.5 `tests/`, `tests_v2/`, and `tests_integration/`

This split reflects the code split:

1. v1 and shared-runtime tests in `tests/`
2. v2-specific tests in `tests_v2/`
3. opt-in live-cloud or integration scenarios in `tests_integration/`

### 6.6 `infra/terraform/`

The infrastructure tree provisions the target environment and supporting services. The runtime assumes infrastructure exists, but the repository also gives operators a repeatable way to create it.

### 6.7 `docs/`

The documentation folder is intentionally numbered for onboarding flow, but the canonical deep references are now:

1. `docs/19_ARCHITECTURE_REFERENCE.md`
2. `docs/20_OPERATIONS_RUNBOOK.md`
3. `docs/21_CONFIG_REFERENCE.md`
4. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
5. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`

## 7. v1 Architecture Deep Dive

The v1 pipeline is the mature, mapping-centric migration path from Cosmos SQL API to Cloud Spanner.

### 7.1 v1 runtime entrypoints

The v1 flow is built around three operational scripts:

1. `scripts/preflight.py`
2. `scripts/backfill.py`
3. `scripts/validate.py`

Each script represents a distinct operational phase, and that separation is important.

#### Preflight

Preflight confirms the basic assumptions are true before a write-bearing run starts:

1. the source can be accessed
2. the target schema exists
3. configured columns are valid
4. credentials and connectivity are in place

#### Backfill

Backfill performs full or incremental movement of data. It owns:

1. reading
2. transforming
3. batching
4. writing
5. watermark advancement
6. distributed coordination
7. reader cursor persistence

#### Validation

Validation compares source and target after or during migration. The repository intentionally keeps validation separate from writes so cutover evidence can be generated independently.

### 7.2 v1 configuration model

The v1 config has three major sections:

1. `source`
2. `target`
3. `runtime`
4. `mappings`

#### Source config

The source section identifies the Cosmos account, key source, and database. The design assumes secrets should be pulled from the environment rather than embedded in committed config.

#### Target config

The target section identifies the GCP project, Spanner instance, and database. The v1 architecture assumes target schema provisioning is done ahead of migration, not synthesized implicitly during writes.

#### Runtime config

The runtime section is where most of the operational leverage exists. It includes:

1. batch size
2. page size
3. logging format and level
4. watermark location
5. reader cursor location
6. lease and progress locations
7. release-gate settings
8. retry settings
9. error mode
10. DLQ path
11. metrics path and format
12. sharding-related controls via mappings
13. `max_docs_per_container` for scoped or capped runs

#### Mapping config

Each mapping defines how one Cosmos source container becomes one Spanner target table. A mapping includes:

1. source container
2. target table
3. key columns
4. write mode
5. full and incremental queries
6. shard behavior
7. column rules
8. static columns
9. validation columns
10. delete rules

Architecturally, the mapping is the unit of transformation semantics, while the distributed scheduler may split that mapping into shards.

### 7.3 Source read path

The source read path lives primarily in `migration/cosmos_reader.py`.

The reader:

1. creates a Cosmos client
2. opens the container
3. executes a cross-partition query
4. iterates page-by-page
5. persists page-level and row-level resume state
6. supports transient retry of stream iteration

This is more than a convenience wrapper. It is a correctness component.

#### Why the reader tracks both page token and row boundary

A continuation token alone is not always enough for safe restart when a process fails mid-page. The current design tracks:

1. `page_start_token`
2. `last_source_key`
3. `last_watermark`
4. scope information tying the state to a specific query shape

That allows the reader to reopen the page and skip already-safe records when needed.

#### Scope validation matters

The resume state includes a scope derived from:

1. container name
2. query text
3. query parameters
4. page size
5. max-doc cap

This prevents accidentally reusing a cursor across incompatible workloads. That is a subtle but important correctness property.

### 7.4 Transformation layer

The transformation layer lives in `migration/transform.py`. It converts a Cosmos document into one of two logical outcomes:

1. a row to write
2. a delete operation to apply

The transform result is not just "row or exception." It preserves:

1. extracted target values
2. type conversions
3. default handling
4. required-field enforcement
5. delete-event detection

The architecture keeps transformation separate from writing for two reasons:

1. it makes validation and testing easier
2. it keeps target-specific persistence logic out of mapping interpretation

#### Column rules

Column rules can extract nested values and apply converters such as:

1. string normalization
2. timestamp conversion
3. boolean conversion
4. JSON string rendering

This is enough to cover a wide range of migration needs without turning the runtime into an embedded DSL engine.

#### Static columns

Static columns such as `__NOW_UTC__` let operators stamp migration-time metadata without changing application data.

#### Delete rules

Delete rules allow the pipeline to interpret source documents as tombstones. That matters for incremental catch-up when deletes are represented as state in the source dataset rather than hard deletes.

### 7.5 Spanner write path

The v1 write layer lives in `migration/spanner_writer.py`.

The writer supports:

1. upsert
2. insert
3. update
4. replace
5. delete
6. schema inspection
7. row counting
8. targeted row reads for validation
9. full iteration for checksum reconciliation

Architecturally, the most important property is that the default mode is replay-safe. For migration work, `upsert` is the safest default because it tolerates reruns without introducing duplicates.

#### Why writes are batched

Batching exists for both performance and checkpoint semantics. The system flushes work in groups so that:

1. network and client overhead are reduced
2. watermark advancement can be tied to successful batch completion
3. reader cursor persistence can occur at a safe flush boundary

### 7.6 v1 checkpoint model

The v1 primary checkpoint is the watermark store in `migration/state_store.py`, driven by logic in `scripts/backfill.py`.

This is the authoritative progress marker for incremental correctness. It is intentionally separate from the reader cursor.

#### Mapping-scoped checkpoint keys

One of the important hardening changes in the repository is that v1 checkpoints are now keyed by mapping identity rather than just by container name. The work key follows the logical shape:

`v1:<source_container>-><target_table>` plus shard suffix where applicable.

This matters because a source container is not always a sufficient identity boundary if multiple mappings are defined over the same container. A container-only key can cause independent mappings to overwrite each other's progress. The current mapping-scoped design is the correct one.

#### Shard-scoped checkpoint keys

When `shard_count > 1`, each shard gets its own checkpoint key. This prevents one shard from moving the checkpoint for another shard and is essential for distributed correctness.

#### Legacy fallback support

The repository still supports reading legacy container-scoped checkpoint keys if they exist. That allows upgrade without losing historical state, while writing all future progress to the stronger key model.

### 7.7 v1 reader cursor model

The optional reader cursor store lives in `migration/resume.py`.

It is not a second checkpoint. It is a restart optimization layer.

That distinction matters. The reader cursor answers the question:

"If a process dies after making some progress but before the next safe flush boundary, how can the source read restart close to the last safe point instead of rescanning from the beginning?"

The watermark answers a different question:

"What source watermark boundary is known to have been successfully committed to the target?"

Confusing those two concepts is dangerous. The architecture keeps them separate so operators and maintainers do not accidentally treat a reader cursor as proof of write durability.

### 7.8 v1 distributed full-run model

The v1 backfill runner can execute in distributed fashion using:

1. `lease_file`
2. `progress_file`
3. `run_id`
4. shard-aware work keys

The control-plane logic is shared through `migration/coordination.py`.

The normal distributed full-run flow is:

1. build the work plan across selected mappings and shards
2. claim the next available work item via the coordinator
3. mark the item running
4. renew the lease while work is in progress
5. mark complete or failed when done
6. release the lease

This lets multiple workers collaborate without overlapping on the same shard.

### 7.9 v1 flush boundaries and safe state persistence

The hard part in v1 is not reading or writing in isolation. The hard part is deciding when state is safe to persist.

The current architecture uses flush boundaries to coordinate:

1. batched target writes
2. watermark updates
3. reader cursor updates
4. metrics publication

The guiding rule is:

1. write the target first
2. then update the primary checkpoint
3. then persist reader-side resume state reflecting the last safe point

That ordering prevents the system from believing work is durable before the target has actually accepted it.

### 7.10 v1 failure handling

v1 supports two high-level record-failure modes:

1. `error_mode=fail`
2. `error_mode=skip`

In fail mode, a problematic record or write error aborts the run. This is the right choice for early production rehearsals and correctness-first campaigns.

In skip mode, the runtime can write a dead-letter entry and continue. That improves continuity for long campaigns but must be paired with explicit DLQ review before cutover.

The DLQ design is intentionally simple: JSONL output that preserves enough context to replay or investigate later.

### 7.11 Module-by-module walk through of a single v1 record

To really understand v1, it helps to walk a single record through the code path.

1. `scripts/backfill.py` selects the mapping set and resolves whether the run is full or incremental.
2. The script constructs the query and parameters for the mapping, including overlap logic for incrementals.
3. `CosmosReader.iter_documents()` opens the source query and starts page iteration.
4. If a persisted reader cursor exists for the work key, the reader uses it to reopen at the last safe page and row boundary.
5. The runner optionally applies local shard filtering for `client_hash` mode.
6. `transform_document()` extracts mapped fields, applies conversion, and identifies delete events when relevant.
7. The runner groups upserts and deletes into separate batches.
8. `SpannerWriter` executes the target mutation for the batch.
9. The runner advances internal counters and tracks the highest source watermark seen and the highest successful watermark committed.
10. At the flush boundary, the runner persists the mapping-scoped watermark, then persists the safe reader cursor, then publishes metrics.
11. On clean completion, the runner clears the reader cursor because future restarts no longer need an in-flight position.

That sequence matters because it shows where correctness actually comes from. It does not come from any single module. It comes from the ordering across modules.

### 7.12 v1 full run versus incremental run

The v1 architecture deliberately supports two distinct modes because production migrations need both.

#### Full runs

Full runs are about baseline state creation. They usually happen:

1. during early dry-runs
2. during initial historical migration
3. during selective container recovery
4. during reruns after major mapping changes

In a full run:

1. the source query is the full source query
2. the watermark store may be irrelevant or only updated at the end of successful work
3. progress manifests matter if the run is distributed
4. validation often follows as a separate phase

#### Incremental runs

Incremental runs are about catch-up and cutover reduction. They usually happen:

1. after the first full backfill
2. on a repeating cadence while the source stays live
3. during the final pre-cutover convergence window

In an incremental run:

1. the source query is parameterized by the last watermark minus overlap
2. the watermark becomes the primary progress boundary
3. missed-boundary risk matters much more than in a full run
4. checkpoint safety matters more than raw throughput

This distinction is why the architecture does not simply label one mode "initial" and another "resume." They are operationally different workloads with different risk profiles.

## 8. v2 Architecture Deep Dive

The v2 pipeline is a routed migration engine for Cosmos Mongo and Cassandra sources. It exists because the data model and storage strategy are materially different from v1.

### 8.1 v2 entrypoints

The v2 operational scripts are:

1. `scripts/v2_preflight.py`
2. `scripts/v2_route_migrate.py`
3. `scripts/v2_validate.py`

Their separation mirrors the v1 lifecycle, but the underlying runtime semantics are different.

### 8.2 v2 configuration model

The v2 config contains:

1. `runtime`
2. `routing`
3. `targets`
4. `jobs`

#### Runtime section

This controls:

1. full vs incremental mode
2. batch size
3. dry-run behavior
4. logging and metrics
5. state store locations
6. route-registry location
7. reader cursor location
8. leases and progress
9. release gates
10. retry settings
11. max-record caps by job

#### Routing section

This section defines the storage decision boundary:

1. `firestore_lt_bytes`
2. `spanner_max_payload_bytes`
3. `payload_size_overhead_bytes`

The repository currently uses:

1. Firestore for payloads below 1 MiB by default
2. Spanner for payloads up to a safe cap of `8_388_608` bytes
3. an additional overhead allowance to stay under the effective write boundary

This is an architectural safety decision, not an arbitrary numeric preference.

#### Targets section

v2 explicitly defines both Firestore and Spanner targets because routing is intrinsic to runtime behavior.

#### Jobs section

Each job represents a source workload with fields such as:

1. job name
2. API type
3. enabled flag
4. connection information
5. namespace
6. key fields
7. source query
8. incremental field
9. page size
10. shard behavior

The job is the v2 equivalent of the v1 mapping, but because v2 is routing-centric, the job describes source extraction and identity rather than target-column mapping.

### 8.3 Canonical record model

The v2 source adapters emit canonical records rather than direct target rows.

That design is critical. It lets the rest of the pipeline operate on a stable record contract independent of whether the source was Mongo or Cassandra.

A canonical record conceptually includes:

1. route key
2. source namespace
3. payload
4. payload size
5. checksum
6. watermark value

This is the right abstraction because routing, deduplication, reconciliation, and cleanup all depend on these fields.

### 8.4 Source adapters

The source adapter boundary is one of the cleanest parts of the architecture.

#### Mongo adapter

The Mongo adapter is responsible for:

1. opening the source collection
2. applying the configured source query
3. ordering incremental reads deterministically enough for resume logic
4. emitting canonical records
5. integrating retry and resume behavior

#### Cassandra adapter

The Cassandra adapter performs the same high-level role for Cassandra-style sources, but with different source assumptions around query behavior and ordering.

One important architectural note is that Cassandra resume safety depends more heavily on stable query ordering assumptions. The runbook calls this out because it is not something the runtime can fully guarantee for every query shape.

### 8.5 Size router

The router in `migration_v2/router.py` decides the authoritative destination based on effective payload size.

This means v2 correctness is not just "write somewhere." It is:

1. evaluate the payload size safely
2. route to the correct sink
3. persist the authoritative destination
4. reconcile cleanup if the destination changed

This is why the v2 pipeline needs a route registry and cannot rely on the sinks alone.

### 8.6 Route registry

The route registry in `migration_v2/state_store.py` is the central v2 correctness structure.

Each route key can track:

1. current destination
2. checksum
3. payload size
4. updated timestamp
5. sync state
6. cleanup origin when a move is in progress

The registry turns storage routing into explicit state instead of a side effect.

Without that registry, the system could not answer key questions:

1. where should this record live right now?
2. did the record move between sinks?
3. is there cleanup debt left from a previous interrupted move?
4. should this source record be skipped as unchanged?

### 8.7 Move semantics and `pending_cleanup`

The most important v2 design choice is the `pending_cleanup` state.

When a record needs to move from one sink to the other, the pipeline does not:

1. delete the old copy first
2. then write the new copy

That order would risk data loss if the process failed between the two steps.

Instead, the pipeline:

1. writes the new destination first
2. records the route entry as `pending_cleanup`
3. attempts to delete the old sink copy
4. finalizes the registry as `complete`

This is the correct order because it is biased toward temporary duplication rather than data loss.

If cleanup fails in skip mode, the entry remains marked `pending_cleanup`, and a later run can reconcile the leftover cleanup deterministically. That is a strong design choice.

### 8.8 v2 checkpoint model

The v2 watermark store is more sophisticated than v1 because incremental replay must handle boundary records safely.

The checkpoint contains:

1. the last committed watermark value
2. the set of route keys already processed at that watermark boundary
3. an updated timestamp

This enables inclusive replay from the last watermark while deduplicating records already committed at the boundary.

That matters because a strict `>` query can miss records near the edge, while an inclusive replay can duplicate records unless boundary deduplication exists.

### 8.9 Out-of-order protection

The v2 runtime explicitly tracks out-of-order incremental records. If it detects unsafe ordering or failure conditions, it can block checkpoint advancement.

This is one of the reasons the pipeline deserves to be called production-oriented. It does not assume the source stream always arrives in a perfectly monotonic way.

### 8.10 v2 reader cursor model

Like v1, v2 can persist reader cursors independently from the primary checkpoint. This helps a later process restart resume close to the last safe source boundary after a flush.

Again, the architecture keeps the reader cursor separate from the watermark checkpoint because they answer different durability questions.

### 8.11 v2 distributed execution

The v2 runtime supports:

1. cooperative leases
2. progress manifests for full runs
3. shard-aware work keys
4. progress-based skipping of already completed shards
5. stale-shard recovery through the control-plane status tool

In full distributed mode, the system claims shards from a run-wide work plan instead of serially draining one job at a time. That improves scheduler flexibility and utilization.

### 8.12 v2 failure behavior

Failure handling in v2 is more complex because failures can occur at:

1. source extraction
2. routing
3. destination write
4. cleanup delete
5. checkpoint advancement
6. distributed lease renewal

The pipeline therefore tracks a richer set of stats:

1. docs seen
2. Firestore writes
3. Spanner writes
4. moved records
5. unchanged skips
6. checkpoint skips
7. out-of-order records
8. rejected records
9. failed records
10. cleanup failures
11. lease conflicts
12. progress skips

This is not over-instrumentation. It is necessary to reason about where routed migration campaigns are succeeding or degrading.

### 8.13 Module-by-module walk through of a single v2 record

The simplest way to understand why v2 is more complex than v1 is to trace a single record.

1. `scripts/v2_route_migrate.py` loads the config and selects enabled jobs.
2. `V2MigrationPipeline.run()` decides whether it is running in full or incremental mode and whether distributed progress tracking is active.
3. The chosen source adapter emits a `CanonicalRecord` with route identity, payload, checksum, and watermark.
4. The pipeline checks whether the current checkpoint already contains the record at the watermark boundary. If so, it skips it as replay-safe duplicate work.
5. The `SizeRouter` computes the authoritative destination.
6. The pipeline loads the previous route-registry entry, if any.
7. If the registry indicates `pending_cleanup`, the pipeline reconciles that cleanup debt before proceeding.
8. If the destination and checksum match the prior `complete` entry, the pipeline skips the record as unchanged.
9. If the destination changed, the pipeline writes the new destination first.
10. The registry is written as `pending_cleanup` with `cleanup_from_destination` pointing at the old sink.
11. The old sink copy is deleted.
12. The registry is finalized as `complete`.
13. At the flush boundary, the registry is flushed, then the reader cursor is persisted, then metrics are published.
14. If the run is incremental and no blocking condition occurred, the candidate checkpoint is committed.

This sequence shows why the route registry is central. Without it, the runtime could write data, but it could not reason about move correctness or interrupted cleanup.

### 8.14 Why v2 uses canonical records instead of direct sink rows

It may be tempting to ask why v2 does not simply transform each record directly into the Firestore or Spanner representation and skip the canonical layer. The reason is that v2 needs a cross-sink identity plane.

Canonical records make the following possible:

1. route decisions independent of source API specifics
2. registry entries keyed by route identity rather than sink-specific identity
3. exact reconciliation across source, registry, and the union of sinks
4. deterministic move handling when a record crosses the routing boundary
5. change detection via checksum before deciding whether a sink write is necessary

If the architecture skipped canonicalization, each new source or sink would inject more branching into the core pipeline. The current approach keeps the orchestration logic coherent.

### 8.15 Why `pending_cleanup` is superior to best-effort delete

Many migration systems would write the new target and then attempt a delete of the previous target as a best-effort side effect. That design is simpler on paper, but it has a serious blind spot: the system loses explicit knowledge of incomplete moves.

The `pending_cleanup` model is better because:

1. interrupted moves are visible in durable state
2. future runs know exactly which records still need cleanup
3. validation can reason about intentional temporary duplication versus steady-state correctness
4. operators can quantify cleanup backlog as a first-class operational signal

That is a Principal-level design difference. It converts an implicit side effect into an explicit, repairable state machine.

## 9. Shared Control Plane and State Backends

The control plane is one of the best architectural decisions in the repository. The system does not hardcode local files as the only state mechanism. Instead, it defines abstractions that can be backed by:

1. local JSON files
2. `gs://` objects
3. `spanner://...` control-plane rows

### 9.1 Why a shared control plane exists

Multiple features need durable, concurrent-safe state:

1. v1 watermarks
2. v2 watermarks
3. reader cursors
4. route registry
5. leases
6. progress manifests
7. release-gate attestations

Unifying the access patterns across these features lowers the complexity of moving from local to distributed environments.

### 9.2 Local JSON backend

The local JSON backend is the simplest path. It is suitable for:

1. developer testing
2. single-runner experiments
3. local dry-runs

Its strengths are simplicity and inspectability. Its weaknesses are:

1. limited concurrency characteristics
2. local-disk dependence
3. poor fit for distributed runners

### 9.3 GCS-backed object state

`gs://` state uses generation preconditions and merge behavior to provide optimistic concurrency semantics.

This is a strong middle ground for:

1. shared single-runner or lightly distributed workflows
2. stage environments
3. production setups where a full Spanner-backed control plane is not yet required

### 9.4 Spanner-backed control plane

The strongest backend is the Spanner control-plane table defined in `infra/ddl/spanner_control_plane.sql`.

This backend matters because it supports:

1. row-based state rather than whole-object state
2. transactional updates
3. namespace isolation within the same table
4. better behavior for heavily concurrent distributed runs

Architecturally, using the same table with different namespaces for:

1. leases
2. progress
3. watermarks
4. route registry
5. reader cursors
6. release gates

is the cleanest production posture because all control-plane state becomes durable, centralized, and transactionally addressable.

### 9.5 Namespace strategy

Namespaces are critical. They allow one physical control-plane table to store multiple logical stores without mixing meanings.

Examples include:

1. `v1-watermarks`
2. `v1-reader-cursors`
3. `v1-leases`
4. `v1-progress`
5. `v2-watermarks`
6. `v2-route-registry`
7. `v2-reader-cursors`
8. `v2-leases`
9. `v2-progress`
10. `release-gates`

This design keeps the runtime flexible while avoiding the operational sprawl of many separate tables.

### 9.6 Transactional versus merge semantics across backends

One subtle but important aspect of the control-plane architecture is that the logical behavior is the same across backends even though the physical implementation differs.

For local JSON and GCS-like object stores, updates are merge-based:

1. load current object state
2. apply a merge function
3. write the updated state back atomically or with optimistic concurrency

For the Spanner control-plane backend, updates are row-based and can be transactional:

1. load the relevant row or rows within a transaction
2. evaluate current ownership or progress state
3. upsert or delete rows in the same transaction

The architectural insight is that the repository exposes a common logical behavior while using the strongest concurrency semantics each backend can support. This prevents the code from being locked to a single persistence strategy.

### 9.7 Why the control plane is separate from business data

It may seem obvious that checkpoints and leases should not live inside the migrated business tables, but it is worth stating the deeper reason. Control-plane state has very different lifecycle, retention, access, and concurrency requirements than migrated application data.

Keeping control-plane state separate improves:

1. blast-radius isolation
2. schema evolution independence
3. operational inspection
4. permissions design
5. replay and rollback hygiene

This is especially important in Spanner-backed environments where application tables and migration-control tables may have different owners and retention requirements.

## 10. Distributed Execution Model

Distributed execution is intentionally optional, but when enabled it is a first-class part of the design.

### 10.1 Lease model

Leases prevent two workers from processing the same work item simultaneously. A lease record stores:

1. owner id
2. lease expiration
3. heartbeat timestamp
4. acquisition timestamp
5. metadata

The key property is that a lease can expire, allowing work to be reclaimed after worker failure.

### 10.2 Progress manifest model

Progress manifests track whether work items are:

1. pending
2. running
3. completed
4. failed

This allows:

1. resuming full runs using the same `run_id`
2. skipping already completed work
3. identifying failed or stale work
4. reclaiming stale running shards

### 10.3 Worker identity

Workers default to `<hostname>:<pid>` unless overridden, which is a practical compromise between usability and traceability.

### 10.4 Lease renewal

Long-running work periodically renews its lease. This matters because a lease without renewal is only a startup lock, not a liveness signal.

### 10.5 Stale-shard recovery

The repository includes `scripts/control_plane_status.py` to inspect progress and reclaim stale running shards.

This is an example of architectural completeness. A distributed system that can create stale work but provides no repair interface is incomplete.

### 10.6 Sharding model

Both pipelines support sharding. The meaning of sharding is:

1. divide a logical mapping or job into deterministic work partitions
2. give each partition separate coordination identity
3. optionally route different workers to different shards

Supported strategies include:

1. no sharding
2. client-hash sharding
3. query-template sharding

#### Client-hash sharding

This is deterministic and safe, but every worker may still scan source data and filter locally.

#### Query-template sharding

This is usually better for large workloads because it can push the shard predicate into the source query and reduce duplicated reads.

### 10.7 Full-run orchestration pattern

For full distributed campaigns, the recommended flow is:

1. set shared `lease_file`
2. set shared `progress_file`
3. set a stable `run_id`
4. set shard counts deliberately
5. start multiple workers against the same config
6. monitor progress and stale work through the control-plane status tool

The architecture is intentionally explicit here because ad hoc distributed execution is how migrations get into trouble.

### 10.8 Failure scenarios in distributed runs

Distributed migration systems are often judged by happy-path throughput, but they are defined by how they behave under failure. The current architecture is designed around several concrete failure cases.

#### Worker dies after claiming work but before doing anything

Expected behavior:

1. the lease eventually expires
2. the progress item may remain `running`
3. an operator or automation uses `control_plane_status.py --reclaim-stale`
4. the shard becomes reclaimable by another worker

#### Worker dies mid-shard after writing some data but before the next flush

Expected behavior:

1. target writes completed before the crash remain durable
2. the primary checkpoint only reflects the last safe flush boundary
3. the reader cursor, if enabled, lets the replacement run resume close to the last safe boundary
4. replay may occur, but idempotent writes and checkpoint safety preserve correctness

#### Worker loses lease while still running

Expected behavior:

1. the runner detects failed renewal
2. the work item is failed rather than silently continuing under ambiguous ownership
3. another worker can later reclaim the work

#### Multiple workers start with the same config

Expected behavior:

1. one worker acquires a shard lease
2. others skip that shard due to lease conflict
3. progress and metrics reflect the contention rather than duplicating work

These scenarios are exactly why leases, progress manifests, and safe flush boundaries belong in the architecture.

### 10.9 Why full-run progress is keyed by `run_id`

A full run is not just "all work ever." It is a specific migration campaign. The `run_id` boundary prevents completion state from one campaign from being applied blindly to another. That allows:

1. retrying the same campaign safely
2. keeping distinct campaigns separate
3. avoiding accidental skip behavior when the workload definition changes

This is one of those details that looks administrative until a team reruns a campaign months later and needs to know which completions still count.

## 11. Idempotency, Replay, and Checkpoint Design

The migration architecture is built on replay-aware semantics.

### 11.1 Why replay safety matters

Migration systems always replay work, whether by design or by accident:

1. a worker restarts
2. a process times out
3. a scheduler reruns a task
4. a cutover rehearsal is repeated
5. a checkpoint is intentionally overlapped

If replay safety is not explicit, the system is brittle.

### 11.2 v1 replay model

v1 relies on:

1. replay-safe write modes, especially upsert
2. watermark advancement only after successful writes
3. reader cursor persistence only after safe flush boundaries
4. mapping-scoped and shard-scoped watermark keys

### 11.3 v2 replay model

v2 relies on:

1. inclusive watermark replay
2. route-key deduplication at the boundary
3. route registry as the record of authoritative destination
4. `pending_cleanup` for interrupted moves
5. checkpoint hold behavior on failure or out-of-order conditions

### 11.4 Why checkpoints are conservative

A common failure mode in migration tools is to move checkpoints aggressively because it improves reported throughput. This repository takes the opposite approach. Checkpoints move only when the runtime can defend that the work is safe.

That is the correct trade-off for migration tooling.

## 12. Reader Cursor Design and Crash Recovery

Reader cursors deserve separate attention because they are often misunderstood.

### 12.1 Primary checkpoint vs reader cursor

The primary checkpoint tells the system what source progress is durably reflected in the target.

The reader cursor tells the system how to resume the source scan after a process restart without unnecessary rescanning.

These are related but not interchangeable.

### 12.2 Why reader cursors are optional

The system can operate correctly without them because the primary checkpoint preserves correctness. Reader cursors mainly improve efficiency and time-to-recovery.

### 12.3 Safe persistence rule

The runtime only persists reader cursors after a safe boundary, typically after target state and primary checkpoint state have been flushed. This ordering is essential. Persisting a cursor too early can create the illusion that more work was safely written than actually was.

### 12.4 Clear-on-completion behavior

When a shard or work item completes cleanly, the corresponding reader cursor can be cleared. This avoids stale resume state lingering past the useful lifetime of the workload.

## 13. Data Integrity and Validation Strategy

Validation is built into both pipelines because correctness cannot rely on faith.

### 13.1 v1 validation modes

v1 supports:

1. sampled validation
2. checksum reconciliation

#### Sampled validation

Sampled validation is faster and useful for:

1. smoke tests
2. non-critical rehearsal checks
3. quick post-run confidence checks

#### Checksum reconciliation

Checksum reconciliation is the high-assurance path. It compares normalized source and target row digests across the workload.

The repository uses a disk-backed SQLite store for large reconciliations. That is important because it avoids requiring the full source and target keyspace in memory at once.

### 13.2 v2 exact reconciliation

v2 validation is stronger and more complex because it must reconcile:

1. selected source jobs
2. route registry entries
3. Firestore state
4. Spanner state

This exact reconciliation ensures the route registry and physical sinks agree, which is essential for a routed architecture.

### 13.3 Checksums and comparison columns

Checksums are computed from canonicalized values and selected comparison columns. This reduces noise from representation differences while still preserving strong integrity signals.

### 13.4 Validation as cutover evidence

The architecture treats validation outputs as artifacts for operational decision-making. They are not just debug tools. In a production migration, validation is a release input.

### 13.5 Integrity strategy by migration phase

Different migration phases require different integrity techniques.

#### During early development

The main integrity objective is catching mapping and connectivity mistakes quickly. Sample validation, schema preflight, and small dry-runs are enough.

#### During initial backfill rehearsal

The objective is proving that the pipeline can move representative data without silent corruption. At this point:

1. checksum reconciliation for critical mappings becomes important
2. DLQ analysis should be strict
3. routed v2 exact validation should be exercised end-to-end

#### During incremental catch-up

The objective is boundary correctness and stable lag reduction. Here, the most important integrity signals are:

1. checkpoint progression
2. replay-skip counts
3. cleanup backlog
4. mismatch deltas over time

#### During final cutover

The objective is executive confidence and operational sign-off. At this stage:

1. high-assurance validation should be treated as mandatory for critical workloads
2. unresolved DLQ records should be understood explicitly
3. pending cleanup in v2 should be zero or formally accepted with rationale

The architecture supports all four phases instead of pretending there is a single validation mode that fits every moment.

## 14. Observability Model

Observability has three layers in the repository:

1. structured logs
2. metrics snapshots
3. summary counters and state inspection

### 14.1 Structured logging

The logging layer supports:

1. text formatting for local readability
2. JSON formatting for machine ingestion
3. static execution context such as pipeline, environment, run id, and worker id

This is materially better than unstructured `print` style logging because distributed systems need correlation context.

### 14.2 Metrics collector

The metrics collector can emit:

1. Prometheus textfile format
2. JSON snapshots

Metrics are written atomically to a configured file path. This allows external agents or sidecars to scrape them safely.

### 14.3 Metric coverage

v1 and v2 both publish progress-oriented metrics. Examples include:

1. docs seen
2. rows or sink writes
3. failed records
4. rejected records
5. cleanup failures
6. checkpoint skips
7. watermark lag
8. duration
9. lease conflicts
10. progress skips
11. out-of-order records

### 14.4 Why file-based metrics still help

The current metrics implementation is file-based rather than a continuously served endpoint. That is still a meaningful improvement over log-only monitoring because:

1. it is easy to integrate into scheduled or batch environments
2. it supports Prometheus textfile collectors
3. it provides structured snapshots even when no long-lived service exists

### 14.5 Observability limitations

The architecture still depends on external collection and alerting infrastructure to achieve full production observability. The repository emits structured data, but it does not itself provide a hosted telemetry backend.

### 14.6 How observability ties back to architecture

Observability is not a decorative add-on in this repository. It is how operators validate whether the architecture is behaving according to its promises.

For example:

1. if lease conflicts spike, that tells you the distributed ownership layer is active and possibly oversubscribed
2. if checkpoint-skipped counts rise during incrementals, that tells you replay-safe deduplication is doing real work at the boundary
3. if cleanup failures or `pending_cleanup` backlog rise in v2, that tells you move semantics are protecting correctness but the system is accumulating cleanup debt
4. if watermark lag grows while docs-seen remains high, that suggests the source side is healthy while the write or commit side is falling behind
5. if progress skips rise in a rerun, that proves the progress manifest is preventing duplicate full-run work

This is why the metrics and logs are architecture-level signals rather than generic telemetry. They tell you whether specific design guarantees are still holding in production.

### 14.7 Why summary tooling matters

The architecture includes not just emitters but also inspection tools such as `control_plane_status.py`. That is important because durable state without a human-readable inspection path often becomes operationally opaque.

A Principal-level migration system needs both:

1. machine-readable state that other automation can consume
2. operator-readable summaries that humans can use during incidents

That balance is present here, and it is one reason the repository is more than a library of helper modules.

## 15. Security, Safety, and Release Governance

Migration tooling needs more than code correctness. It needs guardrails.

### 15.1 Secrets handling

The config model expects sensitive material to come from environment variables rather than checked-in plain text.

### 15.2 Identifier validation

The configuration loaders validate target identifiers and relevant source/query settings. This reduces the risk of malformed or unsafe runtime behavior.

### 15.3 CI quality gates

The repository includes:

1. unit tests
2. Ruff linting
3. mypy type checking
4. Bandit security scanning
5. pip-audit dependency scanning

That combination is important because migration tools are operationally critical, and silent regressions are expensive.

### 15.4 Release gates

One of the more mature safety mechanisms in the repository is the release gate. The release gate turns "we should rehearse this in stage first" into machine-readable state.

The release-gate design includes:

1. a shared attestation store
2. a scope or campaign identifier
3. logical workload fingerprints
4. freshness windows
5. enforcement in production configs

This means a production run can refuse to start unless a matching and recent stage rehearsal passed.

### 15.5 Why workload fingerprints matter

A stage run is only useful if it represents the same logical workload as the production run. Fingerprinting mappings or jobs helps prevent a false pass where stage validated one shape of work and production attempts another.

### 15.6 Release governance as an architectural boundary

Most migration repositories treat release governance as external process. This repository partially internalizes it. That is a meaningful architectural choice.

The release gate does not replace business approval, CAB, or executive sign-off. What it does do is encode a technical invariant:

1. production execution should not proceed if a matching stage rehearsal has not passed recently enough

By making that invariant machine-checkable, the repository narrows the gap between "what the team says it will do" and "what the system will actually allow."

That has two architectural benefits:

1. it reduces dependence on tribal knowledge
2. it makes the runtime safer to operate under schedule pressure

In other words, release governance is not bolted on after the fact. It is part of the runtime contract.

## 16. Infrastructure and Deployment Topology

The runtime and infrastructure are designed to align.

### 16.1 Environment progression

The expected progression is:

1. dev
2. stage
3. prod

This seems obvious, but the architecture encodes this progression in config and release-gate behavior rather than assuming operators will remember it.

### 16.2 Terraform role

Terraform provisions:

1. APIs
2. IAM
3. service accounts
4. Spanner resources
5. Firestore where needed
6. optional state buckets

The infrastructure layer is therefore not separate from architecture. It is part of how the system becomes repeatable and auditable.

### 16.3 Shared vs isolated control-plane resources

For strong distributed execution, the best production posture is usually:

1. shared control-plane state
2. stable namespaces
3. dedicated service accounts
4. environment-specific configs
5. release-gate scope discipline

### 16.4 Script-to-environment boundary

The scripts are environment-agnostic. The config and infrastructure define environment specifics. This is a healthy boundary because it avoids environment-specific branching in the pipeline code.

### 16.5 Recommended deployment patterns

The repository can be deployed in several patterns, and the architecture supports all of them, but they are not equally strong.

#### Local developer pattern

Best for:

1. feature development
2. unit and dry-run testing
3. config experimentation

Characteristics:

1. local JSON state
2. text logs
3. no leases
4. no progress manifests

#### Single-runner stage pattern

Best for:

1. controlled rehearsal
2. pre-production validation
3. early live-cloud testing

Characteristics:

1. shared GCS or Spanner state
2. optional reader cursors
3. release-gate writing
4. metrics snapshots for stage dashboards

#### Multi-runner production pattern

Best for:

1. large historical backfills
2. narrow cutover windows
3. long-running incremental convergence with operational resilience

Characteristics:

1. shared Spanner-backed control plane preferred
2. lease and progress manifests enabled
3. unique `run_id`
4. structured JSON logs
5. Prometheus or JSON metrics snapshots collected externally
6. release-gate enforcement turned on for production campaigns

This section matters because architecture is not only about code shape. It is also about what deployment patterns the design can support safely.

## 17. Extensibility Model

A strong migration codebase should be extendable without destabilizing correctness. This repository is reasonably well positioned for that.

### 17.1 Adding a new v1 mapping

The simplest extension is a new v1 mapping. Because the engine is YAML-driven, this usually requires no code changes if the transformation logic fits existing converters.

### 17.2 Adding a new converter

If a source field needs a new conversion rule, `migration/transform.py` is the right extension point. This keeps mapping semantics centralized.

### 17.3 Adding a new v2 source adapter

To add another source family in v2, the correct pattern is:

1. implement a source adapter that emits canonical records
2. keep the rest of the pipeline unchanged
3. integrate configuration parsing cleanly
4. add validation coverage

This is one of the advantages of the canonical record abstraction.

### 17.4 Adding a new sink

Adding another sink in v2 is more invasive because route semantics and registry meaning would need to evolve. It is possible, but it should be treated as an architecture change, not a small plugin task.

### 17.5 Extending control-plane storage

The control-plane abstraction is already separated enough that new storage backends could be added if needed. The bar for doing so should be high because correctness under concurrency is difficult to get right.

### 17.6 Extension checklist for maintainers

When extending the repository, maintainers should force themselves through a checklist. This prevents architecture drift.

1. What is the unit of work identity for the new feature?
2. Does the new feature need primary checkpoint state, reader cursor state, or both?
3. How will the feature behave under replay?
4. Does the feature need to participate in distributed leases or progress manifests?
5. What validation evidence will prove the feature is correct?
6. What metrics or logs will make the feature observable in production?
7. Does the feature need release-gate awareness?
8. Is the feature a pipeline-specific behavior or a true shared abstraction?

This checklist sounds procedural, but it reflects the architecture directly. Most migration regressions come from extending the happy path without extending the control plane, replay model, or observability model.

## 18. Key Design Decisions and Trade-Offs

No architecture is free. This repository makes several conscious trade-offs.

### 18.1 Split pipelines instead of forcing reuse

Benefit:

1. clearer semantics
2. lower risk of leaky abstractions
3. easier reasoning about correctness

Cost:

1. some duplicate patterns across v1 and v2
2. more code surface to maintain

This is still the right decision.

### 18.2 Externalized state instead of in-memory simplicity

Benefit:

1. restartability
2. distributed execution
3. inspectable control plane

Cost:

1. more operational components
2. more state modeling to understand

Again, correct for migration software.

### 18.3 Conservative checkpoint advancement

Benefit:

1. lower risk of missed records
2. safer reruns

Cost:

1. more reprocessing at boundaries
2. potentially lower apparent throughput

Correct trade-off.

### 18.4 File-based metrics over a hosted telemetry service

Benefit:

1. simple batch-friendly integration
2. no need to run a server in the worker

Cost:

1. less real-time telemetry out of the box
2. requires external scraping/collection

Reasonable at the current system maturity.

### 18.5 YAML-driven runtime over code-centric configuration

Benefit:

1. operators can change behavior without code edits
2. mappings and jobs become easier to review

Cost:

1. config correctness becomes a major concern
2. documentation burden rises

That burden is exactly why this document exists.

### 18.6 Architectural choices that raise or lower production confidence

It is useful to be blunt about which choices make the system more production-grade and which choices quietly weaken it.

Choices that increase confidence:

1. mapping-scoped and shard-scoped checkpoints
2. reader cursor persistence only after safe flush boundaries
3. route-registry-backed move semantics
4. explicit stale-shard recovery tools
5. structured logging with run and worker identity
6. release-gate enforcement for production
7. exact validation for high-risk cutovers

Choices that reduce confidence if misused:

1. loose incremental source queries without stable ordering assumptions
2. ad hoc shard changes in the middle of a campaign
3. skip mode without disciplined DLQ review
4. local-only control-plane state in distributed environments
5. treating reader cursors as checkpoints
6. ignoring `pending_cleanup` backlog during v2 cutover

This kind of framing matters because "production-grade" is not a single boolean property. It is the cumulative result of many architectural decisions and operator choices.

## 19. Limitations and Operational Caveats

This architecture is strong, but it is not magic. Maintainers should understand its boundaries.

### 19.1 Source ordering assumptions still matter

The runtime can do a lot to protect checkpoint safety, but it cannot force an arbitrary source query to become stably ordered. Operators must choose sane incremental query shapes, especially for Cassandra.

### 19.2 Reader cursors improve recovery but are not mandatory

If reader cursors are disabled, correctness still holds, but resume efficiency drops.

### 19.3 Metrics are emitted, not centrally aggregated

A production deployment still needs external alerting and dashboards.

### 19.4 Validation can be expensive

High-assurance validation is worth the cost, but operators should plan for the runtime and storage impact of exact reconciliation.

### 19.5 Progress manifests require discipline

Reusing `run_id` incorrectly or changing shard configuration mid-run can create confusing behavior. The runbook covers the operational rules for using these features safely.

## 20. Reading Order for Maintainers

If you are onboarding as an engineer or reviewer, the recommended reading order is:

1. `README.md`
2. `docs/00_START_HERE.md`
3. this file
4. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
5. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
6. `docs/21_CONFIG_REFERENCE.md`
7. `docs/20_OPERATIONS_RUNBOOK.md`
8. `docs/07_CODEBASE_STRUCTURE.md`
9. `docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`
10. the relevant runtime code in `migration/` or `migration_v2/`

For code reviewers, the key files are:

1. `scripts/backfill.py`
2. `migration/cosmos_reader.py`
3. `migration/resume.py`
4. `migration/state_store.py`
5. `migration/coordination.py`
6. `migration/release_gate.py`
7. `migration_v2/pipeline.py`
8. `migration_v2/state_store.py`
9. `migration_v2/reconciliation.py`

## 21. Glossary

### Backfill

The process of migrating the historical dataset rather than only newly arriving records.

### Checkpoint

The primary durable progress marker that indicates what source boundary has been safely reflected in the target system.

### Control plane

The collection of durable runtime state that coordinates execution, ownership, progress, and release safety.

### DLQ

Dead-letter queue. In this repository, usually a JSONL file containing records and error context for skipped failures.

### Full run

A migration pass over the complete selected workload rather than only incremental changes.

### Incremental run

A run that reads from a watermark boundary onward rather than rescanning the entire dataset.

### Lease

A time-bounded ownership record that prevents multiple workers from processing the same work item simultaneously.

### Mapping

In v1, the configuration object that describes how one Cosmos container maps to one Spanner table.

### Pending cleanup

In v2, a route-registry state meaning the new destination has been written but the old sink still needs cleanup.

### Progress manifest

A store of work-item lifecycle states such as pending, running, completed, and failed.

### Reader cursor

An optional persisted source-resume position used to accelerate restart after interruption. It is not the same as the primary checkpoint.

### Release gate

A durable attestation that a matching stage rehearsal passed recently enough for a production run to be allowed.

### Route registry

The v2 store that records the authoritative destination and cleanup state for each routed record.

### Route key

The stable identity used by v2 to track a canonical record across source, registry, and sinks.

### Shard

A deterministic partition of a mapping or job used to divide work across runners.

### Stage rehearsal

A stage-environment preflight and validation campaign whose successful result can be recorded and enforced as a production prerequisite.

### Watermark

A source progress value, such as Cosmos `_ts`, used to delimit incremental processing.

## Closing Perspective

The most important thing to understand about this repository is that it is not architected as a throwaway migration script. It is architected as a replay-aware, stateful migration system with two distinct engines and a shared control plane. The architectural center of gravity is not the transformation code by itself. The center of gravity is the combination of:

1. explicit state
2. safe replay
3. distributed ownership
4. validation
5. release governance

If you preserve those five properties, you can extend the repository safely. If you optimize one at the expense of the others without understanding the trade-off, you will degrade the part of the system that actually makes it production-credible.

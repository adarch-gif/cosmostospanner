# 02 - Architecture and Data Flow

This document is now a short index. The canonical architecture and flow references are:

1. `docs/ARCHITECTURE.md`
2. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
3. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
4. `docs/RUNBOOK.md`

Use this file as a quick orientation page, not as the full source of truth.

## A. v1 architecture (Cosmos SQL API -> Spanner)

### Components

1. `scripts/preflight.py`
2. `scripts/backfill.py`
3. `scripts/validate.py`
4. `migration/*` core modules

### Data flow

1. Read from Cosmos container query
2. Apply configured field mapping + converters
3. Upsert/delete into Spanner
4. Persist watermark state (incremental mode)
5. Validate with either sampled checks or full checksum reconciliation

### Reliability controls

1. Retry/backoff for source and sink calls
2. Optional skip mode + dead-letter log
3. Config validation before execution

## B. v2 architecture (Mongo/Cassandra -> Firestore/Spanner)

### Components

1. `scripts/v2_preflight.py`
2. `scripts/v2_route_migrate.py`
3. `migration_v2/source_adapters/*`
4. `migration_v2/router.py`
5. `migration_v2/sink_adapters/*`
6. `migration_v2/state_store.py`
7. `migration_v2/pipeline.py`

### Canonical flow

1. Source adapter yields canonical record
2. Router computes destination using payload size rule
3. Destination sink upsert
4. If destination changed, registry is marked `pending_cleanup`
5. Old sink delete is attempted
6. Incremental watermark update advances only on successful records

### Move semantics

If a record previously in Firestore grows beyond threshold, pipeline:

1. Writes record to Spanner
2. Marks move intent in the route registry
3. Deletes record from Firestore
4. Finalizes route registry state

This avoids dual-active copies as the intended steady state.

## C. Infrastructure architecture

Managed via Terraform:

1. APIs
2. IAM service accounts and roles
3. Secret Manager secret containers
4. Spanner infrastructure
5. Firestore DB (v2)
6. Optional state bucket for `gs://` watermark and route-registry storage

Environment wrappers:

- `infra/terraform/envs/dev`
- `infra/terraform/envs/stage`
- `infra/terraform/envs/prod`

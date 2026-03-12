# 02 - Architecture and Data Flow

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
5. Validate counts, key existence, and sampled values

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
4. If destination changed, old sink delete
5. Route registry update
6. Incremental watermark update

### Move semantics

If a record previously in Firestore grows beyond threshold, pipeline:

1. Writes record to Spanner
2. Deletes record from Firestore
3. Updates route registry

This avoids dual-active copies as the intended steady state.

## C. Infrastructure architecture

Managed via Terraform:

1. APIs
2. IAM service accounts and roles
3. Secret Manager secret containers
4. Spanner infrastructure
5. Firestore DB (v2)
6. Optional state bucket

Environment wrappers:

- `infra/terraform/envs/dev`
- `infra/terraform/envs/stage`
- `infra/terraform/envs/prod`


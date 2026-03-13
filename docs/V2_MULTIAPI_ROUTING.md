# V2 Multi-API Routing Guide

## Why v2 is separate

The v2 pipeline is isolated from the existing SQL API path so routing requirements can evolve without destabilizing v1.

v1 remains in `migration/` + `scripts/backfill.py`.
v2 is implemented in `migration_v2/` + `scripts/v2_route_migrate.py`.

## Supported source APIs

- Cosmos DB API for MongoDB
- Cosmos DB API for Cassandra

## Dynamic routing rule

- If payload effective size is `< routing.firestore_lt_bytes`, route to Firestore.
- If payload effective size is `>= routing.firestore_lt_bytes` and `<= routing.spanner_max_payload_bytes`, route to Spanner.
- If payload effective size exceeds `routing.spanner_max_payload_bytes`, record is rejected (DLQ in `error_mode=skip`).

Effective size = serialized JSON payload bytes + `routing.payload_size_overhead_bytes`.

## Idempotency and move semantics

Each record has deterministic `route_key = route_namespace + "|" + source_key`.

Route registry (`runtime.route_registry_file`) stores destination + checksum + sync state.

Behavior:

1. If destination + checksum unchanged, skip write.
2. If destination same but checksum changed, upsert in same sink.
3. If destination changed (size crossed threshold), upsert new sink first.
4. Registry is marked `pending_cleanup` with old destination metadata.
5. Pipeline deletes old sink record.
6. On successful cleanup, registry transitions to `complete`.
7. If cleanup fails in `error_mode=skip`, entry remains `pending_cleanup` and is reconciled on future runs.

This avoids losing move intent on interruptions and makes recovery deterministic.

## Required Spanner table for v2

Create a table compatible with `SpannerSinkAdapter`:

```sql
CREATE TABLE RoutedDocuments (
  RouteKey STRING(MAX) NOT NULL,
  SourceJob STRING(128) NOT NULL,
  SourceApi STRING(32) NOT NULL,
  SourceNamespace STRING(MAX) NOT NULL,
  SourceKey STRING(MAX) NOT NULL,
  PayloadJson STRING(MAX) NOT NULL,
  PayloadSizeBytes INT64 NOT NULL,
  Checksum STRING(64) NOT NULL,
  EventTs STRING(MAX) NOT NULL,
  UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (RouteKey);
```

If you do not use commit timestamp options, regular UTC timestamp writes still work.

## Preflight

Run:

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

Checks:

- Firestore connectivity
- Spanner table existence and required columns
- Optional source-read check per selected job

## Execution commands

Full run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

Incremental run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Dry run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

Single job:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

## Cassandra incremental query safety

For Cassandra jobs running incrementally with custom `source_query`:

1. Use `%s` placeholder for watermark parameter binding.
2. Do not use string interpolation placeholders.
3. If no custom `source_query` is provided, the runner builds a parameterized query automatically.

## Operational state files

- Watermarks: `runtime.state_file`
- Route registry: `runtime.route_registry_file`
- Dead-letter records: `runtime.dlq_file_path`

All are JSON/JSONL and should be stored in durable storage for production runners.
Flushes are lock-file guarded and atomic for safer crash/concurrency behavior.

## Remaining known limits

- Hard deletes in source are not auto-discovered unless represented in source query results.
- Multi-runner distributed locking is not built in for state files.
- Full dataset checksum parity is out of scope; use additional reconciliation if required.

# V2 Multi-API Routing Guide

## Why v2 is separate

The v2 pipeline is isolated from the existing SQL API path so routing requirements can evolve without destabilizing v1.

v1 remains in `migration/` and `scripts/backfill.py`.
v2 is implemented in `migration_v2/`, `scripts/v2_route_migrate.py`, and `scripts/v2_validate.py`.

## Supported source APIs

1. Cosmos DB API for MongoDB
2. Cosmos DB API for Cassandra

## Dynamic routing rule

1. If effective payload size is below `routing.firestore_lt_bytes`, route to Firestore.
2. If effective payload size is at or above `routing.firestore_lt_bytes` and at or below `routing.spanner_max_payload_bytes`, route to Spanner.
3. If effective payload size exceeds `routing.spanner_max_payload_bytes`, reject the record.

Effective payload size equals serialized JSON payload bytes plus `routing.payload_size_overhead_bytes`.

The repository now enforces a safer default Spanner ceiling of `8,388,608` bytes.

## Idempotency and move semantics

Each record has a deterministic `route_key = route_namespace + "|" + source_key`.

Route registry entries store:

1. Current destination
2. Payload checksum
3. Payload size
4. Sync state
5. Cleanup origin when a move is in progress

Behavior:

1. If destination and checksum are unchanged, the write is skipped.
2. If destination is unchanged but checksum changed, the record is upserted in place.
3. If destination changes, the new sink is written first.
4. The registry is marked `pending_cleanup`.
5. The old sink record is deleted.
6. On success, the registry transitions to `complete`.
7. On cleanup failure in `error_mode=skip`, the registry remains `pending_cleanup` and is retried on later runs.

## Incremental checkpoint semantics

1. v2 resumes from the stored watermark inclusively.
2. Records already processed at that watermark are skipped by `route_key`.
3. This avoids missing records that share the same watermark value.
4. If record-level failures or out-of-order incremental records are detected, checkpoint advancement is blocked.

## Cassandra incremental query safety

For Cassandra jobs running incrementally with a custom `source_query`:

1. Use `%s` placeholder binding for watermark parameters.
2. Do not use string interpolation placeholders.
3. Prefer a query ordered by the incremental field.
4. If no custom query is provided, the runner builds a parameterized query automatically.

## Operational state

State artifacts:

1. Watermarks: `runtime.state_file`
2. Route registry: `runtime.route_registry_file`
3. For large distributed runs, both can use `spanner://<project>/<instance>/<database>/<table>?namespace=<name>` to avoid whole-object registry/checkpoint state.
3. Dead-letter records: `runtime.dlq_file_path`

Use `gs://` paths for the watermark and route-registry objects when running shared or orchestrated jobs.

## Routed validation

Use `scripts/v2_validate.py` before cutover for routed jobs.

The validator:

1. Re-reads the selected source jobs in `full` mode.
2. Recomputes the routed destination for each source record.
3. Compares that expected routed state with the route registry.
4. Streams Firestore and Spanner and compares the actual sink state with the expected routed state.
5. Fails validation if it finds rejected source records, pending cleanup entries, missing/extra/mismatched registry rows, missing/extra/mismatched target rows, or sink metadata mismatches.

## Required Spanner table for v2

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

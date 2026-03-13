# 06 - Config Parameters and Secrets

This document lists the required inputs to run the repository successfully.

## 1. Global runtime prerequisites

1. Python 3.10+
2. GCP access with required IAM
3. ADC configured with either `GOOGLE_APPLICATION_CREDENTIALS` or workload identity
4. Network access to Azure Cosmos DB and GCP services

## 2. v1 required parameters

From `config/migration.yaml`:

1. `source.endpoint`
2. `source.database`
3. `target.project`
4. `target.instance`
5. `target.database`
6. At least one mapping in `mappings[]`

v1 secrets:

1. `source.key` or `source.key_env` such as `COSMOS_KEY`

## 3. v2 required parameters

From `config/v2.multiapi-routing.yaml`:

1. `targets.firestore.project`
2. `targets.spanner.project`
3. `targets.spanner.instance`
4. `targets.spanner.database`
5. `targets.spanner.table`
6. `routing.firestore_lt_bytes`
7. `routing.spanner_max_payload_bytes`
8. At least one job in `jobs[]`

Mongo job secrets:

1. `connection_string` or `connection_string_env`

Cassandra job secrets:

1. `username` or `username_env`
2. `password` or `password_env`
3. `contact_points`

## 4. State backend parameters

1. `runtime.watermark_state_file`, `runtime.state_file`, and `runtime.route_registry_file` accept local paths.
2. They also accept `gs://bucket/object` paths.
3. They also support `spanner://<project>/<instance>/<database>/<table>?namespace=<name>` for row-based control-plane storage.
4. `runtime.reader_cursor_state_file`, `runtime.lease_file`, and `runtime.progress_file` also support `spanner://<project>/<instance>/<database>/<table>?namespace=<name>`.
5. Use `gs://` or `spanner://` paths for shared runners, rehearsals, and production orchestration.
6. `runtime.reader_cursor_state_file` is optional and stores restart-aware source cursor hints. Put it on the same durable backend family as the other shared state if you want crash recovery across processes.
7. `runtime.lease_file` is optional but required if you want multiple runners to coordinate work ownership safely.
8. `runtime.worker_id` is optional. If omitted, the process defaults to `<hostname>:<pid>`. You can also set `MIGRATION_WORKER_ID`.
9. `runtime.lease_duration_seconds` must be greater than `runtime.heartbeat_interval_seconds`.
10. For distributed runs, keep the lease, progress, cursor, watermark, and route-registry locations in the same durable shared backend family.
11. `runtime.retry_attempts` and related `retry_*` settings also govern mid-stream source reader restarts after transient failures.
12. `runtime.progress_file` is optional and is intended for distributed `full` runs so completed shards can be skipped on reruns.
13. `runtime.run_id` is required when `runtime.progress_file` is configured. Use a unique run id per migration campaign.
14. If you use `spanner://` for `runtime.lease_file` and `runtime.progress_file`, point them at the same Spanner control-plane table with different `namespace` values so shard claims can be updated transactionally.
15. You can also point `runtime.watermark_state_file`, `runtime.state_file`, and `runtime.route_registry_file` at that same table with separate namespaces to eliminate whole-object checkpoint and registry state.
16. The required Spanner DDL lives at [spanner_control_plane.sql](C:/Users/ados1/cosmos-to-spanner-migration/infra/ddl/spanner_control_plane.sql).
17. `runtime.deployment_environment` supports `dev`, `stage`, and `prod`.
18. `runtime.release_gate_file` stores stage rehearsal attestations.
19. `runtime.release_gate_scope` is the campaign key shared between stage attestation and prod execution.
20. `runtime.release_gate_max_age_hours` controls how old a stage attestation may be before prod rejects it.
21. `runtime.require_stage_rehearsal_for_prod` blocks prod migration runs unless a fresh matching stage attestation exists.

## 5. Security validation constraints

The loaders reject unsafe identifiers up front:

1. v1 `target_table`, `columns[].target`, `key_columns[]`, `static_columns` keys, and `validation_columns[]` must be valid Spanner identifiers.
2. v2 `targets.spanner.table` must be a valid Spanner identifier.
3. v2 Cassandra `keyspace`, `table`, and `incremental_field` must be valid Cassandra identifiers when provided.
4. v2 `routing.spanner_max_payload_bytes` cannot exceed the repository safe limit of `8,388,608` bytes.

For incremental Cassandra jobs with a custom `source_query`:

1. Use `%s` placeholder binding for watermark values.
2. Do not use string interpolation placeholders such as `{last_watermark}`.

## 6. Sharding parameters for large runs

v1 mapping-level sharding:

1. `mappings[].shard_count` defaults to `1`.
2. `mappings[].shard_mode` supports `none`, `client_hash`, and `query_template`.
3. `mappings[].shard_key_source` is required when `shard_mode=client_hash` and `shard_count > 1`.
4. For `query_template`, `source_query` or `incremental_query` must contain `{{SHARD_INDEX}}` and/or `{{SHARD_COUNT}}`.

v2 job-level sharding:

1. `jobs[].shard_count` defaults to `1`.
2. `jobs[].shard_mode` supports `none`, `client_hash`, and `query_template`.
3. v2 `client_hash` mode hashes the canonical `route_key`, so it does not require an extra `shard_key_source`.
4. v2 `query_template` requires shard placeholders in `source_query`.

Sharding guidance:

1. Use `client_hash` when you want deterministic ownership without changing the upstream query shape.
2. Use `query_template` when the source query can push partitioning down efficiently.
3. For very large datasets, prefer `query_template` because it reduces duplicate source scans across runners.
4. Keep `shard_count` aligned with the number of runners you intend to operate concurrently.
5. Reader cursor state is scoped to the active source query shape. If you intentionally want to restart a full job from the beginning, clear the cursor state file first.

## 7. Terraform required inputs

For each environment wrapper:

1. `project_id`
2. `region`
3. Spanner naming and config values for v1 and v2
4. Firestore location for v2
5. Optional explicit state bucket names

## 8. Secret Manager mapping

Terraform creates secret containers. You still need secret versions with values.

v1 secret IDs:

1. `cosmos-sql-endpoint`
2. `cosmos-sql-key`

v2 secret IDs:

1. `cosmos-mongo-connection-string`
2. `cosmos-cassandra-username`
3. `cosmos-cassandra-password`

## 9. Credentials model

This repository does not require a separate application-level `client id` or `client secret` if you use ADC.

Common patterns:

1. Service account key file:
   Set `GOOGLE_APPLICATION_CREDENTIALS`
2. Workload identity or attached service account:
   No key file is required

For Azure Cosmos authentication:

1. SQL API: account key
2. Mongo API: connection string
3. Cassandra API: username and password

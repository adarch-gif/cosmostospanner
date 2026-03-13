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
3. Use `gs://` paths for shared runners, rehearsals, and production orchestration.

## 5. Security validation constraints

The loaders reject unsafe identifiers up front:

1. v1 `target_table`, `columns[].target`, `key_columns[]`, `static_columns` keys, and `validation_columns[]` must be valid Spanner identifiers.
2. v2 `targets.spanner.table` must be a valid Spanner identifier.
3. v2 Cassandra `keyspace`, `table`, and `incremental_field` must be valid Cassandra identifiers when provided.
4. v2 `routing.spanner_max_payload_bytes` cannot exceed the repository safe limit of `8,388,608` bytes.

For incremental Cassandra jobs with a custom `source_query`:

1. Use `%s` placeholder binding for watermark values.
2. Do not use string interpolation placeholders such as `{last_watermark}`.

## 6. Terraform required inputs

For each environment wrapper:

1. `project_id`
2. `region`
3. Spanner naming and config values for v1 and v2
4. Firestore location for v2
5. Optional explicit state bucket names

## 7. Secret Manager mapping

Terraform creates secret containers. You still need secret versions with values.

v1 secret IDs:

1. `cosmos-sql-endpoint`
2. `cosmos-sql-key`

v2 secret IDs:

1. `cosmos-mongo-connection-string`
2. `cosmos-cassandra-username`
3. `cosmos-cassandra-password`

## 8. Credentials model

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

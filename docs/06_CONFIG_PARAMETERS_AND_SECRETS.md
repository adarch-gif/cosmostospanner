# 06 - Config Parameters and Secrets

This document lists all required inputs to run the repo successfully.

## 1. Global runtime prerequisites

1. Python 3.10+
2. GCP access with required IAM
3. ADC configured:
   - `GOOGLE_APPLICATION_CREDENTIALS` or workload identity
4. Network access to Cosmos + GCP services

## 2. v1 required parameters

From `config/migration.yaml`:

1. `source.endpoint`
2. `source.database`
3. `target.project`
4. `target.instance`
5. `target.database`
6. At least one mapping in `mappings[]`

v1 secrets:

1. `source.key` or `source.key_env` (for example `COSMOS_KEY`)

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

## 4. Terraform required inputs

For each env wrapper:

1. `project_id`
2. `region`
3. Spanner naming/config values for v1 and v2
4. Firestore location for v2
5. Optional explicit state bucket names

## 5. Secret Manager mapping

Terraform creates secret containers. You still need secret **versions** with values.

### v1 secret IDs

1. `cosmos-sql-endpoint`
2. `cosmos-sql-key`

### v2 secret IDs

1. `cosmos-mongo-connection-string`
2. `cosmos-cassandra-username`
3. `cosmos-cassandra-password`

## 6. Credentials model (client ID / key / secret guidance)

This repo does not require a separate “client id/client secret” inside app configs if you use ADC.

You have two common patterns:

1. **Service account key file**:
   - set `GOOGLE_APPLICATION_CREDENTIALS`
2. **Workload identity / attached SA**:
   - no key file, runtime identity handles auth

For Azure Cosmos authentication:

1. SQL API: account key
2. Mongo API: connection string (contains credentials)
3. Cassandra API: username + password


# 10 - Integration Testing

This document explains the live-cloud integration harness included in `tests_integration/`.

## 1. What it covers

1. v1 preflight against live Cosmos SQL API and Spanner resources.
2. v2 preflight against live Mongo/Cassandra source jobs and Firestore/Spanner targets.
3. Credential and connectivity validation before production cutover.

## 2. What it does not do

1. It does not provision resources.
2. It does not run by default in CI.
3. It does not replace full migration dress rehearsals.

## 3. Required environment variables

1. `RUN_LIVE_INTEGRATION_TESTS=true`
2. `COSMOS_TO_SPANNER_V1_INTEGRATION_CONFIG=<absolute path>`
3. `COSMOS_TO_SPANNER_V2_INTEGRATION_CONFIG=<absolute path>`

The referenced config files should point to real non-production test resources.

## 4. How to run

```powershell
python -m pytest -q -m integration
```

## 5. When to run

1. After Terraform changes.
2. Before a production migration rehearsal.
3. Before a production cutover.

## 6. Security guidance

1. Use dedicated non-production credentials.
2. Do not commit live integration configs to git.
3. Prefer `gs://` state objects for shared runners during rehearsal and cutover.

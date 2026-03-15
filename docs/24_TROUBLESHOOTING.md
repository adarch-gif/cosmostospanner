# 24 - Troubleshooting Guide

## Common startup failures

### `Cosmos source key is missing`

Cause:

- `source.key` and `source.key_env` are both unresolved.

Fix:

- Set `source.key` in config, or set the environment variable named by `source.key_env`.

### `Spanner target project is missing`

Cause:

- `target.project` missing and `GOOGLE_CLOUD_PROJECT` not set.

Fix:

- Set `target.project` in YAML or export `GOOGLE_CLOUD_PROJECT`.

### `No mappings selected. Check --container filters.`

Cause:

- `--container` filter values do not match configured `source_container` names.

Fix:

- Confirm exact container names in config.

### Preflight schema failure (`Target table does not exist` / `missing columns`)

Cause:

- Spanner DDL does not match mapping.

Fix:

- Create missing table/columns and rerun:
  - `python .\scripts\preflight.py --config .\config\migration.yaml`

## Configuration validation errors

### `key_columns must be a non-empty list`

Cause:

- Invalid or empty `key_columns`.

Fix:

- Use list format, for example: `key_columns: ["user_id"]`.

### `Duplicate target column in mapping`

Cause:

- Same target column appears more than once in `columns`.

Fix:

- Remove duplicate or merge mapping logic into one rule.

### `Key columns [...] are not produced by columns/static_columns`

Cause:

- One or more key columns do not appear in output row mapping.

Fix:

- Add key output to `columns` or `static_columns`.

## Runtime transformation failures

### `Missing required source field`

Cause:

- `required: true` column rule path not present in a document.

Fix:

- Adjust `source` path, add `default`, or update source data quality.

### `Cannot convert ... to timestamp/bool/int/float`

Cause:

- Source value incompatible with converter.

Fix:

- Correct converter type, clean source values, or use safer converter path.

## Runtime write failures

### Spanner mutation errors (type mismatch, missing table/column)

Cause:

- Target schema does not match mapping output.

Fix:

- Reconcile Spanner DDL with mapping column names and data types.

### Permission denied errors

Cause:

- Service account lacks required IAM permissions.

Fix:

- Grant required roles for Spanner read/write and database access.

## Incremental sync issues

### Watermark not advancing

Cause:

- No new documents matched incremental query.
- `_ts` not present or not increasing for source docs.

Fix:

- Validate incremental query and source update behavior.

### Duplicate processing during incremental

Cause:

- Overlap window intentionally re-reads recent data.

Fix:

- Expected behavior with `upsert`; reduce overlap if safe.

### DLQ file grows rapidly

Cause:

- Frequent transform or write errors while `error_mode=skip`.

Fix:

- Inspect `runtime.dlq_file_path` JSONL entries for root causes.
- Correct mapping/type issues and rerun targeted container backfill.

## Validation failures

### Count delta is not zero

Possible causes:

- Ongoing source writes during validation.
- Missing delete semantics.
- Query filters differ from expected target scope.

Fix:

- Freeze writes for deterministic validation window.
- Confirm `delete_rule`.
- Compare effective source/target scopes.

### Sample missing keys > 0

Possible causes:

- Incomplete backfill.
- Key mapping issue.
- Write failures in migration run.

Fix:

- Re-run targeted backfill for affected mapping.
- Verify key column transform output.
- Inspect logs for write exceptions.

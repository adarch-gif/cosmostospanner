# Configuration Reference

Main config file format: YAML.

Start with `config/migration.example.yaml`, then create `config/migration.yaml`.

## Root sections

- `source`: Cosmos connection settings.
- `target`: Spanner connection settings.
- `runtime`: execution behavior.
- `mappings`: container-to-table mappings.

## `source`

- `endpoint` (required): Cosmos account endpoint URL.
- `database` (required): Cosmos database name.
- `key` (optional): Cosmos key inline.
- `key_env` (optional): environment variable name that contains Cosmos key.

Validation rule:

- One of `key` or `key_env` must resolve to a non-empty key.

## `target`

- `project` (required unless `GOOGLE_CLOUD_PROJECT` is set): GCP project ID.
- `instance` (required): Spanner instance ID.
- `database` (required): Spanner database ID.

## `runtime`

- `batch_size` (default `500`, must be `> 0`): rows written per Spanner batch.
- `query_page_size` (default `200`, must be `> 0`): max documents returned per Cosmos page.
- `dry_run` (default `false`): if true, no Spanner writes are executed.
- `log_level` (default `INFO`): `DEBUG`, `INFO`, `WARNING`, `ERROR`.
- `watermark_state_file` (default `state/watermarks.json`): local watermark JSON file path.
- `watermark_overlap_seconds` (default `5`, must be `>= 0`): overlap window for incremental reads.
- `flush_watermark_each_mapping` (default `true`): flush watermark file after each mapping in incremental mode.
- `error_mode` (default `fail`): `fail` or `skip`.
- `dlq_file_path` (default `state/dead_letter.jsonl`): dead-letter JSONL output path.
- `retry_attempts` (default `5`, must be `>= 1`): max retry attempts for retriable I/O operations.
- `retry_initial_delay_seconds` (default `0.5`, must be `>= 0`): first retry delay.
- `retry_max_delay_seconds` (default `15.0`, must be `>= 0`): max delay cap.
- `retry_backoff_multiplier` (default `2.0`, must be `>= 1`): exponential backoff multiplier.
- `retry_jitter_seconds` (default `0.25`, must be `>= 0`): random jitter added to retry delay.
- `max_docs_per_container` (optional): dictionary of container limit overrides.
  - `0` or `null` means unlimited.

## `mappings[]`

Each mapping defines one Cosmos container to one Spanner table migration.

- `source_container` (required): Cosmos container name.
- `target_table` (required): Spanner table name.
- `key_columns` (required): non-empty list of target key columns.
- `columns` (required): field mapping rules.
- `mode` (default `upsert`): `upsert`, `insert`, `update`, `replace`.
- `source_query` (default `SELECT * FROM c`): query for full/backfill and validation scans.
- `incremental_query` (optional): query for incremental mode.
  - If omitted, default is `SELECT * FROM c WHERE c._ts > @last_ts`.
- `static_columns` (optional): constant or marker values set on every row.
- `delete_rule` (optional): converts matching documents into delete operations.
- `validation_columns` (optional): explicit column list for value comparison in validation.
  - If omitted, validation compares columns from `columns` mapping rules.

Validation rules:

- `key_columns` must be a non-empty list of non-empty strings.
- Mapping mode must be one of supported values.
- `columns` target names must be unique.
- Every `key_columns` entry must be produced by either:
  - a `columns[].target`, or
  - a `static_columns` key.
- `validation_columns` must be unique and must exist in mapped output columns.

## `columns` formats

You can use either list form or dictionary form.

### List form

```yaml
columns:
  - target: "user_id"
    source: "id"
    converter: "string"
    required: true
```

### Dictionary form

```yaml
columns:
  user_id:
    source: "id"
    converter: "string"
    required: true
  email: "profile.email"
```

## Converter reference

- `identity`: no conversion.
- `string`: `str(value)`.
- `int`: `int(value)`.
- `float`: `float(value)`.
- `bool`: supports `true/false`, `1/0`, `yes/no`.
- `timestamp` / `datetime`: converts to UTC `datetime`.
- `json_string`: JSON serializes value to a compact string.

## Nested field paths

- Dot notation supported for nested objects: `profile.email`.
- List indexing supported with numeric segments: `addresses.0.city`.

## Marker values

Supported marker in `static_columns` and defaults:

- `__NOW_UTC__`: injects current UTC timestamp.

## Delete rule

Example:

```yaml
delete_rule:
  field: "isDeleted"
  equals: true
```

If the source document matches rule, migration emits a Spanner delete for `key_columns`.

## Example complete mapping

```yaml
mappings:
  - source_container: "users"
    target_table: "Users"
    key_columns: ["user_id"]
    mode: "upsert"
    source_query: "SELECT * FROM c"
    incremental_query: "SELECT * FROM c WHERE c._ts > @last_ts"
    columns:
      - target: "user_id"
        source: "id"
        converter: "string"
        required: true
      - target: "email"
        source: "profile.email"
        converter: "string"
      - target: "created_at"
        source: "createdAt"
        converter: "timestamp"
    static_columns:
      migrated_at: "__NOW_UTC__"
    validation_columns:
      - "email"
      - "created_at"
    delete_rule:
      field: "isDeleted"
      equals: true
```

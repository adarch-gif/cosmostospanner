# 07 - Codebase Structure

## 1. Top-level layout

1. `config/`: example YAML configuration files
2. `migration/`: v1 core runtime modules
3. `migration_v2/`: v2 core runtime modules
4. `scripts/`: executable entrypoints
5. `tests/`: v1-related tests + shared runtime tests
6. `tests_v2/`: v2-specific tests
7. `tests_integration/`: opt-in live-cloud smoke tests
8. `infra/terraform/`: IaC module, stacks, and environment wrappers
9. `docs/`: numbered and deep-dive documentation

## 2. v1 code map

1. `migration/config.py`: v1 config schema and parsing
2. `migration/cosmos_reader.py`: Cosmos SQL source reads
3. `migration/transform.py`: mapping and type conversion
4. `migration/spanner_writer.py`: Spanner reads/writes for v1
5. `migration/state_store.py`: watermark persistence
6. `migration/json_state_backend.py`: local and `gs://` JSON state backend
7. `scripts/preflight.py`: source/target checks
8. `scripts/backfill.py`: migration execution
9. `scripts/validate.py`: sampled and checksum reconciliation

## 3. v2 code map

1. `migration_v2/config.py`: v2 config schema
2. `migration_v2/source_adapters/`:
   - `mongo_cosmos.py`
   - `cassandra_cosmos.py`
3. `migration_v2/router.py`: payload-size routing
4. `migration_v2/sink_adapters/`:
   - `firestore_sink.py`
   - `spanner_sink.py`
5. `migration_v2/pipeline.py`: orchestration + route moves + checkpoint safety
6. `migration_v2/state_store.py`: watermarks + route registry
7. `migration_v2/reconciliation.py`: exact routed validation across source, registry, and sinks
8. `scripts/v2_preflight.py`
9. `scripts/v2_route_migrate.py`
10. `scripts/v2_validate.py`

## 4. Shared operational modules

1. `migration/retry_utils.py`: retry policy
2. `migration/dead_letter.py`: JSONL DLQ sink
3. `migration/logging_utils.py`: log bootstrap

## 5. Test coverage map

1. Config parsing validation
2. Transformation correctness
3. Retry behavior
4. v2 router decisions
5. v2 route-move logic
6. Incremental checkpoint behavior
7. State backend round-trips for local and `gs://` paths
8. v2 routed reconciliation and manifest-backed distributed execution
9. control-plane coordination, stage release gates, and stale-shard recovery

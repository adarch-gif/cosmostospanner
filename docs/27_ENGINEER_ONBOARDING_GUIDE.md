# Cosmos-to-Spanner Migration Toolkit — Complete Engineer Onboarding Guide

**Purpose**: This document gives a new engineer everything needed to understand, run, debug, and extend this codebase. Read it front-to-back before touching any code.

**Last updated**: 2026-03-13

---

## Table of Contents

1. [What This Project Does](#1-what-this-project-does)
2. [Architecture Overview](#2-architecture-overview)
3. [Repository Structure](#3-repository-structure)
4. [Technology Stack](#4-technology-stack)
5. [V1 Pipeline Deep Dive — Cosmos SQL API → Spanner](#5-v1-pipeline-deep-dive)
6. [V2 Pipeline Deep Dive — Cosmos MongoDB/Cassandra → Firestore/Spanner](#6-v2-pipeline-deep-dive)
7. [Shared Infrastructure Modules](#7-shared-infrastructure-modules)
8. [Configuration Reference](#8-configuration-reference)
9. [State Management & Persistence](#9-state-management--persistence)
10. [Distributed Coordination](#10-distributed-coordination)
11. [Sharding Strategy](#11-sharding-strategy)
12. [Fault Tolerance & Recovery](#12-fault-tolerance--recovery)
13. [Validation & Reconciliation](#13-validation--reconciliation)
14. [Release Gates & Production Safety](#14-release-gates--production-safety)
15. [Infrastructure as Code (Terraform)](#15-infrastructure-as-code-terraform)
16. [CI/CD Pipeline](#16-cicd-pipeline)
17. [Local Development Setup](#17-local-development-setup)
18. [Running the Migration](#18-running-the-migration)
19. [Troubleshooting Guide](#19-troubleshooting-guide)
20. [Code Walkthrough: Processing a Single Document](#20-code-walkthrough-processing-a-single-document)
21. [Key Design Decisions & Trade-offs](#21-key-design-decisions--trade-offs)
22. [Glossary](#22-glossary)

---

## 1. What This Project Does

This toolkit migrates data from **Azure Cosmos DB** to **Google Cloud Spanner** (and optionally Firestore). It handles:

- **Bulk backfill**: Migrate all existing data from Cosmos to Spanner
- **Incremental sync**: Continuously pull new/changed documents using watermark timestamps
- **Multi-API support**: Works with Cosmos SQL API (V1), MongoDB API, and Cassandra API (V2)
- **Dynamic routing**: V2 routes small documents (<1 MiB) to Firestore and large documents to Spanner
- **Distributed execution**: Multiple workers can process different shards simultaneously
- **Crash recovery**: Reader cursors and watermarks persist across restarts
- **Validation**: Post-migration reconciliation verifies data integrity

### Why Two Pipelines?

| | V1 Pipeline | V2 Pipeline |
|---|---|---|
| **Source API** | Cosmos SQL API | Cosmos MongoDB API, Cassandra API |
| **Target** | Spanner only | Firestore + Spanner (routed by size) |
| **Use case** | Structured SQL-queryable data | Document stores, mixed payload sizes |
| **Maturity** | Primary pipeline | Newer, more flexible |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Migration Toolkit                            │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ V1 Pipeline (migration/ + scripts/backfill.py)           │   │
│  │                                                           │   │
│  │  Cosmos SQL API ──► CosmosReader ──► transform_document() │  │
│  │       │                                    │              │   │
│  │       │                                    ▼              │   │
│  │       │                              SpannerWriter        │   │
│  │       │                              (batch upsert/       │   │
│  │       │                               delete/insert)      │   │
│  └──────┼────────────────────────────────────────────────────┘  │
│          │                                                       │
│  ┌──────┼────────────────────────────────────────────────────┐  │
│  │ V2 Pipeline (migration_v2/ + scripts/v2_route_migrate.py) │  │
│  │       │                                                    │  │
│  │  Cosmos Mongo ──► MongoCosmosSourceAdapter ──┐            │  │
│  │  Cosmos Cass. ──► CassandraCosmosSourceAdapter│           │  │
│  │                                               ▼            │  │
│  │                                         SizeRouter         │  │
│  │                                        /         \         │  │
│  │                                       ▼           ▼        │  │
│  │                               FirestoreSink  SpannerSink   │  │
│  │                               (< 1 MiB)     (>= 1 MiB)   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Shared Infrastructure                                     │   │
│  │  • WorkCoordinator (leases, progress)                     │   │
│  │  • WatermarkStore (checkpoint persistence)                │   │
│  │  • ReaderCursorStore (stream resumption)                  │   │
│  │  • DeadLetterSink (failed records)                        │   │
│  │  • RetryPolicy (exponential backoff + jitter)             │   │
│  │  • ReleaseGateStore (stage rehearsal attestation)         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ State Backends                                            │   │
│  │  • Local JSON files (development)                         │   │
│  │  • Google Cloud Storage (single-worker production)        │   │
│  │  • Spanner MigrationControlPlane table (multi-worker)     │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

1. **Read**: Pull documents from Cosmos DB (paginated, with continuation tokens)
2. **Filter**: Apply shard filter (if sharded) — only process documents belonging to this shard
3. **Transform** (V1): Convert Cosmos document to Spanner row using column mapping rules
4. **Route** (V2): Decide Firestore vs Spanner based on payload size
5. **Write**: Batch-write to target (Spanner `insert_or_update` / Firestore `set`)
6. **Checkpoint**: Update watermark so the next run picks up where this one left off
7. **Validate**: Post-migration reconciliation compares source ↔ target counts and checksums

---

## 3. Repository Structure

```
cosmos-to-spanner-migration/
│
├── migration/                      # V1 pipeline core modules
│   ├── __init__.py
│   ├── config.py                   # V1 YAML config parser + validation
│   ├── cosmos_reader.py            # Reads from Cosmos SQL API
│   ├── spanner_writer.py           # Writes to Spanner (batch ops)
│   ├── transform.py                # Document → Spanner row transformation
│   ├── state_store.py              # Watermark persistence (JSON/GCS/Spanner)
│   ├── coordination.py             # Distributed lease + work progress tracking
│   ├── resume.py                   # Reader cursor state for crash recovery
│   ├── reconciliation.py           # Checksum-based source↔target comparison
│   ├── sharding.py                 # Deterministic shard assignment (SHA256)
│   ├── retry_utils.py              # Exponential backoff retry wrapper
│   ├── dead_letter.py              # Failed record JSONL sink
│   ├── release_gate.py             # Stage rehearsal → prod gating
│   ├── control_plane_backend.py    # Spanner-backed state (spanner:// URIs)
│   ├── json_state_backend.py       # JSON file / GCS-backed state
│   ├── logging_utils.py            # Log level configuration
│   └── file_lock.py                # OS file lock for state concurrency
│
├── migration_v2/                   # V2 pipeline core modules
│   ├── __init__.py
│   ├── config.py                   # V2 YAML config parser + validation
│   ├── models.py                   # CanonicalRecord, RoutedSinkRecord
│   ├── router.py                   # Size-based routing (Firestore vs Spanner)
│   ├── pipeline.py                 # V2 orchestrator (main run loop)
│   ├── state_store.py              # V2 watermarks + route registry
│   ├── reconciliation.py           # V2 validation runner
│   ├── utils.py                    # JSON, checksum, nested-get utilities
│   ├── source_adapters/
│   │   ├── base.py                 # SourceAdapter protocol
│   │   ├── mongo_cosmos.py         # Cosmos MongoDB API reader
│   │   └── cassandra_cosmos.py     # Cosmos Cassandra API reader
│   └── sink_adapters/
│       ├── base.py                 # SinkAdapter protocol
│       ├── firestore_sink.py       # Firestore writer
│       └── spanner_sink.py         # Spanner writer (V2 schema)
│
├── scripts/                        # CLI entry points
│   ├── backfill.py                 # V1 migration runner
│   ├── preflight.py                # V1 pre-migration health check
│   ├── validate.py                 # V1 post-migration reconciliation
│   ├── v2_route_migrate.py         # V2 migration runner
│   ├── v2_preflight.py             # V2 pre-migration health check
│   ├── v2_validate.py              # V2 post-migration reconciliation
│   ├── release_gate.py             # Stage rehearsal attestation
│   ├── control_plane_status.py     # Distributed run monitoring
│   └── bootstrap_env.ps1           # Windows environment bootstrap
│
├── tests/                          # V1 unit tests
├── tests_v2/                       # V2 unit tests
├── tests_integration/              # Live resource integration tests
│
├── config/                         # Example configuration files
│   ├── migration.example.yaml      # V1 config template
│   └── v2.multiapi-routing.example.yaml  # V2 config template
│
├── infra/                          # Infrastructure
│   ├── ddl/
│   │   └── spanner_control_plane.sql  # Spanner control plane table DDL
│   └── terraform/
│       ├── modules/                # Reusable Terraform modules
│       ├── stacks/                 # V1 and V2 stack definitions
│       └── envs/                   # dev/stage/prod environment wrappers
│
├── docs/                           # Documentation
├── .github/workflows/              # CI/CD
│   ├── ci.yml                      # Tests + lint + security + terraform
│   └── stage-release-gate.yml      # Manual stage attestation workflow
│
├── pyproject.toml                  # Ruff + Mypy config
├── requirements.txt                # Runtime dependencies
├── requirements-dev.txt            # Dev/lint dependencies
├── pytest.ini                      # Test markers
└── mypy.ini                        # Type checking config
```

---

## 4. Technology Stack

| Component | Technology | Version |
|---|---|---|
| Language | Python | 3.11+ |
| Source DB | Azure Cosmos DB | SQL, MongoDB, Cassandra APIs |
| Target DB | Google Cloud Spanner | Latest |
| Target Store | Google Cloud Firestore | Latest (V2 only) |
| State Storage | Local JSON / GCS / Spanner | - |
| IaC | Terraform | 1.7.5 |
| CI/CD | GitHub Actions | - |
| Linting | Ruff | 0.4.8+ |
| Type Checking | Mypy | 1.10+ |
| Security Scan | Bandit + pip-audit | Latest |
| Testing | pytest | 8.0+ |

### Key Python Dependencies

```
azure-cosmos>=4.6.0          # Cosmos SQL API client
google-cloud-spanner>=3.46.0 # Spanner client (gRPC)
google-cloud-firestore>=2.16.0  # Firestore client
google-cloud-storage>=2.16.0 # GCS client (for state files)
PyYAML>=6.0.1                # Config parsing
pymongo>=4.7.0               # Cosmos MongoDB API client
cassandra-driver>=3.29.0     # Cosmos Cassandra API client
```

---

## 5. V1 Pipeline Deep Dive

### 5.1 Entry Point: `scripts/backfill.py`

This is the main runner. CLI usage:

```bash
# Full backfill (all containers)
python scripts/backfill.py --config config/migration.yaml

# Incremental sync (watermark-based)
python scripts/backfill.py --config config/migration.yaml --incremental

# Filter to specific containers
python scripts/backfill.py --config config/migration.yaml --container users --container orders

# Override watermark start point
python scripts/backfill.py --config config/migration.yaml --incremental --since-ts 1700000000

# Dry run (read + transform, skip writes)
python scripts/backfill.py --config config/migration.yaml --dry-run
```

### 5.2 Execution Flow

```
main()
  ├── load_config(args.config)           # Parse YAML → MigrationConfig
  ├── enforce_stage_rehearsal_or_raise()  # Block if prod without stage gate
  ├── Initialize: reader, writer, watermark_store, cursor_store, dead_letter, coordinator
  │
  ├── [If coordinator + full run + progress_enabled]:
  │   └── _build_full_run_work_plan()    # Build all work items across all mappings
  │       └── While pending_work_items:
  │           └── coordinator.claim_next()  # Claim next available shard
  │               └── _process_mapping()    # Process one mapping/shard
  │
  ├── [Else - standard execution]:
  │   └── For each mapping:
  │       └── For each shard_index:
  │           ├── Check: already completed? lease held?
  │           └── _process_mapping()
  │
  └── watermark_store.flush()            # Persist final watermarks
```

### 5.3 The `_process_mapping()` Function (the core loop)

This is the heart of V1. Located at `scripts/backfill.py:254`. Here's what happens for each mapping:

```python
# Pseudocode for _process_mapping():

1. Determine watermark (from store or --since-ts override)
2. Build SQL query (full or incremental with watermark overlap)
3. Open reader cursor (resume from last position if crash-recovered)

4. For each document from Cosmos:
   a. Heartbeat lease renewal (if distributed)
   b. Check shard membership (if client_hash sharding)
   c. Transform document → Spanner row (or detect delete)
   d. Buffer into upsert_buffer or delete_buffer
   e. When buffer reaches batch_size → flush to Spanner
      - On batch failure in skip mode: fall back to row-by-row writes
      - Failed rows → dead letter queue
   f. Persist reader cursor periodically

5. Flush remaining buffers
6. Advance watermark (only if zero failures)
7. Clear reader cursor (only if fully successful)
```

### 5.4 Document Transformation (`migration/transform.py`)

The transform layer converts a Cosmos JSON document into a flat Spanner row dict:

```python
# Given this Cosmos document:
{
    "id": "user-123",
    "profile": {"email": "alice@example.com", "age": 30},
    "_ts": 1700000000,
    "isDeleted": false
}

# And this mapping config:
columns:
  - target: "user_id"
    source: "id"
    converter: "string"
    required: true
  - target: "email"
    source: "profile.email"    # ← nested path!
    converter: "string"
  - target: "age"
    source: "profile.age"
    converter: "int"
static_columns:
  migrated_at: "__NOW_UTC__"   # ← resolved to current UTC datetime

# Produces this Spanner row:
{
    "user_id": "user-123",
    "email": "alice@example.com",
    "age": 30,
    "migrated_at": datetime(2026, 3, 13, ...)
}
```

**Converters available**: `identity`, `string`, `int`, `float`, `bool`, `timestamp`, `json_string`

**Delete detection**: If `delete_rule` is configured (e.g., `field: "isDeleted", equals: true`), matching documents produce a delete operation instead of an upsert.

### 5.5 Write Operations (`migration/spanner_writer.py`)

Supported write modes (per mapping):
- **`upsert`** (default): `insert_or_update` — creates or updates
- **`insert`**: `insert` — fails if row exists
- **`update`**: `update` — fails if row doesn't exist
- **`replace`**: `replace` — replaces entire row

All writes are batched (default 500 rows per commit) and retry-wrapped.

---

## 6. V2 Pipeline Deep Dive

### 6.1 Entry Point: `scripts/v2_route_migrate.py`

```bash
# Full migration (all jobs)
python scripts/v2_route_migrate.py --config config/v2.yaml

# Incremental mode
python scripts/v2_route_migrate.py --config config/v2.yaml --incremental

# Filter to specific jobs
python scripts/v2_route_migrate.py --config config/v2.yaml --job mongo_users --job cassandra_events

# Dry run
python scripts/v2_route_migrate.py --config config/v2.yaml --dry-run
```

### 6.2 The Canonical Record Model

V2 normalizes all source data into a `CanonicalRecord` regardless of source API:

```python
@dataclass(frozen=True)
class CanonicalRecord:
    source_job: str           # "mongo_users"
    source_api: str           # "mongodb" or "cassandra"
    source_namespace: str     # "mongodb.appdb.users"
    source_key: str           # Composite key from key_fields
    route_key: str            # "{namespace}:{source_key}"
    payload: dict             # Original document as dict
    payload_size_bytes: int   # len(json.dumps(payload))
    checksum: str             # SHA256 of normalized payload
    event_ts: str             # ISO timestamp
    watermark_value: Any      # Value of incremental_field
```

### 6.3 Size-Based Routing (`migration_v2/router.py`)

The router decides where each record goes based on payload size:

```
payload_size + overhead < firestore_lt_bytes (1 MiB)  → FIRESTORE
payload_size + overhead < spanner_max_bytes (8 MiB)   → SPANNER
payload_size + overhead >= spanner_max_bytes           → REJECT (too large)
```

Default thresholds:
- Firestore: < 1,048,576 bytes (1 MiB)
- Spanner: < 8,388,608 bytes (8 MiB)
- Overhead: 2,048 bytes (for metadata columns)

### 6.4 Route Registry — Handling Destination Changes

When a document's payload size changes (e.g., grows past 1 MiB), it needs to **move** from Firestore to Spanner. The route registry tracks this:

```
1. Record was in Firestore (checksum=abc)
2. New version is larger → routes to Spanner
3. Write new version to Spanner
4. Set registry: destination=SPANNER, sync_state=PENDING_CLEANUP, cleanup_from=FIRESTORE
5. Delete from Firestore
6. Set registry: sync_state=COMPLETE, cleanup_from=None
```

If the process crashes between steps 4 and 5, the `PENDING_CLEANUP` state ensures the next run completes the deletion.

### 6.5 Incremental Watermark with Boundary Deduplication

V2 uses a sophisticated checkpoint that includes both a watermark value AND the set of route keys at that watermark boundary. This prevents re-processing records that share the same timestamp:

```python
WatermarkCheckpoint(
    watermark=1700000000,          # Last processed timestamp
    route_keys=["ns:key1", "ns:key2"],  # Keys at this exact timestamp
    updated_at="2026-03-13T..."
)
```

On replay, records with `watermark < checkpoint.watermark` are skipped. Records with `watermark == checkpoint.watermark` are skipped only if their `route_key` is in `checkpoint.route_keys`.

---

## 7. Shared Infrastructure Modules

### 7.1 Retry Policy (`migration/retry_utils.py`)

All external calls (Cosmos reads, Spanner writes, GCS operations) go through the retry wrapper:

```python
@dataclass
class RetryPolicy:
    max_attempts: int = 5
    initial_delay_seconds: float = 0.5
    max_delay_seconds: float = 15.0
    backoff_multiplier: float = 2.0
    jitter_seconds: float = 0.25
```

Behavior: `0.5s → 1.0s → 2.0s → 4.0s → 8.0s` (with ±0.25s jitter), capped at 15s.

Two wrappers:
- `run_with_retry(fn)`: Retry a single operation
- `iter_with_retry(fn, resume_fn)`: Retry a streaming operation, calling `resume_fn` to reopen the stream from the last successful position

### 7.2 Dead Letter Queue (`migration/dead_letter.py`)

Failed records are written to a JSONL file with full diagnostic context:

```json
{
    "timestamp": "2026-03-13T10:30:00Z",
    "stage": "write_upsert",
    "mapping_name": "users->Users",
    "source_container": "users",
    "target_table": "Users",
    "error_type": "google.api_core.exceptions.Aborted",
    "error_message": "Row already exists",
    "source_document": {"id": "user-123", ...},
    "transformed_row": {"user_id": "user-123", ...}
}
```

### 7.3 Reader Cursor Store (`migration/resume.py`)

Tracks the reader's position in the Cosmos change stream so that a crashed worker can resume without re-processing:

```python
@dataclass
class StreamResumeState:
    last_source_key: str      # Last processed document ID
    last_watermark: int       # Last processed _ts value
    emitted_count: int        # Documents emitted so far
    page_start_token: str     # Cosmos continuation token
    scope: str                # Identifier for this stream
```

Persisted after each batch flush. On restart, the reader opens the stream at `page_start_token` and skips documents until it reaches `last_source_key`.

---

## 8. Configuration Reference

### 8.1 V1 Configuration (`config/migration.example.yaml`)

```yaml
source:
  endpoint: "https://your-cosmos.documents.azure.com:443/"
  key_env: "COSMOS_KEY"          # Environment variable name
  database: "app_db"

target:
  project: "gcp-project-id"      # Or GOOGLE_CLOUD_PROJECT env var
  instance: "spanner-instance"
  database: "spanner-db"

runtime:
  deployment_environment: "dev"   # dev | stage | prod
  batch_size: 500                 # Rows per Spanner commit
  query_page_size: 200            # Cosmos documents per page
  dry_run: false
  log_level: "INFO"
  error_mode: "fail"              # fail | skip (skip → DLQ)

  # State persistence (choose one backend):
  watermark_state_file: "state/watermarks.json"     # Local
  # watermark_state_file: "gs://bucket/watermarks.json"  # GCS
  # watermark_state_file: "spanner://project/instance/db"  # Spanner

  # Crash recovery:
  reader_cursor_state_file: "state/cursors.json"

  # Distributed coordination:
  lease_file: "spanner://project/instance/db"
  progress_file: "spanner://project/instance/db"
  run_id: "backfill-2026-03-13"
  worker_id: ""                   # Auto-detected: hostname:pid
  lease_duration_seconds: 120
  heartbeat_interval_seconds: 30

  # Production safety:
  release_gate_file: "spanner://project/instance/db"
  release_gate_scope: "q1-migration"
  require_stage_rehearsal_for_prod: true
  release_gate_max_age_hours: 72

  # Retry policy:
  retry_attempts: 5
  retry_initial_delay_seconds: 0.5
  retry_max_delay_seconds: 15.0
  retry_backoff_multiplier: 2.0
  retry_jitter_seconds: 0.25

  # Incremental:
  watermark_overlap_seconds: 5    # Re-process 5s window for safety

mappings:
  - source_container: "users"
    target_table: "Users"
    key_columns: ["user_id"]
    mode: "upsert"                # upsert | insert | update | replace
    source_query: "SELECT * FROM c"
    incremental_query: "SELECT * FROM c WHERE c._ts > @last_ts ORDER BY c._ts"
    shard_count: 4
    shard_mode: "client_hash"     # none | client_hash | query_template
    shard_key_source: "id"        # Field used for hash-based sharding

    columns:
      - target: "user_id"
        source: "id"
        converter: "string"
        required: true
      - target: "email"
        source: "profile.email"
        converter: "string"
      - target: "created_ts"
        source: "_ts"
        converter: "timestamp"

    static_columns:
      migrated_at: "__NOW_UTC__"

    validation_columns: ["email", "created_ts"]

    delete_rule:
      field: "isDeleted"
      equals: true
```

### 8.2 V2 Configuration (`config/v2.multiapi-routing.example.yaml`)

```yaml
runtime:
  deployment_environment: "dev"
  mode: "full"                    # full | incremental
  batch_size: 200
  state_file: "state/v2_watermarks.json"
  route_registry_file: "state/v2_route_registry.json"
  error_mode: "fail"
  dry_run: false

routing:
  firestore_lt_bytes: 1048576     # < 1 MiB → Firestore
  spanner_max_payload_bytes: 8388608  # < 8 MiB → Spanner, else REJECT

targets:
  firestore:
    project: "gcp-project"
    collection: "cosmos_router_v2"
  spanner:
    project: "gcp-project"
    instance: "spanner-instance"
    database: "spanner-db"
    table: "RoutedDocuments"

jobs:
  - name: "mongo_users"
    api: "mongodb"
    enabled: true
    connection_string_env: "COSMOS_MONGO_CONN"
    database: "appdb"
    collection: "users"
    route_namespace: "mongodb.appdb.users"
    key_fields: ["_id"]
    incremental_field: "_ts"
    page_size: 100
    shard_count: 1
    shard_mode: "none"

  - name: "cassandra_events"
    api: "cassandra"
    enabled: true
    contact_points: ["cosmos-cassandra.cosmos.azure.com"]
    port: 10350
    username_env: "CASS_USER"
    password_env: "CASS_PASS"
    keyspace: "app"
    table: "events"
    route_namespace: "cassandra.app.events"
    key_fields: ["event_id"]
    incremental_field: "updated_at"
```

---

## 9. State Management & Persistence

### 9.1 Three Backend Options

The toolkit supports three backends for all state (watermarks, leases, progress, cursors, release gates):

| Backend | URI Pattern | Best For | Concurrency |
|---|---|---|---|
| Local JSON | `state/watermarks.json` | Development, single worker | File lock |
| GCS | `gs://bucket/path.json` | Single worker, cloud | Optimistic (generation tag) |
| Spanner | `spanner://project/instance/db` | Multi-worker, production | Transactional |

### 9.2 The Spanner Control Plane Table

When using the Spanner backend, all state lives in one table:

```sql
CREATE TABLE MigrationControlPlane (
    namespace   STRING(255)  NOT NULL,   -- e.g., "v1-watermarks", "v2-leases"
    record_key  STRING(1024) NOT NULL,   -- e.g., "users:shard=0"
    payload_json STRING(MAX) NOT NULL,   -- JSON-encoded state
    status      STRING(32),              -- "running", "completed", etc.
    owner_id    STRING(255),             -- "hostname:pid"
    lease_expires_at TIMESTAMP,          -- Lease expiry for coordination
    updated_at  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (namespace, record_key);
```

**Namespace convention**:
- `v1-watermarks` — V1 watermark checkpoints
- `v1-leases` — V1 distributed leases
- `v1-progress:{run_id}` — V1 shard completion tracking
- `v1-reader-cursors` — V1 reader cursor positions
- `v2-watermarks` — V2 watermark checkpoints
- `v2-leases` — V2 distributed leases
- `release-gates` — Stage rehearsal attestations

### 9.3 State File Lifecycle

```
[Full Run]
1. Start: watermark = 0 (or from state file)
2. Process documents...
3. End: watermark = max(_ts) of processed documents
4. Persist watermark → state file
5. Clear reader cursor → state file

[Incremental Run]
1. Start: watermark = value from state file
2. Query: SELECT * FROM c WHERE c._ts > (watermark - overlap_seconds)
3. Process documents...
4. End: watermark = max(_ts) of successfully processed documents
5. Persist watermark ONLY if zero failures occurred
```

---

## 10. Distributed Coordination

### 10.1 How Multi-Worker Execution Works

When `lease_file` is configured, multiple workers can process the same migration concurrently:

```
Worker A (hostname:1234)          Worker B (hostname:5678)
  │                                  │
  ├── claim_next(work_items)         ├── claim_next(work_items)
  │   → gets "users:shard=0"        │   → gets "users:shard=1"
  │                                  │
  ├── acquire("users:shard=0")       ├── acquire("users:shard=1")
  │   → writes lease to Spanner     │   → writes lease to Spanner
  │                                  │
  ├── [processing...]                ├── [processing...]
  │   ├── renew_if_due() every 30s  │   ├── renew_if_due() every 30s
  │   └── heartbeat updates lease   │   └── heartbeat updates lease
  │                                  │
  ├── mark_completed("users:shard=0")├── mark_completed("users:shard=1")
  └── release("users:shard=0")      └── release("users:shard=1")
```

### 10.2 Lease Mechanics

- **Lease duration**: 120s (default)
- **Heartbeat interval**: 30s (must be < lease duration)
- **Lease acquisition**: Transactional read-then-write on Spanner, fails if lease is held by another worker with non-expired TTL
- **Stale lease recovery**: If a worker crashes, its lease expires after 120s and other workers can claim it
- **`renew_if_due()`**: Called on every document iteration; renews lease if heartbeat interval has elapsed

### 10.3 Work Progress Tracking

For full runs (not incremental), the system tracks shard completion:

```
work_key: "v1:users->Users:shard=0"
status: "completed" | "running" | "failed" | "pending"
```

On restart with the same `run_id`, completed shards are skipped. This enables resumable full migrations.

---

## 11. Sharding Strategy

### 11.1 Three Sharding Modes

| Mode | How It Works | When to Use |
|---|---|---|
| `none` | No sharding, process everything | Small datasets (<100K docs) |
| `client_hash` | SHA256(shard_key) % shard_count → shard assignment. Every worker reads ALL documents but filters locally. | When Cosmos query doesn't support server-side partitioning |
| `query_template` | `{{SHARD_INDEX}}` and `{{SHARD_COUNT}}` placeholders in SQL query. Cosmos does server-side filtering. | Large datasets where you want to reduce reads |

### 11.2 Client Hash Sharding Example

```yaml
shard_count: 4
shard_mode: "client_hash"
shard_key_source: "id"          # Use document's "id" field
```

Document with `id="user-123"`:
```python
sha256("user-123") → int → mod 4 → shard 2
```

Worker 0 processes shard 0, Worker 1 processes shard 1, etc. All workers read from the same Cosmos query but each processes only 1/4 of documents.

### 11.3 Query Template Sharding Example

```yaml
shard_count: 4
shard_mode: "query_template"
source_query: "SELECT * FROM c WHERE c.partition_id % {{SHARD_COUNT}} = {{SHARD_INDEX}}"
```

Each worker gets a query with `{{SHARD_INDEX}}` replaced by its assigned shard number. Cosmos does the filtering server-side.

---

## 12. Fault Tolerance & Recovery

### 12.1 Error Modes

| Mode | Behavior |
|---|---|
| `fail` (default) | First error aborts the entire run |
| `skip` | Failed records go to dead letter queue, processing continues |

### 12.2 Recovery Scenarios

**Scenario: Worker crashes mid-batch**
1. Reader cursor was persisted after last successful batch
2. On restart, reader resumes from cursor position
3. Watermark was NOT advanced (only advances on success)
4. Some documents may be re-processed (at-least-once)

**Scenario: Spanner write times out**
1. Retry policy retries with backoff (up to 5 attempts)
2. If all retries fail:
   - `error_mode=fail`: Run aborts, watermark not advanced
   - `error_mode=skip`: Record goes to DLQ, processing continues

**Scenario: Batch upsert partially fails (skip mode)**
1. Entire batch retry fails
2. Falls back to row-by-row writes
3. Individual failures go to DLQ
4. Successful rows are counted; watermark advances up to last success

**Scenario: V2 document moves from Firestore to Spanner (crash during move)**
1. New version written to Spanner ✓
2. Registry updated: `PENDING_CLEANUP, cleanup_from=FIRESTORE` ✓
3. ← CRASH HERE →
4. On restart, `_finalize_pending_cleanup()` detects PENDING_CLEANUP state
5. Deletes old version from Firestore
6. Updates registry to COMPLETE

---

## 13. Validation & Reconciliation

### 13.1 V1 Validation (`scripts/validate.py`)

Two modes:

**Sampled mode** (default):
```bash
python scripts/validate.py --config migration.yaml --sample-size 1000
```
- Randomly samples N rows from Spanner
- Looks up corresponding documents in Cosmos
- Compares key columns and optionally value columns
- Fast but not exhaustive

**Checksums mode**:
```bash
python scripts/validate.py --config migration.yaml --reconciliation-mode checksums
```
- Iterates ALL rows from both source and target
- Computes SHA256 digest per row (using `validation_columns`)
- Stores digests in temporary SQLite database (avoids OOM)
- Reports: missing rows, extra rows, mismatched rows, aggregate checksums
- Exhaustive but slow

### 13.2 V2 Validation (`scripts/v2_validate.py`)

Validates that:
1. Every source record exists in the route registry
2. Every registry entry points to the correct destination (Firestore or Spanner)
3. The record exists in the target with matching checksum
4. No orphaned records in targets

---

## 14. Release Gates & Production Safety

### 14.1 The Stage Rehearsal Pattern

Before running in production, you must prove the migration works in stage:

```
1. Run migration in stage environment
2. Run stage-release-gate.yml GitHub workflow
   → Computes logical_fingerprint (hash of config + mappings)
   → Writes attestation record to release gate store
3. Run migration in prod environment
   → enforce_stage_rehearsal_or_raise() checks:
     a. Attestation exists for this scope
     b. Attestation is < 72 hours old
     c. Logical fingerprint matches (config hasn't changed)
   → If any check fails: ABORT
```

### 14.2 Logical Fingerprint

A SHA256 hash of the migration configuration that changes when:
- Column mappings change
- Source containers change
- Target tables change
- Shard configuration changes

Does NOT change when:
- Runtime parameters change (batch_size, retry settings)
- Environment-specific values change (endpoint URLs, project IDs)

This ensures that the exact migration logic tested in stage is what runs in prod.

---

## 15. Infrastructure as Code (Terraform)

### 15.1 Module Architecture

```
infra/terraform/
├── modules/
│   └── gcp_migration_platform/     # Reusable module
│       ├── main.tf                  # Service accounts, IAM, Secrets, GCS, Spanner
│       ├── variables.tf
│       └── outputs.tf
├── stacks/
│   ├── v1_sql_api/                  # V1-specific resources
│   │   ├── main.tf                  # Uses gcp_migration_platform module
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── v2_multiapi_router/         # V2-specific resources (adds Firestore)
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
└── envs/
    ├── dev/                         # Dev environment
    │   └── main.tf                  # Instantiates v1 + v2 stacks with dev vars
    ├── stage/
    │   └── main.tf
    └── prod/
        └── main.tf
```

### 15.2 Resources Created

- **Service Account**: Dedicated SA for migration with least-privilege IAM
- **Spanner Instance + Database**: Migration target (+ control plane table)
- **GCS Bucket**: State file storage
- **Secret Manager Secrets**: Cosmos keys, connection strings
- **Firestore Database**: V2 document routing target (V2 stack only)

### 15.3 Deploying Infrastructure

```bash
cd infra/terraform/envs/dev
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

---

## 16. CI/CD Pipeline

### 16.1 `ci.yml` — Runs on every push/PR

| Job | What It Does |
|---|---|
| `test` | `pytest -q -m "not integration"` — runs all unit tests |
| `quality` | `ruff check .` + `mypy migration migration_v2 scripts` |
| `security` | `bandit -r migration migration_v2 scripts` + `pip_audit -r requirements.txt` |
| `terraform` | `terraform fmt -check` + `terraform validate` for all stacks/envs |

### 16.2 `stage-release-gate.yml` — Manual trigger

- Triggered manually via GitHub UI
- Inputs: `gate_scope`, `run_v1` (bool), `run_v2` (bool)
- Writes stage attestation for release gate checking

---

## 17. Local Development Setup

### 17.1 Prerequisites

- Python 3.11+
- Azure Cosmos DB account (or emulator)
- Google Cloud SDK with Spanner/Firestore access
- Terraform 1.7+ (for infra changes)

### 17.2 Setup Steps

```bash
# Clone the repo
git clone <repo-url>
cd cosmos-to-spanner-migration

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Set environment variables
export COSMOS_KEY="your-cosmos-key"
export GOOGLE_CLOUD_PROJECT="your-gcp-project"
# For V2 MongoDB:
export COSMOS_MONGO_CONN="mongodb://..."
# For V2 Cassandra:
export CASS_USER="..."
export CASS_PASS="..."

# Run tests
pytest -q -m "not integration"

# Run linting
ruff check .
mypy migration migration_v2 scripts
```

### 17.3 Running Against Local State

For development, use local JSON state files:

```yaml
runtime:
  watermark_state_file: "state/watermarks.json"
  reader_cursor_state_file: "state/cursors.json"
  # Leave lease_file and progress_file empty for single-worker
```

---

## 18. Running the Migration

### 18.1 Pre-Migration Checklist

```bash
# 1. Run preflight checks
python scripts/preflight.py --config config/migration.yaml --check-source
# Validates: Spanner tables exist, columns match, Cosmos accessible

# 2. Dry run
python scripts/backfill.py --config config/migration.yaml --dry-run
# Reads and transforms documents without writing to Spanner

# 3. Small test run
python scripts/backfill.py --config config/migration.yaml --container users
# Migrate only the "users" container first
```

### 18.2 Full Migration Workflow

```bash
# Phase 1: Full backfill
python scripts/backfill.py --config config/migration.yaml

# Phase 2: Validate
python scripts/validate.py --config config/migration.yaml --reconciliation-mode checksums

# Phase 3: Start incremental sync (run repeatedly or via cron)
python scripts/backfill.py --config config/migration.yaml --incremental

# Phase 4: Final validation before cutover
python scripts/validate.py --config config/migration.yaml --reconciliation-mode checksums --compare-values
```

### 18.3 Multi-Worker Execution

```bash
# Worker A
python scripts/backfill.py --config config/migration.yaml \
  --worker-id worker-a

# Worker B (different machine or pod)
python scripts/backfill.py --config config/migration.yaml \
  --worker-id worker-b
```

With `lease_file` configured, workers automatically distribute shards.

---

## 19. Troubleshooting Guide

### 19.1 Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| "Missing required config key: endpoint" | YAML indentation or missing field | Check config against example |
| "Cosmos source key is missing" | `COSMOS_KEY` env var not set | `export COSMOS_KEY=...` |
| "table does not exist" on preflight | Spanner DDL not applied | Run DDL scripts first |
| Watermark not advancing | Failures occurred during run | Check DLQ file, fix errors, re-run |
| "Lost distributed lease" | Worker took too long on a batch | Increase `lease_duration_seconds` or reduce `batch_size` |
| "Stage rehearsal required" | Prod run without stage gate | Run migration in stage first, then run release gate |
| Zero documents processed | Wrong container name or empty query | Verify `source_container` and `source_query` |
| Duplicate key errors | Using `mode: insert` with existing data | Switch to `mode: upsert` |

### 19.2 Examining State

```bash
# View watermarks (local file)
cat state/watermarks.json | python -m json.tool

# View distributed state (Spanner)
python scripts/control_plane_status.py --config config/migration.yaml

# View dead letter queue
cat state/dead_letter.jsonl | head -5
```

### 19.3 Recovering from a Failed Run

```bash
# 1. Check what failed
cat state/dead_letter.jsonl | python -m json.tool | grep error_type

# 2. Fix the root cause (schema mismatch, permission issue, etc.)

# 3. Re-run (watermarks ensure it picks up where it left off)
python scripts/backfill.py --config config/migration.yaml --incremental

# 4. Or force a fresh start from a specific timestamp
python scripts/backfill.py --config config/migration.yaml --incremental --since-ts 1700000000
```

---

## 20. Code Walkthrough: Processing a Single Document

Let's trace a single document through the V1 pipeline:

### Input Document (Cosmos DB)

```json
{
    "id": "order-456",
    "customerId": "cust-123",
    "items": [{"sku": "WIDGET", "qty": 3}],
    "total": 29.97,
    "_ts": 1710300000,
    "isDeleted": false
}
```

### Step 1: Configuration

```yaml
mappings:
  - source_container: "orders"
    target_table: "Orders"
    key_columns: ["order_id"]
    mode: "upsert"
    columns:
      - target: "order_id"
        source: "id"
        converter: "string"
        required: true
      - target: "customer_id"
        source: "customerId"
        converter: "string"
      - target: "order_total"
        source: "total"
        converter: "float"
      - target: "items_json"
        source: "items"
        converter: "json_string"
    static_columns:
      migrated_at: "__NOW_UTC__"
    delete_rule:
      field: "isDeleted"
      equals: true
```

### Step 2: Read (cosmos_reader.py)

```python
# CosmosReader.iter_documents() yields:
document = {
    "id": "order-456",
    "customerId": "cust-123",
    "items": [{"sku": "WIDGET", "qty": 3}],
    "total": 29.97,
    "_ts": 1710300000,
    "isDeleted": False
}
```

### Step 3: Shard Check (backfill.py:136-147)

```python
# If shard_mode="client_hash", shard_count=4, shard_key_source="id":
shard = stable_shard_for_text("order-456", 4)  # → e.g., 2
# If this worker owns shard 2 → process. Otherwise → skip.
```

### Step 4: Transform (transform.py)

```python
result = transform_document(document, mapping)
# Checks delete_rule: document["isDeleted"] == False → not a delete

# For each column rule:
#   "order_id":   extract "id"          → "order-456"     (string)
#   "customer_id": extract "customerId" → "cust-123"      (string)
#   "order_total": extract "total"      → 29.97           (float)
#   "items_json":  extract "items"      → '[{"sku":"WIDGET","qty":3}]' (json_string)

# Static columns:
#   "migrated_at": "__NOW_UTC__" → datetime(2026, 3, 13, ...)

# Result:
TransformOutput(
    row={
        "order_id": "order-456",
        "customer_id": "cust-123",
        "order_total": 29.97,
        "items_json": '[{"sku":"WIDGET","qty":3}]',
        "migrated_at": datetime(2026, 3, 13, 10, 30, 0, tzinfo=UTC)
    },
    is_delete=False,
    source_ts=1710300000
)
```

### Step 5: Buffer & Batch Write (backfill.py:378-392)

```python
upsert_buffer.append((result, resume_state.clone()))

# When buffer reaches batch_size (500):
writer.write_rows("Orders", [row1, row2, ..., row500], "upsert")
# → Spanner insert_or_update mutation committed as a batch
```

### Step 6: Checkpoint (backfill.py:420-434)

```python
# After stream completes with no errors:
watermarks.set("orders", 1710300000)  # Advance to max _ts seen
watermarks.flush()                     # Persist to state file
clear_reader_cursor()                  # No longer needed
```

---

## 21. Key Design Decisions & Trade-offs

### 21.1 Why At-Least-Once (not Exactly-Once)?

Exactly-once would require wrapping every Spanner write + state update in a single transaction. This is possible with the Spanner control-plane backend but would:
- Require the state store to be in the same Spanner database as the target
- Reduce throughput (larger transactions)
- Not work with GCS or local JSON backends

The toolkit uses **at-least-once with idempotent upserts** as the default mode. For `insert` mode, duplicates can cause errors — use `upsert` for retry safety.

### 21.2 Why SQLite for Reconciliation?

Full-dataset reconciliation requires comparing millions of row digests. Loading them all into memory causes OOM. SQLite provides:
- Disk-backed storage (unlimited dataset size)
- Indexed lookups (fast comparison)
- Automatic cleanup (temp file deleted on close)

### 21.3 Why Separate V1 and V2?

The Cosmos SQL API and MongoDB/Cassandra APIs have fundamentally different:
- Query languages (SQL vs MongoDB query vs CQL)
- Pagination mechanisms (continuation tokens vs cursors)
- Document structures (SQL API has `_ts`, MongoDB has `_id`)

Trying to unify them would create a leaky abstraction. Separate pipelines with shared infrastructure is cleaner.

### 21.4 Why Route Registry for V2?

Without the registry, a document that grows past 1 MiB would exist in BOTH Firestore and Spanner after re-routing. The registry:
- Tracks the current destination for each document
- Enables two-phase moves (write new → delete old)
- Survives crashes via `PENDING_CLEANUP` state

---

## 22. Glossary

| Term | Definition |
|---|---|
| **Backfill** | Full migration of all existing data (vs incremental) |
| **Watermark** | Timestamp marking the last successfully processed point in the source stream |
| **Shard** | A partition of the data assigned to a specific worker |
| **Lease** | A time-limited lock that grants a worker exclusive access to a shard |
| **Heartbeat** | Periodic lease renewal to prove the worker is still alive |
| **Dead Letter Queue (DLQ)** | File where failed records are written for manual review |
| **Reader Cursor** | Saved position in the Cosmos change stream for crash recovery |
| **Route Registry** | V2 state tracking which target (Firestore/Spanner) holds each document |
| **Release Gate** | Attestation that a stage rehearsal was successful, required before prod |
| **Logical Fingerprint** | Hash of the migration config that must match between stage and prod |
| **Control Plane** | Spanner table used for distributed state (leases, watermarks, progress) |
| **Continuation Token** | Cosmos DB pagination cursor for resuming reads |
| **Canonical Record** | V2's normalized representation of a source document (API-agnostic) |
| **Preflight** | Pre-migration checks that validate connectivity and schema compatibility |
| **Reconciliation** | Post-migration comparison of source and target data integrity |

---

*This document covers the complete codebase as of 2026-03-13. For the latest changes, check `git log --oneline -20`.*

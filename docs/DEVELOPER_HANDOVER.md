# Developer Handover Document — Cosmos-to-Spanner Migration Toolkit

> **Audience**: A new developer joining this project with zero prior context.
> **Goal**: Get productive within your first week.
> **Last updated**: 2026-03-13

---

## Table of Contents

1. [Project Purpose & Business Context](#1-project-purpose--business-context)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Tech Stack & Dependencies](#3-tech-stack--dependencies)
4. [Directory Structure & Module Responsibilities](#4-directory-structure--module-responsibilities)
5. [Configuration System](#5-configuration-system)
6. [Local Development Environment Setup](#6-local-development-environment-setup)
7. [Running, Testing & Debugging](#7-running-testing--debugging)
8. [CI/CD Pipeline](#8-cicd-pipeline)
9. [Infrastructure & Deployment (Terraform)](#9-infrastructure--deployment-terraform)
10. [Key Design Decisions & Trade-offs](#10-key-design-decisions--trade-offs)
11. [Common Workflows & Operational Runbook](#11-common-workflows--operational-runbook)
12. [Known Limitations & Gotchas](#12-known-limitations--gotchas)
13. [Suggested Onboarding Reading Order](#13-suggested-onboarding-reading-order)
14. [Glossary](#14-glossary)

---

## 1. Project Purpose & Business Context

### What is this project?

This is a **production-grade migration toolkit** that moves data from **Azure Cosmos DB** to **Google Cloud** data stores (primarily **Cloud Spanner**, with **Firestore** as a secondary target for smaller payloads).

### Why does it exist?

Organizations migrating from Azure to GCP need a reliable, repeatable, and auditable way to move data with:
- Zero data loss guarantees
- Minimal downtime during cutover
- Incremental catch-up capability (not just one-time bulk copy)
- Validation/reconciliation to prove correctness

### The Two Pipelines

The project contains **two independent migration pipelines** that share no runtime code:

| Aspect | v1 Pipeline | v2 Pipeline |
|--------|-------------|-------------|
| **Source** | Cosmos DB SQL API | Cosmos DB MongoDB API / Cassandra API |
| **Target** | Cloud Spanner (only) | Firestore (< 1 MiB) OR Spanner (1–8 MiB) |
| **Status** | Stable, production-hardened | Evolving, newer addition |
| **Routing** | Direct (1:1 mapping) | Size-based dynamic routing |
| **Config** | `config/migration.yaml` | `config/v2.multiapi-routing.yaml` |
| **Code** | `migration/` | `migration_v2/` |
| **Scripts** | `scripts/backfill.py`, `preflight.py`, `validate.py` | `scripts/v2_route_migrate.py`, `v2_preflight.py` |

### Business-Critical Concepts

- **RPO (Recovery Point Objective)**: How much data loss is acceptable. The incremental watermark sync reduces RPO to the interval between sync runs.
- **RTO (Recovery Time Objective)**: How quickly you can switch over. This toolkit supports both dual-write and freeze-window cutover strategies.
- **Idempotent writes**: All writes use upsert mode by default — you can safely re-run any step without creating duplicates.

---

## 2. Architecture & Data Flow

### v1 Pipeline: SQL API → Spanner

```
┌─────────────────────┐
│  Azure Cosmos DB    │
│  (SQL API)          │
└────────┬────────────┘
         │  Cross-partition query with pagination
         │  Retry with exponential backoff + jitter
         ▼
┌─────────────────────┐
│  CosmosReader       │  migration/cosmos_reader.py
│  iter_documents()   │  Yields documents page-by-page
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  transform_document │  migration/transform.py
│  - Field mapping    │  Maps source paths → target columns
│  - Type conversion  │  e.g., epoch → datetime, nested → JSON string
│  - Static columns   │  e.g., __NOW_UTC__ for audit timestamps
│  - Delete detection │  Tombstone field matching
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  SpannerWriter      │  migration/spanner_writer.py
│  - Batch upsert     │  Configurable batch_size (default: 500)
│  - Mode: upsert /   │  insert / update / replace
│    insert / update   │
│  - Delete execution │  When delete_rule matches
└────────┬────────────┘
         │
         ├── On success ──► Advance watermark checkpoint
         │                   (state_store.py → local JSON or gs://)
         │
         └── On error ────► error_mode=skip: log to Dead Letter Queue
                             error_mode=fail: raise immediately
```

### v2 Pipeline: MongoDB/Cassandra → Dynamic Routing

```
┌──────────────────────────────┐
│  Cosmos DB                   │
│  (MongoDB API or Cassandra)  │
└────────────┬─────────────────┘
             │  MongoCosmosSourceAdapter or CassandraCosmosSourceAdapter
             │  Yields CanonicalRecord objects
             ▼
┌──────────────────────────────┐
│  SizeRouter.decide()         │  migration_v2/router.py
│                              │
│  payload_bytes < 1 MiB       │──► Route to Firestore
│  1 MiB ≤ payload < 8 MiB    │──► Route to Spanner
│  payload ≥ 8 MiB            │──► REJECT (dead-letter)
└────────────┬─────────────────┘
             │
     ┌───────┴────────┐
     ▼                ▼
┌──────────┐   ┌──────────────┐
│ Firestore│   │ Spanner Sink │
│ Sink     │   │              │
└──────────┘   └──────────────┘
             │
             ▼
┌──────────────────────────────┐
│  Route Registry              │  Tracks which sink holds each doc
│  (detects route moves)       │  If doc size changes → write to new
│                              │  sink, delete from old, update registry
└──────────────────────────────┘
```

### State Management Architecture

```
                   ┌─────────────────────┐
                   │  State Backend      │
                   │                     │
  Development:     │  Local JSON file    │  state/watermarks.json
                   │  + file locking     │
                   │                     │
  Production:      │  GCS object         │  gs://bucket/watermarks.json
                   │  + atomic merge     │
                   └─────────────────────┘

  Watermark flow:
  1. Read last timestamp from state
  2. Query source for docs with _ts > last_timestamp - overlap_window
  3. Process documents
  4. On success: write new watermark = max(_ts) of processed batch
  5. On resume: replay from last watermark (overlap ensures no gaps)
```

---

## 3. Tech Stack & Dependencies

### Language & Runtime

- **Python 3.11+** (required — uses dataclasses, type hints, modern syntax)
- No web framework — this is a CLI-based batch tool

### Runtime Dependencies (`requirements.txt`)

| Package | Version | Purpose |
|---------|---------|---------|
| `azure-cosmos` | ≥ 4.6.0 | Cosmos DB SQL API client |
| `google-cloud-spanner` | ≥ 3.46.0 | Cloud Spanner client |
| `google-cloud-firestore` | ≥ 2.16.0 | Firestore client (v2 only) |
| `google-cloud-storage` | ≥ 2.16.0 | GCS state backend |
| `PyYAML` | ≥ 6.0.1 | YAML config parsing |
| `pymongo` | ≥ 4.7.0 | Cosmos MongoDB API (v2, lazy import) |
| `cassandra-driver` | ≥ 3.29.0 | Cosmos Cassandra API (v2, lazy import) |
| `pytest` | ≥ 8.0.0 | Test framework |

### Dev/QA Dependencies (`requirements-dev.txt`)

| Package | Purpose |
|---------|---------|
| `ruff` ≥ 0.4.8 | Linter + formatter (replaces flake8/black/isort) |
| `mypy` ≥ 1.10.0 | Static type checker |
| `bandit` ≥ 1.7.8 | Security linter (finds common vulnerabilities) |
| `types-PyYAML` | Type stubs for PyYAML |
| `pip-audit` | Dependency vulnerability scanner |

### Infrastructure Tools

| Tool | Version | Purpose |
|------|---------|---------|
| Terraform | ≥ 1.7.5 | GCP resource provisioning |
| GitHub Actions | — | CI/CD pipeline |

---

## 4. Directory Structure & Module Responsibilities

```
cosmos-to-spanner-migration/
│
├── config/                              # ── CONFIGURATION TEMPLATES ──
│   ├── migration.example.yaml           #   v1 config template (copy & edit)
│   └── v2.multiapi-routing.example.yaml #   v2 config template (copy & edit)
│
├── scripts/                             # ── ENTRY POINTS (run these) ──
│   ├── preflight.py                     #   v1: validates source/target readiness
│   ├── backfill.py                      #   v1: runs full backfill or incremental sync
│   ├── validate.py                      #   v1: post-migration validation & reconciliation
│   ├── v2_preflight.py                  #   v2: validates Mongo/Cassandra source + targets
│   ├── v2_route_migrate.py              #   v2: runs size-routed migration
│   └── bootstrap_env.ps1               #   Terraform init/plan/apply helper (PowerShell)
│
├── migration/                           # ── V1 CORE LIBRARY ──
│   ├── __init__.py
│   ├── config.py                        #   Dataclasses: CosmosConfig, SpannerConfig,
│   │                                    #     RuntimeConfig, TableMapping, ColumnRule
│   │                                    #   parse_config() loads & validates YAML
│   ├── cosmos_reader.py                 #   CosmosReader: paginated iteration, retry, count
│   ├── spanner_writer.py               #   SpannerWriter: batch upsert/insert/update/replace/delete
│   ├── transform.py                     #   transform_document(): field mapping + type converters
│   │                                    #     Converters: str, int, float, bool, json_dumps,
│   │                                    #     epoch_to_datetime, epoch_to_date, iso_to_datetime
│   ├── state_store.py                   #   WatermarkStateStore: read/write watermark checkpoints
│   │                                    #     Supports local file or gs:// paths
│   ├── json_state_backend.py            #   JSONStateBackend: atomic JSON read/write with locking
│   ├── file_lock.py                     #   FileLock: OS-level file locking for state safety
│   ├── retry_utils.py                   #   retry_with_backoff(): exponential backoff + jitter
│   ├── dead_letter.py                   #   DeadLetterLogger: JSONL file for failed records
│   ├── reconciliation.py               #   ReconciliationRunner: sampled or full checksum validation
│   └── logging_utils.py                 #   setup_logging(): configures log format and level
│
├── migration_v2/                        # ── V2 CORE LIBRARY ──
│   ├── __init__.py
│   ├── config.py                        #   Dataclasses: PipelineV2Config, MongoJobConfig,
│   │                                    #     CassandraJobConfig, RoutingConfig, RuntimeV2Config
│   ├── models.py                        #   CanonicalRecord: unified document representation
│   ├── router.py                        #   SizeRouter: payload size → firestore|spanner|reject
│   ├── pipeline.py                      #   V2MigrationPipeline: full orchestration loop
│   │                                    #     Handles: iteration, routing, writing, watermarks,
│   │                                    #     route moves, dead-letter, cleanup
│   ├── state_store.py                   #   WatermarkStore + RouteRegistryStore (JSON or gs://)
│   ├── utils.py                         #   nested_get, json_size_bytes, payload_checksum,
│   │                                    #     build_source_key, to_jsonable
│   ├── source_adapters/                 #   Source-specific iteration adapters
│   │   ├── base.py                      #     SourceAdapter ABC: iter_records(), count(), close()
│   │   ├── mongo_cosmos.py              #     MongoCosmosSourceAdapter (pymongo)
│   │   └── cassandra_cosmos.py          #     CassandraCosmosSourceAdapter (cassandra-driver)
│   └── sink_adapters/                   #   Target-specific write adapters
│       ├── base.py                      #     SinkAdapter ABC: write(), delete(), close()
│       ├── firestore_sink.py            #     FirestoreSinkAdapter
│       └── spanner_sink.py              #     SpannerSinkAdapter
│
├── tests/                               # ── V1 UNIT TESTS ──
│   ├── test_config.py                   #   Config parsing, validation, edge cases
│   ├── test_transform.py               #   Field mapping, type converters, static columns
│   ├── test_reconciliation.py           #   Validation logic
│   ├── test_retry_utils.py              #   Backoff calculation, jitter
│   ├── test_state_store.py              #   Watermark persistence round-trips
│   └── test_backfill_incremental.py     #   Incremental sync logic
│
├── tests_v2/                            # ── V2 UNIT TESTS ──
│   ├── test_v2_config.py               #   v2 config parsing
│   ├── test_router.py                   #   Routing decisions (size thresholds)
│   ├── test_v2_state_store.py           #   v2 watermark + route registry
│   └── test_pipeline_logic.py           #   End-to-end pipeline with mocks
│
├── tests_integration/                   # ── LIVE CLOUD TESTS (opt-in) ──
│   ├── test_live_preflight.py           #   Smoke test against real cloud resources
│   └── README.md                        #   Setup instructions
│
├── infra/terraform/                     # ── INFRASTRUCTURE AS CODE ──
│   ├── modules/gcp_migration_platform/  #   Reusable module: APIs, IAM, Spanner, Firestore, GCS
│   ├── stacks/v1_sql_api/               #   Stack root: v1 infrastructure
│   ├── stacks/v2_multiapi_router/       #   Stack root: v2 infrastructure
│   ├── envs/{dev,stage,prod}/           #   Environment wrappers (deploy both stacks)
│   └── README.md
│
├── docs/                                # ── DOCUMENTATION ──
│   ├── 00_START_HERE.md → 10_INTEGRATION_TESTING.md  # Numbered onboarding sequence
│   ├── ARCHITECTURE.md                  #   Deep-dive architecture
│   ├── CONFIG_REFERENCE.md              #   Exhaustive parameter reference
│   ├── RUNBOOK.md                       #   Operational runbook
│   ├── TROUBLESHOOTING.md               #   Common issues and fixes
│   ├── GO_LIVE_CHECKLIST.md             #   Pre-cutover checklist
│   ├── SENIOR_REVIEW.md                 #   Code review guidelines
│   └── V2_MULTIAPI_ROUTING.md           #   v2 routing deep-dive
│
├── .github/workflows/ci.yml            # ── CI/CD ──
├── pyproject.toml                       # Ruff + MyPy config
├── mypy.ini                             # Type checking policy
├── pytest.ini                           # Test runner config
├── requirements.txt                     # Runtime deps
├── requirements-dev.txt                 # Dev/QA deps
├── .gitignore                           # Excludes state/, configs, .terraform/
└── README.md                            # Project overview + quick start
```

---

## 5. Configuration System

### How Configuration Works

All migration behavior is driven by **YAML configuration files** — no behavior is hardcoded. The templates live in `config/` and are `.gitignore`'d once copied (because they contain environment-specific values).

### v1 Configuration (`config/migration.yaml`)

The config is parsed by `migration/config.py` into typed dataclasses:

```yaml
# ── Source (Azure Cosmos DB) ──
cosmos:
  endpoint: "https://<account>.documents.azure.com:443/"
  key_env: "COSMOS_KEY"          # reads from $COSMOS_KEY env var
  database: "mydb"

# ── Target (Google Cloud Spanner) ──
spanner:
  project: "my-gcp-project"
  instance: "my-instance"
  database: "my-database"

# ── Runtime Behavior ──
runtime:
  batch_size: 500                # rows per Spanner commit
  query_page_size: 200           # docs per Cosmos query page
  dry_run: false                 # true = read+transform only, no writes
  watermark_state_file: "state/watermarks.json"  # or gs://bucket/path
  watermark_overlap_seconds: 5   # replay overlap to catch boundary events
  error_mode: "fail"             # "fail" | "skip" (skip → DLQ)
  dlq_file_path: "state/dead_letters.jsonl"
  log_level: "INFO"
  # Retry policy
  retry_attempts: 5
  retry_initial_delay_seconds: 1.0
  retry_max_delay_seconds: 30.0
  retry_backoff_multiplier: 2.0
  retry_jitter_seconds: 0.5

# ── Table Mappings (one per source container) ──
mappings:
  - source_container: "users"
    target_table: "Users"
    key_columns: ["user_id"]
    mode: "upsert"               # upsert | insert | update | replace
    columns:
      - source: "id"
        target: "user_id"
        converter: "str"
      - source: "profile.email"   # nested field access via dot notation
        target: "email"
        converter: "str"
      - source: "_ts"
        target: "updated_at"
        converter: "epoch_to_datetime"
    static_columns:
      migrated_at: "__NOW_UTC__"   # auto-filled with current UTC timestamp
    delete_rule:
      field: "status"
      equals: "deleted"            # docs matching this → DELETE in Spanner
    incremental_query: "SELECT * FROM c WHERE c._ts > @last_ts"
```

### v2 Configuration (`config/v2.multiapi-routing.yaml`)

```yaml
routing:
  firestore_lt_bytes: 1048576        # < 1 MiB → Firestore
  spanner_max_payload_bytes: 8388608 # cap at 8 MiB
  payload_size_overhead_bytes: 2048  # encoding overhead estimate

firestore:
  project: "my-gcp-project"
  database: "(default)"
  collection_prefix: "migrated_"

spanner:
  project: "my-gcp-project"
  instance: "my-instance"
  database: "my-database"
  table: "large_docs"

runtime:
  state_file: "state/v2_watermarks.json"       # or gs://
  route_registry_file: "state/v2_route_registry.json"
  error_mode: "skip"
  batch_size: 100

jobs:
  - name: "mongo-users"
    api: "mongodb"
    connection_string_env: "MONGO_CONN"
    database: "mydb"
    collection: "users"
    key_fields: ["_id"]
    incremental_field: "_ts"
    route_namespace: "mongo_users"

  - name: "cassandra-events"
    api: "cassandra"
    contact_points: ["cosmos-cass.cassandra.cosmos.azure.com"]
    port: 10350
    username_env: "CASS_USER"
    password_env: "CASS_PASS"
    keyspace: "events_ks"
    table: "events"
    key_fields: ["event_id"]
    incremental_field: "updated_at"
    route_namespace: "cass_events"
```

### Config Loading Flow

```
YAML file
  → yaml.safe_load()
  → parse_config() / parse_v2_config()
  → Validation (required fields, identifier regex, env var resolution)
  → Typed dataclass instances (CosmosConfig, SpannerConfig, etc.)
  → Passed to pipeline components
```

### Key Config Conventions

1. **Secrets via env vars**: Any `*_env` field reads from an environment variable (e.g., `key_env: "COSMOS_KEY"` reads `$COSMOS_KEY`).
2. **State paths**: Local paths like `state/foo.json` for dev, `gs://bucket/foo.json` for production.
3. **Identifier validation**: Table names, column names are regex-validated at config load time to prevent injection.
4. **Dot notation**: Source field paths support dots for nested access (e.g., `profile.email`).

---

## 6. Local Development Environment Setup

### Prerequisites

- Python 3.11 or later
- Git
- (Optional) Terraform 1.7.5+ if working on infrastructure
- (Optional) Azure Cosmos DB Emulator or live Cosmos DB account for integration testing
- (Optional) GCP project with Spanner/Firestore for integration testing

### Step-by-Step Setup

```powershell
# 1. Clone the repository
git clone <repo-url> C:\Users\ados1\cosmos-to-spanner-migration
cd C:\Users\ados1\cosmos-to-spanner-migration

# 2. Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1    # PowerShell
# or: source .venv/bin/activate  # bash/Linux

# 3. Install all dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
python -m pip install pip-audit

# 4. Verify installation
python -m pytest -q                    # run all unit tests
ruff check .                           # lint check
mypy migration migration_v2 scripts    # type check

# 5. Copy and edit config templates
Copy-Item config\migration.example.yaml config\migration.yaml
Copy-Item config\v2.multiapi-routing.example.yaml config\v2.multiapi-routing.yaml
# Edit the copied files with your environment-specific values

# 6. Set required environment variables
$env:COSMOS_KEY = "<your-cosmos-primary-key>"
# For v2 MongoDB:
$env:MONGO_CONN = "<your-mongodb-connection-string>"
# For v2 Cassandra:
$env:CASS_USER = "<cassandra-username>"
$env:CASS_PASS = "<cassandra-password>"
```

### IDE Setup (VS Code Recommended)

```json
// .vscode/settings.json (create if needed)
{
  "python.defaultInterpreterPath": ".venv/Scripts/python.exe",
  "python.linting.enabled": true,
  "python.analysis.typeCheckingMode": "basic",
  "[python]": {
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "charliermarsh.ruff"
  }
}
```

### What NOT to commit

The `.gitignore` protects against accidental commits of:
- `config/migration.yaml` / `config/v2.multiapi-routing.yaml` (contain real endpoints)
- `state/` directory (runtime watermarks)
- `.env` files
- `*.tfstate`, `terraform.tfvars`, `backend.tf` (Terraform state/secrets)
- `.terraform/` directory

---

## 7. Running, Testing & Debugging

### Running Migrations

#### v1: Full Lifecycle

```powershell
# Step 1: Preflight — validates connectivity, schema, permissions
python scripts/preflight.py --config config/migration.yaml

# Step 2: Dry run — reads and transforms, but writes nothing
python scripts/backfill.py --config config/migration.yaml --dry-run

# Step 3: Full backfill — migrates all data
python scripts/backfill.py --config config/migration.yaml

# Step 4: Incremental sync — picks up changes since last run
python scripts/backfill.py --config config/migration.yaml --incremental

# Step 5: Validate — compare source vs target
python scripts/validate.py --config config/migration.yaml --sample-size 200
# For full checksum comparison:
python scripts/validate.py --config config/migration.yaml --reconciliation-mode checksums
```

#### v2: Full Lifecycle

```powershell
# Step 1: Preflight
python scripts/v2_preflight.py --config config/v2.multiapi-routing.yaml

# Step 2: Full migration
python scripts/v2_route_migrate.py --config config/v2.multiapi-routing.yaml

# Step 3: Incremental sync
python scripts/v2_route_migrate.py --config config/v2.multiapi-routing.yaml --incremental
```

### Running Tests

```powershell
# All unit tests (excludes integration tests)
python -m pytest -q -m "not integration"

# Specific test file
python -m pytest tests/test_transform.py -v

# Specific test
python -m pytest tests/test_transform.py::test_epoch_conversion -v

# With coverage (if installed)
python -m pytest --cov=migration --cov=migration_v2 -q

# Integration tests (requires live cloud credentials)
python -m pytest tests_integration/ -v -m integration
```

### Quality Checks (Same as CI)

```powershell
# Lint
ruff check .

# Type check
mypy migration migration_v2 scripts

# Security scan
bandit -q -r migration migration_v2 scripts -x tests,tests_v2

# Dependency vulnerabilities
python -m pip_audit -r requirements.txt
```

### Debugging Tips

1. **Increase log verbosity**: Set `runtime.log_level: "DEBUG"` in your config YAML.
2. **Dry run first**: Always use `--dry-run` before writing to a real Spanner instance.
3. **Check dead-letter file**: If `error_mode: "skip"`, failed records go to `dlq_file_path`. Each line is a JSON object with the record, error, and timestamp.
4. **Check watermark state**: Look at `state/watermarks.json` to see the last processed timestamp per container.
5. **Limit scope for testing**: Use `runtime.max_docs_per_container` to process only N documents.
6. **Transform debugging**: Run backfill with `--dry-run` and `DEBUG` logging to see exactly how each document is transformed.

---

## 8. CI/CD Pipeline

The CI pipeline is defined in `.github/workflows/ci.yml` and runs on every push to `main` and every pull request.

### Pipeline Jobs (all run in parallel)

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐
│   test   │  │ quality  │  │ security │  │ terraform │
│          │  │          │  │          │  │           │
│ pytest   │  │ ruff     │  │ bandit   │  │ fmt check │
│ (unit)   │  │ mypy     │  │ pip_audit│  │ validate  │
└──────────┘  └──────────┘  └──────────┘  └───────────┘
```

| Job | What it does | Failure means |
|-----|-------------|---------------|
| **test** | `pytest -q -m "not integration"` | Unit test failure — fix your code |
| **quality** | `ruff check .` + `mypy migration migration_v2 scripts` | Lint or type error — fix before merging |
| **security** | `bandit` scan + `pip_audit` | Security vulnerability — must fix |
| **terraform** | `terraform fmt -check` + `terraform validate` on all stacks/envs | IaC formatting or syntax error |

### All jobs use:
- **Python 3.11** on `ubuntu-latest`
- **Terraform 1.7.5** (for the terraform job)
- Both `requirements.txt` and `requirements-dev.txt` are installed

### Adding a New CI Check

Edit `.github/workflows/ci.yml`. Each job is independent, so add a new job block if the check is orthogonal to existing ones.

---

## 9. Infrastructure & Deployment (Terraform)

### Architecture

```
infra/terraform/
├── modules/
│   └── gcp_migration_platform/     # Reusable module — provisions:
│       ├── APIs (Spanner, Firestore, Storage, Secret Manager)
│       ├── Service account + IAM bindings
│       ├── Secret Manager secrets (for Cosmos key, etc.)
│       ├── Spanner instance + database (optional)
│       ├── Firestore database (optional)
│       └── GCS bucket for state files (optional)
│
├── stacks/
│   ├── v1_sql_api/                  # Calls module with v1-specific inputs
│   └── v2_multiapi_router/         # Calls module with v2-specific inputs
│
└── envs/
    ├── dev/                         # Deploys both stacks for dev
    ├── stage/                       # Deploys both stacks for staging
    └── prod/                        # Deploys both stacks for production
```

### Deploying Infrastructure

```powershell
# Option A: Using the bootstrap helper script
.\scripts\bootstrap_env.ps1 -Environment dev -Action plan
.\scripts\bootstrap_env.ps1 -Environment dev -Action apply

# Option B: Manual Terraform
cd infra/terraform/envs/dev
Copy-Item backend.tf.example backend.tf
Copy-Item terraform.tfvars.example terraform.tfvars
# Edit backend.tf (GCS state backend) and terraform.tfvars (inputs)
terraform init
terraform plan
terraform apply
```

### Environment Promotion Flow

```
dev → stage → prod
 │       │       │
 └───────┴───────┘
 Same module, different tfvars
```

Each environment uses the same Terraform module but with different variable values (project ID, instance size, naming prefix, etc.).

---

## 10. Key Design Decisions & Trade-offs

### Decision 1: Two Separate Pipelines (v1 vs v2)

**Why**: Cosmos DB has fundamentally different APIs (SQL, MongoDB, Cassandra). Rather than building a complex abstraction layer, the team chose to keep v1 (SQL API) and v2 (MongoDB/Cassandra) completely separate.

**Trade-off**: Some code duplication (config parsing, state management) but each pipeline can evolve independently and a bug in one cannot affect the other.

**When this matters to you**: If you're adding a feature, make sure you're adding it to the right pipeline. If it's needed in both, implement it twice (don't try to create a shared base class).

### Decision 2: YAML-Driven Configuration (No Hardcoding)

**Why**: Migration parameters change per environment and per table. YAML config makes it possible to run the same code against dev/stage/prod with different configs without code changes.

**Trade-off**: More complex config parsing logic, but zero code changes for new environments.

### Decision 3: Watermark-Based Incremental (Not CDC)

**Why**: Cosmos DB change feed has limitations (especially for cross-partition queries) and not all APIs support it equally. Timestamp-based watermarks are simpler, universally supported, and sufficient for most migration use cases.

**Trade-off**: Not truly real-time. There's a window between sync runs where data could be stale. The `overlap_seconds` parameter mitigates boundary misses but doesn't eliminate them.

**When this matters**: If you need sub-second replication, you'll need a different approach (e.g., change data capture with Cosmos change feed + event streaming).

### Decision 4: Idempotent Upsert as Default

**Why**: Migrations fail and need to be re-run. Upsert (insert-or-update) means you can safely re-run any step without creating duplicates or failing on "already exists" errors.

**Trade-off**: Slightly slower than pure INSERT (needs to check existence), but dramatically safer for operations.

### Decision 5: Size-Based Routing in v2

**Why**: Firestore has a 1 MiB document limit. Rather than rejecting large documents, the v2 pipeline routes them to Spanner (which handles up to 10 MiB per row). This maximizes Firestore's strengths (low latency, simple queries) while using Spanner for the heavy payloads.

**Trade-off**: Application code reading migrated data needs to know which store to query. The route registry makes this deterministic.

### Decision 6: Lazy Imports for Optional Dependencies

**Why**: `pymongo` and `cassandra-driver` are only needed for v2. Rather than requiring everyone to install them, they're imported only when v2 jobs need them.

**Trade-off**: Import errors surface at runtime instead of install time. Preflight checks catch this early.

---

## 11. Common Workflows & Operational Runbook

### Workflow: Adding a New Table Mapping (v1)

1. Add a new entry to `mappings[]` in your `config/migration.yaml`:
   ```yaml
   - source_container: "orders"
     target_table: "Orders"
     key_columns: ["order_id"]
     mode: "upsert"
     columns:
       - source: "id"
         target: "order_id"
         converter: "str"
       # ... more columns
   ```
2. Run preflight: `python scripts/preflight.py --config config/migration.yaml`
3. Dry-run: `python scripts/backfill.py --config config/migration.yaml --dry-run`
4. Execute: `python scripts/backfill.py --config config/migration.yaml`

### Workflow: Adding a New Source Adapter (v2)

1. Create a new adapter in `migration_v2/source_adapters/` implementing `SourceAdapter` ABC.
2. Must implement: `iter_records()` → yields `CanonicalRecord`, `count()`, `close()`.
3. Register it in the pipeline's job dispatch logic in `migration_v2/pipeline.py`.
4. Add corresponding config dataclass in `migration_v2/config.py`.
5. Add unit tests in `tests_v2/`.

### Workflow: Investigating a Failed Migration

1. **Check logs**: Look for ERROR/WARNING messages.
2. **Check dead-letter file**: `state/dead_letters.jsonl` — each line has the failed record and error.
3. **Check watermark**: `state/watermarks.json` — shows where the migration stopped.
4. **Re-run incrementally**: Fix the issue, then run `--incremental` to pick up from the last checkpoint.
5. **Validate**: Run `scripts/validate.py` to confirm data integrity.

### Workflow: Production Cutover

See `docs/GO_LIVE_CHECKLIST.md` for the complete checklist. High-level:

1. Run preflight checks ✓
2. Complete full backfill ✓
3. Run incremental sync until lag < RPO ✓
4. Run full validation (checksum mode) ✓
5. Freeze source writes (or start dual-write)
6. Run final incremental sync
7. Run final validation
8. Switch application traffic to Spanner
9. Monitor for 24–48 hours
10. Decommission Cosmos after acceptance window

### Workflow: Resetting State (Starting Over)

```powershell
# Delete local state files to re-run from scratch
Remove-Item state\watermarks.json -ErrorAction SilentlyContinue
Remove-Item state\v2_watermarks.json -ErrorAction SilentlyContinue
Remove-Item state\v2_route_registry.json -ErrorAction SilentlyContinue
Remove-Item state\dead_letters.jsonl -ErrorAction SilentlyContinue

# For GCS state: delete the objects in your bucket
# gsutil rm gs://your-bucket/watermarks.json
```

---

## 12. Known Limitations & Gotchas

### Architectural Limitations

| Limitation | Impact | Workaround |
|-----------|--------|------------|
| **Not real CDC** | Watermark-based sync has inherent lag | Run incremental more frequently; use overlap window |
| **No parallelism** | Single-threaded per container | Run multiple processes with different configs for different containers |
| **No schema migration** | Only migrates data, not DDL | Create Spanner schema manually before migration |
| **State file concurrency** | Local JSON state is not safe for concurrent writers | Use `gs://` state backend for multi-instance safety |

### Common Gotchas

1. **Forgetting `key_columns`**: Without `key_columns`, upsert mode won't work correctly. You'll get errors or unexpected behavior.

2. **Nested field access**: Use dot notation (`profile.email`), not bracket notation (`profile["email"]`). The `nested_get` utility handles the traversal.

3. **Timezone handling**: `epoch_to_datetime` converts Unix timestamps to UTC datetime. If your source uses a different timezone, you'll need a custom converter.

4. **GCS state requires auth**: If using `gs://` state paths, ensure your environment has GCP Application Default Credentials (`gcloud auth application-default login`).

5. **Lazy import failures**: If you see `ImportError: No module named 'pymongo'` when running v2, you need to install optional dependencies (`pip install pymongo` or `pip install cassandra-driver`).

6. **Config YAML in .gitignore**: Your actual config files (`config/migration.yaml`) are gitignored. Don't be confused when they don't show up in git status — this is intentional to protect secrets.

7. **Spanner identifier limits**: Table and column names are validated against `^[A-Za-z][A-Za-z0-9_]{0,127}$`. Names that don't match will fail at config load time, not at runtime.

8. **Dead-letter file grows**: In `error_mode: "skip"`, the DLQ file grows indefinitely. Monitor its size in long-running sync loops.

9. **Overlap window trade-off**: `watermark_overlap_seconds: 5` means each incremental run re-processes ~5 seconds of already-processed data. This is safe because of upsert mode but adds minor extra cost. Don't set this too high.

10. **v2 route moves**: If a document's size changes (e.g., grows from 500 KB to 2 MB), the v2 pipeline will: write to the new sink, delete from the old sink, update the route registry. This is eventually consistent — there's a brief window where the doc exists in both or neither.

---

## 13. Suggested Onboarding Reading Order

### Day 1: Orientation (2–3 hours)

1. **This document** — You're reading it now. Bookmark it.
2. `docs/00_START_HERE.md` — Quick orientation
3. `docs/01_REPO_OVERVIEW.md` — Why two pipelines, design principles
4. `docs/02_ARCHITECTURE_AND_DATA_FLOW.md` — Visual data flow

### Day 2: Hands-On Setup (2–3 hours)

5. **Set up your dev environment** (Section 6 above)
6. `docs/06_CONFIG_PARAMETERS_AND_SECRETS.md` — Understand every config parameter
7. `docs/03_V1_SQL_API_QUICKSTART.md` — Run v1 with a dry-run
8. Run the tests: `python -m pytest -q`

### Day 3: Deep Dive into Code (3–4 hours)

9. `docs/07_CODEBASE_STRUCTURE.md` — Module-by-module guide
10. Read these key source files in order:
    - `migration/config.py` — How config becomes typed objects
    - `migration/transform.py` — How documents are transformed
    - `migration/cosmos_reader.py` — How source data is read
    - `migration/spanner_writer.py` — How data is written
    - `scripts/backfill.py` — How it all ties together

### Day 4: v2 Pipeline + Infrastructure (2–3 hours)

11. `docs/04_V2_MULTIAPI_ROUTER_QUICKSTART.md`
12. `docs/V2_MULTIAPI_ROUTING.md` — Deep dive on routing
13. Read:
    - `migration_v2/router.py` — Size-based routing logic
    - `migration_v2/pipeline.py` — v2 orchestration
14. `docs/05_TERRAFORM_IAC_GUIDE.md` — Infrastructure provisioning

### Day 5: Operations & Production (2–3 hours)

15. `docs/08_OPERATIONS_AND_SRE.md` — How to run in production
16. `docs/09_PRODUCTION_READINESS_REVIEW.md` — Checklist before go-live
17. `docs/RUNBOOK.md` — Operational procedures
18. `docs/TROUBLESHOOTING.md` — Common issues and fixes
19. `docs/GO_LIVE_CHECKLIST.md` — Cutover procedure

### Ongoing Reference

- `docs/CONFIG_REFERENCE.md` — Look up any config parameter
- `docs/SENIOR_REVIEW.md` — Code review standards
- `docs/10_INTEGRATION_TESTING.md` — When you need live cloud tests

---

## 14. Glossary

| Term | Definition |
|------|-----------|
| **Backfill** | One-time bulk copy of all data from source to target |
| **Incremental sync** | Copying only changes since the last watermark |
| **Watermark** | A timestamp checkpoint (`_ts`) marking the last successfully processed point |
| **DLQ (Dead Letter Queue)** | A JSONL file where failed/skipped records are logged for later investigation |
| **Upsert** | Insert if new, update if exists — idempotent write operation |
| **Preflight** | Pre-migration checks: connectivity, schema existence, permissions |
| **Reconciliation** | Post-migration validation comparing source and target data |
| **Route registry** | v2 concept: tracks which sink (Firestore or Spanner) holds each document |
| **Route move** | v2 concept: moving a document from one sink to another when its size changes |
| **Canonical record** | v2 concept: a unified representation of a source document, regardless of API |
| **Overlap window** | Seconds of re-processing on each incremental run to avoid missing boundary events |
| **Tombstone** | A document with a marker field indicating it should be deleted in the target |
| **RPO** | Recovery Point Objective — maximum acceptable data loss (measured in time) |
| **RTO** | Recovery Time Objective — maximum acceptable downtime during cutover |
| **ADC** | Application Default Credentials — GCP's credential chain for local/cloud auth |

---

## Quick Reference Card

```
┌──────────────────────────────────────────────────────────────┐
│                    QUICK REFERENCE                           │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  v1 COMMANDS                                                 │
│  preflight:    python scripts/preflight.py --config <yaml>   │
│  dry-run:      python scripts/backfill.py --config <yaml>    │
│                  --dry-run                                    │
│  backfill:     python scripts/backfill.py --config <yaml>    │
│  incremental:  python scripts/backfill.py --config <yaml>    │
│                  --incremental                                │
│  validate:     python scripts/validate.py --config <yaml>    │
│                  --sample-size 200                            │
│                                                              │
│  v2 COMMANDS                                                 │
│  preflight:    python scripts/v2_preflight.py --config <yaml>│
│  migrate:      python scripts/v2_route_migrate.py            │
│                  --config <yaml>                              │
│  incremental:  python scripts/v2_route_migrate.py            │
│                  --config <yaml> --incremental                │
│                                                              │
│  QUALITY CHECKS                                              │
│  tests:        python -m pytest -q                           │
│  lint:         ruff check .                                  │
│  types:        mypy migration migration_v2 scripts           │
│  security:     bandit -q -r migration migration_v2 scripts   │
│  audit:        python -m pip_audit -r requirements.txt       │
│                                                              │
│  TERRAFORM                                                   │
│  bootstrap:    .\scripts\bootstrap_env.ps1 -Environment dev  │
│                  -Action plan                                 │
│                                                              │
│  KEY FILES                                                   │
│  v1 config:    config/migration.example.yaml                 │
│  v2 config:    config/v2.multiapi-routing.example.yaml       │
│  v1 entry:     scripts/backfill.py                           │
│  v2 entry:     scripts/v2_route_migrate.py                   │
│  state:        state/watermarks.json                         │
│  dead-letter:  state/dead_letters.jsonl                      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

*This document was generated on 2026-03-13 for developer handover purposes. For the most current information, always refer to the numbered docs in `docs/` and the source code itself.*

# Cosmos DB -> Cloud Spanner Migration Toolkit

This repository is a production-oriented migration toolkit for moving data from Azure Cosmos DB to Google Cloud data stores.

It supports:

1. full backfill
2. watermark-based incremental catch-up
3. distributed execution with leases and progress tracking
4. replay-safe restart behavior with persisted checkpoints and reader cursors
5. validation and reconciliation
6. stage release-gate enforcement
7. structured logging and metrics snapshots

## Pipelines

| Pipeline | Source | Target | Best for |
| --- | --- | --- | --- |
| `v1` | Cosmos SQL API | Cloud Spanner | mapping-driven SQL API migrations |
| `v2` | Cosmos MongoDB API / Cassandra API | Firestore or Cloud Spanner | routed document workloads with mixed payload sizes |

### v2 routing rules

1. payloads below `1 MiB` route to Firestore by default
2. larger payloads route to Spanner up to a safer configured cap
3. route ownership is tracked in a route registry so cross-sink moves are restart-safe

## Start Here

If you only want the shortest serious reading path:

1. [Executive Summary](docs/14_EXECUTIVE_SUMMARY.md)
2. [Operator Quick Reference](docs/15_OPERATOR_QUICK_REFERENCE.md)
3. [Architecture Reference](docs/ARCHITECTURE.md)
4. [Operations Runbook](docs/RUNBOOK.md)

If you want the grouped documentation map:

1. [Documentation Index](docs/16_DOCUMENTATION_INDEX.md)

If you are brand new to the repo:

1. [Start Here](docs/00_START_HERE.md)

## Choose Your Path

### Executive, approver, or reviewer

1. [Executive Summary](docs/14_EXECUTIVE_SUMMARY.md)
2. [Architecture Reference](docs/ARCHITECTURE.md)
3. [Operations Runbook](docs/RUNBOOK.md)
4. [Production Readiness Review](docs/09_PRODUCTION_READINESS_REVIEW.md)

### Operator or release engineer

1. [Operator Quick Reference](docs/15_OPERATOR_QUICK_REFERENCE.md)
2. [Operations Runbook](docs/RUNBOOK.md)
3. [Release Gate And Stage Rehearsal](docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md)
4. [Troubleshooting](docs/TROUBLESHOOTING.md)

### Maintainer or code reviewer

1. [Architecture Reference](docs/ARCHITECTURE.md)
2. [Detailed Architecture Diagram](docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md)
3. [Detailed Data Flow Diagram](docs/13_DETAILED_DATA_FLOW_DIAGRAM.md)
4. [Codebase Structure](docs/07_CODEBASE_STRUCTURE.md)
5. [Operations Runbook](docs/RUNBOOK.md)

## Canonical Source-of-Truth Docs

These are the primary references for the current system:

1. [Architecture Reference](docs/ARCHITECTURE.md)
2. [Operations Runbook](docs/RUNBOOK.md)
3. [Detailed Architecture Diagram](docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md)
4. [Detailed Data Flow Diagram](docs/13_DETAILED_DATA_FLOW_DIAGRAM.md)
5. [Config Reference](docs/CONFIG_REFERENCE.md)

## Quick Start

### 1. Install dependencies

```powershell
cd C:\Users\ados1\cosmos-to-spanner-migration
python -m pip install -r requirements.txt
```

### 2. Choose a config template

For `v1`:

```powershell
Copy-Item .\config\migration.example.yaml .\config\migration.yaml
```

For `v2`:

```powershell
Copy-Item .\config\v2.multiapi-routing.example.yaml .\config\v2.multiapi-routing.yaml
```

### 3. Run preflight

For `v1`:

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

For `v2`:

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

### 4. Run a dry-run first

For `v1`:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

For `v2`:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

For the full operating model, do not stop here. Read the [Operations Runbook](docs/RUNBOOK.md).

## Common Commands

### v1

Full backfill:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

Incremental:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

Validation:

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

### v2

Full run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

Incremental:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Exact validation:

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

### Shared operational commands

Inspect distributed progress:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>"
```

Write a stage release gate:

```powershell
python .\scripts\release_gate.py --gate-file "<path>" --scope "<scope>" --v1-config "<stage-v1-config>" --v2-config "<stage-v2-config>"
```

## Repository Layout

### Runtime and scripts

1. `migration/`: v1 runtime plus shared infrastructure modules
2. `migration_v2/`: v2 runtime, adapters, router, and reconciliation
3. `scripts/`: operational entrypoints

### Config and infrastructure

1. `config/`: example YAML configs
2. `infra/terraform/`: infrastructure as code
3. `infra/terraform/envs/`: environment wrappers for `dev`, `stage`, and `prod`

### Tests and quality

1. `tests/`: v1 and shared-runtime tests
2. `tests_v2/`: v2-specific tests
3. `tests_integration/`: opt-in live-cloud or integration scenarios
4. `.github/workflows/ci.yml`: CI workflow
5. `.github/workflows/stage-release-gate.yml`: release-gate workflow

### Documentation

1. `docs/`: numbered guides, canonical references, diagrams, and review artifacts

## Quality Gates

The repository uses these local and CI checks:

```powershell
python -m pytest -q
ruff check .
python -m mypy migration migration_v2 scripts
bandit -q -r migration migration_v2 scripts -x tests,tests_v2
python -m pip_audit -r requirements.txt
```

## Production Notes

For serious stage or production campaigns:

1. use shared durable state for checkpoints, leases, and progress
2. prefer Spanner-backed control-plane state for distributed runs
3. treat validation as a required release artifact
4. use release-gate enforcement for production where appropriate
5. keep rollback ownership explicit before the run starts

Do not treat the README as the full operating manual. Use the [Operations Runbook](docs/RUNBOOK.md) for real migration work.

## Documentation Paths

### Architecture and design

1. [Architecture Reference](docs/ARCHITECTURE.md)
2. [Detailed Architecture Diagram](docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md)
3. [Detailed Data Flow Diagram](docs/13_DETAILED_DATA_FLOW_DIAGRAM.md)
4. [Codebase Structure](docs/07_CODEBASE_STRUCTURE.md)

### Operations and cutover

1. [Operations Runbook](docs/RUNBOOK.md)
2. [Operator Quick Reference](docs/15_OPERATOR_QUICK_REFERENCE.md)
3. [Operations and SRE Guidance](docs/08_OPERATIONS_AND_SRE.md)
4. [Release Gate And Stage Rehearsal](docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md)
5. [Troubleshooting](docs/TROUBLESHOOTING.md)

### Quickstarts and setup

1. [Start Here](docs/00_START_HERE.md)
2. [Repository Overview](docs/01_REPO_OVERVIEW.md)
3. [v1 SQL API Quickstart](docs/03_V1_SQL_API_QUICKSTART.md)
4. [v2 Multi-API Router Quickstart](docs/04_V2_MULTIAPI_ROUTER_QUICKSTART.md)
5. [Terraform IaC Guide](docs/05_TERRAFORM_IAC_GUIDE.md)
6. [Config Parameters And Secrets](docs/06_CONFIG_PARAMETERS_AND_SECRETS.md)

### Reviews and onboarding

1. [Executive Summary](docs/14_EXECUTIVE_SUMMARY.md)
2. [Documentation Index](docs/16_DOCUMENTATION_INDEX.md)
3. [Production Readiness Review](docs/09_PRODUCTION_READINESS_REVIEW.md)
4. [Senior Review](docs/SENIOR_REVIEW.md)
5. [Developer Handover](docs/DEVELOPER_HANDOVER.md)
6. [Engineer Onboarding Guide](docs/ENGINEER_ONBOARDING_GUIDE.md)

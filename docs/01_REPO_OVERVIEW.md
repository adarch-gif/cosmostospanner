# 01 - Repository Overview

## Repository mission

Provide reusable, repeatable, and auditable migration toolchains from Azure Cosmos DB to GCP data stores.

## Why there are two pipelines

### v1 (stable path)

- Source: Cosmos SQL API
- Target: Spanner
- Use when all migrated entities should be in Spanner.

### v2 (evolving path)

- Sources: Cosmos MongoDB API + Cosmos Cassandra API
- Dynamic target routing by payload size:
  - small payloads -> Firestore
  - large payloads -> Spanner
- Use when document size and access patterns vary significantly.

## Design principles used in this repo

1. **Separation of concerns**: v1 and v2 are isolated.
2. **Config-first**: behavior is driven by YAML.
3. **Idempotent writes**: reruns are safe.
4. **Operational safety**: preflight checks, retries, DLQ, state files.
5. **Infrastructure reproducibility**: Terraform stacks and environment wrappers.

## High-level runtime components

1. Source adapters (Cosmos API specific)
2. Transformation / canonicalization
3. Routing decision (v2)
4. Sink adapters (Spanner/Firestore)
5. State and route registry
6. Validation and preflight checks
7. Terraform-managed cloud resources


# 14 - Executive Summary

This document is the shortest serious overview of the repository. It is intended for engineering leaders, reviewers, approvers, and senior operators who need to understand what the system does, why it is architected the way it is, and how it should be operated without reading the full architecture and runbook first.

## What this repository is

This repository is a production-oriented migration toolkit for moving data from Azure Cosmos DB to Google Cloud data stores.

It contains two distinct pipelines:

1. `v1`: Cosmos SQL API to Cloud Spanner
2. `v2`: Cosmos MongoDB API and Cassandra API to Firestore or Cloud Spanner, routed by payload size

The repository is not just a one-time copy tool. It supports:

1. full backfill
2. watermark-based incremental catch-up
3. distributed execution
4. replay-safe restart behavior
5. validation and reconciliation
6. stage release-gate enforcement
7. structured logging and metrics

## Why the design matters

Migration systems fail when they assume the happy path. This repository was deliberately hardened around the failure cases that matter in real production campaigns:

1. process crashes
2. worker overlap
3. partial replay
4. checkpoint boundary errors
5. incomplete cross-sink moves
6. weak cutover evidence

The codebase addresses those risks through explicit control-plane state:

1. checkpoints
2. reader cursors
3. leases
4. progress manifests
5. route registry for v2
6. release-gate attestations

That explicit state is what makes the system production-credible.

## Why there are two pipelines

The repository contains two pipelines because the problem shapes are materially different.

`v1` is mapping-centric:

1. source query from Cosmos SQL API
2. column mapping and conversion
3. writes into Spanner tables
4. mapping-scoped incremental checkpoints

`v2` is routing-centric:

1. canonical records from Mongo or Cassandra sources
2. size-based routing to Firestore or Spanner
3. authoritative route registry
4. move semantics with `pending_cleanup`
5. exact validation across source, registry, and both sinks

Trying to force both pipelines into one generic abstraction would make correctness harder to reason about.

## High-level architecture

The architecture has six layers:

1. source systems
2. operational entrypoints
3. pipeline runtimes
4. shared control plane
5. targets and operational artifacts
6. infrastructure and release governance

That layered model is important because many migration incidents are not caused by a bad transform function. They are caused by weak state management, unsafe replay, poor coordination, or missing validation.

## Most important correctness behaviors

The most important properties to understand are:

1. primary checkpoints are separate from reader cursors
2. checkpoints advance only after safe write boundaries
3. distributed ownership is coordinated through leases and progress manifests
4. v2 route moves are stateful and repairable because the route registry records `pending_cleanup`
5. validation is built in, not left to manual SQL after the fact

These are the features that move the repository from "functional" to "operationally trustworthy."

## Production operating model

The intended operating model is:

1. develop and test in `dev`
2. rehearse in `stage`
3. validate and attest stage success
4. enforce stage rehearsal for `prod` when required
5. use durable shared state for serious campaigns
6. treat cutover and rollback as explicit runbook events

For small local runs, local JSON state is acceptable. For real stage or production work, shared durable state is strongly preferred, and Spanner-backed control-plane state is the strongest model for distributed execution.

## When to use v1

Use v1 when:

1. the source is Cosmos SQL API
2. the target is Spanner only
3. the migration is naturally expressed as field-to-column mappings
4. the workload does not need cross-sink routing

## When to use v2

Use v2 when:

1. the source is Cosmos MongoDB API or Cassandra API
2. payload sizes vary enough that Firestore and Spanner should both be used
3. the system needs route ownership and move semantics
4. exact routed validation is part of the acceptance criteria

## What a reviewer should care about

A senior reviewer or approver should focus on:

1. whether state backends are appropriate for the campaign
2. whether checkpoint identity is stable and scoped correctly
3. whether distributed execution is using shared lease and progress state
4. whether validation depth matches the business risk
5. whether cutover is blocked on stage rehearsal where appropriate
6. whether rollback remains practical

## What an operator should care about

An operator should focus on:

1. correct config
2. correct environment
3. correct state paths
4. observability output
5. DLQ and validation status
6. release-gate status
7. rollback clarity

## Recommended reading paths

If you are an executive sponsor or approver:

1. this file
2. `docs/16_DOCUMENTATION_INDEX.md`
3. `docs/ARCHITECTURE.md` sections on goals, control plane, and release governance
4. `docs/RUNBOOK.md` sections on cutover and rollback

If you are an operator:

1. this file
2. `docs/15_OPERATOR_QUICK_REFERENCE.md`
3. `docs/RUNBOOK.md`

If you are a maintainer or reviewer:

1. this file
2. `docs/ARCHITECTURE.md`
3. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
4. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
5. `docs/RUNBOOK.md`

## Bottom line

The repository is strongest when it is used as a stateful migration platform with explicit replay, validation, and release governance. It is weakest when operators treat it like a throwaway batch script and bypass the control-plane and validation features that make it safe.

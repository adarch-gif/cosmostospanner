# 11 - Release Gate And Stage Rehearsal

This document describes how to turn stage rehearsal into a machine-enforced gate for production runs.

## Why this exists

1. Live preflight and validation are already available in the repository.
2. Without an attestation, production runs still depend on operator discipline.
3. The release gate adds a durable record that a matching stage rehearsal succeeded recently enough.

## What the release gate checks

The stage gate runs:

1. v1 preflight plus v1 validation
2. v2 preflight plus v2 exact routed validation

Each successful run writes an attestation keyed by:

1. pipeline (`v1` or `v2`)
2. environment (`stage`)
3. `release_gate_scope`

The attestation also stores a logical workload fingerprint, so a prod run only passes if the stage rehearsal covered the same logical mapping/job set.

## Shared attestation store

`runtime.release_gate_file` supports:

1. local JSON paths
2. `gs://bucket/object`
3. `spanner://<project>/<instance>/<database>/<table>?namespace=<name>`

For distributed or long-lived operations, prefer `gs://` or `spanner://`.

## Stage rehearsal command

Run the stage gate after stage infrastructure is live and stage data has been migrated/reconciled:

```powershell
python scripts/release_gate.py `
  --gate-file "spanner://my-project/my-instance/my-db/MigrationControlPlane?namespace=release-gates" `
  --scope "cutover-20260313" `
  --v1-config "C:\configs\stage-v1.yaml" `
  --v2-config "C:\configs\stage-v2.yaml"
```

Notes:

1. Stage configs must set `runtime.deployment_environment: stage`.
2. v1 uses `checksums` validation by default.
3. The command writes a passed or failed attestation for each selected pipeline.

## Enforcing the gate in production

For v1 or v2 production configs:

1. set `runtime.deployment_environment: prod`
2. set `runtime.release_gate_file`
3. set `runtime.release_gate_scope`
4. optionally tune `runtime.release_gate_max_age_hours`
5. set `runtime.require_stage_rehearsal_for_prod: true`

When enabled, `scripts/backfill.py` and `scripts/v2_route_migrate.py` refuse to run unless:

1. a matching stage attestation exists
2. it is marked `passed`
3. it is not older than `release_gate_max_age_hours`
4. its logical workload fingerprint matches the current selected mapping/job set
5. it contains both `preflight` and `validation`

## GitHub Actions workflow

The repository includes `.github/workflows/stage-release-gate.yml`.

It is intended for `workflow_dispatch` use with stage credentials/config supplied by secrets.

Expected secrets:

1. `STAGE_RELEASE_GATE_FILE`
2. `STAGE_V1_CONFIG_YAML` if you want v1 gating
3. `STAGE_V2_CONFIG_YAML` if you want v2 gating

The workflow writes the YAML secrets to temporary config files and runs `scripts/release_gate.py`.

## Recommended production pattern

1. Run Terraform in `stage`.
2. Run stage migration.
3. Run stage validation.
4. Run `scripts/release_gate.py` or the GitHub Actions workflow.
5. Use the same `release_gate_scope` in production config.
6. Start prod run only after the gate passes.

## Limits

1. This enforces a successful stage rehearsal, not business-signoff or CAB approval.
2. The gate verifies logical workload shape, not every environment-specific credential or resource name.
3. The workflow still depends on real Azure/GCP credentials being available to the runner.

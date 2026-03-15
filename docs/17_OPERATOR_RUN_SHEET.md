# 17 - Operator Run Sheet

This is the compact printable run sheet for active migration windows. It is intentionally short. Use it beside the terminal. For details, use `RUNBOOK.md`.

## Before You Start

Confirm:

1. correct repo and branch
2. correct config path
3. correct environment variables
4. correct state paths
5. rollback owner is named
6. logging and metrics output path is known

## Preflight

v1:

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

v2:

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

Do not continue if preflight fails.

## Dry-Run

v1:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

v2:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

## Main Commands

### v1

Full:

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

Full:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

Incremental:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Validation:

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

## Distributed Commands

Inspect progress:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>"
```

Reclaim stale:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>" --reclaim-stale
```

Release gate:

```powershell
python .\scripts\release_gate.py --gate-file "<path>" --scope "<scope>" --v1-config "<stage-v1-config>" --v2-config "<stage-v2-config>"
```

## Watch During Runs

Watch:

1. failures and DLQ
2. watermark progress
3. distributed shard status
4. v2 cleanup backlog
5. target latency and errors

## Cutover Checks

Before cutover:

1. latest incrementals stable
2. validation acceptable
3. release gate passed if required
4. rollback trigger and owner clear

After cutover:

1. application errors
2. target latency
3. DLQ
4. v2 `pending_cleanup` backlog
5. focused spot checks

## Rollback Triggers

Rollback when:

1. correctness is uncertain
2. user-visible failures are severe and sustained
3. target instability is unacceptable

When rolling back:

1. switch traffic back
2. stop or pause migrations
3. preserve state and evidence
4. document the trigger

## Do Not Do

1. do not treat reader cursors as checkpoints
2. do not mix stage and prod state
3. do not reuse an old `run_id` for a new campaign
4. do not bypass release gates casually
5. do not manually edit control-plane state unless the run is stopped and evidence is preserved

## Escalate Immediately

Escalate when:

1. correctness is uncertain
2. multiple shards or jobs fail systemically
3. manual state edits would affect multiple workload units
4. rollback is being debated without clear evidence

# 15 - Operator Quick Reference

This document is the fast operational reference for active migration sessions. Use it during rehearsals, cutovers, or incident response when you need the right commands and decision points without reading the full runbook.

For full context, see `docs/RUNBOOK.md`. For design context, see `docs/ARCHITECTURE.md`.

## 1. Core principles

1. Use the approved config file, not a remembered path.
2. Run preflight before the first write-bearing run in an environment.
3. Use shared durable state for distributed runs.
4. Treat reader cursors as restart aids, not as checkpoints.
5. Treat validation as release evidence, not as optional cleanup.
6. Do not improvise rollback ownership during the event.

## 2. Session start checklist

Before running anything:

1. confirm repo root
2. confirm branch or commit
3. confirm config path
4. confirm environment variables
5. confirm state paths
6. confirm who owns rollback

## 3. Canonical docs to keep open

Keep these open in parallel:

1. `docs/RUNBOOK.md`
2. `docs/ARCHITECTURE.md`
3. `docs/11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`
4. `docs/TROUBLESHOOTING.md`

## 4. v1 command set

Preflight:

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

Dry-run:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

Full backfill:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

Container-scoped run:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --container users
```

Incremental:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

Incremental with override:

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental --since-ts 1700000000
```

Validation:

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

## 5. v2 command set

Preflight:

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

Dry-run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

Full run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

Job-scoped run:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

Incremental:

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Exact validation:

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

## 6. Shared distributed-operations commands

Inspect progress:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>"
```

Reclaim stale work:

```powershell
python .\scripts\control_plane_status.py --progress-file "<path>" --lease-file "<path>" --run-id "<run-id>" --reclaim-stale
```

Write stage release gate:

```powershell
python .\scripts\release_gate.py --gate-file "<path>" --scope "<scope>" --v1-config "<stage-v1-config>" --v2-config "<stage-v2-config>"
```

## 7. Pre-run decision checklist

Before a write-bearing run, answer yes to all:

1. Is the config file the approved one?
2. Is the environment correct?
3. Are the state paths correct for this campaign?
4. Is the target schema ready?
5. Is logging and metrics output configured as intended?
6. Is rollback ownership clear?

## 8. Distributed-run checklist

Before launching more than one worker:

1. `lease_file` is set
2. `progress_file` is set for resumable full runs
3. `run_id` is set and documented
4. state backends are shared and durable
5. shard counts are final for the campaign

Do not change shard counts mid-run.

## 9. Validation checklist

Before declaring a workload healthy:

1. preflight passed
2. run completed or the failure profile is understood
3. DLQ is zero or understood
4. validation ran at the required depth
5. in v2, `pending_cleanup` backlog is acceptable and understood

## 10. Cutover checklist

Immediately before cutover:

1. recent incremental runs are stable
2. validation is acceptable
3. release gate passed if required
4. rollback trigger and owner are clear
5. observability is staffed

Immediately after cutover:

1. watch application errors
2. watch target latency
3. watch DLQ
4. watch cleanup backlog in v2
5. run focused spot checks if needed

## 11. Rollback checklist

Rollback when:

1. correctness is uncertain
2. user-visible failures are severe and sustained
3. target instability is unacceptable

When rolling back:

1. switch traffic back as planned
2. stop or pause migrations as appropriate
3. preserve state and evidence
4. document the trigger

## 12. Incident triage shortcuts

If source reads fail:

1. check credentials
2. rerun preflight
3. check endpoint and database

If target writes fail:

1. check schema
2. check IAM
3. check target health

If distributed runs stall:

1. inspect control-plane status
2. verify shared state paths
3. reclaim stale work only if the old worker is gone

If v2 cleanup backlog rises:

1. inspect cleanup failures
2. check sink health
3. do not cut over blindly

If validation fails:

1. classify mismatch type
2. inspect DLQ and logs
3. decide whether replay, config correction, or rollback is needed

## 13. What not to do

1. Do not treat reader cursors as primary checkpoints.
2. Do not mix stage and prod state locations.
3. Do not reuse an old `run_id` for a different campaign.
4. Do not bypass release gates casually.
5. Do not use skip mode without a DLQ review plan.
6. Do not manually edit control-plane state unless you have stopped the run and preserved evidence.

## 14. Escalate when

Escalate immediately if:

1. correctness is uncertain
2. multiple shards or jobs fail in ways that suggest systemic issues
3. a manual state edit would affect more than one workload unit
4. rollback is being debated without clear evidence

## 15. Fast reading path by situation

If you are about to run a migration:

1. this file
2. `docs/RUNBOOK.md` relevant section

If you are in an incident:

1. this file
2. `docs/TROUBLESHOOTING.md`
3. `docs/RUNBOOK.md` incident and rollback sections

If you are trying to understand behavior:

1. `docs/ARCHITECTURE.md`
2. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`

## 16. Final reminder

The system is designed to be replay-safe and recoverable. That only helps if operators preserve state, preserve evidence, and make deliberate decisions under pressure.

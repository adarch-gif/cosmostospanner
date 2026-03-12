# Go-Live Checklist

Use this checklist during final migration readiness and cutover.

## A. Pre-cutover readiness

- [ ] `scripts/preflight.py` passes for all mappings.
- [ ] Dry-run completed with zero unexpected transform errors.
- [ ] Full backfill completed successfully.
- [ ] Incremental runs show stable low lag.
- [ ] Validation reports:
  - [ ] `count_delta = 0`
  - [ ] `sample_missing = 0`
  - [ ] `sample_value_mismatch_rows = 0`
- [ ] DLQ reviewed and resolved or accepted with explicit waiver.
- [ ] Rollback runbook reviewed and owner assigned.

## B. Cutover execution

- [ ] Freeze non-essential schema changes.
- [ ] Run final incremental sync.
- [ ] Run final validation.
- [ ] Switch read traffic to Spanner.
- [ ] Switch write traffic to Spanner (or complete dual-write exit plan).
- [ ] Confirm business critical flows in production.

## C. Immediate post-cutover (0-24h)

- [ ] Error rate within SLO.
- [ ] p95/p99 latencies within SLO.
- [ ] No critical data correctness incidents.
- [ ] Watermark progression and job health are stable.
- [ ] No unexplained DLQ spikes.

## D. Stabilization window

- [ ] Daily validation checks pass for critical entities.
- [ ] Stakeholders sign off on KPI parity.
- [ ] Rollback criteria expiry date reached.
- [ ] Cosmos decommission date approved.


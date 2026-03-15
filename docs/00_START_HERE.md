# 00 - Start Here

This is the first document to read if you are new to the repository.

## Canonical deep references

For the current source-of-truth material, read these after this file:

1. `docs/14_EXECUTIVE_SUMMARY.md`
2. `docs/15_OPERATOR_QUICK_REFERENCE.md`
3. `docs/16_DOCUMENTATION_INDEX.md`
4. `docs/ARCHITECTURE.md`
5. `docs/RUNBOOK.md`
6. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
7. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`
8. `docs/CONFIG_REFERENCE.md`

## What this repo does

This repository contains two migration solutions:

1. **v1 pipeline**: Azure Cosmos DB SQL API -> Google Cloud Spanner
2. **v2 pipeline**: Azure Cosmos DB MongoDB/Cassandra APIs -> dynamic routing:
   - payload `< 1 MB` -> Firestore
   - payload `>= 1 MB` -> Spanner up to a safer configured cap

Both pipelines are configurable and production-oriented.

## Numbered documentation map

1. `00_START_HERE.md` (this file)
2. `01_REPO_OVERVIEW.md`
3. `02_ARCHITECTURE_AND_DATA_FLOW.md`
4. `03_V1_SQL_API_QUICKSTART.md`
5. `04_V2_MULTIAPI_ROUTER_QUICKSTART.md`
6. `05_TERRAFORM_IAC_GUIDE.md`
7. `06_CONFIG_PARAMETERS_AND_SECRETS.md`
8. `07_CODEBASE_STRUCTURE.md`
9. `08_OPERATIONS_AND_SRE.md`
10. `09_PRODUCTION_READINESS_REVIEW.md`
11. `10_INTEGRATION_TESTING.md`
12. `11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`
13. `12_DETAILED_ARCHITECTURE_DIAGRAM.md`
14. `13_DETAILED_DATA_FLOW_DIAGRAM.md`
15. `14_EXECUTIVE_SUMMARY.md`
16. `15_OPERATOR_QUICK_REFERENCE.md`
17. `16_DOCUMENTATION_INDEX.md`

## Fastest path for first-time setup

1. Read `14_EXECUTIVE_SUMMARY.md` for the shortest serious overview.
2. Read `16_DOCUMENTATION_INDEX.md` to choose the right depth and path.
3. Read `06_CONFIG_PARAMETERS_AND_SECRETS.md` to understand required inputs.
4. Read `ARCHITECTURE.md` for the full control-plane and pipeline model.
5. Read `RUNBOOK.md` for the full operating model.
6. Provision infra with Terraform:
   - `05_TERRAFORM_IAC_GUIDE.md`
7. Run preflight + migration:
   - v1: `03_V1_SQL_API_QUICKSTART.md`
   - v2: `04_V2_MULTIAPI_ROUTER_QUICKSTART.md`
8. Review SRE and cutover guidance:
   - `08_OPERATIONS_AND_SRE.md`
9. Run live-cloud preflight smoke tests before rehearsal/cutover:
   - `10_INTEGRATION_TESTING.md`
10. If prod runs must be blocked without successful stage rehearsal:
   - `11_RELEASE_GATE_AND_STAGE_REHEARSAL.md`

# 00 - Start Here

This is the first document to read if you are new to the repository.

## What this repo does

This repository contains two migration solutions:

1. **v1 pipeline**: Azure Cosmos DB SQL API -> Google Cloud Spanner
2. **v2 pipeline**: Azure Cosmos DB MongoDB/Cassandra APIs -> dynamic routing:
   - payload `< 1 MB` -> Firestore
   - payload `>= 1 MB` -> Spanner

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

## Fastest path for first-time setup

1. Read `06_CONFIG_PARAMETERS_AND_SECRETS.md` to understand required inputs.
2. Provision infra with Terraform:
   - `05_TERRAFORM_IAC_GUIDE.md`
3. Run preflight + migration:
   - v1: `03_V1_SQL_API_QUICKSTART.md`
   - v2: `04_V2_MULTIAPI_ROUTER_QUICKSTART.md`
4. Review SRE and cutover guidance:
   - `08_OPERATIONS_AND_SRE.md`


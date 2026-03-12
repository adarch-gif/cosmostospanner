# Terraform Infrastructure for v1 and v2 Pipelines

This folder provides reproducible GCP provisioning for both migration pipelines:

- `stacks/v1_sql_api`: Cosmos SQL API -> Spanner pipeline infra.
- `stacks/v2_multiapi_router`: Cosmos Mongo/Cassandra size-router infra (Firestore + Spanner).

## Directory structure

- `modules/gcp_migration_platform`: reusable module with API enablement, IAM, secrets, Spanner, Firestore, and optional state bucket.
- `stacks/v1_sql_api`: v1 stack root.
- `stacks/v2_multiapi_router`: v2 stack root.

## Terraform execution pattern

For each stack:

```powershell
cd infra/terraform/stacks/<stack_name>
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

## Suggested remote backend

Create a dedicated Terraform state bucket and use a GCS backend.

Each stack includes `backend.tf.example`. Copy to `backend.tf` and edit values.

## Post-apply operational steps

1. Add secret versions for created Secret Manager secret IDs.
2. Grant the output service account to runtime environments where scripts execute.
3. Point pipeline config files to provisioned resources:
   - v1: `config/migration.yaml`
   - v2: `config/v2.multiapi-routing.yaml`

## Recommended promotion model

Use separate `terraform.tfvars` and state prefixes per environment:

- `dev`
- `staging`
- `prod`

Keep the same module code and vary only variables.


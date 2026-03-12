# Terraform Infrastructure for v1 and v2 Pipelines

This folder provides reproducible GCP provisioning for both migration pipelines:

- `stacks/v1_sql_api`: Cosmos SQL API -> Spanner pipeline infra.
- `stacks/v2_multiapi_router`: Cosmos Mongo/Cassandra size-router infra (Firestore + Spanner).
- `envs/dev|stage|prod`: environment wrappers provisioning both stacks together.

## Directory structure

- `modules/gcp_migration_platform`: reusable module with API enablement, IAM, secrets, Spanner, Firestore, and optional state bucket.
- `stacks/v1_sql_api`: v1 stack root.
- `stacks/v2_multiapi_router`: v2 stack root.
- `envs/dev`: deploys v1 + v2 for development.
- `envs/stage`: deploys v1 + v2 for stage.
- `envs/prod`: deploys v1 + v2 for production.

## Terraform execution pattern

Preferred: run via environment wrapper.

```powershell
cd infra/terraform/envs/dev
Copy-Item backend.tf.example backend.tf
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

You can also run stack roots directly if you only want one stack.

## Suggested remote backend

Create a dedicated Terraform state bucket and use a GCS backend.

Each environment wrapper and stack includes `backend.tf.example`. Copy to `backend.tf` and edit values.

## Post-apply operational steps

1. Add secret versions for created Secret Manager secret IDs.
2. Grant the output service account to runtime environments where scripts execute.
3. Point pipeline config files to provisioned resources:
   - v1: `config/migration.yaml`
   - v2: `config/v2.multiapi-routing.yaml`

## Recommended promotion model

Use separate `terraform.tfvars` and state prefixes per environment:

- `dev`
- `stage`
- `prod`

Keep the same module code and vary only variables.

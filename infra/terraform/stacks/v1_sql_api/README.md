# Terraform Stack: v1_sql_api

Provisions GCP infra required for the v1 Cosmos SQL API -> Spanner migration pipeline.

## What it creates

- Required APIs
- Migration runner service account + IAM roles
- Secret Manager placeholders for Cosmos SQL credentials
- Optional GCS bucket for migration state
- Spanner instance + database + custom DDL

## CLI usage

```powershell
cd infra/terraform/stacks/v1_sql_api
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```


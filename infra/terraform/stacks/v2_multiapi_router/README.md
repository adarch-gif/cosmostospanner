# Terraform Stack: v2_multiapi_router

Provisions GCP infra required for the v2 Cosmos Mongo/Cassandra -> Firestore/Spanner routing pipeline.

## What it creates

- Required APIs
- Migration runner service account + IAM roles
- Secret Manager placeholders for Mongo/Cassandra credentials
- Optional GCS bucket for migration state
- Firestore database
- Spanner instance + database
- `RoutedDocuments` table required by v2 sink adapter

## CLI usage

```powershell
cd infra/terraform/stacks/v2_multiapi_router
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```


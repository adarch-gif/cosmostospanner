# 05 - Terraform IaC Guide

## 1. Terraform layout

1. `infra/terraform/modules/gcp_migration_platform` (shared building block)
2. `infra/terraform/stacks/v1_sql_api` (v1 resources)
3. `infra/terraform/stacks/v2_multiapi_router` (v2 resources)
4. `infra/terraform/envs/dev|stage|prod` (environment wrappers deploying both stacks)

## 2. Preferred way to run Terraform

Use environment wrappers so v1 + v2 are provisioned consistently per environment.

### Option A (recommended): bootstrap helper script

Run from repo root:

```powershell
.\scripts\bootstrap_env.ps1 -Environment dev -Action plan
```

Common variants:

```powershell
# Scaffold only (copy backend.tf / terraform.tfvars from *.example)
.\scripts\bootstrap_env.ps1 -Environment all -Action none -SkipTerraform

# Init + plan for all envs
.\scripts\bootstrap_env.ps1 -Environment all -Action plan

# Apply for stage with explicit approval bypass
.\scripts\bootstrap_env.ps1 -Environment stage -Action apply -AutoApprove

# Force overwrite local backend.tf / terraform.tfvars from templates
.\scripts\bootstrap_env.ps1 -Environment prod -Action init -ForceTemplateCopy
```

Parameters:

1. `-Environment`: `dev|stage|prod|all` (default: `all`)
2. `-Action`: `none|init|plan|apply|destroy` (default: `plan`)
3. `-SkipTerraform`: scaffold templates but do not invoke Terraform
4. `-ForceTemplateCopy`: overwrite existing `backend.tf` / `terraform.tfvars`
5. `-AutoApprove`: append `-auto-approve` for `apply` / `destroy`
6. `-TerraformBinary`: override terraform executable name/path

### Option B: manual commands per environment

#### Example: dev

```powershell
cd infra/terraform/envs/dev
Copy-Item backend.tf.example backend.tf
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Repeat the same in `envs/stage` and `envs/prod`.

## 3. Backend/state conventions

Use one remote state bucket with different prefixes:

1. `cosmos-to-spanner/envs/dev`
2. `cosmos-to-spanner/envs/stage`
3. `cosmos-to-spanner/envs/prod`

## 4. What gets provisioned

### v1

1. Spanner instance + DB
2. v1 runner service account + IAM
3. v1 secrets for Cosmos SQL endpoint/key
4. Optional v1 state bucket

### v2

1. Firestore DB
2. Spanner instance + DB + `RoutedDocuments` table
3. v2 runner service account + IAM
4. v2 secrets for Mongo/Cassandra credentials
5. Optional v2 state bucket

## 5. Post-apply checklist

1. Add secret **versions** for generated secrets.
2. Export/apply outputs into app config files.
3. Ensure runner runtime has ADC auth and correct SA permissions.
4. Run preflight scripts for v1 and v2.

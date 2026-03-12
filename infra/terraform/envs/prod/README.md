# Terraform Environment Wrapper: prod

Deploys v1 and v2 stacks for the `prod` environment from one Terraform root.

## Usage

```powershell
cd infra/terraform/envs/prod
Copy-Item backend.tf.example backend.tf
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```


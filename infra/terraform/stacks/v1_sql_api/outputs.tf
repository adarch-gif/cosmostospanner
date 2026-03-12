output "runner_service_account_email" {
  description = "Service account used by v1 migration runtime."
  value       = module.platform.runner_service_account_email
}

output "state_bucket_name" {
  description = "Bucket for state artifacts when enabled."
  value       = module.platform.state_bucket_name
}

output "spanner_instance_name" {
  description = "v1 Spanner instance."
  value       = module.platform.spanner_instance_name
}

output "spanner_database_name" {
  description = "v1 Spanner database."
  value       = module.platform.spanner_database_name
}

output "secret_ids" {
  description = "Secret Manager secrets created for v1."
  value       = module.platform.secret_ids
}


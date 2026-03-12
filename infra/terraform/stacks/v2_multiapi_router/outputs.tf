output "runner_service_account_email" {
  description = "Service account used by v2 migration runtime."
  value       = module.platform.runner_service_account_email
}

output "state_bucket_name" {
  description = "Bucket for state artifacts when enabled."
  value       = module.platform.state_bucket_name
}

output "firestore_database_name" {
  description = "Firestore database provisioned for v2."
  value       = module.platform.firestore_database_name
}

output "spanner_instance_name" {
  description = "v2 Spanner instance."
  value       = module.platform.spanner_instance_name
}

output "spanner_database_name" {
  description = "v2 Spanner database."
  value       = module.platform.spanner_database_name
}

output "secret_ids" {
  description = "Secret Manager secrets created for v2."
  value       = module.platform.secret_ids
}


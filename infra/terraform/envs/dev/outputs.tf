output "v1_runner_service_account_email" {
  value       = try(module.v1_sql_api[0].runner_service_account_email, null)
  description = "v1 runner service account email."
}

output "v2_runner_service_account_email" {
  value       = try(module.v2_multiapi_router[0].runner_service_account_email, null)
  description = "v2 runner service account email."
}

output "v1_secret_ids" {
  value       = try(module.v1_sql_api[0].secret_ids, [])
  description = "v1 Secret Manager IDs."
}

output "v2_secret_ids" {
  value       = try(module.v2_multiapi_router[0].secret_ids, [])
  description = "v2 Secret Manager IDs."
}

output "v1_spanner_database_name" {
  value       = try(module.v1_sql_api[0].spanner_database_name, null)
  description = "v1 Spanner database."
}

output "v2_spanner_database_name" {
  value       = try(module.v2_multiapi_router[0].spanner_database_name, null)
  description = "v2 Spanner database."
}

output "v2_firestore_database_name" {
  value       = try(module.v2_multiapi_router[0].firestore_database_name, null)
  description = "v2 Firestore database."
}


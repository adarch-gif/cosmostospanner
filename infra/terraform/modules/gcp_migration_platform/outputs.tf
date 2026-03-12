output "runner_service_account_email" {
  description = "Migration runner service account email."
  value       = google_service_account.runner.email
}

output "secret_ids" {
  description = "Secret Manager secret IDs created by this stack."
  value       = [for s in google_secret_manager_secret.secrets : s.secret_id]
}

output "state_bucket_name" {
  description = "GCS state bucket name when enabled."
  value       = try(google_storage_bucket.state[0].name, null)
}

output "firestore_database_name" {
  description = "Firestore database name when enabled."
  value       = try(google_firestore_database.firestore[0].name, null)
}

output "spanner_instance_name" {
  description = "Spanner instance name when enabled."
  value       = try(google_spanner_instance.instance[0].name, null)
}

output "spanner_database_name" {
  description = "Spanner database name when enabled."
  value       = try(google_spanner_database.database[0].name, null)
}


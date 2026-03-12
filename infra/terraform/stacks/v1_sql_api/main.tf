module "platform" {
  source = "../../modules/gcp_migration_platform"

  project_id = var.project_id
  region     = var.region
  labels     = var.labels

  service_name                 = "cosmos-sql-v1"
  service_account_id           = "cosmos-sql-v1-runner"
  service_account_display_name = "Cosmos SQL v1 Migration Runner"

  enabled_apis = [
    "iam.googleapis.com",
    "logging.googleapis.com",
    "secretmanager.googleapis.com",
    "spanner.googleapis.com"
  ]

  service_account_roles = [
    "roles/logging.logWriter",
    "roles/secretmanager.secretAccessor",
    "roles/spanner.databaseAdmin",
    "roles/spanner.databaseUser"
  ]

  secret_names = [
    "cosmos-sql-endpoint",
    "cosmos-sql-key"
  ]

  create_state_bucket      = var.create_state_bucket
  state_bucket_name        = var.state_bucket_name
  state_bucket_force_destroy = var.state_bucket_force_destroy

  create_spanner            = true
  spanner_instance_name     = var.spanner_instance_name
  spanner_instance_config   = var.spanner_instance_config
  spanner_processing_units  = var.spanner_processing_units
  spanner_database_name     = var.spanner_database_name
  spanner_database_ddl      = var.spanner_database_ddl

  create_firestore_database = false
}


locals {
  default_v2_spanner_ddl = [
    "CREATE TABLE RoutedDocuments ( RouteKey STRING(MAX) NOT NULL, SourceJob STRING(128) NOT NULL, SourceApi STRING(32) NOT NULL, SourceNamespace STRING(MAX) NOT NULL, SourceKey STRING(MAX) NOT NULL, PayloadJson STRING(MAX) NOT NULL, PayloadSizeBytes INT64 NOT NULL, Checksum STRING(64) NOT NULL, EventTs STRING(MAX) NOT NULL, UpdatedAt TIMESTAMP NOT NULL ) PRIMARY KEY (RouteKey)"
  ]
}

module "platform" {
  source = "../../modules/gcp_migration_platform"

  project_id = var.project_id
  region     = var.region
  labels     = var.labels

  service_name                 = "cosmos-v2-router"
  service_account_id           = "cosmos-v2-router-runner"
  service_account_display_name = "Cosmos v2 Multi-API Router Runner"

  enabled_apis = [
    "firestore.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "secretmanager.googleapis.com",
    "spanner.googleapis.com"
  ]

  service_account_roles = [
    "roles/datastore.user",
    "roles/logging.logWriter",
    "roles/secretmanager.secretAccessor",
    "roles/spanner.databaseAdmin",
    "roles/spanner.databaseUser"
  ]

  secret_names = [
    "cosmos-mongo-connection-string",
    "cosmos-cassandra-username",
    "cosmos-cassandra-password"
  ]

  create_state_bucket        = var.create_state_bucket
  state_bucket_name          = var.state_bucket_name
  state_bucket_force_destroy = var.state_bucket_force_destroy

  create_firestore_database = var.create_firestore_database
  firestore_database_name   = var.firestore_database_name
  firestore_location_id     = var.firestore_location_id

  create_spanner           = true
  spanner_instance_name    = var.spanner_instance_name
  spanner_instance_config  = var.spanner_instance_config
  spanner_processing_units = var.spanner_processing_units
  spanner_database_name    = var.spanner_database_name
  spanner_database_ddl     = concat(local.default_v2_spanner_ddl, var.spanner_database_ddl_additional)
}


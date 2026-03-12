locals {
  environment = basename(path.cwd)
}

module "v1_sql_api" {
  count  = var.enable_v1 ? 1 : 0
  source = "../../stacks/v1_sql_api"

  project_id = var.project_id
  region     = var.region
  labels = merge(var.common_labels, {
    environment = local.environment
    stack       = "v1-sql-api"
  })

  create_state_bucket       = var.v1_create_state_bucket
  state_bucket_name         = var.v1_state_bucket_name
  state_bucket_force_destroy = var.v1_state_bucket_force_destroy

  spanner_instance_name    = var.v1_spanner_instance_name
  spanner_instance_config  = var.v1_spanner_instance_config
  spanner_processing_units = var.v1_spanner_processing_units
  spanner_database_name    = var.v1_spanner_database_name
  spanner_database_ddl     = var.v1_spanner_database_ddl
}

module "v2_multiapi_router" {
  count  = var.enable_v2 ? 1 : 0
  source = "../../stacks/v2_multiapi_router"

  project_id = var.project_id
  region     = var.region
  labels = merge(var.common_labels, {
    environment = local.environment
    stack       = "v2-multiapi-router"
  })

  create_state_bucket        = var.v2_create_state_bucket
  state_bucket_name          = var.v2_state_bucket_name
  state_bucket_force_destroy = var.v2_state_bucket_force_destroy

  create_firestore_database = var.v2_create_firestore_database
  firestore_database_name   = var.v2_firestore_database_name
  firestore_location_id     = var.v2_firestore_location_id

  spanner_instance_name          = var.v2_spanner_instance_name
  spanner_instance_config        = var.v2_spanner_instance_config
  spanner_processing_units       = var.v2_spanner_processing_units
  spanner_database_name          = var.v2_spanner_database_name
  spanner_database_ddl_additional = var.v2_spanner_database_ddl_additional
}


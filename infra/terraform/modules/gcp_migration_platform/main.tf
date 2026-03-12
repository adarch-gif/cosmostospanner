locals {
  state_bucket_name = coalesce(var.state_bucket_name, "${var.project_id}-${var.service_name}-state")
  firestore_location = coalesce(var.firestore_location_id, var.region)
}

resource "google_project_service" "enabled" {
  for_each           = toset(var.enabled_apis)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_service_account" "runner" {
  project      = var.project_id
  account_id   = var.service_account_id
  display_name = var.service_account_display_name
}

resource "google_project_iam_member" "runner_roles" {
  for_each = toset(var.service_account_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_secret_manager_secret" "secrets" {
  for_each  = toset(var.secret_names)
  project   = var.project_id
  secret_id = each.key
  labels    = var.labels

  replication {
    auto {}
  }
}

resource "google_storage_bucket" "state" {
  count         = var.create_state_bucket ? 1 : 0
  project       = var.project_id
  name          = local.state_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = var.state_bucket_force_destroy
  labels        = var.labels

  uniform_bucket_level_access = true
}

resource "google_firestore_database" "firestore" {
  count       = var.create_firestore_database ? 1 : 0
  project     = var.project_id
  name        = var.firestore_database_name
  location_id = local.firestore_location
  type        = "FIRESTORE_NATIVE"

  depends_on = [
    google_project_service.enabled
  ]
}

resource "google_spanner_instance" "instance" {
  count        = var.create_spanner ? 1 : 0
  project      = var.project_id
  name         = var.spanner_instance_name
  config       = var.spanner_instance_config
  display_name = "${var.service_name} instance"

  processing_units = var.spanner_processing_units
  labels           = var.labels

  depends_on = [
    google_project_service.enabled
  ]
}

resource "google_spanner_database" "database" {
  count    = var.create_spanner ? 1 : 0
  project  = var.project_id
  instance = google_spanner_instance.instance[0].name
  name     = var.spanner_database_name
  ddl      = var.spanner_database_ddl
}


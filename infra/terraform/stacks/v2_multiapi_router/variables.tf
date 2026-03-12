variable "project_id" {
  description = "GCP project ID for v2 multi-API router stack."
  type        = string
}

variable "region" {
  description = "Primary region for this stack."
  type        = string
}

variable "labels" {
  description = "Labels applied to supported resources."
  type        = map(string)
  default     = {}
}

variable "create_state_bucket" {
  description = "Create GCS bucket for migration state artifacts."
  type        = bool
  default     = true
}

variable "state_bucket_name" {
  description = "Optional explicit bucket name for state artifacts."
  type        = string
  default     = null
}

variable "state_bucket_force_destroy" {
  description = "Allow deleting non-empty state bucket."
  type        = bool
  default     = false
}

variable "create_firestore_database" {
  description = "Create Firestore database resource."
  type        = bool
  default     = true
}

variable "firestore_database_name" {
  description = "Firestore database name, usually (default)."
  type        = string
  default     = "(default)"
}

variable "firestore_location_id" {
  description = "Firestore location, for example us-central."
  type        = string
}

variable "spanner_instance_name" {
  description = "Spanner instance name for v2 routing."
  type        = string
}

variable "spanner_instance_config" {
  description = "Spanner instance config, for example regional-us-central1."
  type        = string
}

variable "spanner_processing_units" {
  description = "Spanner processing units."
  type        = number
  default     = 100
}

variable "spanner_database_name" {
  description = "Spanner database name for v2 routing."
  type        = string
}

variable "spanner_database_ddl_additional" {
  description = "Optional additional DDL statements beyond default RoutedDocuments table."
  type        = list(string)
  default     = []
}


variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Primary GCP region for regional resources."
  type        = string
}

variable "labels" {
  description = "Common labels to apply where supported."
  type        = map(string)
  default     = {}
}

variable "service_name" {
  description = "Short service identifier used in resource naming."
  type        = string
}

variable "enabled_apis" {
  description = "List of Google APIs to enable for this stack."
  type        = list(string)
}

variable "service_account_id" {
  description = "Service account ID (without domain)."
  type        = string
}

variable "service_account_display_name" {
  description = "Service account display name."
  type        = string
}

variable "service_account_roles" {
  description = "IAM roles granted to migration runner service account."
  type        = list(string)
}

variable "secret_names" {
  description = "Secret Manager secret IDs to create."
  type        = list(string)
  default     = []
}

variable "create_state_bucket" {
  description = "Whether to create a GCS bucket for migration state/dlq backups."
  type        = bool
  default     = false
}

variable "state_bucket_name" {
  description = "GCS bucket name for migration state artifacts."
  type        = string
  default     = null
}

variable "state_bucket_force_destroy" {
  description = "Whether Terraform can destroy bucket even if non-empty."
  type        = bool
  default     = false
}

variable "create_firestore_database" {
  description = "Whether to create Firestore database for this stack."
  type        = bool
  default     = false
}

variable "firestore_database_name" {
  description = "Firestore database ID. Use '(default)' for default DB."
  type        = string
  default     = "(default)"
}

variable "firestore_location_id" {
  description = "Firestore location ID (regional or multi-region)."
  type        = string
  default     = null
}

variable "create_spanner" {
  description = "Whether to create Spanner instance/database."
  type        = bool
  default     = false
}

variable "spanner_instance_name" {
  description = "Spanner instance name."
  type        = string
  default     = null
}

variable "spanner_instance_config" {
  description = "Spanner instance config, e.g. regional-us-central1."
  type        = string
  default     = null
}

variable "spanner_processing_units" {
  description = "Spanner processing units for instance sizing."
  type        = number
  default     = 100
}

variable "spanner_database_name" {
  description = "Spanner database name."
  type        = string
  default     = null
}

variable "spanner_database_ddl" {
  description = "Spanner DDL statements executed on database creation."
  type        = list(string)
  default     = []
}


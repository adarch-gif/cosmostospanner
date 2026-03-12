variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Primary region."
  type        = string
}

variable "enable_v1" {
  description = "Whether to deploy v1 SQL API stack."
  type        = bool
  default     = true
}

variable "enable_v2" {
  description = "Whether to deploy v2 multi-API router stack."
  type        = bool
  default     = true
}

variable "common_labels" {
  description = "Common labels for both stacks."
  type        = map(string)
  default     = {}
}

variable "v1_create_state_bucket" {
  type        = bool
  default     = true
  description = "Create v1 state bucket."
}

variable "v1_state_bucket_name" {
  type        = string
  default     = null
  description = "Optional v1 explicit state bucket name."
}

variable "v1_state_bucket_force_destroy" {
  type        = bool
  default     = false
  description = "Allow deleting non-empty v1 state bucket."
}

variable "v1_spanner_instance_name" {
  type        = string
  description = "v1 Spanner instance name."
}

variable "v1_spanner_instance_config" {
  type        = string
  description = "v1 Spanner instance config."
}

variable "v1_spanner_processing_units" {
  type        = number
  default     = 100
  description = "v1 Spanner processing units."
}

variable "v1_spanner_database_name" {
  type        = string
  description = "v1 Spanner database name."
}

variable "v1_spanner_database_ddl" {
  type        = list(string)
  default     = []
  description = "v1 Spanner DDL statements."
}

variable "v2_create_state_bucket" {
  type        = bool
  default     = true
  description = "Create v2 state bucket."
}

variable "v2_state_bucket_name" {
  type        = string
  default     = null
  description = "Optional v2 explicit state bucket name."
}

variable "v2_state_bucket_force_destroy" {
  type        = bool
  default     = false
  description = "Allow deleting non-empty v2 state bucket."
}

variable "v2_create_firestore_database" {
  type        = bool
  default     = true
  description = "Create Firestore DB for v2."
}

variable "v2_firestore_database_name" {
  type        = string
  default     = "(default)"
  description = "Firestore DB name for v2."
}

variable "v2_firestore_location_id" {
  type        = string
  description = "Firestore location for v2."
}

variable "v2_spanner_instance_name" {
  type        = string
  description = "v2 Spanner instance name."
}

variable "v2_spanner_instance_config" {
  type        = string
  description = "v2 Spanner instance config."
}

variable "v2_spanner_processing_units" {
  type        = number
  default     = 100
  description = "v2 Spanner processing units."
}

variable "v2_spanner_database_name" {
  type        = string
  description = "v2 Spanner database name."
}

variable "v2_spanner_database_ddl_additional" {
  type        = list(string)
  default     = []
  description = "Additional v2 Spanner DDL statements."
}


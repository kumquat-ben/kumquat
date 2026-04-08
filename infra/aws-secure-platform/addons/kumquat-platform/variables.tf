# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
variable "kubeconfig_path" {
  type = string
}

variable "namespace" {
  type    = string
  default = "kumquat"
}

variable "mysql_operator_namespace" {
  type    = string
  default = "mysql-operator"
}

variable "hostname" {
  type    = string
  default = "kumquat.info"
}

variable "backend_image_repository" {
  type = string
}

variable "backend_image_tag" {
  type = string
}

variable "backend_secret_key" {
  type      = string
  sensitive = true
}

variable "google_oauth_client_id" {
  type      = string
  sensitive = true
}

variable "google_oauth_client_secret" {
  type      = string
  sensitive = true
}

variable "google_oauth_redirect_uri" {
  type = string
}

variable "mysql_root_password" {
  type      = string
  sensitive = true
}

variable "mysql_app_password" {
  type      = string
  sensitive = true
}

variable "mysql_database_name" {
  type    = string
  default = "kumquat"
}

variable "mysql_app_user" {
  type    = string
  default = "kumquat"
}

variable "mysql_storage_size" {
  type    = string
  default = "50Gi"
}

variable "mysql_storage_class_name" {
  type    = string
  default = "kumquat-mysql-gp3"
}

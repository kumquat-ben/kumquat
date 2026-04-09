# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
variable "aws_region" {
  type    = string
  default = "us-west-2"
}

variable "bucket_name" {
  type    = string
  default = ""
}

variable "project_name" {
  type    = string
  default = "kumquat"
}

variable "environment" {
  type    = string
  default = "production"
}

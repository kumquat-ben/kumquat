# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
variable "name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "server_subnet_indexes" {
  description = "Indexes into private_subnet_ids for placing the three control-plane nodes."
  type        = list(number)
  default     = [0, 1, 2]

  validation {
    condition     = length(var.server_subnet_indexes) == 3
    error_message = "server_subnet_indexes must contain exactly three subnet indexes for the k3s servers."
  }
}

variable "vpn_client_cidr" {
  type = string
}

variable "ssh_key_name" {
  description = "Optional EC2 key pair. Leave null to disable SSH key injection."
  type        = string
  default     = null
}

variable "api_tls_certificate_arn" {
  description = "Optional ACM certificate ARN for terminating TLS on the application ALB."
  type        = string
  default     = null
}

variable "allowed_app_ingress_cidrs" {
  description = "Optional CIDRs allowed to reach the application ALB. Defaults depend on whether the ALB is public."
  type        = list(string)
  default     = []
}

variable "public_app_load_balancer" {
  description = "Whether to expose the application ALB publicly on internet-facing subnets."
  type        = bool
  default     = false
}

variable "server_instance_type" {
  type    = string
  default = "m6i.large"
}

variable "worker_instance_type" {
  type    = string
  default = "m6i.large"
}

variable "worker_min_size" {
  type    = number
  default = 2
}

variable "worker_desired_size" {
  type    = number
  default = 2
}

variable "worker_max_size" {
  type    = number
  default = 6
}

variable "server_ami_id" {
  description = "Pinned AMI ID for k3s server nodes. Leave null to use the latest AL2023 x86_64 AMI."
  type        = string
  default     = null
}

variable "worker_ami_id" {
  description = "Pinned AMI ID for k3s worker nodes. Leave null to use the latest AL2023 x86_64 AMI."
  type        = string
  default     = null
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
}

variable "project_name" {
  description = "Project name used for naming resources."
  type        = string
}

variable "environment" {
  description = "Environment name, for example production."
  type        = string
  default     = "production"
}

variable "availability_zones" {
  description = "At least two AZs for HA."
  type        = list(string)
}

variable "vpc_cidr" {
  description = "Primary VPC CIDR block."
  type        = string
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets used only where necessary, mainly NAT gateways."
  type        = list(string)
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private application subnets."
  type        = list(string)
}

variable "isolated_subnet_cidrs" {
  description = "CIDR blocks for isolated subnets reserved for future stateful services."
  type        = list(string)
}

variable "vpn_client_cidr" {
  description = "CIDR assigned to AWS Client VPN clients."
  type        = string
}

variable "client_vpn_server_certificate_arn" {
  description = "ACM server certificate ARN for the Client VPN endpoint."
  type        = string
}

variable "client_vpn_root_certificate_chain_arn" {
  description = "ACM certificate ARN for the root certificate chain used by Client VPN mutual authentication."
  type        = string
}

variable "allowed_app_ingress_cidrs" {
  description = "Optional extra CIDR ranges allowed to reach the application ALB. Defaults to 0.0.0.0/0 when public, otherwise VPC and VPN CIDRs."
  type        = list(string)
  default     = []
}

variable "public_app_load_balancer" {
  description = "Whether to expose the application ALB publicly on the internet."
  type        = bool
  default     = true
}

variable "ecr_repositories" {
  description = "Map of logical repository keys to ECR repository names."
  type        = map(string)
  default = {
    sample_app      = "sample-app"
    website_backend = "website-backend"
  }
}

variable "tags" {
  description = "Extra tags to apply."
  type        = map(string)
  default     = {}
}

variable "k3s_server_instance_type" {
  description = "Instance type for the three fixed k3s server nodes."
  type        = string
  default     = "m6i.large"
}

variable "k3s_server_subnet_indexes" {
  description = "Indexes into the private subnet list for placing the three k3s server nodes."
  type        = list(number)
  default     = [0, 1, 2]
}

variable "k3s_worker_instance_type" {
  description = "Instance type for autoscaled k3s worker nodes."
  type        = string
  default     = "m6i.large"
}

variable "k3s_worker_min_size" {
  type    = number
  default = 2
}

variable "k3s_worker_desired_size" {
  type    = number
  default = 2
}

variable "k3s_worker_max_size" {
  type    = number
  default = 6
}

variable "k3s_server_ami_id" {
  description = "Pinned AMI ID for the k3s server nodes."
  type        = string
  default     = null
}

variable "k3s_worker_ami_id" {
  description = "Pinned AMI ID for the k3s worker nodes."
  type        = string
  default     = null
}

variable "app_ingress_acm_certificate_arn" {
  description = "Optional ACM certificate ARN for HTTPS on the application ALB."
  type        = string
  default     = null
}

variable "ssh_key_name" {
  description = "Optional EC2 key pair name. Leave null for SSM-only administration."
  type        = string
  default     = null
}

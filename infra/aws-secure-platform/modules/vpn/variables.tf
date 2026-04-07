variable "name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "associated_subnet_ids" {
  type = list(string)
}

variable "client_cidr_block" {
  type = string
}

variable "server_certificate_arn" {
  type = string
}

variable "root_certificate_chain_arn" {
  type = string
}

variable "cloudwatch_log_retention_in_days" {
  type    = number
  default = 90
}

variable "tags" {
  type    = map(string)
  default = {}
}

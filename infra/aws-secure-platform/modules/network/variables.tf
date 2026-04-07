variable "name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "availability_zones" {
  type = list(string)
}

variable "vpc_cidr" {
  type = string
}

variable "public_subnet_cidrs" {
  type = list(string)
}

variable "private_subnet_cidrs" {
  type = list(string)
}

variable "isolated_subnet_cidrs" {
  type = list(string)
}

variable "vpn_client_cidr" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

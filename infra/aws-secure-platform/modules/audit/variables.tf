variable "name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "flow_log_iam_scope" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

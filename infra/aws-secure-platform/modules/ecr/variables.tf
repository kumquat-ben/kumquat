# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
variable "name" {
  type = string
}

variable "repositories" {
  type = map(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}

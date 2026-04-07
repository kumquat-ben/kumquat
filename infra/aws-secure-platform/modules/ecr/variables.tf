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

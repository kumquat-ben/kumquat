# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
variable "kubeconfig_path" {
  type = string
}

variable "namespace" {
  type    = string
  default = "kumquat"
}

variable "release_name" {
  type    = string
  default = "kumquat-blockchain"
}

variable "image_repository" {
  type = string
}

variable "image_tag" {
  type = string
}

variable "image_pull_secrets" {
  type    = list(string)
  default = ["ecr-pull-secret"]
}

variable "replica_count" {
  type    = number
  default = 1
}

variable "network_name" {
  type    = string
  default = "dev"
}

variable "storage_class_name" {
  type    = string
  default = "kumquat-blockchain-gp3"
}

variable "create_storage_class" {
  type    = bool
  default = true
}

variable "existing_storage_class_name" {
  type    = string
  default = null
}

variable "existing_pvc_name" {
  type    = string
  default = null
}

variable "storage_size" {
  type    = string
  default = "200Gi"
}

variable "node_selector" {
  type = map(string)
  default = {
    workload = "application"
  }
}

variable "rpc_service_enabled" {
  type    = bool
  default = true
}

variable "rpc_service_type" {
  type    = string
  default = "ClusterIP"
}

variable "bootstrap_nodes" {
  type    = list(string)
  default = []
}

variable "dht_bootstrap_nodes" {
  type    = list(string)
  default = []
}

variable "enable_mining" {
  type    = bool
  default = true
}

variable "mining_threads" {
  type    = number
  default = 1
}

variable "chain_id" {
  type    = number
  default = 1337
}

variable "genesis_timestamp" {
  type    = number
  default = 1744067299
}

variable "genesis_initial_accounts" {
  type = list(object({
    address      = string
    balance      = number
    account_type = string
  }))
  default = [
    {
      address      = "0000000000000000000000000000000000000000000000000000000000000001"
      balance      = 1000000000
      account_type = "User"
    },
    {
      address      = "0000000000000000000000000000000000000000000000000000000000000002"
      balance      = 1000000000
      account_type = "User"
    },
  ]
}

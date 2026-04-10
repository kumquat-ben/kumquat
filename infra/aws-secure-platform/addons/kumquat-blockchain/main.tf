# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
locals {
  blockchain_storage_class_name = var.create_storage_class ? kubernetes_storage_class_v1.blockchain[0].metadata[0].name : var.existing_storage_class_name
}

resource "kubernetes_namespace_v1" "kumquat" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_storage_class_v1" "blockchain" {
  count = var.create_storage_class ? 1 : 0

  metadata {
    name = var.storage_class_name
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Retain"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  parameters = {
    type      = "gp3"
    encrypted = "true"
    fsType    = "ext4"
  }
}

resource "helm_release" "blockchain" {
  name             = var.release_name
  chart            = "${path.module}/../../helm/apps/kumquat-blockchain"
  namespace        = kubernetes_namespace_v1.kumquat.metadata[0].name
  create_namespace = false

  values = [yamlencode({
    image = {
      repository  = var.image_repository
      tag         = var.image_tag
      pullSecrets = var.image_pull_secrets
    }
    replicaCount = var.replica_count
    networkName  = var.network_name
    service = {
      rpc = {
        enabled = var.rpc_service_enabled
        type    = var.rpc_service_type
      }
    }
    config = {
      bootstrapNodes    = var.bootstrap_nodes
      dhtBootstrapNodes = var.dht_bootstrap_nodes
      chainId           = var.chain_id
      enableMining      = var.enable_mining
      miningThreads     = var.mining_threads
      initialDifficulty = 100
    }
    genesis = {
      chainId           = var.chain_id
      timestamp         = var.genesis_timestamp
      initialDifficulty = 100
      initialAccounts = [
        for account in var.genesis_initial_accounts : {
          address     = account.address
          balance     = account.balance
          accountType = account.account_type
        }
      ]
    }
    persistence = {
      existingClaim    = var.existing_pvc_name
      storageClassName = local.blockchain_storage_class_name
      size             = var.storage_size
    }
    nodeSelector = var.node_selector
  })]

  depends_on = [
    kubernetes_namespace_v1.kumquat,
    kubernetes_storage_class_v1.blockchain,
  ]
}

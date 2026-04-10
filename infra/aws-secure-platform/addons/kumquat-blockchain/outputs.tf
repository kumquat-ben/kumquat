# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "namespace" {
  value = var.namespace
}

output "storage_class_name" {
  value = local.blockchain_storage_class_name
}

output "release_name" {
  value = helm_release.blockchain.name
}

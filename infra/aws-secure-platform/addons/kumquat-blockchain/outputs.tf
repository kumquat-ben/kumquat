# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "namespace" {
  value = var.namespace
}

output "storage_class_name" {
  value = kubernetes_storage_class_v1.blockchain.metadata[0].name
}

output "release_name" {
  value = helm_release.blockchain.name
}

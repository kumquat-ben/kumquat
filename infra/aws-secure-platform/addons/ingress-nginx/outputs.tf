# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "helm_release_status" {
  value = helm_release.ingress_nginx.status
}

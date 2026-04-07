output "helm_release_status" {
  value = helm_release.ingress_nginx.status
}

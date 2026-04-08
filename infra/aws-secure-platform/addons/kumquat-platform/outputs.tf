output "backend_namespace" {
  value = var.namespace
}

output "mysql_storage_class_name" {
  value = kubernetes_storage_class_v1.mysql.metadata[0].name
}

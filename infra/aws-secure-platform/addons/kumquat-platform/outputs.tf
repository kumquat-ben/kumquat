# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "backend_namespace" {
  value = var.namespace
}

output "mysql_storage_class_name" {
  value = kubernetes_storage_class_v1.mysql.metadata[0].name
}

output "mysql_backup_bucket_name" {
  value = aws_s3_bucket.mysql_backup.bucket
}

output "mysql_backup_cronjob_name" {
  value = kubernetes_cron_job_v1.mysql_backup.metadata[0].name
}

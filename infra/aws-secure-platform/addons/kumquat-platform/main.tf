# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
locals {
  mysql_release_name          = "kumquat-mysql"
  elasticsearch_release_name  = "kumquat-elasticsearch"
  backend_secret_name         = "kumquat-backend-env"
  mysql_bootstrap_secret_name = "kumquat-mysql-bootstrap"
  mysql_backup_secret_name    = "kumquat-mysql-backup-aws"
  mysql_backup_job_name       = "kumquat-mysql-backup"
  mysql_service_host          = "${local.mysql_release_name}.${var.namespace}.svc.cluster.local"
  mysql_backup_bucket_name    = coalesce(var.mysql_backup_bucket_name, "${replace(var.hostname, ".", "-")}-${var.namespace}-${data.aws_caller_identity.current.account_id}-mysql-backups")
}

data "aws_caller_identity" "current" {}

resource "aws_kms_key" "mysql_backup" {
  description             = "KMS key for MySQL backups stored in S3 for ${var.namespace}"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "mysql_backup" {
  name          = "alias/${var.namespace}-mysql-backup"
  target_key_id = aws_kms_key.mysql_backup.key_id
}

resource "aws_s3_bucket" "mysql_backup" {
  bucket        = local.mysql_backup_bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "mysql_backup" {
  bucket = aws_s3_bucket.mysql_backup.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mysql_backup" {
  bucket = aws_s3_bucket.mysql_backup.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.mysql_backup.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "mysql_backup" {
  bucket = aws_s3_bucket.mysql_backup.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "mysql_backup" {
  bucket = aws_s3_bucket.mysql_backup.id

  rule {
    id     = "expire-old-backups"
    status = "Enabled"

    filter {}

    expiration {
      days = var.mysql_backup_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.mysql_backup_retention_days
    }
  }
}

resource "aws_s3_bucket_policy" "mysql_backup" {
  bucket = aws_s3_bucket.mysql_backup.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.mysql_backup.arn,
          "${aws_s3_bucket.mysql_backup.arn}/*",
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
    ]
  })
}

resource "aws_iam_user" "mysql_backup" {
  name = "${var.namespace}-mysql-backup"
}

resource "aws_iam_access_key" "mysql_backup" {
  user = aws_iam_user.mysql_backup.name
}

resource "aws_iam_user_policy" "mysql_backup" {
  name = "${var.namespace}-mysql-backup"
  user = aws_iam_user.mysql_backup.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBackupBucket"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
        ]
        Resource = aws_s3_bucket.mysql_backup.arn
      },
      {
        Sid    = "WriteBackups"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.mysql_backup.arn}/${var.mysql_backup_bucket_prefix}/*"
      },
      {
        Sid    = "UseBackupKey"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:GenerateDataKey",
        ]
        Resource = aws_kms_key.mysql_backup.arn
      },
    ]
  })
}

resource "kubernetes_namespace_v1" "mysql_operator" {
  metadata {
    name = var.mysql_operator_namespace
  }
}

resource "kubernetes_namespace_v1" "kumquat" {
  metadata {
    name = var.namespace
  }
}

resource "helm_release" "aws_ebs_csi_driver" {
  name              = "aws-ebs-csi-driver"
  repository        = "https://kubernetes-sigs.github.io/aws-ebs-csi-driver"
  chart             = "aws-ebs-csi-driver"
  namespace         = "kube-system"
  version           = "2.38.1"
  dependency_update = true
}

resource "kubernetes_storage_class_v1" "mysql" {
  metadata {
    name = var.mysql_storage_class_name
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

  depends_on = [helm_release.aws_ebs_csi_driver]
}

resource "helm_release" "mysql_operator" {
  name              = "kumquat-mysql-operator"
  chart             = "${path.module}/../../helm/platform/kumquat-mysql-operator"
  namespace         = kubernetes_namespace_v1.mysql_operator.metadata[0].name
  create_namespace  = false
  dependency_update = true
}

resource "kubernetes_secret_v1" "mysql_bootstrap" {
  metadata {
    name      = local.mysql_bootstrap_secret_name
    namespace = kubernetes_namespace_v1.kumquat.metadata[0].name
  }

  data = {
    root-password = var.mysql_root_password
    app-password  = var.mysql_app_password
  }
}

resource "helm_release" "mysql_cluster" {
  name              = local.mysql_release_name
  chart             = "${path.module}/../../helm/apps/kumquat-mysql-cluster"
  namespace         = kubernetes_namespace_v1.kumquat.metadata[0].name
  create_namespace  = false
  dependency_update = true

  values = [yamlencode({
    mysql-innodbcluster = {
      serverInstances = 3
      routerInstances = 2
      datadirVolumeClaimTemplate = {
        accessModes      = "ReadWriteOnce"
        storageClassName = var.mysql_storage_class_name
        resources = {
          requests = {
            storage = var.mysql_storage_size
          }
        }
      }
      podSpec = {
        nodeSelector = {
          workload = "application"
        }
      }
      router = {
        instances = 2
      }
    }
    bootstrap = {
      enabled      = true
      secretName   = local.mysql_bootstrap_secret_name
      databaseName = var.mysql_database_name
      appUser      = var.mysql_app_user
    }
  })]

  set_sensitive {
    name  = "mysql-innodbcluster.credentials.root.password"
    value = var.mysql_root_password
  }

  depends_on = [
    helm_release.mysql_operator,
    kubernetes_storage_class_v1.mysql,
    kubernetes_secret_v1.mysql_bootstrap,
  ]
}

resource "kubernetes_secret_v1" "mysql_backup_aws" {
  metadata {
    name      = local.mysql_backup_secret_name
    namespace = kubernetes_namespace_v1.kumquat.metadata[0].name
  }

  data = {
    AWS_ACCESS_KEY_ID     = aws_iam_access_key.mysql_backup.id
    AWS_SECRET_ACCESS_KEY = aws_iam_access_key.mysql_backup.secret
    AWS_DEFAULT_REGION    = var.aws_region
    S3_BUCKET_NAME        = aws_s3_bucket.mysql_backup.bucket
    S3_PREFIX             = var.mysql_backup_bucket_prefix
  }
}

resource "kubernetes_cron_job_v1" "mysql_backup" {
  metadata {
    name      = local.mysql_backup_job_name
    namespace = kubernetes_namespace_v1.kumquat.metadata[0].name
  }

  lifecycle {
    ignore_changes = [
      metadata[0].annotations,
      metadata[0].labels,
      spec[0].job_template[0].metadata[0],
      spec[0].job_template[0].spec[0].completions,
      spec[0].job_template[0].spec[0].parallelism,
      spec[0].job_template[0].spec[0].template[0].metadata[0],
      spec[0].job_template[0].spec[0].template[0].spec[0].automount_service_account_token,
      spec[0].job_template[0].spec[0].template[0].spec[0].enable_service_links,
      spec[0].job_template[0].spec[0].template[0].spec[0].scheduler_name,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].resources,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].termination_message_policy,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[4].value_from[0].secret_key_ref[0].optional,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[6].value_from[0].secret_key_ref[0].optional,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[7].value_from[0].secret_key_ref[0].optional,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[8].value_from[0].secret_key_ref[0].optional,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[9].value_from[0].secret_key_ref[0].optional,
      spec[0].job_template[0].spec[0].template[0].spec[0].container[0].env[10].value_from[0].secret_key_ref[0].optional,
    ]
  }

  spec {
    schedule                      = var.mysql_backup_schedule
    concurrency_policy            = "Forbid"
    failed_jobs_history_limit     = 3
    successful_jobs_history_limit = 3
    suspend                       = var.mysql_backup_suspend

    job_template {
      metadata {}

      spec {
        backoff_limit = 1

        template {
          metadata {
            labels = {
              app = local.mysql_backup_job_name
            }
          }

          spec {
            restart_policy                  = "Never"
            automount_service_account_token = true
            node_selector = {
              workload = "application"
            }

            image_pull_secrets {
              name = "ecr-registry"
            }

            container {
              name              = "mysql-backup"
              image             = "${var.mysql_backup_image_repository}:${var.mysql_backup_image_tag}"
              image_pull_policy = var.mysql_backup_image_pull_policy

              env {
                name  = "MYSQL_HOST"
                value = local.mysql_service_host
              }

              env {
                name  = "MYSQL_PORT"
                value = "3306"
              }

              env {
                name  = "MYSQL_DATABASE"
                value = var.mysql_database_name
              }

              env {
                name  = "MYSQL_USER"
                value = "root"
              }

              env {
                name = "MYSQL_PASSWORD"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_bootstrap.metadata[0].name
                    key  = "root-password"
                  }
                }
              }

              env {
                name  = "MYSQLDUMP_EXTRA_ARGS"
                value = "--single-transaction --quick --lock-tables=false --routines --triggers --events"
              }

              env {
                name = "AWS_ACCESS_KEY_ID"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_backup_aws.metadata[0].name
                    key  = "AWS_ACCESS_KEY_ID"
                  }
                }
              }

              env {
                name = "AWS_SECRET_ACCESS_KEY"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_backup_aws.metadata[0].name
                    key  = "AWS_SECRET_ACCESS_KEY"
                  }
                }
              }

              env {
                name = "AWS_DEFAULT_REGION"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_backup_aws.metadata[0].name
                    key  = "AWS_DEFAULT_REGION"
                  }
                }
              }

              env {
                name = "S3_BUCKET_NAME"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_backup_aws.metadata[0].name
                    key  = "S3_BUCKET_NAME"
                  }
                }
              }

              env {
                name = "S3_PREFIX"

                value_from {
                  secret_key_ref {
                    name = kubernetes_secret_v1.mysql_backup_aws.metadata[0].name
                    key  = "S3_PREFIX"
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  depends_on = [
    helm_release.mysql_cluster,
    kubernetes_secret_v1.mysql_backup_aws,
  ]
}

resource "kubernetes_secret_v1" "backend_env" {
  metadata {
    name      = local.backend_secret_name
    namespace = kubernetes_namespace_v1.kumquat.metadata[0].name
  }

  data = {
    DJANGO_SECRET_KEY              = var.backend_secret_key
    GOOGLE_OAUTH_CLIENT_ID         = var.google_oauth_client_id
    GOOGLE_OAUTH_CLIENT_SECRET     = var.google_oauth_client_secret
    GOOGLE_OAUTH_REDIRECT_URI      = var.google_oauth_redirect_uri
    VONAGE_ACCOUNT_SECRET          = var.vonage_account_secret
    VONAGE_SMS_SIGNATURE_SECRET    = var.vonage_sms_signature_secret
    VONAGE_SMS_SIGNATURE_ALGORITHM = var.vonage_sms_signature_algorithm
    MYSQL_DATABASE                 = var.mysql_database_name
    MYSQL_USER                     = var.mysql_app_user
    MYSQL_PASSWORD                 = var.mysql_app_password
    MYSQL_HOST                     = local.mysql_service_host
    MYSQL_PORT                     = "3306"
  }
}

resource "helm_release" "elasticsearch" {
  name             = local.elasticsearch_release_name
  chart            = "${path.module}/../../helm/apps/kumquat-elasticsearch"
  namespace        = kubernetes_namespace_v1.kumquat.metadata[0].name
  create_namespace = false

  values = [yamlencode({
    persistence = {
      storageClassName = var.mysql_storage_class_name
    }
    nodeSelector = {
      workload = "application"
    }
  })]

  depends_on = [
    kubernetes_storage_class_v1.mysql,
  ]
}

resource "helm_release" "backend" {
  name             = "kumquat-backend"
  chart            = "${path.module}/../../helm/apps/kumquat-backend"
  namespace        = kubernetes_namespace_v1.kumquat.metadata[0].name
  create_namespace = false

  values = [yamlencode({
    replicaCount = 2
    image = {
      repository = var.backend_image_repository
      tag        = var.backend_image_tag
    }
    env = {
      existingSecret = local.backend_secret_name
      common = {
        PORT                         = "8000"
        DJANGO_DEBUG                 = "false"
        DJANGO_ALLOWED_HOSTS         = "*"
        DJANGO_CSRF_TRUSTED_ORIGINS  = "https://${var.hostname},https://*.node.${var.hostname}"
        DJANGO_SESSION_COOKIE_DOMAIN = ".${var.hostname}"
        DJANGO_CSRF_COOKIE_DOMAIN    = ".${var.hostname}"
        GUNICORN_WORKERS             = "2"
        GUNICORN_THREADS             = "2"
        GUNICORN_TIMEOUT             = "60"
        NODE_LAUNCHER_ENABLED        = "true"
        ELASTICSEARCH_URL            = "http://${local.elasticsearch_release_name}:9200"
        ELASTICSEARCH_INDEX_PREFIX   = "kumquat"
      }
    }
    ingress = {
      enabled   = true
      className = "nginx"
      host      = var.hostname
      paths = [
        {
          path     = "/"
          pathType = "Prefix"
        },
      ]
      extraHosts = [
        {
          host = "*.node.${var.hostname}"
          paths = [
            {
              path     = "/"
              pathType = "Prefix"
            },
          ]
        },
      ]
    }
    nodeLauncher = {
      dind = {
        enabled = true
      }
    }
    nodeSelector = {
      workload = "application"
    }
  })]

  depends_on = [
    helm_release.mysql_cluster,
    helm_release.elasticsearch,
    kubernetes_secret_v1.backend_env,
  ]
}

locals {
  mysql_release_name          = "kumquat-mysql"
  backend_secret_name         = "kumquat-backend-env"
  mysql_bootstrap_secret_name = "kumquat-mysql-bootstrap"
  mysql_router_host           = "${local.mysql_release_name}-router.${var.namespace}.svc.cluster.local"
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

resource "kubernetes_secret_v1" "backend_env" {
  metadata {
    name      = local.backend_secret_name
    namespace = kubernetes_namespace_v1.kumquat.metadata[0].name
  }

  data = {
    DJANGO_SECRET_KEY = var.backend_secret_key
    MYSQL_DATABASE    = var.mysql_database_name
    MYSQL_USER        = var.mysql_app_user
    MYSQL_PASSWORD    = var.mysql_app_password
    MYSQL_HOST        = local.mysql_router_host
    MYSQL_PORT        = "3306"
  }
}

resource "helm_release" "backend" {
  name             = "kumquat-backend"
  chart            = "${path.module}/../../helm/apps/kumquat-backend"
  namespace        = kubernetes_namespace_v1.kumquat.metadata[0].name
  create_namespace = false

  values = [yamlencode({
    image = {
      repository = var.backend_image_repository
      tag        = var.backend_image_tag
    }
    env = {
      existingSecret = local.backend_secret_name
      common = {
        PORT                        = "8000"
        DJANGO_DEBUG                = "false"
        DJANGO_ALLOWED_HOSTS        = "*"
        DJANGO_CSRF_TRUSTED_ORIGINS = "https://${var.hostname}"
        GUNICORN_WORKERS            = "2"
        GUNICORN_THREADS            = "2"
        GUNICORN_TIMEOUT            = "60"
      }
    }
    ingress = {
      enabled   = true
      className = "nginx"
      host      = var.hostname
      path      = "/api"
      pathType  = "Prefix"
    }
    nodeSelector = {
      workload = "application"
    }
  })]

  depends_on = [
    helm_release.mysql_cluster,
    kubernetes_secret_v1.backend_env,
  ]
}

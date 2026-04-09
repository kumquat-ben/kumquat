# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
provider "aws" {
  region = var.aws_region
}

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes {
    config_path = var.kubeconfig_path
  }
}

# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.tags
  }
}

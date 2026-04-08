# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "repository_urls" {
  value = {
    for key, repo in aws_ecr_repository.this : key => repo.repository_url
  }
}

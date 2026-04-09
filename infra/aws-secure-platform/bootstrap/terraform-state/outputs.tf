# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "bucket_name" {
  value = aws_s3_bucket.terraform_state.bucket
}

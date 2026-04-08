# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

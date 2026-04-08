# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
output "client_vpn_endpoint_id" {
  value = aws_ec2_client_vpn_endpoint.this.id
}

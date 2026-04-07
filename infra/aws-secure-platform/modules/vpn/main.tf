resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/${var.name}/client-vpn"
  retention_in_days = var.cloudwatch_log_retention_in_days
}

resource "aws_cloudwatch_log_stream" "this" {
  name           = "connections"
  log_group_name = aws_cloudwatch_log_group.this.name
}

resource "aws_security_group" "vpn" {
  name        = "${var.name}-client-vpn-sg"
  description = "Restricts Client VPN traffic into the VPC."
  vpc_id      = var.vpc_id

  egress {
    description = "Permit VPN clients to reach the VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = merge(var.tags, {
    Name = "${var.name}-client-vpn-sg"
  })
}

resource "aws_ec2_client_vpn_endpoint" "this" {
  description            = "${var.name} administrators VPN"
  server_certificate_arn = var.server_certificate_arn
  client_cidr_block      = var.client_cidr_block
  split_tunnel           = true
  security_group_ids     = [aws_security_group.vpn.id]
  vpc_id                 = var.vpc_id
  self_service_portal    = "disabled"
  dns_servers            = [cidrhost(var.vpc_cidr, 2)]
  transport_protocol     = "udp"
  vpn_port               = 443

  authentication_options {
    type                       = "certificate-authentication"
    root_certificate_chain_arn = var.root_certificate_chain_arn
  }

  connection_log_options {
    enabled               = true
    cloudwatch_log_group  = aws_cloudwatch_log_group.this.name
    cloudwatch_log_stream = aws_cloudwatch_log_stream.this.name
  }

  tags = merge(var.tags, {
    Name = "${var.name}-client-vpn"
  })
}

resource "aws_ec2_client_vpn_network_association" "this" {
  for_each = {
    for idx, subnet_id in var.associated_subnet_ids : idx => subnet_id
  }

  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.this.id
  subnet_id              = each.value
}

resource "aws_ec2_client_vpn_authorization_rule" "vpc" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.this.id
  target_network_cidr    = var.vpc_cidr
  authorize_all_groups   = true
  description            = "Allow VPN clients to reach the VPC"
}

resource "aws_cloudwatch_log_metric_filter" "failed_connections" {
  name           = "${var.name}-client-vpn-failed-connections"
  log_group_name = aws_cloudwatch_log_group.this.name
  pattern        = "\"FAILED\""

  metric_transformation {
    name      = "FailedClientVpnConnections"
    namespace = "${var.name}/VPN"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "failed_connections" {
  alarm_name          = "${var.name}-client-vpn-failed-connections"
  alarm_description   = "Client VPN connection failures detected."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.failed_connections.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.failed_connections.metric_transformation[0].namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"
}

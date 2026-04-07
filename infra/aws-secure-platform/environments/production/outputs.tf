output "aws_region" {
  description = "AWS region used by the deployment."
  value       = var.aws_region
}

output "vpc_id" {
  description = "VPC ID."
  value       = module.network.vpc_id
}

output "cluster_name" {
  description = "Logical cluster name."
  value       = local.name
}

output "cluster_endpoint" {
  description = "Private k3s Kubernetes API endpoint."
  value       = module.k3s.api_endpoint
}

output "app_alb_dns_name" {
  description = "Application ALB DNS name for website ingress."
  value       = module.k3s.app_alb_dns_name
}

output "k3s_cluster_token_parameter_name" {
  description = "SSM parameter name holding the k3s cluster token."
  value       = module.k3s.cluster_token_parameter_name
}

output "server_instance_ids" {
  description = "k3s server instance IDs."
  value       = module.k3s.server_instance_ids
}

output "client_vpn_endpoint_id" {
  description = "AWS Client VPN endpoint ID."
  value       = module.vpn.client_vpn_endpoint_id
}

output "ecr_repository_urls" {
  description = "ECR repository URLs keyed by logical name."
  value       = module.ecr.repository_urls
}

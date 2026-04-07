output "api_endpoint" {
  value = aws_lb.api.dns_name
}

output "app_internal_alb_dns_name" {
  value = aws_lb.app.dns_name
}

output "cluster_token_parameter_name" {
  value = aws_ssm_parameter.cluster_token.name
}

output "server_instance_ids" {
  value = aws_instance.server[*].id
}

output "worker_autoscaling_group_name" {
  value = aws_autoscaling_group.worker.name
}

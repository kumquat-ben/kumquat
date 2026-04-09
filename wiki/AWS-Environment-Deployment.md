# AWS Environment Deployment

Use this guide when you need to create or recreate the AWS foundation for Kumquat.

## What Gets Created

- a multi-AZ VPC
- public, private, and isolated subnets
- an AWS Client VPN endpoint
- ECR repositories for the website and backend
- a private k3s cluster on EC2
- logging, audit, and monitoring resources

## Prerequisites

- Terraform `>= 1.5`
- AWS CLI v2
- `jq`
- `kubectl`
- `helm`
- Docker

You also need the required ACM certificate ARNs for VPN and ingress.

## Basic Flow

```bash
cd infra/aws-secure-platform/environments/production
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

## Export Important Outputs

```bash
terraform output -raw aws_region
terraform output -raw cluster_name
terraform output -raw cluster_endpoint
terraform output -raw client_vpn_endpoint_id
terraform output -raw app_alb_dns_name
terraform output -json ecr_repository_urls | jq
```

## Common Mistakes

- running Terraform in the wrong AWS account
- forgetting ACM certificate ARNs
- attempting cluster access before the VPN path works
- assuming the cluster API is public

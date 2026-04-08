# AWS Environment Deployment

This document explains how to provision the Kumquat AWS environment using Terraform and the AWS CLI. It is the environment build guide, not the day-to-day app deployment guide.

Use this when you need to create or recreate the production-minded AWS foundation:

- VPC and subnets
- AWS Client VPN
- Amazon ECR repositories
- k3s control plane and workers
- baseline logging and audit resources

For VPN connection details after the environment exists, use [vpn-and-helm-deployment.md](vpn-and-helm-deployment.md).

## What Gets Created

The AWS environment under [`infra/aws-secure-platform`](../infra/aws-secure-platform) provisions:

- a multi-AZ VPC
- public, private, and isolated subnets
- an AWS Client VPN endpoint for operator access
- ECR repositories for the website and backend
- a private k3s cluster on EC2
- logging, audit, and monitoring resources

## Prerequisites

Install these tools on the operator workstation:

- Terraform `>= 1.5`
- AWS CLI v2
- `jq`
- `kubectl`
- `helm`
- Docker

You also need:

- an AWS account with permissions to create networking, IAM, EC2, Auto Scaling, ECR, load balancers, SSM, CloudTrail, CloudWatch, and Client VPN resources
- ACM certificate ARNs for the AWS Client VPN server certificate and client root certificate chain
- an ACM certificate ARN for the application load balancer if HTTPS termination is enabled there

## 1. Authenticate With AWS CLI

Confirm the CLI is pointed at the correct account before touching Terraform:

```bash
aws configure
aws sts get-caller-identity
aws ec2 describe-availability-zones --region us-west-2
```

If your team uses named profiles, prefer:

```bash
export AWS_PROFILE=production
export AWS_REGION=us-west-2
aws sts get-caller-identity
```

## 2. Configure The Production Environment

Move into the production stack:

```bash
cd infra/aws-secure-platform/environments/production
cp terraform.tfvars.example terraform.tfvars
```

Set real values in `terraform.tfvars` for:

- `aws_region`
- `project_name`
- `environment`
- `availability_zones`
- `vpc_cidr`
- private, public, and isolated subnet CIDRs
- `vpn_client_cidr`
- `client_vpn_server_certificate_arn`
- `client_vpn_root_certificate_chain_arn`
- `app_ingress_acm_certificate_arn`
- `public_app_load_balancer`

## 3. Initialize And Review Terraform

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan
```

Review the plan carefully. Pay attention to:

- region and account alignment
- VPN endpoint creation
- ECR repositories
- instance counts for k3s servers and workers
- load balancer and certificate wiring

## 4. Apply The Environment

```bash
terraform apply
```

After the apply completes, export the outputs you will need later:

```bash
terraform output
terraform output -raw aws_region
terraform output -raw cluster_name
terraform output -raw cluster_endpoint
terraform output -raw client_vpn_endpoint_id
terraform output -raw app_alb_dns_name
terraform output -json ecr_repository_urls | jq
```

## 5. Verify AWS Resources With AWS CLI

Use the AWS CLI to confirm the environment matches the Terraform outputs:

```bash
aws ec2 describe-vpcs --region "$AWS_REGION"
aws ec2 describe-client-vpn-endpoints --region "$AWS_REGION"
aws ecr describe-repositories --region "$AWS_REGION"
aws autoscaling describe-auto-scaling-groups --region "$AWS_REGION"
```

Optional focused checks:

```bash
aws elbv2 describe-load-balancers --region "$AWS_REGION"
aws ssm describe-parameters --region "$AWS_REGION"
aws cloudtrail describe-trails --region "$AWS_REGION"
```

## 6. Build The Operator Handoff

Once the base environment exists, record the values operators need:

- AWS region
- cluster endpoint
- Client VPN endpoint ID
- ALB DNS name
- ECR repository URLs
- kubeconfig retrieval procedure

The next operational step is private-cluster access and application rollout. Use [vpn-and-helm-deployment.md](vpn-and-helm-deployment.md).

## Common Mistakes

- Running Terraform in the wrong AWS account or profile
- Forgetting to supply ACM certificate ARNs for VPN or ingress
- Attempting `kubectl` access before the VPN path is working
- Assuming the cluster API is public; it is private-only by design

## Related Repo References

- [`infra/aws-secure-platform/README.md`](../infra/aws-secure-platform/README.md)
- [`infra/aws-secure-platform/environments/production`](../infra/aws-secure-platform/environments/production)
- [`infra/aws-secure-platform/VPN.md`](../infra/aws-secure-platform/VPN.md)

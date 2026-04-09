# VPN and Deployment

Use this after the AWS environment already exists.

## Why This Exists

Kumquat runs on a private k3s API. Operators cannot safely deploy the backend platform until they:

1. connect to AWS Client VPN
2. verify private cluster reachability
3. prepare a working kubeconfig
4. run the backend deployment flow

## Export The VPN Profile

```bash
aws ec2 export-client-vpn-client-configuration \
  --region us-west-2 \
  --client-vpn-endpoint-id cvpn-endpoint-08962574330901031 \
  --query 'ClientConfiguration' \
  --output text > .local/aws-secure-platform/kumquat-production.ovpn
```

## Verify Access

```bash
KUBECONFIG="$(pwd)/.local/aws-secure-platform/kubeconfig-production" kubectl get nodes
```

## Push Application Images

```bash
cd infra/aws-secure-platform/environments/production
AWS_REGION="$(terraform output -raw aws_region)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
```

## Deploy The Website

```bash
cd infra/aws-secure-platform/kubernetes/example-app
kubectl apply -k .
kubectl rollout status deployment/sample-app -n sample-app
```

## Deploy The Backend Platform

```bash
cd infra/aws-secure-platform/addons/kumquat-platform
cp backend.hcl.example backend.hcl
terraform init -backend-config=backend.hcl
cp terraform.tfvars.example terraform.tfvars
```

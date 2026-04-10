# VPN And Helm Deployment

This document explains how operators connect to the private production cluster and deploy the Kumquat application components that depend on cluster access.

Use this after the AWS environment already exists.

## Why This Exists

Kumquat runs on a private k3s API. Operators cannot safely deploy the backend platform until they:

1. connect to AWS Client VPN
2. verify private cluster reachability
3. prepare a working kubeconfig
4. run the add-on deployment flow that installs the backend platform with Helm via Terraform

## Deployment Model

Current production rollout is split across two paths:

- website frontend: container image push plus Kubernetes manifests under [`infra/aws-secure-platform/kubernetes/example-app`](../infra/aws-secure-platform/kubernetes/example-app)
- backend platform: Terraform add-on under [`infra/aws-secure-platform/addons/kumquat-platform`](../infra/aws-secure-platform/addons/kumquat-platform), which deploys Helm-managed resources for the backend and MySQL stack

## 1. Export The VPN Profile

The production VPN is AWS Client VPN in `us-west-2`. Export the profile:

```bash
aws ec2 export-client-vpn-client-configuration \
  --region us-west-2 \
  --client-vpn-endpoint-id cvpn-endpoint-08962574330901031 \
  --query 'ClientConfiguration' \
  --output text > .local/aws-secure-platform/kumquat-production.ovpn
```

If your endpoint ID changes in Terraform outputs, use the new value instead of the example above.

## 2. Attach Client Certificates

This repo already expects certificate material under `.local/aws-secure-platform/certs/`:

- `.local/aws-secure-platform/certs/vpn-ca.crt`
- `.local/aws-secure-platform/certs/vpn-client.crt`
- `.local/aws-secure-platform/certs/vpn-client.key`

Either reference them from the `.ovpn` file:

```ovpn
ca /absolute/path/to/.local/aws-secure-platform/certs/vpn-ca.crt
cert /absolute/path/to/.local/aws-secure-platform/certs/vpn-client.crt
key /absolute/path/to/.local/aws-secure-platform/certs/vpn-client.key
```

Or embed them if your client requires that. Do not commit embedded private key material.

## 3. Connect And Verify The VPN

Import the profile into AWS VPN Client, OpenVPN Connect, or another compatible OpenVPN client, then connect.

Verify private access:

```bash
nslookup kumquat-production-api-a26a03b163798460.elb.us-west-2.amazonaws.com
KUBECONFIG="$(pwd)/.local/aws-secure-platform/kubeconfig-production" kubectl get nodes
```

If `kubectl` hangs, the most likely causes are:

- VPN is disconnected
- the wrong kubeconfig is active
- private DNS is not resolving through the VPN path

## 4. Prepare kubeconfig

If a current kubeconfig is not already present locally, generate one from a k3s server node:

```bash
cd infra/aws-secure-platform/environments/production
K3S_ENDPOINT="$(terraform output -raw cluster_endpoint)"
aws ssm start-session --target "$(terraform output -json server_instance_ids | jq -r '.[0]')"
```

On the server node:

```bash
sudo cat /etc/rancher/k3s/k3s.yaml | \
  sed "s#https://127.0.0.1:6443#https://${K3S_ENDPOINT}:6443#" > ~/kubeconfig
```

Move that kubeconfig to the operator workstation through an approved path, then:

```bash
export KUBECONFIG="$PWD/.local/aws-secure-platform/kubeconfig-production"
kubectl get nodes
kubectl get pods -A
```

## 5. Install Or Update ingress-nginx

If ingress has not been installed yet, apply the ingress add-on:

```bash
cd infra/aws-secure-platform/addons/ingress-nginx
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -var="kubeconfig_path=$KUBECONFIG"
terraform apply -var="kubeconfig_path=$KUBECONFIG"
```

## 6. Push Application Images

From the production environment outputs:

```bash
cd infra/aws-secure-platform/environments/production
AWS_REGION="$(terraform output -raw aws_region)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
```

Push the website image:

```bash
WEBSITE_REPO="$(terraform output -json ecr_repository_urls | jq -r '.sample_app')"
cd ../../../website
docker buildx build --platform linux/amd64 -t "$WEBSITE_REPO:website-YYYYMMDD-HHMMSS" --push .
```

Push the backend image:

```bash
BACKEND_REPO="$(terraform output -json ecr_repository_urls | jq -r '.website_backend')"
cd ../website-backend
docker buildx build --platform linux/amd64 -t "$BACKEND_REPO:backend-YYYYMMDD-HHMMSS" --push .
```

## 7. Deploy The Website

The website is currently deployed with Kubernetes manifests, not Helm:

```bash
cd ../infra/aws-secure-platform/kubernetes/example-app
kubectl apply -k .
kubectl rollout status deployment/sample-app -n sample-app
kubectl get ingress -n sample-app
```

Before applying, update the image tag in [`infra/aws-secure-platform/kubernetes/example-app/deployment.yaml`](../infra/aws-secure-platform/kubernetes/example-app/deployment.yaml).

## 8. Deploy The Backend Platform With Helm Via Terraform

The backend platform add-on manages:

- AWS EBS CSI driver
- storage class
- MySQL operator
- MySQL InnoDB cluster
- Kumquat backend deployment and ingress

Initialize backend state:

```bash
cd ../../addons/kumquat-platform
cp backend.hcl.example backend.hcl
terraform init -backend-config=backend.hcl
cp terraform.tfvars.example terraform.tfvars
```

Plan and apply with real values:

```bash
terraform plan \
  -var="kubeconfig_path=$KUBECONFIG" \
  -var="backend_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/website-backend" \
  -var="backend_image_tag=backend-YYYYMMDD-HHMMSS" \
  -var="backend_secret_key=replace-me" \
  -var="google_oauth_client_id=replace-me" \
  -var="google_oauth_client_secret=replace-me" \
  -var="google_oauth_redirect_uri=https://kumquat.info/api/auth/google/callback" \
  -var="mysql_root_password=replace-me" \
  -var="mysql_app_password=replace-me"

terraform apply \
  -var="kubeconfig_path=$KUBECONFIG" \
  -var="backend_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/website-backend" \
  -var="backend_image_tag=backend-YYYYMMDD-HHMMSS" \
  -var="backend_secret_key=replace-me" \
  -var="google_oauth_client_id=replace-me" \
  -var="google_oauth_client_secret=replace-me" \
  -var="google_oauth_redirect_uri=https://kumquat.info/api/auth/google/callback" \
  -var="mysql_root_password=replace-me" \
  -var="mysql_app_password=replace-me"
```

## 9. Verify The Rollout

```bash
kubectl get pods -n sample-app
kubectl get pods -n kumquat
kubectl get ingress -A
curl -I https://kumquat.info/
curl -I https://kumquat.info/api/healthz
```

## Failure Recovery Notes

- If `kubectl` cannot reach the cluster, stop and fix VPN connectivity first
- If the backend plan fails, check image tag existence, secret values, and kubeconfig path
- If ingress is unhealthy, inspect ingress-nginx service state, ALB health, and namespace ingress objects

## Related Repo References

- [`infra/aws-secure-platform/VPN.md`](../infra/aws-secure-platform/VPN.md)
- [`infra/aws-secure-platform/README.md`](../infra/aws-secure-platform/README.md)
- [`infra/aws-secure-platform/addons/kumquat-platform`](../infra/aws-secure-platform/addons/kumquat-platform)
- [`infra/aws-secure-platform/kubernetes/example-app`](../infra/aws-secure-platform/kubernetes/example-app)

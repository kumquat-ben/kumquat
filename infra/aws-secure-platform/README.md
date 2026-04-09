# Secure AWS Container Platform

This stack builds a production-minded container platform on AWS using Terraform, k3s, Amazon ECR, ACM, CloudTrail, CloudWatch, VPC Flow Logs, SSM Parameter Store, and SSM Session Manager.

## Architecture Summary

### Chosen Design

- k3s rather than EKS, because this repository is now targeting a self-managed k3s cluster on AWS. To avoid an unsafe quorum design, the stack provisions three private k3s server nodes rather than two.
- The Kubernetes API is exposed only through an internal Network Load Balancer on port `6443`. Administrative access is expected to come through AWS Client VPN into the VPC.
- Private worker nodes only. Worker capacity is provided by an Auto Scaling Group in private subnets.
- A dedicated VPC spans at least two AZs with:
  - public subnets used only for NAT gateways
  - private application subnets for k3s server nodes, workers, and internal load balancers
  - isolated subnets reserved for future databases or stateful services
- Ingress is handled by `ingress-nginx` inside the cluster, fronted by a Terraform-managed ALB with ACM TLS termination. The example application can be public or private depending on configuration.
- VPC endpoints are enabled for core AWS services used by the cluster so routine platform traffic stays on AWS private networking as much as possible.
- ECR repositories use immutable tags, on-push vulnerability scanning, lifecycle policies, and encryption at rest.
- CloudTrail, VPC Flow Logs, Client VPN logs, and CloudWatch alarms provide baseline auditability and failure visibility.

### Why This Is Secure

- No direct public SSH access exists. All nodes are private, and the instance roles include SSM so operators can use Session Manager instead of opening SSH to the internet.
- The Kubernetes API is reachable only through an internal NLB, and the server-node security group allows API access only from the VPN CIDR and cluster nodes.
- Least-privilege IAM is used for k3s server nodes, worker nodes, ECR, flow logs, and access to the SecureString cluster token in SSM Parameter Store.
- Storage is encrypted at rest using KMS where it materially improves control: the k3s bootstrap token, CloudTrail, Flow Logs log groups, ECR repositories, and EC2 root volumes.
- Public exposure is minimized. Public subnets exist only because NAT gateways are required for reliable managed node operation and patching. Application ingress remains internal.

### Tradeoffs

- NAT gateways are kept because they remain the most operationally reliable default for node bootstrap, OS updates, and package retrieval. Cost is higher than a NAT instance or endpoint-only design, but reliability and simplicity are better.
- Self-managing k3s on EC2 adds more operational burden than EKS. That is the explicit tradeoff for choosing k3s, and it is why the stack uses three server nodes and opinionated bootstrap automation.
- The application ALB can terminate TLS with ACM, but traffic from the ALB to the ingress controller uses private VPC networking. If strict end-to-end TLS is required, add backend TLS certificates or a service mesh as a follow-on hardening step.
- AWS Client VPN uses certificate-based authentication inputs rather than creating a private CA inside this stack. That keeps the platform stack focused and avoids forcing ACM PCA cost, but certificate issuance and rotation remain an explicit operational step.

## Repository Layout

```text
infra/aws-secure-platform/
├── README.md
├── addons
│   └── ingress-nginx
│       ├── main.tf
│       ├── outputs.tf
│       ├── providers.tf
│       ├── terraform.tfvars.example
│       ├── variables.tf
│       └── versions.tf
├── environments
│   └── production
│       ├── main.tf
│       ├── outputs.tf
│       ├── providers.tf
│       ├── terraform.tfvars.example
│       ├── variables.tf
│       └── versions.tf
├── kubernetes
│   └── example-app
│       ├── deployment.yaml
│       ├── ingress-internal.yaml
│       ├── kustomization.yaml
│       ├── namespace.yaml
│       ├── network-policy.yaml
│       └── service.yaml
└── modules
    ├── audit
    │   ├── main.tf
    │   ├── outputs.tf
    │   └── variables.tf
    ├── ecr
    │   ├── main.tf
    │   ├── outputs.tf
    │   └── variables.tf
    ├── k3s
    │   ├── main.tf
    │   ├── outputs.tf
    │   ├── templates
    │   │   ├── server-userdata.sh.tftpl
    │   │   └── worker-userdata.sh.tftpl
    │   └── variables.tf
    ├── network
    │   ├── main.tf
    │   ├── outputs.tf
    │   └── variables.tf
    └── vpn
        ├── main.tf
        ├── outputs.tf
        └── variables.tf
```

## Prerequisites

- Terraform `>= 1.5`
- AWS CLI v2
- `kubectl`
- `helm`
- `jq`
- An AWS account with permissions to create networking, IAM, EC2, Auto Scaling, load balancers, ECR, ACM-linked resources, CloudTrail, CloudWatch, SSM, and Client VPN resources
- Existing ACM certificate ARNs for AWS Client VPN:
  - a server certificate ARN
  - a client root certificate chain ARN for mutual certificate authentication
- An ACM certificate ARN for the application ALB if you want HTTPS termination there

## Deployment Instructions

### 1. Configure the production stack

```bash
cd infra/aws-secure-platform/environments/production
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and provide real values for:

- `aws_region`
- `project_name`
- `environment`
- `availability_zones`
- `vpc_cidr`
- subnet CIDRs
- `vpn_client_cidr`
- `client_vpn_server_certificate_arn`
- `client_vpn_root_certificate_chain_arn`
- `app_ingress_acm_certificate_arn`
- `public_app_load_balancer`

### 2. Initialize and apply infrastructure

```bash
terraform init
terraform plan
terraform apply
```

### 3. Export outputs for operator use

```bash
terraform output
terraform output -raw cluster_name
terraform output -raw aws_region
terraform output -json ecr_repository_urls | jq
terraform output -raw cluster_endpoint
terraform output -raw client_vpn_endpoint_id
terraform output -raw app_alb_dns_name
```

### 4. Connect through AWS Client VPN

Export the Client VPN configuration from AWS after the endpoint is created, then connect using your approved client certificate.

For the concrete Kumquat production operator flow, including the current endpoint ID, local cert paths, export command, and verification steps, use [VPN.md](VPN.md).

Typical flow:

1. In AWS, download the Client VPN endpoint configuration.
2. Add your client certificate and key references as required by your VPN client.
3. Connect to the VPN.
4. Verify you can resolve and reach private VPC resources.

The cluster API is private-only by design. `kubectl`, Helm, and Terraform add-on steps that talk to the cluster must run after the VPN connection is active or from a trusted network path inside the VPC.

### 5. Configure kubectl access

```bash
K3S_ENDPOINT="$(terraform output -raw cluster_endpoint)"
aws ssm start-session --target "$(terraform output -json server_instance_ids | jq -r '.[0]')"
```

Then on a server node accessed through Session Manager:

```bash
sudo cat /etc/rancher/k3s/k3s.yaml | \
  sed "s#https://127.0.0.1:6443#https://${K3S_ENDPOINT}:6443#" > ~/kubeconfig
```

Copy that kubeconfig to your operator workstation over an approved path, then:

```bash
export KUBECONFIG="$PWD/kubeconfig"
kubectl get nodes
```

### 6. Install ingress-nginx

After VPN access and kubeconfig are working:

```bash
cd ../../addons/ingress-nginx
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -var="kubeconfig_path=$KUBECONFIG"
terraform apply -var="kubeconfig_path=$KUBECONFIG"
```

### 7. Push container images to ECR

From the production stack:

```bash
cd ../../environments/production
AWS_REGION="$(terraform output -raw aws_region)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REPO_URL="$(terraform output -json ecr_repository_urls | jq -r '.sample_app')"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

docker build -t sample-app:1.0.0 .
docker tag sample-app:1.0.0 "$REPO_URL:1.0.0"
docker push "$REPO_URL:1.0.0"
```

For the production website image:

```bash
REPO_URL="$(terraform output -json ecr_repository_urls | jq -r '.sample_app')"

cd ../../../website
docker buildx build --platform linux/amd64 -t "$REPO_URL:website-YYYYMMDD-HHMMSS" --push .
```

For the Django backend image:

```bash
REPO_URL="$(terraform output -json ecr_repository_urls | jq -r '.website_backend')"

cd ../../../website-backend
docker buildx build --platform linux/amd64 -t "$REPO_URL:backend-YYYYMMDD-HHMMSS" --push .
```

For the MySQL backup job image:

```bash
REPO_URL="$(terraform output -json ecr_repository_urls | jq -r '.mysql_backup')"

cd ../../../images/mysql-backup
docker buildx build --platform linux/amd64 -t "$REPO_URL:mysql-backup-YYYYMMDD-HHMMSS" --push .
```

### 8. Deploy the website manifests to k3s

Update the image in `kubernetes/example-app/deployment.yaml`, then apply:

```bash
cd ../../kubernetes/example-app
kubectl apply -k .
kubectl rollout status deployment/sample-app -n sample-app
kubectl get ingress -n sample-app
```

The website ingress can be exposed publicly when `public_app_load_balancer = true`. In that case, point your DNS record for `kumquat.info` or `www.kumquat.info` at the `app_alb_dns_name` Terraform output.

### 9. Deploy the Kumquat backend platform add-on

This add-on installs the AWS EBS CSI driver, a gp3-backed storage class, the MySQL operator, a MySQL InnoDB cluster, a private/versioned/KMS-encrypted S3 bucket for MySQL dumps, a Kubernetes CronJob that uploads compressed backups to that bucket, and the Django backend routed at `/api/`. It also injects the Google OAuth client ID, secret, and redirect URI into the backend environment secret.

Before the first `terraform init`, create a private S3 bucket for Terraform state and use it as the add-on backend:

```bash
cd ../../addons/kumquat-platform
cp backend.hcl.example backend.hcl
# set the bucket value in backend.hcl to your private state bucket
terraform init -backend-config=backend.hcl
```

```bash
cd ../../addons/kumquat-platform
cp terraform.tfvars.example terraform.tfvars
terraform plan \
  -var="aws_region=$(cd ../../environments/production && terraform output -raw aws_region)" \
  -var="kubeconfig_path=$KUBECONFIG" \
  -var="backend_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/website-backend" \
  -var="backend_image_tag=backend-20260407-1" \
  -var="mysql_backup_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/mysql-backup" \
  -var="mysql_backup_image_tag=mysql-backup-20260409-1" \
  -var="backend_secret_key=replace-me" \
  -var="google_oauth_client_id=replace-me" \
  -var="google_oauth_client_secret=replace-me" \
  -var="google_oauth_redirect_uri=https://kumquat.info/auth/google/callback" \
  -var="mysql_root_password=replace-me" \
  -var="mysql_app_password=replace-me"
terraform apply \
  -var="aws_region=$(cd ../../environments/production && terraform output -raw aws_region)" \
  -var="kubeconfig_path=$KUBECONFIG" \
  -var="backend_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/website-backend" \
  -var="backend_image_tag=backend-20260407-1" \
  -var="mysql_backup_image_repository=351381968847.dkr.ecr.us-west-2.amazonaws.com/mysql-backup" \
  -var="mysql_backup_image_tag=mysql-backup-20260409-1" \
  -var="backend_secret_key=replace-me" \
  -var="google_oauth_client_id=replace-me" \
  -var="google_oauth_client_secret=replace-me" \
  -var="google_oauth_redirect_uri=https://kumquat.info/auth/google/callback" \
  -var="mysql_root_password=replace-me" \
  -var="mysql_app_password=replace-me"
```

Tune these backup-specific variables as needed:

- `mysql_backup_schedule` to change the Cron expression
- `mysql_backup_suspend` to pause the CronJob without deleting it
- `mysql_backup_bucket_name` to override the generated private bucket name
- `mysql_backup_retention_days` to change S3 lifecycle retention

### 10. Verify the live rollout

```bash
kubectl get pods -n sample-app
kubectl get pods -n kumquat
kubectl get ingress -A
curl -I https://kumquat.info/
curl -I https://kumquat.info/api/healthz
```

## Security Checklist

Applied by default:

- Three private k3s server nodes for quorum, not two
- Internal-only Kubernetes API behind a private NLB
- Worker nodes in private subnets only with Auto Scaling
- No public SSH or bastion hosts
- Session Manager policy attached to node instances
- NAT only in public subnets; workloads stay private
- Internal-only application ALB
- ECR tag immutability, lifecycle policy, and scan-on-push
- KMS-backed encryption for the k3s cluster token, CloudTrail, Flow Logs log group, and ECR
- CloudTrail enabled with log file validation and CloudWatch log delivery
- VPC Flow Logs enabled
- CloudWatch alarms for unauthorized API calls and Client VPN connection failures
- VPC endpoints for ECR, S3, STS, CloudWatch Logs, Secrets Manager, SSM, ELB, Auto Scaling, EC2 messages, and monitoring
- Kubernetes API security group restricted to the VPN CIDR and cluster nodes
- No `0.0.0.0/0` ingress rules in the Terraform implementation

Manual review still required:

- Review AWS Client VPN certificate issuance, storage, and rotation process
- Review any optional SSH key use and keep `ssh_key_name = null` unless there is a compelling break-glass requirement
- Decide whether dedicated separate AWS accounts should isolate production from non-production
- Tune node group sizes and pod disruption budgets for actual workload characteristics
- Review and possibly tighten network policies once application communication paths are known
- Add centralized alert destinations such as SNS, PagerDuty, or Slack integration
- Decide whether GuardDuty, Security Hub, AWS Config, Falco, and runtime security tooling should be mandatory in your environment

## Operational Guidance

### Rotate Secrets

- Store application secrets in AWS Secrets Manager or SSM Parameter Store.
- Mount or sync them into pods through a dedicated controller only after reviewing its permissions model.
- Rotate secrets in the secrets store first, then restart or roll deployments to pick up new values.

### Update Worker Nodes

- Update the launch templates or AMI selection through Terraform.
- Replace worker instances gradually by adjusting the Auto Scaling Group and draining nodes before termination.
- Rotate server nodes one at a time to preserve etcd quorum.

### Scale the Platform

- Increase or decrease worker ASG `desired`, `min`, and `max` values in Terraform.
- Add dedicated worker groups only if workload isolation or instance specialization justifies the additional complexity.
- Add a Kubernetes-aware autoscaler only after a separate security review, because it expands infrastructure-level permissions.

### Review Logs

- Use CloudWatch Logs for VPC Flow Logs and Client VPN logs.
- Use CloudTrail to review API activity and detect unauthorized attempts.
- Use `kubectl logs`, `kubectl describe`, and `kubectl get events` after connecting through the VPN.

### Recover from Common Failures

- If nodes fail to join the cluster, verify private subnet routes, SSM parameter access, NAT or endpoints, security group rules for `6443`, `9345`, and `8472`, and the k3s service status on each node.
- If ingress does not work, verify the `ingress-nginx` NodePort service, ALB target group health checks, worker security group rules, and the application `Ingress` objects.
- If operators cannot reach the cluster, verify the Client VPN connection, authorization rule, route entries, DNS resolution, and internal NLB reachability to `6443`.
- If image pulls fail, verify ECR repository permissions, ECR endpoints, node role permissions, and image tag existence.

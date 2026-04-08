# kumquat

![Kumquat Team](team.kumquat.png)

Kumquat is a small web platform built around a public React website at `/`, a Django backend served at `/api/`, and AWS-hosted k3s infrastructure managed as code.

The frontend lives in [website](website) and is packaged as a containerized Vite app served through the cluster ingress. The backend lives in [website-backend](website-backend) and is set up as a Django service with health and API endpoints intended to sit behind the same `kumquat.info` hostname at `/api/`.

Infrastructure for the AWS private container platform lives in [infra/aws-secure-platform](infra/aws-secure-platform). That folder contains the Terraform for the VPC, VPN, ECR, and k3s cluster, plus Helm and Terraform add-on code for the Kumquat backend platform, including the MySQL operator, a MySQL InnoDB cluster, and EBS-backed persistent storage.

The repository is organized so application code and infrastructure code stay separate, but the deployment flow remains repo-native: build images, publish to ECR, apply the website manifests to k3s, and update the backend platform add-on through Terraform and Helm from the same codebase.

## Deployment Overview

Production runs on k3s behind `kumquat.info`.

- [website](website) builds to the `sample-app` ECR repository and is deployed with the manifests in [infra/aws-secure-platform/kubernetes/example-app](infra/aws-secure-platform/kubernetes/example-app).
- [website-backend](website-backend) builds to the `website-backend` ECR repository and is deployed through the Terraform add-on in [infra/aws-secure-platform/addons/kumquat-platform](infra/aws-secure-platform/addons/kumquat-platform).
- The current production kubeconfig in this workspace is `.local/aws-secure-platform/kubeconfig-production`.

For the detailed rollout procedure, including Google OAuth secret injection for the backend, use [infra/aws-secure-platform/README.md](infra/aws-secure-platform/README.md).

For the operator VPN connection procedure used to reach the private k3s API, use [infra/aws-secure-platform/VPN.md](infra/aws-secure-platform/VPN.md).

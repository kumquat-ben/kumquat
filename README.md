# kumquat

![Kumquat Team](team.kumquat.png)

Kumquat is a small web platform built around a public React website at `/`, a Django backend served at `/api/`, and AWS-hosted k3s infrastructure managed as code.

The frontend lives in [website](/Users/armenmerikyan/Desktop/wd/kumquat/website) and is packaged as a containerized Vite app served through the cluster ingress. The backend lives in [website-backend](/Users/armenmerikyan/Desktop/wd/kumquat/website-backend) and is set up as a Django service with health and API endpoints intended to sit behind the same `kumquat.info` hostname at `/api/`.

Infrastructure for the AWS private container platform lives in [infra/aws-secure-platform](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform). That folder contains the Terraform for the VPC, VPN, ECR, and k3s cluster, plus Helm and Terraform add-on code for the Kumquat backend platform, including the MySQL operator, a MySQL InnoDB cluster, and EBS-backed persistent storage.

The repository is organized so application code and infrastructure code stay separate, but the deployment flow remains repo-native: build images, publish to ECR, apply the website manifests to k3s, and update the backend platform add-on through Terraform and Helm from the same codebase.

## Deployment Overview

Production runs on k3s behind `kumquat.info`.

- [website](/Users/armenmerikyan/Desktop/wd/kumquat/website) builds to the `sample-app` ECR repository and is deployed with the manifests in [infra/aws-secure-platform/kubernetes/example-app](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/kubernetes/example-app).
- [website-backend](/Users/armenmerikyan/Desktop/wd/kumquat/website-backend) builds to the `website-backend` ECR repository and is deployed through the Terraform add-on in [infra/aws-secure-platform/addons/kumquat-platform](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/addons/kumquat-platform).
- The current production kubeconfig in this workspace is `.local/aws-secure-platform/kubeconfig-production`.

For the detailed rollout procedure, including Google OAuth secret injection for the backend, use [infra/aws-secure-platform/README.md](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/README.md).

For the operator VPN connection procedure used to reach the private k3s API, use [infra/aws-secure-platform/VPN.md](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/VPN.md).

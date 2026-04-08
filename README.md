# kumquat

![Kumquat Team](team.kumquat.png)

Kumquat is a small web platform built around a public React landing page, a Django backend served at `/api/`, and AWS-hosted k3s infrastructure managed as code.

The frontend lives in [website](/Users/armenmerikyan/Desktop/wd/kumquat/website) and is packaged as a containerized Vite app served through the cluster ingress. The backend lives in [website-backend](/Users/armenmerikyan/Desktop/wd/kumquat/website-backend) and is set up as a Django service with health and API endpoints intended to sit behind the same `kumquat.info` hostname at `/api/`.

Infrastructure for the AWS private container platform lives in [infra/aws-secure-platform](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform). That folder contains the Terraform for the VPC, VPN, ECR, and k3s cluster, plus Helm and Terraform add-on code for the Kumquat backend platform, including the MySQL operator, a MySQL InnoDB cluster, and EBS-backed persistent storage.

The repository is organized so application code and infrastructure code stay separate, but the deployment flow remains repo-native: build images, publish to ECR, and apply platform changes through Terraform and Helm from the same codebase.

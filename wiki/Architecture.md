# Architecture

## Stack Summary

| Layer | Stack | Notes |
|---|---|---|
| Frontend | React + Vite | Containerized and served through k3s ingress at `/` |
| Backend | Django | API and health endpoints served at `/api/` |
| Infrastructure | AWS + k3s + Terraform + Helm | VPC, VPN, ECR, MySQL, and persistent storage |

## Deployment Shape

- the frontend is built as a container image and deployed with Kubernetes manifests
- the backend is built as a container image and deployed through a Terraform-managed add-on
- the cluster runs on private k3s infrastructure in AWS
- operator access to the cluster API goes through AWS Client VPN

## Infrastructure Components

The AWS platform provisions:

- a multi-AZ VPC
- public, private, and isolated subnets
- AWS Client VPN for operator access
- private k3s server and worker nodes
- Amazon ECR repositories
- logging, monitoring, and audit resources

## Design Notes

- the Kubernetes API is private-only
- worker capacity runs in private subnets
- ingress is handled inside the cluster and fronted by AWS load balancers
- operational access is expected through VPN and approved AWS credentials

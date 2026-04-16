# Operator Onboarding

This document is for engineers responsible for staging or production deployment work.

## Operator Responsibilities

Operators are expected to:

- provision or update the AWS environment
- maintain safe access to the private k3s API
- publish application images to ECR
- deploy website and backend changes
- verify the live rollout and watch for regressions

## Access Model

Production cluster access is private-only. Operators need:

- AWS credentials for the correct account
- AWS Client VPN access
- local certificate material for the VPN
- a valid kubeconfig for the private cluster

If any of those are missing, stop before attempting deployment work.

## Minimum Local Tooling

- AWS CLI v2
- Terraform
- Docker
- `kubectl`
- `helm`
- `jq`

## Standard Deployment Order

1. Verify AWS CLI account and region.
2. Confirm VPN connectivity.
3. Confirm `kubectl get nodes` works with the intended kubeconfig.
4. Build and push images to ECR.
5. Deploy the website manifests if frontend changes are included.
6. Deploy the backend platform add-on if backend or platform changes are included.
7. Verify `https://kumquat.info/` and `https://kumquat.info/api/healthz`.

## Managed Node Launcher

The website admin dashboard can launch blockchain nodes into an already-provisioned cluster.

- use the existing kubeconfig path in backend env when possible
- if kubeconfig is not mounted into the backend, a superuser can save a Kubernetes API server, bearer token, base64 CA cert, and namespace in the dashboard for the current session
- this launcher path is for node and miner deployment only; it does not replace Terraform or cluster provisioning

## Files And Paths Operators Use Often

- [`infra/aws-secure-platform/environments/production`](../infra/aws-secure-platform/environments/production)
- [`infra/aws-secure-platform/VPN.md`](../infra/aws-secure-platform/VPN.md)
- [`infra/aws-secure-platform/addons/ingress-nginx`](../infra/aws-secure-platform/addons/ingress-nginx)
- [`infra/aws-secure-platform/addons/kumquat-platform`](../infra/aws-secure-platform/addons/kumquat-platform)
- [`infra/aws-secure-platform/kubernetes/example-app`](../infra/aws-secure-platform/kubernetes/example-app)

## Pre-Deploy Checklist

- working tree reviewed and intentional
- image tags chosen and published
- Terraform variables reviewed for secrets and target tags
- VPN connected
- kubeconfig path set correctly
- rollout commands prepared before applying changes

## Post-Deploy Checklist

- `kubectl get pods -A` shows healthy workloads
- ingress objects are present and attached
- homepage loads at `https://kumquat.info/`
- backend health responds at `https://kumquat.info/api/healthz`
- unexpected Kubernetes events have been reviewed

## Escalation Triggers

Escalate before continuing if:

- Terraform shows destructive changes you did not intend
- VPN connectivity breaks during a rollout
- `kubectl` points at an unexpected cluster
- secrets, certificates, or keys appear to have been exposed

## Related Documents

- [AWS Environment Deployment](aws-environment-deployment.md)
- [VPN And Helm Deployment](vpn-and-helm-deployment.md)
- [Developer Onboarding](developer-onboarding.md)

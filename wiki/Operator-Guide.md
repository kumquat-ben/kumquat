# Operator Guide

This page is for engineers responsible for staging or production deployment work.

## Operator Responsibilities

- provision or update the AWS environment
- maintain safe access to the private k3s API
- publish application images to ECR
- deploy website and backend changes
- verify the live rollout and watch for regressions

## Access Requirements

Operators need:

- AWS credentials for the correct account
- AWS Client VPN access
- local certificate material for the VPN
- a valid kubeconfig for the private cluster

If any of those are missing, stop before attempting deployment work.

## Standard Deployment Order

1. Verify AWS CLI account and region.
2. Confirm VPN connectivity.
3. Confirm `kubectl get nodes` works with the intended kubeconfig.
4. Build and push images to ECR.
5. Deploy website manifests if frontend changes are included.
6. Deploy the backend platform add-on if backend or platform changes are included.
7. Verify `https://kumquat.info/` and `https://kumquat.info/api/healthz`.

# Documents

This folder is the top-level operating manual for Kumquat. It consolidates the deployment flow, onboarding material, and product framing that were previously spread across the repository.

## Contents

- [AWS Environment Deployment](aws-environment-deployment.md): how to provision the AWS environment with Terraform and the AWS CLI
- [VPN And Helm Deployment](vpn-and-helm-deployment.md): how to connect to the private cluster, prepare `kubectl`, and deploy the backend platform add-on
- [Developer Onboarding](developer-onboarding.md): local setup, repository structure, and day-one workflow for engineers
- [Operator Onboarding](operator-onboarding.md): production access, deployment responsibilities, and operational checks
- [Product Requirements Document](product-requirements.md): current product intent, audience, scope, and release assumptions
- [White Paper Initial Draft](whitepaper-initial-draft.md): Markdown draft converted from the provided whitepaper skeleton
- [Community Operations](community-operations.md): GitHub Discussions seeding plan, engagement cadence, and project-management integration
- [Hybrid Denomination Upgrade](hybrid-denomination-upgrade.md): protocol direction note for the hybrid bills-plus-coins model
- [Hybrid Cash Implementation Plan](hybrid-cash-implementation-plan.md): phased implementation plan for the ledger refactor

## Source Material

These documents are aligned with the repo's existing operator references:

- [`README.md`](../README.md)
- [`infra/aws-secure-platform/README.md`](../infra/aws-secure-platform/README.md)
- [`infra/aws-secure-platform/VPN.md`](../infra/aws-secure-platform/VPN.md)

## Changelog

- `2026-04-15`: Added the hybrid cash planning docs to the document index.

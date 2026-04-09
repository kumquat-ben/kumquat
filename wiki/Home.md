# Kumquat Wiki

Kumquat is a digital money product built around a physical cash mental model, where denominations behave like visible units you can hold, read, and transfer.

This repository contains:

- the React frontend in `website/`
- the Django backend in `website-backend/`
- the AWS and k3s infrastructure in `infra/aws-secure-platform/`

## Start Here

- [Getting Started](Getting-Started)
- [Architecture](Architecture)
- [Developer Onboarding](Developer-Onboarding)
- [Operator Guide](Operator-Guide)
- [AWS Environment Deployment](AWS-Environment-Deployment)
- [VPN and Deployment](VPN-and-Deployment)
- [Product Overview](Product-Overview)

## Live Environment

- Production site: <https://kumquat.info>
- Source repository: <https://github.com/kumquatben/kumquat>

## Project Model

A running instance of the stack is treated as a Kumquat Farm: one self-contained node running the farm services together.

The farm architecture combines:

- chain layer for settlement and shared state
- mint layer for issuance logic
- liquidity layer for participation and markets
- compute rental layer for rentable node capacity
- workload execution layer for submitted jobs
- data marketplace layer for data products

These sit on top of shared services such as wallet management, signing, APIs, node orchestration, and operator controls.

# Kumquat Wiki

Kumquat is a digital money product built around a physical cash mental model, where denominations behave like visible units you can hold, read, and transfer. The current protocol direction is a hybrid cash model: bills from `$1` through `$100` are discrete owned units, while coins below `$1` are fungible counted inventory.

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
- [Roadmap](Roadmap)

## Live Environment

- Production site: <https://kumquat.info>
- Source repository: <https://github.com/kumquat-ben/kumquat>

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

## Roadmap

The roadmap is organized across four active tracks:

- product
- protocol
- platform
- community

For the current milestone structure and immediate priorities, see [Roadmap](Roadmap).

## Changelog

- `2026-04-15`: Updated the wiki home page to reflect the hybrid cash protocol direction.

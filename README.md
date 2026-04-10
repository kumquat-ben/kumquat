# kumquat

![React](https://img.shields.io/badge/React-Vite-61DAFB?style=flat-square&logo=react)
![Django](https://img.shields.io/badge/Backend-Django-092E20?style=flat-square&logo=django)
![Terraform](https://img.shields.io/badge/Infra-Terraform-7B42BC?style=flat-square&logo=terraform)
![k3s](https://img.shields.io/badge/Cluster-k3s-FFC61C?style=flat-square&logo=k3s)

![Kumquat Team](team.kumquat.png)

Kumquat is a digital money product built around a physical cash mental model, where denominations behave like visible units you can hold, read, and transfer. This repository contains the public React website at `/`, the Django backend at `/api/`, and the AWS-hosted k3s infrastructure that supports the product.

Live at **[kumquat.info](https://kumquat.info)**.

The frontend lives in [website](website) and is packaged as a containerized Vite app served through the cluster ingress. The backend lives in [website-backend](website-backend) and is set up as a Django service with health and API endpoints intended to sit behind the same `kumquat.info` hostname at `/api/`.

Infrastructure for the AWS private container platform lives in [infra/aws-secure-platform](infra/aws-secure-platform). That folder contains the Terraform for the VPC, VPN, ECR, and k3s cluster, plus Helm and Terraform add-on code for the Kumquat backend platform, including the MySQL operator, a MySQL InnoDB cluster, and EBS-backed persistent storage.

The repository is organized so application code and infrastructure code stay separate, but the deployment flow remains repo-native: build images, publish to ECR, apply the website manifests to k3s, and update the backend platform add-on through Terraform and Helm from the same codebase.

## Kumquat Farm

A single running instance of this repository should be understood as a **Kumquat Farm**: one self-contained node running the farm services together.

In the farm architecture model, a Kumquat Farm combines:

- the **chain layer** for settlement and shared state
- the **mint layer** for coin creation and issuance logic
- the **liquidity layer** for market and provider participation
- the **compute rental layer** for rentable node capacity
- the **workload execution layer** for submitted jobs
- the **data marketplace layer** for publishing and purchasing data products

These modules sit on top of shared farm services such as:

- node runtime and orchestration
- wallet, key management, and signing
- RPC, REST, and WebSocket interfaces
- farm configuration, operator controls, and governance settings

That framing matters for deployment: when you stand up one full instance of the Kumquat stack, you are standing up one Kumquat Farm.

## Architecture

| Layer | Stack | Notes |
|---|---|---|
| **Frontend** | React + Vite | Containerized and served via k3s ingress at `/` |
| **Backend** | Django | Health and API endpoints served at `/api/` on the same hostname |
| **Infrastructure** | AWS + k3s + Terraform + Helm | VPC, VPN, ECR, MySQL InnoDB cluster, and EBS-backed storage |

## Repository Layout

```text
kumquat/
├── website/                  React + Vite frontend
├── website-backend/          Django backend service
└── infra/
    └── aws-secure-platform/
        ├── terraform/        VPC, VPN, ECR, and k3s cluster infrastructure
        └── addons/           MySQL operator, InnoDB cluster,
                              EBS storage, and Helm-managed platform releases
```

## Website UI Guidelines

The website frontend in [website](website) follows a specific product direction for Kumquat Chain. The React implementation should preserve these rules when the homepage or future marketing pages evolve.

### Visual Direction

- Design for a tactile cash metaphor, not a generic crypto dashboard. Bills, coins, trays, and wallet rows should read like physical objects with boundaries and weight.
- Keep the palette warm and editorial: cream backgrounds, citrus orange accents, dark ink text, and restrained gold highlights.
- Favor typography-led composition. Use `DM Serif Display` for major statements, `DM Sans` for body copy, and `DM Mono` for labels, denominations, and UI metadata.
- Maintain generous spacing and visible borders so denomination units stay individually legible.
- Prefer calm, premium surfaces over glossy startup gradients. Background atmosphere should stay subtle and support the content rather than dominate it.

### Homepage Composition

- The homepage should open with a centered hero, strong headline, restrained copy, and sparse primary actions.
- A denomination strip should appear near the top of the page to establish the hierarchy from large bills down to small coins.
- The main product story should explain the mental model in plain language before asking the user to sign in or join early access.
- The "How it works" area should pair sequenced explanatory steps with a wallet preview that fills with discrete units.
- The denomination grid should feel like a tray of money objects rather than a pricing table or feature list.

### Motion Principles

- Motion is explanatory. If an animation does not teach hierarchy, transfer, arrival, or physicality, it should be removed.
- Prefer spring-based motion over generic ease-out transitions when representing bills, rows, or cards. Springs make units feel weighted.
- Reveal content in beats. Step numbers, connector lines, headings, and body copy should not all appear at once.
- Denomination and wallet animations should reinforce that value is made of units. Rows arrive individually, badges pop after the row lands, and totals count up to the finished wallet state.
- Hover states should suggest touch: denomination cards lift, bills rise slightly, and interactive strips respond to the pointer by slowing down.
- Avoid looping attention-seeking effects. One-time emphasis is acceptable; persistent pulsing is not.
- Keep transitions short enough that they support comprehension without becoming the focus.

### Interaction Rules

- Navigation and primary calls to action should remain clear and minimal.
- Tooltips, badges, and denomination labels should use precise, system-like language that supports the object model.
- Auth flows must stay branded and product-native before and after the Google consent screen.
- Admin and utility pages can be simpler than the homepage, but should still use the same typography, palette, border treatment, and spacing language.

## Deployment Overview

Production runs on k3s behind `kumquat.info`.

- [website](website) builds to the `sample-app` ECR repository and is deployed with the manifests in [infra/aws-secure-platform/kubernetes/example-app](infra/aws-secure-platform/kubernetes/example-app).
- [website-backend](website-backend) builds to the `website-backend` ECR repository and is deployed through the Terraform add-on in [infra/aws-secure-platform/addons/kumquat-platform](infra/aws-secure-platform/addons/kumquat-platform).
- The current production kubeconfig in this workspace is `.local/aws-secure-platform/kubeconfig-production`.

For the detailed rollout procedure, including Google OAuth secret injection for the backend, use [infra/aws-secure-platform/README.md](infra/aws-secure-platform/README.md).

For the operator VPN connection procedure used to reach the private k3s API, use [infra/aws-secure-platform/VPN.md](infra/aws-secure-platform/VPN.md).

## Documents

Top-level team documentation now lives in [documents](documents):

- [documents/README.md](documents/README.md)
- [documents/aws-environment-deployment.md](documents/aws-environment-deployment.md)
- [documents/community-operations.md](documents/community-operations.md)
- [documents/vpn-and-helm-deployment.md](documents/vpn-and-helm-deployment.md)
- [documents/developer-onboarding.md](documents/developer-onboarding.md)
- [documents/operator-onboarding.md](documents/operator-onboarding.md)
- [documents/product-requirements.md](documents/product-requirements.md)

## Getting Started

```bash
git clone https://github.com/kumquat-ben/kumquat
cd kumquat

# Frontend dev server
cd website
npm install
npm run dev

# Backend
cd ../website-backend
pip install -r requirements.txt
python manage.py runserver

# Infrastructure
cd ../infra/aws-secure-platform
terraform init
terraform plan
```

## Contributing

Code contributions are welcome through GitHub:

- Open an issue: <https://github.com/kumquat-ben/kumquat/issues>
- Join Discussions: <https://github.com/kumquat-ben/kumquat/discussions>
- Submit a pull request: <https://github.com/kumquat-ben/kumquat/pulls>
- Sponsor the project: <https://github.com/sponsors/kumquat-ben>

Project management and community work should run as part of the normal delivery cycle, not outside it. Use Discussions for `Q&A`, `Ideas`, `Polls`, and `Announcements`, link relevant threads in PRs when community context exists, and keep the recurring engagement cadence documented in [documents/community-operations.md](documents/community-operations.md) visible in project planning.

© 2026 Benjamin Levin. All Rights Reserved.
Unauthorized use, copying, or distribution is strictly prohibited.

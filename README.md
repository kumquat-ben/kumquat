# kumquat

![Kumquat Team](team.kumquat.png)

Kumquat is a small web platform built around a public React website at `/`, a Django backend served at `/api/`, and AWS-hosted k3s infrastructure managed as code.

The frontend lives in [website](website) and is packaged as a containerized Vite app served through the cluster ingress. The backend lives in [website-backend](website-backend) and is set up as a Django service with health and API endpoints intended to sit behind the same `kumquat.info` hostname at `/api/`.

Infrastructure for the AWS private container platform lives in [infra/aws-secure-platform](infra/aws-secure-platform). That folder contains the Terraform for the VPC, VPN, ECR, and k3s cluster, plus Helm and Terraform add-on code for the Kumquat backend platform, including the MySQL operator, a MySQL InnoDB cluster, and EBS-backed persistent storage.

The repository is organized so application code and infrastructure code stay separate, but the deployment flow remains repo-native: build images, publish to ECR, apply the website manifests to k3s, and update the backend platform add-on through Terraform and Helm from the same codebase.

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

© 2026 Benjamin Levin. All Rights Reserved.
Unauthorized use, copying, or distribution is strictly prohibited.

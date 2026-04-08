# Developer Onboarding

This document gets a new engineer productive in the Kumquat repository.

## What You Are Working On

Kumquat is a digital money product built around a physical cash mental model. The product narrative on the website focuses on:

- visible denominations instead of an abstract balance
- transfers that feel like handing over cash
- early access before the chain goes live

The repository contains:

- `website/`: React + Vite frontend
- `website-backend/`: Django backend
- `infra/aws-secure-platform/`: Terraform, Kubernetes, and Helm deployment assets

## Local Tooling

Install:

- Node.js 18+ and npm
- Python 3.11+ and `pip`
- Docker
- Terraform
- AWS CLI v2
- `kubectl`
- `helm`

## Clone And Inspect

```bash
git clone git@github-kumquat:kumquatben/kumquat.git
cd kumquat
git status
```

Read these first:

- [`README.md`](../README.md)
- [`documents/product-requirements.md`](product-requirements.md)
- [`infra/aws-secure-platform/README.md`](../infra/aws-secure-platform/README.md)

## Frontend Setup

```bash
cd website
npm install
npm run dev
```

Frontend notes:

- app entrypoint: [`website/src/App.jsx`](../website/src/App.jsx)
- styling: [`website/src/styles.css`](../website/src/styles.css)
- current stack: React 18, Vite, Framer Motion, Lucide

## Backend Setup

```bash
cd website-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python manage.py runserver
```

Backend notes:

- framework: Django 5
- current requirements live in [`website-backend/requirements.txt`](../website-backend/requirements.txt)
- production routing expects backend endpoints under `/api/`

## Infrastructure Orientation

You do not need production access for every task, but you should understand the flow:

- Terraform provisions AWS infrastructure
- AWS Client VPN gates private operator access
- `kubectl` and Helm activity depends on VPN connectivity
- frontend and backend images are pushed to ECR
- website deploys through Kubernetes manifests
- backend platform deploys through a Terraform add-on that manages Helm releases

## Day-One Workflow

1. Read the product and deployment docs.
2. Run the frontend locally and inspect the homepage copy.
3. Run the backend locally if your task touches auth or API behavior.
4. Stay on the existing branch unless told otherwise.
5. Keep product wording aligned with the actual homepage and current launch state.

## Working Norms

- Treat `.env`, local kubeconfigs, and cert material as sensitive
- Do not commit generated VPN profiles or embedded keys
- Do not assume production cluster access is public
- When editing product copy, check the homepage source before changing repo-level messaging

## Suggested Next Reads

- [`documents/operator-onboarding.md`](operator-onboarding.md)
- [`documents/aws-environment-deployment.md`](aws-environment-deployment.md)
- [`documents/vpn-and-helm-deployment.md`](vpn-and-helm-deployment.md)

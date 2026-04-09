# Developer Onboarding

This page gets a new engineer productive in the Kumquat repository.

## Main Components

- `website/`: React + Vite frontend
- `website-backend/`: Django backend
- `infra/aws-secure-platform/`: Terraform, Kubernetes, and Helm assets

## Local Tooling

Install:

- Node.js 18+ and npm
- Python 3.11+ and `pip`
- Docker
- Terraform
- AWS CLI v2
- `kubectl`
- `helm`

## First Reads

- `README.md`
- `documents/product-requirements.md`
- `infra/aws-secure-platform/README.md`

## Day-One Workflow

1. Read the product and deployment docs.
2. Run the frontend locally and inspect the homepage copy.
3. Run the backend locally if your task touches auth or API behavior.
4. Stay on the existing branch unless told otherwise.
5. Keep product wording aligned with the homepage and launch state.

## Working Norms

- treat `.env`, kubeconfigs, and cert material as sensitive
- do not commit VPN profiles or embedded keys
- do not assume production cluster access is public
- verify product wording against the live code before changing repo messaging

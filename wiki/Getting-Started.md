# Getting Started

## Repository Layout

```text
kumquat/
├── website/                  React + Vite frontend
├── website-backend/          Django backend service
├── documents/                Product and operations documents
├── infra/aws-secure-platform/ AWS, k3s, Terraform, and Helm assets
└── wiki/                     GitHub wiki seed content
```

## Clone

```bash
git clone git@github-kumquat:kumquat-ben/kumquat.git
cd kumquat
```

## Frontend

```bash
cd website
npm install
npm run dev
```

## Backend

```bash
cd website-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python manage.py runserver
```

## Infrastructure

```bash
cd infra/aws-secure-platform/environments/production
terraform init
terraform plan
```

## Important References

- repo overview: `README.md`
- deployment docs: `documents/README.md`
- infrastructure guide: `infra/aws-secure-platform/README.md`

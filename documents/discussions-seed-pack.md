# Discussions Seed Pack

This document turns the discussion plan in `documents/community-operations.md` into ready-to-post GitHub Discussions content.

## Announcements

### What Kumquat Is: Product, Farm, and Repository Overview

Kumquat is a digital money product built around a physical cash mental model. Instead of hiding value inside a single abstract balance, Kumquat treats denominations like visible units you can hold, read, and transfer.

This repository contains the public website, the Django backend, and the AWS-hosted k3s infrastructure that supports the product. One full deployed instance of the stack is treated as a Kumquat Farm: a self-contained node that combines chain, mint, liquidity, compute rental, workload execution, and data marketplace layers on top of shared wallet, signing, API, configuration, and operator services.

Discussions is where product questions, architecture questions, ideas, and prioritization feedback should live in the open. If you are new here, start in Q&A. If you have a feature or protocol direction in mind, post it in Ideas. If you want to help prioritize next steps, watch the Polls category.

### What Is Live Today

Current repository reality:

- the public marketing site is live
- Google sign-in and early-access onboarding are implemented
- the Django backend and admin surface exist for pre-launch operations
- the AWS, Terraform, k3s, ingress, storage, and operator deployment base is in place
- the Rust blockchain codebase is present and being aligned with the current Kumquat product direction

### Monthly Farm Update Template

Use this structure for recurring updates:

- what shipped
- what changed in docs
- what is under active build
- what feedback is needed next

## Q&A

### Ask Anything About Kumquat

Use this thread for general product, architecture, onboarding, and repository questions.

### Questions About Early Access, Wallets, and Kumquats

Use this thread for questions about:

- early access
- rewards
- wallet behavior
- denomination handling
- sign-in and user state

### Questions About Architecture, Deployment, and Operations

Use this thread for questions about:

- React and Vite frontend work
- Django backend behavior
- Terraform and AWS infrastructure
- k3s and ingress deployment
- VPN and operator workflows

## Ideas

### Ideas for Wallet and Transfer Flows

Use this thread for proposals around:

- denomination display
- wallet composition
- transfer UX
- unit movement and legibility

### Ideas for Kumquat Rewards and Conversion Policy

Use this thread for proposals around:

- what earns kumquats in early access
- how kumquats convert at launch
- what constraints or anti-abuse rules should apply

### Ideas for the Kumquat Farm Roadmap

Use this thread for proposals across:

- chain
- mint
- liquidity
- compute rental
- workload execution
- data marketplace

## Polls

### What Should Kumquat Prioritize Next?

Suggested poll options:

- wallet flows
- transfer flows
- early-access rewards policy
- protocol and roadmap documentation
- operator and deployment hardening

### Which Documentation Area Needs the Most Attention?

Suggested poll options:

- product overview
- developer onboarding
- operator onboarding
- deployment runbooks
- protocol and roadmap docs

### What Community Update Cadence Is Most Useful?

Suggested poll options:

- weekly short updates
- biweekly updates
- monthly roundups
- milestone-only updates

## Show And Tell

### Share Mockups, Demos, and Experiments

Use this thread for screenshots, UI ideas, deployment notes, architecture sketches, and test builds.

## Mapping Back To The Project

When a discussion produces actionable work:

1. create or link the repository issue
2. add it to the GitHub project
3. set the correct `Track`, `Status`, and `Milestone`
4. reply in the discussion with the tracking link

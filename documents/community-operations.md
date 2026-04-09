# Community Operations

This document defines the recurring GitHub Discussions, project-management, and PR-engagement cycle for Kumquat.

The goal is simple: keep the repository active, make Q&A easy to find, turn ideas into scoped work, and keep the project board connected to what the community is asking for.

## Discussion Structure

The repository discussions area should stay active across these GitHub categories:

- `Announcements`: official updates, roadmap notes, release notes, and monthly status posts
- `Q&A`: product, architecture, onboarding, and deployment questions with accepted answers where possible
- `Ideas`: feature concepts, protocol directions, policy proposals, and workflow improvements
- `Polls`: lightweight prioritization and validation
- `General`: open-ended discussion that does not fit the other buckets
- `Show and tell`: experiments, screenshots, demos, and operator stories

## Seed Discussions

These are the baseline threads that should exist so the Discussions tab is not empty and new contributors know where to post.

### Announcements

1. `Welcome to Kumquat Discussions`
   - Purpose: pinned orientation post
   - Status: already present in the repository discussions
2. `What Kumquat Is: Product, Farm, and Repository Overview`
   - Summarize the README positioning:
   - Kumquat is digital money built around a physical cash mental model
   - this repo contains the website, backend, and AWS-hosted k3s platform
   - one deployed instance is a Kumquat Farm
   - the farm combines chain, mint, liquidity, compute rental, workload execution, and data marketplace layers
3. `What Is Live Today`
   - State what exists now:
   - public marketing site
   - Google sign-in and early-access onboarding
   - Django backend and admin surface
   - AWS, k3s, Terraform, Helm, MySQL, and storage platform
4. `Monthly Farm Update: Template`
   - Use for recurring progress posts
   - Include:
   - what shipped
   - what changed in docs
   - what is under active build
   - what feedback is needed from the community

### Q&A

1. `Ask Anything About Kumquat`
   - General entry point for new users and contributors
2. `Questions About Early Access, Wallets, and Kumquats`
   - Focus on product behavior, onboarding, and rewards questions
3. `Questions About Architecture, Deployment, and Operations`
   - Focus on React, Django, Terraform, AWS, k3s, VPN, and MySQL operator questions

### Ideas

1. `Ideas for Wallet and Transfer Flows`
   - Collect suggestions for denomination handling, transfer UI, and wallet behavior
2. `Ideas for Kumquat Rewards and Conversion Policy`
   - Collect proposals for early-access rewards and launch conversion rules
3. `Ideas for the Kumquat Farm Roadmap`
   - Collect proposals across chain, mint, liquidity, compute, workload, and data marketplace modules

### Polls

1. `What Should Kumquat Prioritize Next?`
   - Poll options:
   - wallet flows
   - transfer flows
   - early-access rewards policy
   - protocol and roadmap documentation
   - operator and deployment hardening
2. `Which Documentation Area Needs the Most Attention?`
   - Poll options:
   - product overview
   - developer onboarding
   - operator onboarding
   - deployment runbooks
   - protocol and roadmap docs
3. `What Community Update Cadence Is Most Useful?`
   - Poll options:
   - weekly short updates
   - biweekly updates
   - monthly roundups
   - milestone-only updates

### Show And Tell

1. `Share Mockups, Demos, and Experiments`
   - Invite contributors to post UI ideas, architecture sketches, deployments, and test builds

## Posting Guidance

When posting seed content, keep each thread short and durable:

- `Announcements` should summarize current reality, not speculative promises
- `Q&A` threads should make it obvious where first-time contributors can ask questions
- `Ideas` threads should ask for concrete proposals, not broad hype
- `Polls` should help decide priorities that can map back into the GitHub project

## Recurring Community Cycle

This cycle should be treated as routine repository work, not optional outreach.

### Weekly

- Review unanswered `Q&A` discussions and respond or assign an owner
- Review new `Ideas` and decide whether they should stay as discussion, become issues, or become project items
- Post at least one visible community touchpoint:
  - a short answer
  - a clarifying follow-up
  - or a progress note in an existing thread
- Check open PRs for missing context and link any relevant discussion thread
- Check the GitHub project board and align status with what changed that week

### Biweekly Or Monthly

- Publish one `Announcement` covering product, platform, and protocol movement
- Run one `Poll` when a prioritization question needs public input
- Close stale discussion loops by summarizing decisions and linking the resulting issue or PR

### Per Pull Request

- Link the PR to the relevant issue and discussion when there is community context
- Note whether the PR affects:
  - product behavior
  - operator workflow
  - architecture
  - or documentation
- If the PR answers a community question, post the result back in the related `Q&A` or `Ideas` thread

### Per Project Review

- Convert validated ideas from Discussions into tracked issues or draft project items
- Mark items with enough context as `Backlog`, `In progress`, or `Done`
- Keep at least one community or documentation task in each active planning cycle so engagement does not stop when coding work gets busy

## Project Management Integration

The GitHub project should explicitly track community operations alongside product and platform work.

Recommended recurring cards:

- `Review and answer open Q&A discussions`
- `Triage new ideas and convert qualified items into issues`
- `Post monthly Kumquat announcement`
- `Run one prioritization poll`
- `Link merged PR outcomes back to community threads`
- `Refresh roadmap summary based on discussion outcomes`

## PR Expectations

PRs should not be treated as isolated code drops. When relevant, each PR should:

- reference the issue or discussion it addresses
- explain the user, operator, or contributor impact
- note any follow-up discussion that should be reopened after merge

## Ready-To-Post Announcement Draft

Use this for the `What Kumquat Is: Product, Farm, and Repository Overview` announcement:

> Kumquat is a digital money product built around a physical cash mental model. Instead of hiding value inside a single abstract balance, Kumquat treats denominations like visible units you can hold, read, and transfer.
>
> This repository contains the public website, the Django backend, and the AWS-hosted k3s infrastructure that supports the product. One full deployed instance of the stack is treated as a Kumquat Farm: a self-contained node that combines chain, mint, liquidity, compute rental, workload execution, and data marketplace layers on top of shared wallet, signing, API, configuration, and operator services.
>
> Discussions is where we want product questions, architecture questions, ideas, and prioritization feedback to live in the open. If you are new here, start in Q&A. If you have a feature or protocol direction in mind, post it in Ideas. If you want to help us prioritize, watch the Polls category.


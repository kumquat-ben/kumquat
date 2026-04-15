# Product Requirements Document

## Document Status

- Product: Kumquat
- Stage: early-access product with chain launch still ahead
- Source of truth for messaging in this revision: current homepage implementation in `website/src/App.jsx`

## Product Summary

Kumquat is a digital money product that makes value legible through a physical cash mental model. Instead of presenting money as a single abstract balance, it shows denominations as visible units with hierarchy, count, and transfer behavior.

The current website frames the product as:

- money that behaves like objects you can hold
- a denomination-based wallet model
- transfers that read like handing over real cash
- an early-access system that rewards users with kumquats before chain launch

The current protocol direction under that product framing is:

- bills from `$1` through `$100` are discrete wallet objects
- coins below `$1` are fungible counted inventory
- wallets should make the bill-versus-coin distinction legible instead of flattening both into one object model
- compute acts like the underlying metal for coin production
- coins can be melted into actual compute use on the network

## Problem

Digital balances are often hard to parse at a glance. They flatten denomination, count, and transfer behavior into an abstract number. Kumquat aims to make value easier to understand by restoring the logic of cash in a digital interface.

## Product Goals

- Make value feel concrete rather than abstract
- Make denomination hierarchy visible in the wallet
- Make transfers readable as movement of discrete units
- Build early-access interest ahead of chain launch
- Keep onboarding and sign-in within a strong product narrative

## Target Users

- early adopters interested in novel digital money experiences
- users who respond to clearer visual models of value
- founding members joining before the chain is live

## Core User Experience

The current product story implies these primary user experiences:

1. A visitor lands on the homepage and understands the physical cash metaphor quickly.
2. The visitor sees denominations, wallet composition, and transfer logic presented visually.
3. The visitor signs in with Google to join early access.
4. The signed-in user sees that early participation earns kumquats tied to future launch value.

## Current Functional Requirements

- Public marketing homepage at `/`
- Google sign-in entry point
- Django backend served at `/api/`
- Early-access and wallet-model messaging throughout the sign-in flow
- Production deployment at `kumquat.info`

## Current Content Requirements

Website and repo-level product language should stay aligned with these ideas:

- physical cash mental model for the internet
- denominations as visible units
- wallet readability from large units to small remainder
- transfers you can follow
- founding-member participation before chain launch

## Design Requirements

The implementation should preserve the existing direction:

- tactile cash metaphor, not a generic crypto dashboard
- warm editorial palette
- typography-led composition
- denomination trays, wallet rows, and distinct units
- motion that teaches hierarchy, arrival, and transfer

## Non-Goals For This Revision

- a full tokenomics specification
- chain protocol details
- public smart-contract documentation
- a finalized long-term admin dashboard scope

## Open Questions

- What exact actions in early access earn kumquats?
- How will kumquats convert into launch-era units?
- What wallet capabilities will be available before chain launch?
- What admin workflows are required to support early-access users?
- How should the product explain breaking bills into coins and melting coins into compute use?

## Suggested Next Product Documents

If the team wants a fuller product documentation set, add:

- launch checklist
- token or denomination policy
- auth and user-state flows
- admin operations guide

## Changelog

- `2026-04-15`: Updated product requirements to reflect the hybrid cash model with bill objects and fungible sub-dollar coin inventory.
- `2026-04-15`: Added the product direction that compute behaves like metal for coin production and that coins can be melted into actual compute use.
- `2026-04-15`: Removed lower-level protocol-detail questions that are now treated as locked defaults in the planning docs.

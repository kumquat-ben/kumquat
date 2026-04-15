# Product Overview

## Summary

Kumquat is a digital money product that makes value legible through a physical cash mental model. Instead of presenting money as a single abstract balance, it shows denominations as visible units with hierarchy, count, and transfer behavior.

The current protocol direction behind that product model is hybrid cash:

- bills from `$1` through `$100` behave like discrete wallet objects
- coins below `$1` behave like fungible inventory

## Product Goals

- make value feel concrete rather than abstract
- make denomination hierarchy visible in the wallet
- make transfers readable as movement of discrete units
- build early-access interest ahead of chain launch
- keep onboarding and sign-in inside a strong product narrative

## Current User Experience

1. A visitor lands on the homepage and understands the cash metaphor quickly.
2. The visitor sees denominations, wallet composition, and transfer logic presented visually.
3. The visitor signs in with Google to join early access.
4. The signed-in user sees that early participation earns kumquats tied to future launch value.

## Current Scope

- public marketing homepage at `/`
- Google sign-in entry point
- Django backend at `/api/`
- early-access and wallet-model messaging throughout the flow
- production deployment at `kumquat.info`

## Changelog

- `2026-04-15`: Updated the product overview to reflect the hybrid cash model instead of a single denomination-object model.

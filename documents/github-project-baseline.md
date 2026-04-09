# GitHub Project Baseline

## Request

Create and populate the GitHub project at `https://github.com/users/kumquatben/projects/1` using the current repository state as a baseline. The requested scope was lightweight: use the whitepaper, roadmap direction, features already present in the repo, and remaining work without going too deep.

## What Changed

- Confirmed that GitHub Project `kumquatben/projects/1` already existed and treated the task as a populate-and-structure update rather than creating a new project.
- Updated the project README to summarize Kumquat across three active tracks:
  - product
  - platform
  - protocol
- Added baseline draft items to the project and assigned statuses:
  - `Done`
  - `In progress`
  - `Backlog`

## Project Items Added

### Done

- Public marketing site and denomination-first homepage are live
- Google sign-in and early-access onboarding are implemented
- Backend admin and API surface exist for pre-launch operations
- AWS k3s platform, Helm releases, and operator docs are in place

### In Progress

- Whitepaper draft needs to become a real roadmap and protocol spec
- Align Rust blockchain codebase with current Kumquat product direction

### Backlog

- Define early-access rewards policy and kumquat conversion rules
- Ship real wallet and transfer flows beyond the current demo surfaces
- Publish a baseline product-to-protocol roadmap

## Remaining Work

- Turn the whitepaper draft into concrete milestones and implementation phases
- Lock protocol decisions around non-fungible coins, transaction behavior, fees, and governance
- Define pre-launch rewards and conversion policy for kumquats
- Move from product narrative and demo surfaces into real wallet and transfer behavior
- Clarify launch readiness, hardening, and operator workflow gaps

## Suggestions

- Add `Priority` and `Size` values to the current project items so the board becomes easier to triage
- Split future work into explicit product, protocol, and infrastructure tracks once the baseline board settles
- Convert the higher-confidence backlog items into real repository issues and link them into the project

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
- Add a recurring community-operations lane so Discussions, announcements, polls, and Q&A follow-up are tracked as routine work

## Community Operations Lane

The GitHub project should include recurring operational items tied to community engagement:

- Review and answer open `Q&A` discussions
- Triage new `Ideas` discussions and convert qualified items into issues
- Post a monthly `Announcements` update
- Run a prioritization `Poll` when roadmap choices need feedback
- Link merged PR outcomes back to the discussion threads they resolve

This keeps repository operations connected across code, docs, project planning, and public discussion instead of treating community work as ad hoc overhead.

## Milestone Follow-Up 2026-04-09

### What Was Asked

- Add roadmap milestones to GitHub project `kumquatben/projects/1`
- Associate the current project work to the first milestone, `Proof of concept`
- Add the remaining milestone set provided by the user

### What Changed

- Created repository milestone `Proof of concept`
- Converted the active baseline project items from draft cards into repository issues where milestone assignment was needed
- Assigned the current implemented and active work to `Proof of concept`:
  - Public marketing site and denomination-first homepage are live
  - Google sign-in and early-access onboarding are implemented
  - Backend admin and API surface exist for pre-launch operations
  - AWS k3s platform, Helm releases, and operator docs are in place
  - Whitepaper draft needs to become a real roadmap and protocol spec
  - Align Rust blockchain codebase with current Kumquat product direction
- Created the additional repository milestones:
  - `Research & Foundation`
  - `Fundraising`
  - `Technical Development`
  - `Ecosystem Growth`
  - `Adoption & Scaling`
  - `Maturity`

### What Remains

- The remaining backlog cards are still draft issues with `No Milestone`
- The new milestone names exist on the repository, but they will not show as populated milestone buckets in the GitHub project until project items are assigned to them
- The backlog should be converted into repository issues and mapped into the new milestone structure

### Suggestions

- Create milestone-backed issues for each major roadmap bullet under:
  - `Research & Foundation`
  - `Fundraising`
  - `Technical Development`
  - `Ecosystem Growth`
  - `Adoption & Scaling`
  - `Maturity`
- Replace the remaining draft cards with issue-backed project items so milestone grouping works consistently in the project UI
- Add `Priority`, `Size`, and target dates once the milestone-to-issue mapping is stable

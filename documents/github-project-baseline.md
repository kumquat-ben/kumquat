# GitHub Project Baseline

## Request

Create and populate the GitHub project at `https://github.com/users/kumquat-ben/projects/1` using the current repository state as a baseline. The requested scope was lightweight: use the whitepaper, roadmap direction, features already present in the repo, and remaining work without going too deep.

## What Changed

- Confirmed that GitHub Project `kumquat-ben/projects/1` already existed and treated the task as a populate-and-structure update rather than creating a new project.
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

- Add roadmap milestones to GitHub project `kumquat-ben/projects/1`
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

## Roadmap Item Refresh 2026-04-09

### GitHub Write Status

- Direct GitHub project updates could not be applied from this workspace because the local `gh` session returned `Bad credentials (HTTP 401)`.
- The item set below is derived from the current repository state so it can be added to `kumquat-ben/projects/1` once GitHub auth is repaired.

### Recommended Project Items

| Title | Track | Status | Milestone | Evidence In Repo |
|---|---|---|---|---|
| Public marketing site and denomination-first homepage are live | Product | Done | Proof of concept | `README.md`, `website/src/App.jsx` |
| Google sign-in and early-access onboarding are implemented | Product | Done | Proof of concept | `website/src/App.jsx`, `website-backend/api/views.py` |
| Backend admin and API surface exist for pre-launch operations | Product | Done | Proof of concept | `website-backend/api/views.py` |
| AWS k3s platform, Helm releases, and operator docs are in place | Platform | Done | Proof of concept | `infra/aws-secure-platform/README.md` |
| Turn the whitepaper draft into a protocol spec and public roadmap | Protocol | In progress | Research & Foundation | `documents/whitepaper-initial-draft.md` |
| Align the Rust blockchain codebase with the current Kumquat product direction | Protocol | In progress | Research & Foundation | `blockchain/README.md` |
| Define early-access rewards policy and kumquat conversion rules | Product | Backlog | Research & Foundation | `documents/product-requirements.md` |
| Ship a real wallet model beyond the current marketing demo surfaces | Product | Backlog | Technical Development | `website/src/App.jsx`, `documents/product-requirements.md` |
| Implement transfer flows that move discrete denomination units | Product | Backlog | Technical Development | `website/src/App.jsx`, `documents/product-requirements.md` |
| Connect frontend wallet and auth surfaces to persistent backend state | Product | Backlog | Technical Development | `website/src/App.jsx`, `website-backend/api/views.py` |
| Define the chain transaction and denomination object model | Protocol | Backlog | Technical Development | `blockchain/storage/README.md`, `blockchain/README.md` |
| Finish consensus, networking, and sync integration around the current chain design | Protocol | Backlog | Technical Development | `blockchain/consensus/engine.rs`, `blockchain/network/README.md` |
| Harden observability, backups, and operator recovery procedures | Platform | Backlog | Adoption & Scaling | `infra/aws-secure-platform/README.md` |
| Tighten production secrets, rotation, and operational runbooks | Platform | Backlog | Adoption & Scaling | `infra/aws-secure-platform/README.md`, `documents/operator-onboarding.md` |
| Seed Discussions and establish the recurring roadmap/community cycle | Community | Backlog | Ecosystem Growth | `documents/community-operations.md` |
| Publish monthly roadmap and farm updates tied back to project status | Community | Backlog | Ecosystem Growth | `documents/community-operations.md` |

### Suggested Issue Conversion Order

Convert these items from project drafts into repository issues first so milestone filtering works cleanly:

1. Turn the whitepaper draft into a protocol spec and public roadmap
2. Align the Rust blockchain codebase with the current Kumquat product direction
3. Define early-access rewards policy and kumquat conversion rules
4. Ship a real wallet model beyond the current marketing demo surfaces
5. Implement transfer flows that move discrete denomination units
6. Connect frontend wallet and auth surfaces to persistent backend state
7. Define the chain transaction and denomination object model
8. Finish consensus, networking, and sync integration around the current chain design
9. Harden observability, backups, and operator recovery procedures
10. Tighten production secrets, rotation, and operational runbooks
11. Seed Discussions and establish the recurring roadmap/community cycle
12. Publish monthly roadmap and farm updates tied back to project status

### Suggested Project Fields

- `Status`: `Backlog`, `In progress`, `Done`
- `Track`: `Product`, `Protocol`, `Platform`, `Community`
- `Milestone`: `Proof of concept`, `Research & Foundation`, `Technical Development`, `Ecosystem Growth`, `Adoption & Scaling`, `Maturity`
- `Priority`: start with `P1` for roadmap/spec, blockchain alignment, rewards policy, wallet model, and transfer flows
- `Size`: use `S`, `M`, `L` only after the issue breakdown is stable

### Next Action When Auth Is Fixed

- Re-authenticate `gh`
- Create repository issues for the backlog and in-progress items above
- Add each issue to `kumquat-ben/projects/1`
- Set `Status`, `Track`, and `Milestone` on each project item

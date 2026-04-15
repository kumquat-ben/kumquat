# Roadmap

## Current State

Kumquat already has a live marketing site, Google sign-in for early access, a Django backend, and production-minded AWS and k3s infrastructure.

The next phase is to turn the existing product narrative, protocol draft, and infrastructure base into a coherent delivery roadmap.

## Active Tracks

### Product

- define early-access rewards and kumquat conversion rules
- move from marketing-only wallet examples into real wallet state
- implement transfer flows that behave like handing over discrete denomination units
- connect frontend wallet and auth surfaces to persistent backend state

### Protocol

- turn the whitepaper draft into a concrete protocol specification
- align the Rust blockchain codebase with the current Kumquat product model
- define the hybrid cash model and transaction behavior
- complete consensus, networking, and synchronization integration around the current design

### Platform

- keep the AWS and k3s platform stable for production use
- harden observability, backup, and recovery procedures
- tighten secrets handling, operational runbooks, and deployment safety

### Community

- seed GitHub Discussions so product, architecture, and roadmap questions have a clear home
- publish recurring roadmap and farm updates
- convert validated discussion outcomes into issues and project items

## Milestone Shape

The repository milestones currently map to this progression:

1. `Proof of concept`
2. `Research & Foundation`
3. `Fundraising`
4. `Technical Development`
5. `Ecosystem Growth`
6. `Adoption & Scaling`
7. `Maturity`

## Immediate Priorities

1. turn the whitepaper draft into a protocol spec and public roadmap
2. align the Rust blockchain codebase with the hybrid cash product direction
3. define early-access rewards policy and conversion rules
4. build real wallet and transfer flows
5. connect those product flows to persistent backend state

## Related References

- [Product Overview](Product-Overview)
- [Architecture](Architecture)
- [Getting Started](Getting-Started)
- [Home](Home)

## Changelog

- `2026-04-15`: Updated roadmap language to reference the hybrid cash model.

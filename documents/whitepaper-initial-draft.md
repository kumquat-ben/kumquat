# Kumquat White Paper Initial Draft

## Status

- Document type: initial draft
- Source basis: `/Users/armenmerikyan/Downloads/kumquat_whitepaper_skeleton.html`
- Additional source basis: `/Users/armenmerikyan/Downloads/kumquat-farm-concept.html`
- Intended audience: GitHub readers, contributors, and early protocol reviewers
- Current maturity: structure-first draft with explicit open questions
- Draft status note: parts of this document still describe an older all-non-fungible coin direction and need revision toward the hybrid cash model now being planned in the repository

## Editorial Update 2026-04-15

This draft no longer matches the latest protocol direction exactly.

The current direction is:

- bills from `$1` through `$100` are intended to remain non-fungible owned objects
- coins below `$1` are intended to become fungible inventory
- coin issuance is intended to happen in accountable compute-backed batches

Readers should treat the remaining references to "all coins are non-fungible" as legacy draft language pending a full rewrite.

## Abstract

Kumquat proposes a proof-of-work system centered on non-fungible coins rather than a purely fungible balance model. In this framing, newly minted units are treated as individually identifiable objects, with protocol design built around uniqueness, ownership, transfer, and cumulative work.

The broader operating concept is the **Kumquat Farm**: a self-contained node that produces value across three dimensions at once. A farm settles transactions on its own chain, rents compute capacity, and monetizes harvested data, while using a shared native token and common settlement layer across all three activities.

This draft is meant to give readers a high-level map of the protocol direction before the design is fully locked. It outlines the motivating problem, the non-fungible coin model, the role of proof-of-work, the proposed "hash-time" concept, the transaction and networking surfaces, and the major unresolved design questions that still need specification.

Readers should treat this document as a working draft rather than a final protocol specification.

## Changelog

- `2026-04-15`: Added an editorial note that this draft still contains older all-non-fungible language and needs a hybrid cash rewrite.

## 1. Introduction And Motivation

Kumquat starts from the idea that a proof-of-work chain does not need to treat every unit as interchangeable in the same way as a conventional coin ledger. Most proof-of-work systems optimize around fungibility, divisibility, and aggregate balances. Kumquat explores a different direction: coins as distinct minted objects with individual identity.

The motivating question is whether a non-fungible proof-of-work chain can create a different incentive model, different transaction semantics, and a different relationship between mining and ownership than Bitcoin-like systems or general-purpose smart contract chains.

The farm concept extends that question beyond the chain alone. Kumquat is not only asking how a non-fungible proof-of-work ledger should work. It is also asking what happens when one operator-controlled node combines:

- a financial layer with native minting, settlement, and liquidity
- a compute layer with hardware rental and workload execution
- a data layer with continuous harvesting and query-based monetization

In that model, one node is not merely a validator or miner. It is an economic unit.

This design direction is motivated by three broad goals:

- move from abstract balance accounting toward identity-bearing units
- explore how proof-of-work behaves when each minted output is unique
- define a protocol whose core concepts are legible enough to discuss directly in a GitHub-native whitepaper

## 1A. The Kumquat Farm

A **Kumquat Farm** is a sovereign economic node: a single piece of hardware or virtual machine that simultaneously operates a financial system, a compute marketplace, and a data exchange, with everything settled on its own chain.

The name is deliberate. A kumquat is small and self-contained, with nothing wasted. The farm concept applies that logic to infrastructure. The node should not leave hardware, data exhaust, or settlement capacity idle if those resources can be turned into productive markets.

The closest real-world analogy is a bank, a cloud provider, and a data broker collapsed into one operator-owned machine with a shared ledger and a shared native token.

Three properties define the farm concept:

- **Three unified economies**: finance, compute, and data operate as co-equal layers rather than isolated products
- **Shared settlement**: each market uses the same chain and token rails rather than external exchanges or separate billing systems
- **Operator sovereignty**: the farm operator controls their node locally, while the protocol enforces network-wide floors through governance

This section matters because the protocol is not only about non-fungible coins in isolation. It is about the full economic environment those coins are meant to coordinate.

## 2. The Non-Fungible Coin Model

The core premise of Kumquat is that each minted coin is unique. Instead of representing supply as interchangeable divisible units only, the protocol treats each coin as a distinct on-chain object.

At minimum, each coin would need a durable identity committed at mint time. That identity may be derived from protocol data such as:

- block hash
- miner public key
- nonce
- coin-specific commitment data

The exact identity scheme is still to be finalized, but the requirement is clear: two coins minted by Kumquat should not collapse into a single indistinguishable class by default.

This design has direct consequences:

- ownership is ownership of specific coins, not only an abstract balance
- transfers may need to reference exact coin identities
- fees, wallets, and transaction construction may differ materially from both UTXO and account-based models

The whitepaper should ultimately define how Kumquat compares to:

- UTXO systems, where outputs are distinct but denominationally fungible
- account systems, where balances are typically aggregated by account state
- NFT systems, where uniqueness exists but consensus and transfer economics are usually not designed around mined coin issuance

## 3. Hash-Time: A Wall-Clock-Free Clock

One of the novel mechanisms proposed in the skeleton is "hash-time": cumulative proof-of-work difficulty used as a trustless elapsed-time signal.

The motivation is to reduce dependence on wall-clock timestamps, NTP synchronization, or external time assumptions. Instead of asking nodes to trust timestamp declarations directly, Kumquat would explore whether elapsed time can be inferred from cumulative work progression.

The intended properties of hash-time are:

- monotonic progression tied to accumulated proof-of-work
- no dependency on external clock synchronization
- a shared way for nodes to reason about protocol progression

Key questions the full whitepaper must answer:

- how nodes convert cumulative work into an agreed notion of elapsed protocol time
- how difficulty adjustment works without calendar time
- what adversarial strategies exist for manipulating perceived time passage
- whether hash-time is sufficient for all time-sensitive protocol decisions

This chapter will likely become one of the most important parts of the final whitepaper because it is both novel and security-sensitive.

## 4. Proof-Of-Work Consensus

Kumquat uses proof-of-work consensus, but the consensus story must be explained in terms of the non-fungible mint model rather than copied from a fungible-coin design.

The full protocol description should specify:

- hash function choice
- block format and header fields
- valid block conditions
- cumulative-work chain selection rule
- fork choice and orphan handling

The interaction between consensus and asset semantics is especially important. If each block mints a unique coin, then reorgs do not merely reorder fungible issuance; they may invalidate or replace specific identity-bearing coins. That changes the practical meaning of finality, wallet display, and transaction safety.

This section should therefore explain not just how the chain is selected, but what chain selection means for coin identity persistence.

## 5. Minting Protocol

Kumquat’s minting model appears to be one unique coin per valid block rather than a divisible reward output by default.

That implies a minting protocol with the following concerns:

- what event triggers mint creation
- what coin identity fields are committed at creation time
- whether miner identity is bound to the coin permanently or optionally
- how transaction fees are expressed if fees are not naturally represented in fungible units

The supply model also needs careful treatment. If supply is measured in unique coins rather than divisible base units, then issuance policy, scarcity language, and economic reasoning all need their own vocabulary. A final whitepaper should make that vocabulary precise.

## 6. Transaction Model

A non-fungible coin chain needs a transaction model that makes object transfer explicit.

The skeleton highlights the most important unresolved issue: whether coins can be split or must move only as whole objects. That decision affects nearly every downstream property of the protocol.

Questions this section must resolve:

- can a coin be split into smaller units
- can multiple coins be combined into one transaction effect
- what constitutes valid spend authorization
- how double-spend prevention works at the coin-identity level
- whether scripting or programmability is intentionally minimal or more expressive

If Kumquat chooses whole-coin transfers only, the wallet and market model will likely feel more object-native but less flexible. If it allows splitting or recomposition, it may gain usability while giving up some conceptual purity.

The farm concept adds another layer to the transaction discussion: the same payment system is expected to settle several different markets. Compute leases, job execution, liquidity participation, and data purchases may all use the same token and settlement surface. That suggests the eventual transaction model may need:

- escrow or contract-style payment holds
- proof-of-delivery release logic
- support for cross-market payment forms such as data credits or pool-derived claims
- atomic settlement across multiple service types

## 7. Network And Peer-To-Peer Layer

The networking layer must support propagation of blocks, transactions, and coin-state knowledge across nodes.

At minimum, the final whitepaper should specify:

- peer discovery
- block propagation
- transaction relay
- mempool rules
- light client or SPV assumptions

Because Kumquat is not centered on a standard fungible fee market, mempool ordering may need special treatment. If fees are expressed in non-standard terms, the network layer cannot simply inherit the usual miner-priority assumptions from other proof-of-work chains.

At network scale, farms are intended to interoperate rather than remain isolated. That introduces additional architectural questions:

- how compute demand routes to available farm capacity
- how data queries span multiple farms without requiring buyers to know the harvesting node in advance
- how liquidity depth grows across a network of farms using the same token
- how RPC and API gateways expose farm services consistently to external users

## 8. Security Analysis

The security section needs to address both standard proof-of-work attacks and attack surfaces introduced by non-fungible issuance and hash-time.

The draft attack list includes:

- 51% attacks
- hash-time manipulation
- Sybil behavior
- replay and cross-chain attacks

Additional security questions likely belong here as the design matures:

- coin identity forgery or ambiguity
- wallet confusion during short reorgs
- miner incentives under non-fungible issuance
- denial-of-service risks if coin metadata grows too large
- service-delivery fraud in compute or data markets
- false proofs of execution or proof-of-delivery
- abuse of local operator configuration that conflicts with protocol floors

This section should distinguish between attacks inherited from proof-of-work generally and attacks unique to Kumquat’s design choices.

## 9. Implementation Notes

The whitepaper should remain readable to non-implementers, but a GitHub-native protocol draft benefits from concrete implementation guidance.

From the farm perspective, each node may run up to six major modules on top of shared runtime services.

### Farm Modules

- **Chain layer**: consensus, block production, transaction validation, and finality
- **Mint**: issuance and burning of the native token under protocol rules
- **Liquidity**: pool mechanisms for price discovery and fee-bearing market depth
- **Hardware rental**: advertising and leasing raw CPU, GPU, RAM, and storage capacity
- **Workload execution**: running submitted containers or WASM-style jobs and returning results with execution proofs
- **Data marketplace**: registering harvested datasets, exposing queries, and recording provenance and access logs

### Shared Runtime Services

- node runtime and orchestration
- wallet, key management, and signing
- node identity and reputation
- RPC, REST, and WebSocket interfaces
- local operator configuration and governance wiring

This chapter should eventually define:

- block structure
- coin structure
- transaction structure
- storage layout
- validation flow
- test vectors, especially for hash-time

This is also the right place to state what belongs in the reference implementation versus what belongs in future research or optional modules.

## 9A. Governance Model

The farm concept introduces a two-layer governance model.

Local configuration controls the farm. On-chain governance controls the protocol.

That means:

- the operator sets local fees, enabled modules, and hardware allocation
- the protocol sets floors such as minimum fees, proof formats, module standards, and treasury parameters
- if local settings conflict with protocol floors, the protocol floors win

This division preserves operator sovereignty without letting individual farms violate shared network guarantees.

## 10. Open Questions And Future Work

The skeleton correctly treats unresolved issues as first-class content rather than something to hide. For an initial draft, this is a strength.

The largest open questions currently visible are:

- whether coins are splittable
- how a fee market works without default fungibility
- whether miner identity is required, optional, or intentionally anonymous
- whether Kumquat remains narrowly scoped or grows a smart contract layer
- how much of the farm should be mandatory versus optional per operator
- what proof system governs workload execution and proof of delivery
- how cross-farm routing works for compute and data requests
- whether liquidity is local to each farm, shared network-wide, or both

This section should remain explicit and candid in future revisions. If the protocol is still evolving, the whitepaper should say so clearly.

## Appendix A. Notation And Glossary

The final paper should include a concise notation and glossary section. Suggested terms include:

- **Kumquat**: the protocol or chain described in this document
- **non-fungible coin (NFC)**: a uniquely identifiable minted coin object
- **hash-time**: cumulative proof-of-work used as a protocol time signal
- **cumulative work**: total accepted proof-of-work over a chain history
- **mint**: the creation of a new unique coin when a valid block is accepted
- **Kumquat Farm**: one self-contained operator-run node participating in the protocol economy
- **proof of delivery**: evidence used to release escrow after a compute, data, or rental service is fulfilled

References, symbol tables, and implementation cross-links can also live here once the specification matures.

## Suggested Next Draft Steps

1. Decide whether Kumquat is being specified as a protocol whitepaper, a concept paper, or a hybrid.
2. Lock the non-fungible coin identity model.
3. Expand the hash-time section with formal definitions and threat analysis.
4. Resolve the transaction model around splitting, combining, and fees.
5. Add diagrams for minting, transfer flow, and fork behavior.

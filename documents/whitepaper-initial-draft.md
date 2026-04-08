# Kumquat White Paper Initial Draft

## Status

- Document type: initial draft
- Source basis: `/Users/armenmerikyan/Downloads/kumquat_whitepaper_skeleton.html`
- Intended audience: GitHub readers, contributors, and early protocol reviewers
- Current maturity: structure-first draft with explicit open questions

## Abstract

Kumquat proposes a proof-of-work system centered on non-fungible coins rather than a purely fungible balance model. In this framing, newly minted units are treated as individually identifiable objects, with protocol design built around uniqueness, ownership, transfer, and cumulative work.

This draft is meant to give readers a high-level map of the protocol direction before the design is fully locked. It outlines the motivating problem, the non-fungible coin model, the role of proof-of-work, the proposed "hash-time" concept, the transaction and networking surfaces, and the major unresolved design questions that still need specification.

Readers should treat this document as a working draft rather than a final protocol specification.

## 1. Introduction And Motivation

Kumquat starts from the idea that a proof-of-work chain does not need to treat every unit as interchangeable in the same way as a conventional coin ledger. Most proof-of-work systems optimize around fungibility, divisibility, and aggregate balances. Kumquat explores a different direction: coins as distinct minted objects with individual identity.

The motivating question is whether a non-fungible proof-of-work chain can create a different incentive model, different transaction semantics, and a different relationship between mining and ownership than Bitcoin-like systems or general-purpose smart contract chains.

This design direction is motivated by three broad goals:

- move from abstract balance accounting toward identity-bearing units
- explore how proof-of-work behaves when each minted output is unique
- define a protocol whose core concepts are legible enough to discuss directly in a GitHub-native whitepaper

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

## 7. Network And Peer-To-Peer Layer

The networking layer must support propagation of blocks, transactions, and coin-state knowledge across nodes.

At minimum, the final whitepaper should specify:

- peer discovery
- block propagation
- transaction relay
- mempool rules
- light client or SPV assumptions

Because Kumquat is not centered on a standard fungible fee market, mempool ordering may need special treatment. If fees are expressed in non-standard terms, the network layer cannot simply inherit the usual miner-priority assumptions from other proof-of-work chains.

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

This section should distinguish between attacks inherited from proof-of-work generally and attacks unique to Kumquat’s design choices.

## 9. Implementation Notes

The whitepaper should remain readable to non-implementers, but a GitHub-native protocol draft benefits from concrete implementation guidance.

This chapter should eventually define:

- block structure
- coin structure
- transaction structure
- storage layout
- validation flow
- test vectors, especially for hash-time

This is also the right place to state what belongs in the reference implementation versus what belongs in future research or optional modules.

## 10. Open Questions And Future Work

The skeleton correctly treats unresolved issues as first-class content rather than something to hide. For an initial draft, this is a strength.

The largest open questions currently visible are:

- whether coins are splittable
- how a fee market works without default fungibility
- whether miner identity is required, optional, or intentionally anonymous
- whether Kumquat remains narrowly scoped or grows a smart contract layer

This section should remain explicit and candid in future revisions. If the protocol is still evolving, the whitepaper should say so clearly.

## Appendix A. Notation And Glossary

The final paper should include a concise notation and glossary section. Suggested terms include:

- **Kumquat**: the protocol or chain described in this document
- **non-fungible coin (NFC)**: a uniquely identifiable minted coin object
- **hash-time**: cumulative proof-of-work used as a protocol time signal
- **cumulative work**: total accepted proof-of-work over a chain history
- **mint**: the creation of a new unique coin when a valid block is accepted

References, symbol tables, and implementation cross-links can also live here once the specification matures.

## Suggested Next Draft Steps

1. Decide whether Kumquat is being specified as a protocol whitepaper, a concept paper, or a hybrid.
2. Lock the non-fungible coin identity model.
3. Expand the hash-time section with formal definitions and threat analysis.
4. Resolve the transaction model around splitting, combining, and fees.
5. Add diagrams for minting, transfer flow, and fork behavior.

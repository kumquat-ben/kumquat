# Hybrid Denomination Upgrade

## Goal

Adopt a split asset model:

- sub-dollar value (`$0.50`, `$0.25`, `$0.10`, `$0.05`, `$0.01`) remains fungible like physical US coins
- bills (`$1`, `$2`, `$5`, `$10`, `$20`, `$50`, `$100`) remain individually owned non-fungible units
- sub-dollar issuance should require provable compute work
- sub-dollar issuance should happen in accountable batches so single units do not need individual serial-like identities
- the protocol should still preserve accountability for who produced a batch and how much value they introduced

## Current Chain Shape

Today the chain already treats every denomination as a unique owned object:

- [`blockchain/storage/state.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state.rs:288) defines `DenominationToken` with a unique `token_id`, `assignment_index`, `owner`, and `mint_source`
- [`blockchain/storage/state.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state.rs:365) stores all account-held value as `tokens: Vec<DenominationToken>`
- [`blockchain/src/rewards.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/src/rewards.rs:41) mints block rewards as individual `DenominationToken`s
- [`blockchain/executor/mod.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/executor/mod.rs:319) requires transactions to name exact token IDs for payment and fee
- [`blockchain/consensus/validation/transaction_validator.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/consensus/validation/transaction_validator.rs:124) validates ownership by exact token IDs and exact denomination sums

That means the codebase is already closer to "all bills and coins are NFTs" than to "fungible money below `$1`."

## What Must Change

This is a protocol-level breaking change. The smallest coherent design is to move from one asset type to two:

### 1. Keep bills as explicit objects

Bills should continue to use something close to the current `DenominationToken` model:

- each bill has a stable `token_id`
- ownership transfer references exact bill IDs
- wallet UI can still render bills as visible objects
- denominations `$1` through `$100` remain discrete inventory

### 2. Replace sub-dollar objects with pooled fungible coin inventory

Sub-dollar value should stop being represented as individually transferable `DenominationToken`s.

Instead, represent it as:

- a fungible balance bucket per account for each coin denomination, or a single pooled sub-dollar balance
- plus batch metadata proving the account's coins came from one or more minted work batches

A workable structure is:

- `BillToken`: non-fungible bill object
- `CoinBatch`: minted batch metadata with:
  - batch id
  - producer/miner identity
  - denomination mix or total sub-dollar value
  - work proof / compute proof
  - mint block
-- `CoinBalance`: fungible account-held spendable sub-dollar value derived from owned batch issuance

### 3. Track production at the batch layer

Your requirement is optimization, not privacy.

That means the chain only needs:

- a public batch record
- a producer attribution
- a work/compute record
- the denomination mix or total minted sub-dollar value

The chain does not need a permanent public identity for each individual penny/nickel/dime/quarter/half-dollar.

### 4. Split transaction semantics

Transactions need two transfer paths:

- bill transfer: list of exact `bill_token_ids`
- coin transfer: fungible amount against owned coin inventory

That implies replacing the current transaction shape with something more like:

- `bill_transfer_ids: Vec<Hash>`
- `coin_amount_cents: u64`
- `coin_spend_proof: Option<...>`
- `fee_payment: BillFee | CoinFee | HybridFee`

### 5. Redesign minting and reward issuance

Current rewards sample denomination assignment indices and mint direct token objects. That is incompatible with pooled fungible sub-dollar issuance.

New mint behavior should be:

- if reward output is bill-denominated, mint `BillToken`s as today
- if reward output is sub-dollar, accumulate into a `CoinBatch`
- the `CoinBatch` must carry a proof that enough compute work was performed to authorize the minted amount

## Compute-Backed Coin Production

Your "compute is like metal" requirement is a separate economic rule from ordinary block PoW.

The clean model is:

- block production secures consensus
- coin-batch production proves extra compute expenditure for sub-dollar issuance

That can be implemented in increasing order of complexity:

### Option A: Reuse block PoW as the mint right

- each accepted block can mint some amount of coin-batch value
- cheap to implement
- weakest expression of "coins require compute," because no distinct work market exists

### Option B: Add batch-level proof-of-work

- a miner submits a `CoinBatch` with its own nonce/proof
- difficulty can scale with minted sub-dollar value
- closer to "metal extraction"
- still publicly verifiable and simple

### Option C: Add proof-of-useful-compute

- minted coin batches require proof tied to actual rented compute or verified job execution
- strongest alignment with the farm thesis
- much harder, because it needs a secure proof system for completed workloads

For this repository, Option B is the realistic first milestone.

## Batch Production Model

To satisfy "must be in a bag" and "we can trace who produced them," batches should be public while spends remain amount-based.

Recommended shape:

- producer creates batch `B`
- chain records:
  - `batch_id`
  - `producer`
  - `total_coin_value`
  - denomination mix
  - work proof
- recipients receive fungible coin inventory credited from those batches
- when coins move, the chain verifies balances and denomination counts rather than exact coin IDs

This gives:

- producer accountability at the batch layer
- fungibility at the spend layer
- no permanent public identity for each penny-like unit

## Code Areas That Need a Breaking Upgrade

### State model

Refactor [`blockchain/storage/state.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state.rs:334):

- split `AccountState.tokens` into:
  - `bills: Vec<BillToken>`
  - `coin_balances`
- remove the assumption that all value can be rebuilt from denomination tokens
- remove compatibility mint/decompose helpers for sub-dollar value

### Rewards

Refactor [`blockchain/src/rewards.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/src/rewards.rs:41):

- stop minting all rewards as `DenominationToken`
- route bill rewards into `BillToken`
- route sub-dollar rewards into `CoinBatch`
- add batch proof verification hooks

### Transaction format

Refactor [`blockchain/mempool/types.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/mempool/types.rs:11):

- current fields only support explicit token IDs
- add bill transfer fields and coin amount fields separately
- update signing hash format

### Validation

Refactor [`blockchain/consensus/validation/transaction_validator.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/consensus/validation/transaction_validator.rs:124):

- ownership checks for bills remain object-based
- coin spends must verify against fungible account buckets
- exact-token equality checks can no longer be the only balance rule

### Execution

Refactor [`blockchain/executor/mod.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/executor/mod.rs:117):

- current scheduler locks exact token IDs
- bills can still lock by token ID
- coin spends need account-level balance checks and amount accounting

### Storage

Add new indexed stores for:

- `coin_batch:<batch_id>`
- `account_coin_balance:<address>`

### Tooling and wallet logic

Refactor [`blockchain/tools/development.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/tools/development.rs:88):

- exact-token selection only works for bills
- wallet building must select bills explicitly and assemble coin spends from fungible coin state

## Migration Consequences

This is not an in-place patch. It is a ledger migration or chain reset.

Reasons:

- current balances below `$1` are already represented as individually identified tokens
- transaction hashes commit to explicit token IDs
- block reward commitments currently list reward token IDs
- validators, mempool, and executor all assume object-only money semantics

Practical migration choices:

- launch a new network version and reset genesis
- or write a one-time migration that converts legacy sub-dollar `DenominationToken`s into `CoinBatch`-backed balances

The new-network path is much safer.

## Recommended Delivery Order

### Phase 1. Lock the protocol spec

Decide:

- are `$1` bills non-fungible or fungible within the same serial class
- are coin denominations preserved internally, or is all sub-dollar value one fungible pool in cents
- is coin privacy account-based or note-based
- is coin issuance tied to block PoW or separate batch PoW
- can fees be paid in bills, coins, or both

### Phase 2. Implement a minimal hybrid ledger

First production-capable target:

- bills remain `DenominationToken`-like objects
- sub-dollar value becomes fungible account buckets
- coin batch metadata is public and attributable
- no strong privacy yet

This gets the economic split working quickly, but does not satisfy the full "bag" requirement.

### Phase 3. Add compute-priced minting

- define batch mint difficulty schedule
- set conversion from accepted compute work to minted sub-dollar value
- add anti-spam and supply controls

## Recommendation

Do not try to patch this into the current ledger incrementally. Treat it as a protocol redesign with two concrete workstreams:

1. Hybrid asset model
2. Compute-backed coin-batch minting

If the goal is to move fast, build this in two steps:

- first ship bills as NFTs plus fungible sub-dollar account balances
- then add stronger compute-priced `CoinBatch` issuance rules

That keeps the bill model you want while avoiding an unnecessary privacy system rewrite in the same step.

## Changelog

- `2026-04-15`: Revised the upgrade note to match the clarified requirement: optimization and cash-like behavior, not a privacy system.

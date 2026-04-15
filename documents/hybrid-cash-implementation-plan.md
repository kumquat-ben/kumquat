# Hybrid Cash Implementation Plan

## Objective

Refactor the Kumquat ledger into a hybrid cash model:

- bills from `$1` through `$100` are non-fungible owned objects
- coins below `$1` are fungible inventory
- coin issuance happens in attributed work-backed batches
- coin spending moves amounts, not per-coin IDs
- bill spending continues to move exact object IDs

This document is the build plan for the current repository.

This plan is optimization-driven. It is intentionally not a privacy design.

## Locked Design Decisions 2026-04-15

- compute is the metal analogue for coin production
- kumquats are spent to buy the compute consumed in coin minting
- coins can be created in two ways:
  - compute-backed minting
  - breaking larger bill units into coin form
- `$1` can exist in two forms:
  - a non-fungible bill object
  - fungible coin value equal to 100 cents
- melting coin inventory burns the coins and returns actual compute use on the network
- compute use returned by melting should support either immediate execution or reserved capacity

## Current Constraints In The Codebase

The present implementation assumes a single asset model where every denomination is an individually owned token.

Primary coupling points:

- [`blockchain/storage/state.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state.rs:288)
  - `DenominationToken` is the only native money object
  - `AccountState.tokens` is the source of truth for value
- [`blockchain/src/rewards.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/src/rewards.rs:41)
  - block rewards mint `DenominationToken`s directly
- [`blockchain/storage/tx_store.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/tx_store.rs:8)
  - transaction storage assumes payment is described by token ID lists
- [`blockchain/mempool/types.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/mempool/types.rs:11)
  - transaction hash/signature format commits to exact token IDs
- [`blockchain/mempool/pool.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/mempool/pool.rs:173)
  - mempool validation requires exact transfer and fee token ownership
- [`blockchain/consensus/validation/transaction_validator.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/consensus/validation/transaction_validator.rs:124)
  - stateful validation calculates payment from exact token IDs
- [`blockchain/executor/mod.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/executor/mod.rs:117)
  - execution scheduler and locking are token-ID based
- [`blockchain/storage/state_store.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state_store.rs:1)
  - canonical state persistence stores `AccountState` as one object shape
- [`blockchain/tools/development.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/tools/development.rs:88)
  - wallet/test tooling assembles payments by exact token decomposition

This means the change is cross-cutting. There is no safe single-file patch.

## Target Ledger Model

### Bills

Bills remain unique objects:

- `BillToken`
- unique `token_id`
- fixed bill denomination
- owner
- mint block
- mint source
- optional serial-style metadata

Transfer behavior:

- sender names exact bill IDs
- validator checks ownership and versions
- executor locks bills by ID

### Coins

Coins become fungible inventory:

- `CoinInventory`
- tracked as either:
  - per-denomination balances: `half_dollars`, `quarters`, `dimes`, `nickels`, `pennies`
  - or one pooled `sub_dollar_cents` balance plus denomination metadata

Recommended approach for "like real cash":

- keep per-denomination balances in state
- allow transfer either by explicit denomination mix or by total cents with deterministic change-making rules

### Coin Batches

Coin production is recorded separately from coin ownership:

- `CoinBatch`
- `batch_id`
- producer/miner
- mint block
- work proof metadata
- denomination mix minted in the batch
- total face value

Purpose:

- attribute production
- record compute/work cost
- support issuance auditing
- avoid carrying serial identity on every coin
- support melting coin value back into actual compute use

Coins should not inherit a persistent individual on-chain object ID.

## Proposed Data Model Changes

### 1. Replace single-token account storage

Current:

- `AccountState.balance`
- `AccountState.tokens: Vec<DenominationToken>`

Target:

- `AccountState.bill_balance_cents`
- `AccountState.coin_balance_cents`
- `AccountState.bills: Vec<BillToken>`
- `AccountState.coins: CoinInventory`
- `AccountState.compute_entitlements` or equivalent runtime allocation state for redeemed compute use

If we want to minimize disruption, keep:

- `balance` as compatibility mirror

Computed as:

- `sum(bills) + sum(coin inventory)`

### 2. Split denomination enum usage

Keep `Denomination`, but add classification helpers:

- `is_bill()`
- `is_coin()`

Then stop treating all denominations as mintable object IDs.

### 3. Introduce coin batch persistence

New store keys:

- `coin_batch:<batch_id>`
- `coin_batch_by_block:<height>:<batch_id>`
- `coin_batch_by_producer:<producer>:<batch_id>`

Optional accounting keys:

- `coin_supply_total`
- `coin_supply_by_denomination`

## Proposed Transaction Model

The current transaction record is too object-centric.

### Current shape

- `transfer_token_ids: Vec<Hash>`
- `fee_token_id: Option<Hash>`
- `value: u64`

### Target shape

Add explicit payment fields:

- `bill_transfer_ids: Vec<Hash>`
- `coin_transfer: Option<CoinTransfer>`
- `coin_melt: Option<CoinMelt>`
- `fee_payment: FeePayment`
- `value: u64` remains a compatibility mirror or derived total

Suggested supporting types:

- `CoinTransfer`
  - `denomination_amounts` or `total_cents`
- `FeePayment`
  - `BillToken(Hash)`
  - `Coins(CoinTransfer)`
  - `Hybrid { bill_token_id: Option<Hash>, coin_amounts: ... }`
- `CoinMelt`
  - `denomination_amounts` or `total_cents`
  - `compute_use_mode`
    - `ImmediateExecution`
    - `ReservedCapacity`

Recommended first version:

- support fees in coins only
- support bill transfers plus coin transfers in the same transaction

That keeps implementation simpler than hybrid fee routing in the first pass.

## Execution Model Changes

### Bills

Reuse the current object locking pattern:

- declared inputs are bill IDs
- stale-version checks remain
- ownership remains explicit

### Coins

Coins should not be locked by fake token IDs.

Instead:

- lock sender and recipient account coin inventories
- validate sufficient coin inventory
- debit sender denomination counts
- credit recipient denomination counts
- increment sender nonce

For coin melting:

- lock the sender account coin inventory
- burn the requested coin amount
- allocate compute use to the sender according to the selected mode

This implies the executor needs two conflict domains:

- bill object locks
- account-level coin inventory locks

The current executor can be evolved instead of replaced if we generalize its lock set representation.

## Validation Changes

### Mempool

Replace "owns exact token IDs" checks for coins with:

- sender has enough coin inventory for transfer
- sender has enough fee inventory for fee payment
- bill IDs, if present, are owned by sender
- sender has enough coin inventory to melt when redeeming compute use

### Consensus validator

Replace exact-token payment equality with:

- total declared bill value + total declared coin value == `tx.value`
- fee path is valid
- sender has sufficient bill and coin holdings

### State store projection

`StateStore` and projected state-root calculation must derive results from both:

- bill movement
- coin inventory movement
- compute-use redemption state

## Minting And Reward Changes

### Bills

Reward logic can still mint bill objects when the schedule yields bill denominations.

### Coins

Reward logic should stop minting sub-dollar denominations as `DenominationToken`s.

Instead:

- aggregate all sub-dollar reward outputs for the block
- materialize one `CoinBatch`
- credit the miner's `CoinInventory`
- persist the `CoinBatch`

Separate from block rewards, bill-breaking must:

- destroy or transform the bill form being broken
- mint the matching coin inventory outcome
- preserve conservation of face value across forms

### Work-backed production

First implementation target:

- batch carries verifiable work metadata
- verification reuses or extends existing PoW data already available at block time

Do not block the ledger refactor on designing a whole new compute-proof market. The first version can record:

- block hash
- difficulty
- producer
- minted coin mix

Then later strengthen the work model.

## Breaking And Melting Rules

### Breaking bills into coins

- users may convert `$1+` bill objects into fungible coin inventory
- `$1` is a boundary case because it may exist as either bill form or coin form
- breaking should be a protocol state transition, not an off-chain wallet trick

### Melting coins into compute use

- melting burns coin inventory
- melting does not return a tokenized compute credit
- melting returns actual compute use on the network
- the user should be able to choose:
  - immediate execution
  - reserved capacity

## Migration Strategy

This should be treated as a ledger-version migration.

### Safer path

- introduce a new state schema version
- reset network/genesis if production compatibility is not mandatory

### Harder path

Migrate in place:

- convert existing `$1+` `DenominationToken`s into `BillToken`s
- aggregate existing sub-dollar `DenominationToken`s by owner into `CoinInventory`
- record synthetic `CoinBatch` entries tagged as migration-origin

Recommendation:

- use a new network version unless preserving current chain state is essential

## Delivery Phases

### Phase 0. Spec lock

Decisions required before code:

- are coin balances stored by denomination or only total cents
- are fees payable in bills, coins, or coins-only initially
- can transactions request automatic coin change-making
- does a reward block create one coin batch or many
- how compute use allocation is represented in state after coin melting
- whether bill breaking burns the old object or records a reversible form conversion

Exit criteria:

- type definitions chosen
- transaction encoding agreed
- migration path agreed

### Phase 1. State model refactor

Files:

- [`blockchain/storage/state.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state.rs:1)
- [`blockchain/storage/state_store.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/state_store.rs:1)

Tasks:

- add `BillToken`
- add `CoinInventory`
- add `CoinBatch`
- update `AccountState`
- add compatibility total-value helpers
- add serialization tests

Exit criteria:

- account state can represent both bill objects and coin balances

### Phase 2. Transaction schema refactor

Files:

- [`blockchain/mempool/types.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/mempool/types.rs:1)
- [`blockchain/storage/tx_store.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/tx_store.rs:1)
- network transaction codecs/handlers

Tasks:

- replace object-only payment fields
- update transaction signing and hash material
- update serialization tests

Exit criteria:

- transactions can express bill and coin payments separately

### Phase 3. Validation and execution refactor

Files:

- [`blockchain/mempool/pool.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/mempool/pool.rs:1)
- [`blockchain/consensus/validation/transaction_validator.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/consensus/validation/transaction_validator.rs:1)
- [`blockchain/executor/mod.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/executor/mod.rs:1)

Tasks:

- validate bill ownership separately from coin sufficiency
- extend executor lock model
- support coin debit/credit logic
- preserve current nonce semantics

Exit criteria:

- block execution succeeds for mixed bill-plus-coin transactions

### Phase 4. Reward and issuance refactor

Files:

- [`blockchain/src/rewards.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/src/rewards.rs:1)
- [`blockchain/storage/block_store.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/storage/block_store.rs:1)

Tasks:

- mint bills as `BillToken`s
- mint coins as `CoinBatch`
- store batch metadata in block result or side indexes

Exit criteria:

- miner reward output populates both bill inventory and coin inventory correctly

### Phase 5. Tooling and wallet update

Files:

- [`blockchain/tools/development.rs`](/Users/armenmerikyan/Desktop/wd/kumquat/blockchain/tools/development.rs:1)
- CLI wallet/explorer surfaces
- website wallet displays if wired to chain state

Tasks:

- update transaction construction helpers
- show bills as discrete units
- show coins as counted inventory

Exit criteria:

- developers can build and inspect mixed cash transactions locally

### Phase 6. Migration or regenesis

Tasks:

- write one-time converter or genesis loader
- add integration tests from old state to new state

Exit criteria:

- network boot path is defined and reproducible

## Test Plan

Add tests per phase.

Priority cases:

- bill-only transfer
- coin-only transfer
- mixed bill-plus-coin transfer
- fee paid from coin inventory
- insufficient coins with sufficient bills
- insufficient bills with sufficient coins
- reward block producing only bills
- reward block producing only coins
- reward block producing both
- migration of legacy sub-dollar token objects into coin inventory

## Recommended Immediate Next Steps

1. Lock the coin representation decision:
   - per-denomination inventory is the better fit for "real cash"
2. Lock the first fee rule:
   - coins-only fees is the fastest path
3. Implement Phase 1 without trying to preserve old transaction compatibility
4. Follow with Phase 2 and Phase 3 together so the ledger shape and transaction shape stay aligned

## Suggested Initial Defaults

Unless we decide otherwise, the fastest coherent build is:

- `$1` is a bill object
- coins are stored per denomination
- transactions may specify coin denominations explicitly
- fees are paid in coins only for v1
- each block can emit at most one aggregated `CoinBatch` for sub-dollar rewards
- migration uses a new network version rather than in-place state conversion

## Changelog

- `2026-04-15`: Added the first phased implementation plan for the hybrid cash ledger refactor.
- `2026-04-15`: Added the locked decisions that compute acts as metal, kumquats pay for production compute, coins can be broken from bills, and melting coins returns actual compute use.

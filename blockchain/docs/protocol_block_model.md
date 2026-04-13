# Kumquat Block Model

This document defines the current consensus-critical block model for the Rust blockchain implementation.

## Canonical Block Shape

The stored `Block` carries three distinct layers of data:

1. PoW header inputs
2. body payload
3. final post-reward result

The logical split is:

### CanonicalBlockHeader

Defined by `Block::canonical_header()` in `storage/block_store.rs`.

Fields:

- `height`
- `prev_hash`
- `timestamp`
- `miner`
- `pre_reward_state_root`
- `tx_root`
- `nonce`
- `poh_seq`
- `poh_hash`
- `difficulty`
- `total_difficulty`

This is the canonical input to PoW hashing.

### CanonicalBlockBody

Defined by `Block::canonical_body()` in `storage/block_store.rs`.

Fields:

- `transactions`
- `reward_token_ids`
- `state_root`
- `result_commitment`

This captures the block payload plus the final post-reward result.

## Consensus-Critical Hashes And Commitments

The implementation centralizes these functions in `storage/block_store.rs`.

### 1. PoW Hash

Function:

- `pow_hash(header: &CanonicalBlockHeader) -> Hash`

Preimage order:

1. `height`
2. `prev_hash`
3. `timestamp`
4. `miner`
5. `pre_reward_state_root`
6. `tx_root`
7. `nonce`

This is the validated mining hash. Reward lottery derivation is based on this hash.

### 2. Reward Outcome

Function:

- `reward_outcome(owner, block_height, block_hash)`

Implementation:

- `storage/block_store.rs` delegates to `rewards.rs`

Reward lottery inputs:

- validated `block.hash`
- `block.height`

Era schedule and denomination weights are compiled into the node from:

- `blockchain/kumquat_mining_schedule.json`

### 3. Final Result Commitment

Function:

- `result_commitment(block_hash, state_root, reward_token_ids) -> Hash`

Preimage order:

1. `block_hash`
2. `state_root`
3. `reward_token_ids.len()`
4. each `reward_token_id` in order

This binds the validated PoW result to the final reward outcome and final post-reward state root without creating a circular dependency in PoW hashing.

## Block Construction Flow

### Mining

1. Collect pending transactions.
2. Compute `tx_root`.
3. Compute `pre_reward_state_root` by applying transactions only.
4. Build a block template using those header inputs.
5. Mine a nonce against `pow_hash(canonical_header)`.
6. Derive reward tokens from the validated PoW hash and block height.
7. Compute final `state_root` by applying transactions plus block reward.
8. Compute `result_commitment(block.hash, state_root, reward_token_ids)`.
9. Broadcast the completed block.

## Validation Order On Receiving Nodes

Current validator order in `consensus/validation/block_validator.rs`:

1. verify parent presence
2. verify transactions exist and `tx_root` matches
3. recompute transaction-only `pre_reward_state_root`
4. verify recomputed `pre_reward_state_root` equals the transmitted `pre_reward_state_root`
5. recompute `pow_hash(canonical_header)` and verify it equals transmitted `block.hash`
6. recompute reward outcome from validated `block.hash` and `block.height`
7. verify `reward_token_ids`
8. recompute and verify `result_commitment`
9. verify PoW target
10. apply transactions plus reward and verify final `state_root`
11. verify PoH linkage

## Why The Model Is Split

The protocol intentionally separates:

- `pre_reward_state_root` for PoW hash stability
- final `state_root` for post-reward ledger correctness
- `result_commitment` for explicit binding between the two phases

This avoids a circular dependency where the block hash would need the reward result while the reward result itself is derived from the block hash.

## Current Follow-Up Work

- Move all production and test block construction to a single builder API.
- Remove stale alternate block/header code paths that no longer match this model.
- Extend tests so `cargo test --lib` validates this exact flow cleanly.

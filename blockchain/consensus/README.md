# Kumquat Chain Consensus Module

## Overview

The consensus module implements Kumquat Chain's hybrid Proof-of-Work and Proof-of-History design. It combines cumulative-work security with ordered sequencing so the chain can support legible, cash-like digital units and explicit transfer semantics.

The module is responsible for:

- mining new blocks with a PoW nonce
- embedding PoH sequences for ordering
- validating blocks and applying them to state
- resolving forks based on cumulative work
- anchoring mint and transfer finality for Kumquat units

## Architecture

```text
                  ┌─────────────────┐
                  │  ConsensusEngine│
                  └────────┬────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │        Block Validation        │
          └────────┬─────────────┬─────────┘
                   │             │
        ┌──────────┴──────────┐ │ ┌─────────────────┐
        │    Block Mining     │ │ │   Fork Choice   │
        └┬─────┬──────┬──────┬┘ │ └────────┬────────┘
         │     │      │      │  │          │
         ▼     ▼      ▼      ▼  │          ▼
┌──────────┐ ┌───┐ ┌─────┐ ┌───┐│  ┌───────────────┐
│PoW Mining│ │PoH│ │State│ │Diff││  │ Chain Selection│
│          │ │Gen │ │Root │ │Adj ││  │               │
└────┬─────┘ └─┬─┘ └──┬──┘ └─┬─┘│  └───────┬───────┘
     │         │      │      │  │          │
     └─────────┴──────┴──────┴──┴──────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Storage Module │
                  └─────────────────┘
```

## Role In Kumquat

Within Kumquat, consensus is not just about chain selection. It is the layer that makes visible units trustworthy enough to mint, transfer, and display as real objects in the wallet model shown on `kumquat.info`.

It needs to preserve:

- durable identity for minted value
- predictable ordering for transfers
- understandable reorg behavior at the wallet layer
- enough integrity for future denomination-aware mint logic

## Components

### Proof of Work (`pow/`)

- `miner.rs` implements mining
- `difficulty.rs` handles difficulty adjustment

### Proof of History (`poh/`)

- `generator.rs` generates the sequential hash chain
- `verifier.rs` verifies PoH entries

### Validation (`validation/`)

- `block_validator.rs` validates blocks
- `transaction_validator.rs` validates transactions
- `fork_choice.rs` selects the canonical chain

### Mining (`mining/`)

- `block_producer.rs` creates new block templates
- `mempool.rs` manages pending transactions for mining

### Core

- `types/` defines consensus types
- `config.rs` defines consensus configuration
- `engine.rs` ties the module together

## Usage

```rust
use crate::consensus::{start_consensus, ConsensusConfig};
use crate::storage::{BlockStore, TxStore, StateStore};
use std::sync::Arc;
use tokio::sync::mpsc;

let block_store = Arc::new(BlockStore::new(&kv_store));
let tx_store = Arc::new(TxStore::new(&kv_store));
let state_store = Arc::new(StateStore::new(&kv_store));
let (network_tx, _) = mpsc::channel(100);

let config = ConsensusConfig::default()
    .with_target_block_time(15)
    .with_initial_difficulty(100)
    .with_mining_enabled(true);

let _engine = start_consensus(
    config,
    block_store,
    tx_store,
    state_store,
    network_tx,
).await;
```

## Future Improvements

- denomination-aware minting rules
- stronger object and finality semantics
- improved fork choice and checkpointing
- parallel transaction validation
- optimized PoH verification
- light client support

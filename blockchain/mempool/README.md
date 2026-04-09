# Kumquat Chain Transaction Pool (Mempool)

## Overview

The mempool is the staging area for pending Kumquat Chain transfers. It:

- accepts verified transactions from users and peers
- queues them for block producers
- prevents spam and invalid transaction propagation

## Role In Kumquat

The cash-like transfer model starts becoming operational here. Before value can move on chain, pending transfers need to be checked, ordered, and prepared in a way that preserves legibility and prevents abuse.

As Kumquat evolves toward denomination-aware and object-aware transfers, the mempool is where rules around ordering, composition, and spend validity will be enforced before block inclusion.

## Architecture

```text
     ┌──────────────────────────────┐
     │        Network Module        │
     └────────────┬────────────────┘
                  ↓
       Incoming transactions
                  ↓
          ┌───────────────┐
          │   Mempool     │
          └─────┬─────────┘
                ↓
      ┌─────────────────────┐
      │ Consensus / Miner   │
      └─────────────────────┘
```

## Components

### Transaction Record

The `TransactionRecord` structure represents a pending transaction in the pool, including sender, recipient, value, gas fields, nonce, timestamp, signature, and optional data.

### Mempool

The `Mempool` type provides the core functionality:

- `insert`
- `get_pending`
- `remove`
- `mark_included`
- `cleanup_expired`
- `prune_included`

## Prioritization

Transactions are currently prioritized by:

1. gas price
2. timestamp

That keeps higher-priority transactions moving first while preserving reasonable fairness for older entries.

# Kumquat Chain Storage Module

## Overview

The storage module provides the persistence layer for Kumquat Chain. It stores blocks, transactions, account states, Proof of History entries, and object-oriented records on top of RocksDB.

This module matters directly to Kumquat's product concept. If wallets are meant to show visible units, denomination mix, and transfer state, the storage layer has to preserve more than a simple aggregate-balance view.

## Role In Kumquat

Kumquat is centered on a physical-cash mental model for digital money. The storage layer is where that becomes concrete:

- units need durable identity
- ownership needs to be queryable
- denomination-aware state needs to remain legible
- wallet-facing reads need to support both totals and composition

The object store is especially important because it is the closest current primitive to the visible-unit model described on `kumquat.info`.

## Components

### `KVStore`

Defines the storage interface and RocksDB-backed implementation.

### `BlockStore`

Stores and retrieves blocks by height and hash.

### `TxStore`

Stores transaction records with lookup indexes.

### `StateStore`

Maintains account balances, nonces, and contract-related state.

### `ObjectStore`

Implements a Sui-style object ID model so state can be handled in a more object-centric way. That makes it a natural bridge toward Kumquat's denomination-first wallet model.

### `PoHStore`

Persists Proof of History entries used by consensus.

### Trie

Provides a Merkle Patricia Trie for verifiable state storage.

## Data Schema

The module uses prefix-based RocksDB keys for blocks, transactions, state, objects, and PoH history. See the broader storage docs in `blockchain/docs/` for the full schema details.

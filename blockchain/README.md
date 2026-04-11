# Kumquat Chain

Kumquat Chain is the protocol layer for Kumquat's digital cash model. It exists to support money that behaves more like cash on the internet: visible units, explicit transfers, and wallets that show composition instead of only one abstract balance.

The product framing on [kumquat.info](https://kumquat.info) is the guide for this folder:

- digital money should keep the logic of cash
- denominations should feel like units you can hold and hand over
- wallets should show count, composition, and movement clearly
- the chain should support object-like value, not just opaque balance mutation

This directory contains the Rust blockchain implementation, module docs, and local tooling for that direction.

## Overview

The current implementation combines:

- Proof of Work for cumulative-work security
- Proof of History for ordering and verifiable sequencing
- object-aware storage primitives for unit-oriented state
- networking, mempool, and persistence layers for full node operation

Kumquat Chain is not being positioned here as a generic L1. The intended direction is a chain that can back a denomination-first digital money system and eventually plug into the broader Kumquat Farm model described in the root repo docs.

## Directory Layout

- `src/` contains the node entrypoint and binaries
- `consensus/` contains the PoW/PoH consensus logic
- `network/` contains peer-to-peer networking and sync
- `storage/` contains RocksDB-backed persistence and object/state storage
- `crypto/` contains key, signature, and hashing primitives
- `mempool/` contains pending-transaction handling
- `docs/` contains architecture and module documentation
- `scripts/` contains bootstrap helpers

## Getting Started

### Prerequisites

- Rust 1.70 or higher
- Cargo
- 8GB+ RAM
- 100GB+ free disk space
- Linux, macOS, or Windows with WSL

### Build

```bash
cargo build --release
cargo test
```

### Run a Node

Use the generated config tool, genesis tool, and node binary from `./target/release/` to create configuration, build genesis state, and start the node.

This README update is intentionally scoped to documentation only, so command examples stay generic while the underlying codebase still carries older internal names.

### Node Dashboard

The Rust node now exposes an embedded read-only web dashboard when `node.enable_api = true`.

- dashboard: `http://<api_host>:<api_port>/dashboard`
- JSON status: `http://<api_host>:<api_port>/api/status`
- health check: `http://<api_host>:<api_port>/health`

The first implementation reports real values where the runtime already exposes them, such as latest block height, peer count, mempool size, uptime, and database path/size. Fields like hash rate, sync progress, and operator commands are intentionally marked as not fully instrumented yet.

### Docker

```bash
./scripts/bootstrap_devnet.sh
docker-compose -f docker-compose.dev.yml up -d
```

## Core Modules

- [Consensus](./consensus/README.md): block production, ordering, and fork choice
- [Network](./network/README.md): peer discovery, propagation, and synchronization
- [Storage](./storage/README.md): persistent state, objects, and history
- [Cryptography](./crypto/README.md): signatures, keys, and hashing
- [Mempool](./mempool/README.md): pending transaction intake and prioritization

## Documentation

See [docs](./docs) for architecture, API, and module documentation.

## Status

This blockchain folder was copied forward from earlier standalone project work and is now being integrated into the main Kumquat repository. It still carries legacy names internally, so the documentation is being aligned first while the underlying implementation is brought over in stages.

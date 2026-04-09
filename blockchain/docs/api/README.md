# Kumquat Chain API Documentation

This directory contains documentation for the Kumquat Chain APIs.

## API Overview

Kumquat Chain exposes several API surfaces for interacting with the blockchain:

- JSON-RPC API for core node interaction
- WebSocket API for subscriptions and real-time updates
- REST-style endpoints where simpler HTTP access is useful

## API Categories

- [Node API](node.md): interact with nodes
- [Wallet API](wallet.md): manage wallets and signing flows
- [Transaction API](transaction.md): create and manage transfers
- [Smart Contract API](smart_contract.md): deploy and interact with contracts
- [Block API](block.md): query blocks and chain data

## Versioning

APIs are versioned to preserve compatibility as protocol and wallet-facing surfaces evolve. The current version is `v1`.

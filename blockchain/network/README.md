# Kumquat Chain Network Module

## Overview

The network module provides the peer-to-peer layer for Kumquat Chain. It handles peer discovery, connection management, message broadcasting, and blockchain data synchronization.

In Kumquat terms, this is the layer that carries transfer events, chain state, and unit ownership history between nodes so the wallet story can stay consistent across the network.

## Architecture

```text
                  ┌─────────────────┐
                  │  NetworkService │
                  └────────┬────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │      AdvancedPeerRegistry      │
          └────────────────┬───────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────┐
│                  PeerHandler                     │
└──────────┬─────────────────┬────────────┬────────┘
           │                 │            │
           ▼                 ▼            ▼
┌─────────────────┐ ┌───────────────┐ ┌──────────────┐
│ PeerBroadcaster │ │ MessageRouter │ │ SystemRouter │
└────────┬────────┘ └───────┬───────┘ └──────┬───────┘
         │                  │                │
         ▼                  ▼                ▼
┌─────────────────┐ ┌───────────────┐ ┌──────────────┐
│ Network Messages│ │ Message Types │ │ Subsystems   │
└─────────────────┘ └───────────────┘ └──────────────┘
```

## Components

- `types/` defines message and node-info types
- `peer/` handles peer state, registry, broadcasting, and reputation
- `service/` handles listening, dialing, and routing
- `handlers/` handles transaction, block, and sync messages
- `integration/` connects the network to mempool, storage, and consensus
- `events/` provides event-driven communication
- `sync/` handles chain synchronization
- `codec/` handles framing and serialization

## Role In Kumquat

Kumquat is moving toward a model where value is easier to read because the system treats it more like discrete units than a single opaque number. The network layer has to propagate:

- newly minted units
- transfer intent and transaction data
- block history that defines ownership
- synchronization state for nodes joining or recovering

## Future Improvements

- browser-friendly gateway support
- NAT traversal
- DNS-seed discovery
- rate limiting and bandwidth control
- encrypted peer transport
- cleaner farm-to-farm service discovery

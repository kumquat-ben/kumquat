# Kumquat Chain Network Module (Updated)

## Overview

This updated README describes the current network-module structure for Kumquat Chain. It covers peer discovery, connection management, message routing, and synchronization for the protocol layer behind Kumquat's digital cash model.

## Components

### Peer Management

- `PeerRegistry` tracks peer state and metadata
- `PeerHandler` manages individual peer connections
- `PeerBroadcaster` distributes messages to peers

### Message Routing

- `MessageRouter` routes messages to handlers
- `SystemRouter` connects network messages to mempool, storage, and consensus

### Network Service

- `NetworkService` coordinates network activity
- `Listener` accepts inbound connections
- `Dialer` establishes outbound connections

### Message Codec

- framed readers and writers handle serialization and framing

## Key Features

- peer state tracking
- connection health monitoring
- automatic reconnection
- message broadcasting
- subsystem routing
- state synchronization

## Kumquat Context

The live Kumquat product story is built around digital money that behaves more like cash you can read and hand over. The network layer is what makes that model network-native by moving transactions, ownership changes, and chain history between nodes consistently.

## Future Improvements

- browser-friendly client support
- NAT traversal
- DNS-seed discovery
- connection encryption
- DHT-based peer discovery

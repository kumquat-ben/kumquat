// Kumquat Network Module
//
// This module provides the peer-to-peer networking layer for the Kumquat blockchain:
// - Peer discovery and connection management
// - Message broadcasting (blocks, transactions)
// - Blockchain data synchronization
// - Support for distributed consensus

pub mod codec;
pub mod events;
pub mod handlers;
pub mod integration;
pub mod peer;
pub mod service;
pub mod sync;
pub mod types;

use crate::network::handlers::message_handler::HandlerRegistry;
use log::error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

use crate::network::service::advanced_router::AdvancedMessageRouter;
use crate::network::service::NetworkService;
use crate::network::sync::sync_service::SyncService;
use crate::network::types::message::NetMessage;
use crate::storage::block_store::Hash;

#[derive(Clone)]
pub struct EnhancedNetworkHandle {
    pub service: Arc<NetworkService>,
    pub sync_service: Option<Arc<SyncService>>,
}

/// Configuration for the network module
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    /// Local address to bind to
    pub bind_addr: SocketAddr,

    /// List of seed peers to connect to
    pub seed_peers: Vec<SocketAddr>,

    /// Raw bootstrap peer specs that should be re-resolved and retried.
    pub seed_peer_specs: Vec<String>,

    /// Maximum number of outbound connections
    pub max_outbound: usize,

    /// Maximum number of inbound connections
    pub max_inbound: usize,

    /// Node ID (derived from public key)
    pub node_id: String,

    /// Chain identifier advertised during handshake.
    pub chain_id: u64,

    /// Genesis hash advertised during handshake.
    pub genesis_hash: Hash,

    /// Timeout for outbound connection attempts.
    pub connection_timeout: Duration,

    /// Interval between bootstrap discovery and reconnect attempts.
    pub bootstrap_retry_interval: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8765".parse().unwrap(),
            seed_peers: vec![],
            seed_peer_specs: vec![],
            max_outbound: 8,
            max_inbound: 32,
            node_id: "unknown".to_string(),
            chain_id: 0,
            genesis_hash: [0u8; 32],
            connection_timeout: Duration::from_secs(10),
            bootstrap_retry_interval: Duration::from_secs(30),
        }
    }
}

/// Start the network service with the given configuration
pub async fn start_network(config: NetworkConfig) -> Arc<NetworkService> {
    let (_message_tx, message_rx) = mpsc::channel(100);

    let service = NetworkService::new(config, message_rx);
    let service_arc = Arc::new(service);

    // Clone the Arc for the spawned task
    let service_clone = service_arc.clone();

    // Start the network service in a separate task
    tokio::spawn(async move {
        service_clone.run().await;
    });

    service_arc
}

/// Start the enhanced network service with the given configuration
pub async fn start_enhanced_network(
    config: NetworkConfig,
    block_store: Option<Arc<crate::storage::block_store::BlockStore<'static>>>,
    tx_store: Option<Arc<crate::storage::tx_store::TxStore<'static>>>,
    mempool: Option<Arc<crate::mempool::Mempool>>,
    consensus: Option<Arc<crate::consensus::engine::ConsensusEngine>>,
) -> EnhancedNetworkHandle {
    // Create the peer registry and broadcaster
    let peer_registry = Arc::new(peer::registry::PeerRegistry::new());
    let advanced_registry = Arc::new(peer::advanced_registry::AdvancedPeerRegistry::new());
    let broadcaster = Arc::new(peer::broadcaster::PeerBroadcaster::new());

    // Create the basic network service with shared peer infrastructure
    let (_message_tx, message_rx) = mpsc::channel(100);
    let service = NetworkService::new_with_components(
        config,
        message_rx,
        peer_registry.clone(),
        broadcaster.clone(),
        Some(advanced_registry.clone()),
        block_store.clone(),
    );
    let service_arc = Arc::new(service);

    // Create the event bus
    let event_bus = Arc::new(events::event_bus::EventBus::new());

    // Create the reputation system
    let reputation = Arc::new(peer::reputation::ReputationSystem::new());

    // Create the handler registry
    let handler_registry = Arc::new(RwLock::new(HandlerRegistry::new()));

    // Create the advanced router
    let router = Arc::new(AdvancedMessageRouter::new(
        handler_registry,
        peer_registry.clone(),
        broadcaster.clone(),
    ));

    // Add subsystems to the router
    let router = if let Some(mempool) = mempool.clone() {
        router.with_mempool(mempool)
    } else {
        router
    };

    let router = if let Some(block_store) = block_store.clone() {
        router.with_block_store(block_store)
    } else {
        router
    };

    let router = if let Some(tx_store) = tx_store.clone() {
        router.with_tx_store(tx_store)
    } else {
        router
    };

    let _router = if let Some(consensus) = consensus.clone() {
        router.with_consensus(consensus)
    } else {
        router
    };

    // Create the system router
    let router_arc = service_arc.router();
    let system_router =
        service::system_router::SystemRouter::new(router_arc).with_broadcaster(broadcaster.clone());

    let system_router = if let Some(mempool) = mempool.clone() {
        system_router.with_mempool(mempool)
    } else {
        system_router
    };

    let system_router = if let Some(block_store) = block_store.clone() {
        system_router.with_block_store(block_store)
    } else {
        system_router
    };

    let system_router = if let Some(tx_store) = tx_store.clone() {
        system_router.with_tx_store(tx_store)
    } else {
        system_router
    };

    let system_router = if let Some(consensus) = consensus.clone() {
        system_router.with_consensus(consensus)
    } else {
        system_router
    };

    // Initialize the system router
    tokio::spawn(async move {
        if let Err(e) = system_router.initialize().await {
            error!("Failed to initialize system router: {}", e);
        }
    });

    // Create integrations if subsystems are provided
    let mut sync_service_handle = None;

    if let (Some(mempool), Some(block_store_clone)) = (mempool.clone(), block_store.clone()) {
        // Clone block_store for each use to avoid ownership issues
        // Create mempool integration
        let _mempool_integration = integration::mempool_integration::MempoolIntegration::new(
            mempool,
            broadcaster.clone(),
            peer_registry.clone(),
        )
        .with_reputation(reputation.clone());

        // Create storage integration
        let _storage_integration = integration::storage_integration::StorageIntegration::new(
            block_store_clone.clone(),
            broadcaster.clone(),
            peer_registry.clone(),
        )
        .with_reputation(reputation.clone());

        // Create sync service
        let mut response_block_rx = service_arc.router().create_channel("response_block").await;
        let mut response_block_range_rx = service_arc
            .router()
            .create_channel("response_block_range")
            .await;

        let (block_tx, block_rx) = mpsc::channel(100);
        let (block_range_tx, block_range_rx) = mpsc::channel(100);

        tokio::spawn(async move {
            while let Some((peer_id, message)) = response_block_rx.recv().await {
                if let NetMessage::ResponseBlock(Some(block)) = message {
                    if block_tx.send((block, peer_id)).await.is_err() {
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Some((peer_id, message)) = response_block_range_rx.recv().await {
                if let NetMessage::ResponseBlockRange(blocks) = message {
                    if block_range_tx.send((blocks, peer_id)).await.is_err() {
                        break;
                    }
                }
            }
        });

        let sync_service = Arc::new(
            sync::sync_service::SyncService::new(
                block_store_clone.clone(),
                peer_registry.clone(),
                broadcaster.clone(),
            )
            .with_consensus_opt(consensus.clone())
            .with_advanced_registry(advanced_registry.clone())
            .with_chain_identity(service_arc.chain_id(), service_arc.genesis_hash())
            .with_event_bus(event_bus.clone())
            .with_reputation(reputation.clone())
            .with_block_channel(block_rx)
            .with_block_range_channel(block_range_rx),
        );
        sync_service_handle = Some(sync_service.clone());

        // Create sync manager if block_store is available
        if let Some(block_store_clone) = block_store.clone() {
            let sync_manager = Arc::new(
                sync::sync_manager::SyncManager::new(
                    sync_service,
                    block_store_clone,
                    peer_registry.clone(),
                    broadcaster.clone(),
                )
                .with_advanced_registry(advanced_registry.clone())
                .with_event_bus(event_bus.clone())
                .with_reputation(reputation.clone()),
            );

            tokio::spawn(async move {
                if let Err(e) = sync_manager.start().await {
                    error!("Failed to start sync manager: {}", e);
                }
            });
        }

        // We've already started the sync manager in the if block above
    }

    // Start the network service
    let service_clone = service_arc.clone();
    tokio::spawn(async move {
        service_clone.run().await;
    });

    EnhancedNetworkHandle {
        service: service_arc,
        sync_service: sync_service_handle,
    }
}

/// Create a network message sender
pub fn create_message_sender() -> mpsc::Sender<NetMessage> {
    let (tx, _rx) = mpsc::channel(100);
    tx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_config() {
        let config = NetworkConfig::default();
        assert_eq!(config.bind_addr.to_string(), "127.0.0.1:8765");
        assert_eq!(config.max_outbound, 8);
        assert_eq!(config.max_inbound, 32);
    }
}

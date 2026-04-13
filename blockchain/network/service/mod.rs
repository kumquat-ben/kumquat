// Network service module

pub mod advanced_router;
pub mod dialer;
pub mod listener;
pub mod router;
pub mod system_router;

use log::{debug, error};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::network::peer::broadcaster::PeerBroadcaster;
use crate::network::peer::manager::PeerManager;
use crate::network::peer::registry::PeerRegistry;
use crate::network::service::listener::start_listener;
use crate::network::service::router::MessageRouter;
use crate::network::types::message::NetMessage;
use crate::network::types::node_info::NodeInfo;
use crate::network::NetworkConfig;

/// Main network service
pub struct NetworkService {
    /// Network configuration
    config: NetworkConfig,

    /// Peer manager
    peer_manager: PeerManager,

    /// Message router
    router: Arc<MessageRouter>,

    /// Channel for outgoing messages
    message_rx: Arc<Mutex<mpsc::Receiver<NetMessage>>>,

    /// Channel for incoming messages from peers
    incoming_tx: mpsc::Sender<(String, NetMessage)>,

    /// Channel for incoming messages from peers
    incoming_rx: Arc<Mutex<mpsc::Receiver<(String, NetMessage)>>>,
}

// Implement Clone for NetworkService
impl Clone for NetworkService {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            peer_manager: self.peer_manager.clone(),
            router: self.router.clone(),
            message_rx: self.message_rx.clone(),
            incoming_tx: self.incoming_tx.clone(),
            incoming_rx: self.incoming_rx.clone(),
        }
    }
}

impl NetworkService {
    /// Create a new network service
    pub fn new(config: NetworkConfig, message_rx: mpsc::Receiver<NetMessage>) -> Self {
        let peer_registry = Arc::new(PeerRegistry::new());
        let broadcaster = Arc::new(PeerBroadcaster::with_registry(Some(peer_registry.clone())));
        Self::new_with_components(config, message_rx, peer_registry, broadcaster)
    }

    /// Create a new network service with shared peer infrastructure
    pub fn new_with_components(
        config: NetworkConfig,
        message_rx: mpsc::Receiver<NetMessage>,
        peer_registry: Arc<PeerRegistry>,
        broadcaster: Arc<PeerBroadcaster>,
    ) -> Self {
        // Create channels for incoming messages
        let (incoming_tx, incoming_rx) = mpsc::channel(100);

        // Create the message router
        let router = Arc::new(MessageRouter::new());

        // Create the local node info
        let local_node_info = NodeInfo::new(
            "0.1.0".to_string(), // TODO: Get from config
            config.node_id.clone(),
            config.bind_addr,
        );

        // Create the peer manager
        let peer_manager = PeerManager::new(
            local_node_info,
            router.clone(),
            incoming_tx.clone(),
            config.max_outbound,
            config.max_inbound,
            peer_registry,
            broadcaster,
        );

        Self {
            config,
            peer_manager,
            router,
            message_rx: Arc::new(Mutex::new(message_rx)),
            incoming_tx,
            incoming_rx: Arc::new(Mutex::new(incoming_rx)),
        }
    }

    /// Run the network service
    pub async fn run(&self) {
        // Start the peer manager
        self.peer_manager.start().await;

        // Start the listener
        let peer_manager = self.peer_manager.clone();
        let bind_addr = self.config.bind_addr;

        tokio::spawn(async move {
            if let Err(e) = start_listener(bind_addr, peer_manager).await {
                error!("Listener error: {}", e);
            }
        });

        // Connect to seed peers
        for &addr in &self.config.seed_peers {
            self.peer_manager.connect_to_peer(addr).await;
        }

        // Process messages
        self.process_messages().await;
    }

    /// Process incoming and outgoing messages
    async fn process_messages(&self) {
        loop {
            let message_rx = self.message_rx.clone();
            let incoming_rx = self.incoming_rx.clone();
            tokio::select! {
                // Handle outgoing messages
                message = async {
                    let mut rx = message_rx.lock().await;
                    rx.recv().await
                } => {
                    if let Some(message) = message {
                        debug!("Broadcasting message: {:?}", message);
                        self.peer_manager.broadcast(message).await;
                    }
                }

                // Handle incoming messages from peers
                incoming = async {
                    let mut rx = incoming_rx.lock().await;
                    rx.recv().await
                } => {
                    if let Some((node_id, message)) = incoming {
                        debug!("Received message from {}: {:?}", node_id, message);
                        self.router.route_message(node_id, message).await;
                    }
                }
            }
        }
    }

    /// Get the peer manager
    pub fn peer_manager(&self) -> &PeerManager {
        &self.peer_manager
    }

    /// Get the message router
    pub fn router(&self) -> Arc<MessageRouter> {
        self.router.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_network_service_creation() {
        // Create a network config
        let config = NetworkConfig {
            bind_addr: "127.0.0.1:8000".parse().unwrap(),
            seed_peers: vec!["127.0.0.1:8001".parse().unwrap()],
            max_outbound: 8,
            max_inbound: 32,
            node_id: "test-node".to_string(),
        };

        // Create a message channel
        let (message_tx, message_rx) = mpsc::channel(100);

        // Create the network service
        let service = NetworkService::new(config, message_rx);

        // Check that the service was created successfully
        assert_eq!(service.config.bind_addr.to_string(), "127.0.0.1:8000");
        assert_eq!(service.config.seed_peers.len(), 1);
        assert_eq!(service.config.node_id, "test-node");
    }
}

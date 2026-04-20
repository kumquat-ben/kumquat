use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

use crate::storage::block_store::Hash;

/// Information about a node in the network
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct NodeInfo {
    /// Protocol version
    pub version: String,

    /// Unique node identifier
    pub node_id: String,

    /// Address the node is listening on
    pub listen_addr: SocketAddr,

    /// Chain identifier the node believes it is serving.
    pub chain_id: u64,

    /// Genesis hash for the active chain.
    pub genesis_hash: Hash,

    /// Latest committed tip height known to this node.
    pub tip_height: u64,

    /// Latest committed tip hash known to this node.
    pub tip_hash: Hash,

    /// Latest committed cumulative work known to this node.
    pub total_difficulty: u128,
}

impl NodeInfo {
    /// Create a new NodeInfo
    pub fn new(version: String, node_id: String, listen_addr: SocketAddr) -> Self {
        Self {
            version,
            node_id,
            listen_addr,
            chain_id: 0,
            genesis_hash: [0u8; 32],
            tip_height: 0,
            tip_hash: [0u8; 32],
            total_difficulty: 0,
        }
    }

    /// Attach chain identity and tip summary metadata.
    pub fn with_chain_state(
        mut self,
        chain_id: u64,
        genesis_hash: Hash,
        tip_height: u64,
        tip_hash: Hash,
        total_difficulty: u128,
    ) -> Self {
        self.chain_id = chain_id;
        self.genesis_hash = genesis_hash;
        self.tip_height = tip_height;
        self.tip_hash = tip_hash;
        self.total_difficulty = total_difficulty;
        self
    }

    /// Whether the node info includes an explicit chain identity.
    pub fn has_chain_identity(&self) -> bool {
        self.chain_id != 0 || self.genesis_hash != [0u8; 32]
    }

    /// Check if the version is compatible
    pub fn is_compatible(&self, other: &NodeInfo) -> bool {
        // For now, just check if major version matches
        let self_version = self.version.split('.').next().unwrap_or("0");
        let other_version = other.version.split('.').next().unwrap_or("0");

        if self_version != other_version {
            return false;
        }

        if self.has_chain_identity() && other.has_chain_identity() {
            return self.chain_id == other.chain_id && self.genesis_hash == other.genesis_hash;
        }

        true
    }
}

impl fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeInfo {{ id: {}, version: {}, addr: {}, chain_id: {}, genesis: {}, tip_height: {}, total_difficulty: {} }}",
            self.node_id,
            self.version,
            self.listen_addr,
            self.chain_id,
            hex::encode(self.genesis_hash),
            self.tip_height,
            self.total_difficulty,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_info_compatibility() {
        let node1 = NodeInfo::new(
            "1.0.0".to_string(),
            "node1".to_string(),
            "127.0.0.1:8000".parse().unwrap(),
        )
        .with_chain_state(1337, [1; 32], 10, [2; 32], 100);

        let node2 = NodeInfo::new(
            "1.2.3".to_string(),
            "node2".to_string(),
            "127.0.0.1:8001".parse().unwrap(),
        )
        .with_chain_state(1337, [1; 32], 11, [3; 32], 101);

        let node3 = NodeInfo::new(
            "2.0.0".to_string(),
            "node3".to_string(),
            "127.0.0.1:8002".parse().unwrap(),
        );

        // Same major version should be compatible
        assert!(node1.is_compatible(&node2));
        assert!(node2.is_compatible(&node1));

        // Different major version should not be compatible
        assert!(!node1.is_compatible(&node3));
        assert!(!node3.is_compatible(&node1));
    }

    #[test]
    fn test_node_info_chain_identity_must_match() {
        let node1 = NodeInfo::new(
            "1.0.0".to_string(),
            "node1".to_string(),
            "127.0.0.1:8000".parse().unwrap(),
        )
        .with_chain_state(1337, [1; 32], 10, [2; 32], 100);

        let different_chain = NodeInfo::new(
            "1.0.1".to_string(),
            "node2".to_string(),
            "127.0.0.1:8001".parse().unwrap(),
        )
        .with_chain_state(1337, [9; 32], 10, [2; 32], 100);

        assert!(!node1.is_compatible(&different_chain));
    }
}

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use walkdir::WalkDir;

use crate::mempool::Mempool;
use crate::network::service::NetworkService;
use crate::storage::block_store::BlockStore;

#[derive(Debug, Clone, Serialize)]
pub struct MiningStatus {
    pub enabled_in_config: bool,
    pub consensus_running: bool,
    pub status: String,
    pub mining_threads: usize,
    pub hash_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatusSnapshot {
    pub node_name: String,
    pub chain_id: u64,
    pub uptime_seconds: u64,
    pub api_bind_addr: String,
    pub network_bind_addr: String,
    pub data_dir: String,
    pub db_path: String,
    pub db_size_bytes: Option<u64>,
    pub latest_block_height: Option<u64>,
    pub latest_block_hash: Option<String>,
    pub latest_block_timestamp: Option<u64>,
    pub peer_count: usize,
    pub peer_addresses: Vec<String>,
    pub mempool_size: usize,
    pub mining: MiningStatus,
    pub sync_status: String,
    pub data_gaps: Vec<String>,
}

#[derive(Clone)]
pub struct NodeRuntime {
    start_time: Instant,
    node_name: String,
    chain_id: u64,
    api_bind_addr: SocketAddr,
    network_bind_addr: SocketAddr,
    data_dir: PathBuf,
    db_path: PathBuf,
    mining_enabled: bool,
    mining_threads: usize,
    consensus_running: bool,
    block_store: Arc<BlockStore<'static>>,
    mempool: Arc<Mempool>,
    network: Arc<NetworkService>,
}

impl NodeRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_name: String,
        chain_id: u64,
        api_bind_addr: SocketAddr,
        network_bind_addr: SocketAddr,
        data_dir: PathBuf,
        db_path: PathBuf,
        mining_enabled: bool,
        mining_threads: usize,
        consensus_running: bool,
        block_store: Arc<BlockStore<'static>>,
        mempool: Arc<Mempool>,
        network: Arc<NetworkService>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            node_name,
            chain_id,
            api_bind_addr,
            network_bind_addr,
            data_dir,
            db_path,
            mining_enabled,
            mining_threads,
            consensus_running,
            block_store,
            mempool,
            network,
        }
    }

    pub async fn snapshot(&self) -> NodeStatusSnapshot {
        let latest_block = self.block_store.get_latest_block().ok().flatten();
        let peer_addresses = self
            .network
            .peer_manager()
            .connected_peers()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();
        let db_size_bytes = directory_size(&self.db_path);

        let mut data_gaps = Vec::new();
        data_gaps.push("Sync progress is not instrumented yet, so sync status is approximate.".to_string());
        if self.mining_enabled {
            data_gaps.push("Hash rate is not exposed by the mining engine yet.".to_string());
        }
        data_gaps.push("Recent warnings/errors are not yet collected into the dashboard event feed.".to_string());

        NodeStatusSnapshot {
            node_name: self.node_name.clone(),
            chain_id: self.chain_id,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            api_bind_addr: self.api_bind_addr.to_string(),
            network_bind_addr: self.network_bind_addr.to_string(),
            data_dir: self.data_dir.display().to_string(),
            db_path: self.db_path.display().to_string(),
            db_size_bytes,
            latest_block_height: latest_block.as_ref().map(|block| block.height),
            latest_block_hash: latest_block.as_ref().map(|block| hex::encode(block.hash)),
            latest_block_timestamp: latest_block.as_ref().map(|block| block.timestamp),
            peer_count: peer_addresses.len(),
            peer_addresses,
            mempool_size: self.mempool.len(),
            mining: MiningStatus {
                enabled_in_config: self.mining_enabled,
                consensus_running: self.consensus_running,
                status: mining_status_label(self.mining_enabled, self.consensus_running),
                mining_threads: self.mining_threads,
                hash_rate: None,
            },
            sync_status: approximate_sync_status(latest_block.is_some(), self.mining_enabled),
            data_gaps,
        }
    }

    pub fn api_bind_addr(&self) -> SocketAddr {
        self.api_bind_addr
    }
}

fn approximate_sync_status(has_chain_data: bool, mining_enabled: bool) -> String {
    if has_chain_data && mining_enabled {
        "running".to_string()
    } else if has_chain_data {
        "online".to_string()
    } else {
        "starting".to_string()
    }
}

fn mining_status_label(mining_enabled: bool, consensus_running: bool) -> String {
    match (mining_enabled, consensus_running) {
        (true, true) => "configured-on".to_string(),
        (true, false) => "requested-but-not-running".to_string(),
        (false, true) => "consensus-running-mining-disabled".to_string(),
        (false, false) => "disabled".to_string(),
    }
}

fn directory_size(path: &Path) -> Option<u64> {
    if !path.exists() {
        return None;
    }

    let mut total = 0u64;
    for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            total = total.saturating_add(entry.metadata().ok()?.len());
        }
    }

    Some(total)
}

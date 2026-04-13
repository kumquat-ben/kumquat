use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use walkdir::WalkDir;

use crate::consensus::telemetry::{ConsensusTelemetry, ConsensusTelemetrySnapshot};
use crate::mempool::Mempool;
use crate::network::service::NetworkService;
use crate::network::sync::sync_service::SyncService;
use crate::storage::block_store::BlockStore;

#[derive(Debug, Clone, Serialize)]
pub struct MiningStatus {
    pub enabled_in_config: bool,
    pub consensus_running: bool,
    pub status: String,
    pub mining_threads: usize,
    pub hash_rate: Option<f64>,
    pub mining_attempts: u64,
    pub mined_blocks: u64,
    pub failed_mining_attempts: u64,
    pub last_mining_attempt_at: Option<u64>,
    pub last_mining_success_at: Option<u64>,
    pub last_mined_block_height: Option<u64>,
    pub last_mined_block_hash: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub status: String,
    pub in_progress: bool,
    pub current_height: u64,
    pub target_height: u64,
    pub remaining_blocks: u64,
    pub progress_percent: Option<f64>,
    pub sync_peer: Option<String>,
    pub blocks_synced: u64,
    pub failed_requests: u64,
    pub started_at_unix: Option<u64>,
    pub elapsed_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatusSnapshot {
    pub node_name: String,
    pub chain_id: u64,
    pub chain_identity: String,
    pub configured_genesis_hash: Option<String>,
    pub expected_genesis_hash: String,
    pub active_genesis_hash: Option<String>,
    pub genesis_config_path: String,
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
    pub sync: SyncStatus,
    pub data_gaps: Vec<String>,
}

#[derive(Clone)]
pub struct NodeRuntime {
    start_time: Instant,
    node_name: String,
    chain_id: u64,
    configured_genesis_hash: Option<String>,
    expected_genesis_hash: String,
    genesis_config_path: PathBuf,
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
    consensus_telemetry: ConsensusTelemetry,
    sync_service: Option<Arc<SyncService>>,
}

impl NodeRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_name: String,
        chain_id: u64,
        configured_genesis_hash: Option<String>,
        expected_genesis_hash: String,
        genesis_config_path: PathBuf,
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
        consensus_telemetry: ConsensusTelemetry,
        sync_service: Option<Arc<SyncService>>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            node_name,
            chain_id,
            configured_genesis_hash,
            expected_genesis_hash,
            genesis_config_path,
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
            consensus_telemetry,
            sync_service,
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
        let telemetry = self.consensus_telemetry.read().await.clone();
        let sync_snapshot = match &self.sync_service {
            Some(sync_service) => Some(sync_service.get_sync_state().await),
            None => None,
        };

        let mut data_gaps = Vec::new();
        if self.mining_enabled && telemetry.last_mining_success_at.is_none() {
            data_gaps.push("Hash rate is not exposed by the mining engine yet.".to_string());
        }
        if sync_snapshot.is_none() {
            data_gaps
                .push("Sync service telemetry is unavailable for this node instance.".to_string());
        }
        data_gaps.push(
            "Recent warnings/errors are not yet collected into the dashboard event feed."
                .to_string(),
        );

        NodeStatusSnapshot {
            node_name: self.node_name.clone(),
            chain_id: self.chain_id,
            chain_identity: format!("chain-{}:{}", self.chain_id, self.expected_genesis_hash),
            configured_genesis_hash: self.configured_genesis_hash.clone(),
            expected_genesis_hash: self.expected_genesis_hash.clone(),
            active_genesis_hash: self
                .block_store
                .get_block_by_height(0)
                .ok()
                .flatten()
                .map(|block| hex::encode(block.hash)),
            genesis_config_path: self.genesis_config_path.display().to_string(),
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
            mining: mining_status_from_telemetry(
                self.mining_enabled,
                self.consensus_running,
                self.mining_threads,
                telemetry,
            ),
            sync: sync_status_from_state(
                sync_snapshot,
                latest_block.as_ref().map(|block| block.height).unwrap_or(0),
            ),
            data_gaps,
        }
    }

    pub fn api_bind_addr(&self) -> SocketAddr {
        self.api_bind_addr
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

fn mining_status_from_telemetry(
    mining_enabled: bool,
    consensus_running: bool,
    mining_threads: usize,
    telemetry: ConsensusTelemetrySnapshot,
) -> MiningStatus {
    let status = if telemetry.mined_blocks > 0 {
        "mining-successful".to_string()
    } else if telemetry.mining_attempts > 0 && mining_enabled {
        "mining-active".to_string()
    } else {
        mining_status_label(mining_enabled, consensus_running)
    };

    MiningStatus {
        enabled_in_config: mining_enabled,
        consensus_running,
        status,
        mining_threads,
        hash_rate: None,
        mining_attempts: telemetry.mining_attempts,
        mined_blocks: telemetry.mined_blocks,
        failed_mining_attempts: telemetry.failed_mining_attempts,
        last_mining_attempt_at: telemetry.last_mining_attempt_at,
        last_mining_success_at: telemetry.last_mining_success_at,
        last_mined_block_height: telemetry.last_mined_block_height,
        last_mined_block_hash: telemetry.last_mined_block_hash,
        last_error: telemetry.last_error,
    }
}

fn sync_status_from_state(
    sync_state: Option<crate::network::sync::sync_service::SyncState>,
    latest_height: u64,
) -> SyncStatus {
    if let Some(sync_state) = sync_state {
        let remaining_blocks = sync_state.target_height.saturating_sub(latest_height);
        let progress_percent = if sync_state.target_height > sync_state.current_height {
            let completed = latest_height.saturating_sub(sync_state.current_height) as f64;
            let total = sync_state
                .target_height
                .saturating_sub(sync_state.current_height) as f64;
            Some((completed / total * 100.0).clamp(0.0, 100.0))
        } else {
            None
        };
        let elapsed_seconds = sync_state
            .start_time
            .map(|started| started.elapsed().as_secs());
        let started_at_unix = elapsed_seconds.map(|elapsed| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            now.saturating_sub(elapsed)
        });

        SyncStatus {
            status: if sync_state.in_progress {
                "syncing".to_string()
            } else if latest_height >= sync_state.target_height && sync_state.target_height > 0 {
                "caught-up".to_string()
            } else {
                "idle".to_string()
            },
            in_progress: sync_state.in_progress,
            current_height: latest_height,
            target_height: sync_state.target_height,
            remaining_blocks,
            progress_percent,
            sync_peer: sync_state.sync_peer,
            blocks_synced: sync_state.blocks_synced,
            failed_requests: sync_state.failed_requests,
            started_at_unix,
            elapsed_seconds,
        }
    } else {
        SyncStatus {
            status: if latest_height > 0 {
                "online-no-sync-service".to_string()
            } else {
                "starting".to_string()
            },
            in_progress: false,
            current_height: latest_height,
            target_height: latest_height,
            remaining_blocks: 0,
            progress_percent: None,
            sync_peer: None,
            blocks_synced: 0,
            failed_requests: 0,
            started_at_unix: None,
            elapsed_seconds: None,
        }
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

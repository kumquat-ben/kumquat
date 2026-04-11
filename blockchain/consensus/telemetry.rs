use serde::Serialize;
use tokio::sync::RwLock;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Default)]
pub struct ConsensusTelemetrySnapshot {
    pub consensus_started: bool,
    pub mining_enabled: bool,
    pub mining_attempts: u64,
    pub mined_blocks: u64,
    pub failed_mining_attempts: u64,
    pub last_mining_attempt_at: Option<u64>,
    pub last_mining_success_at: Option<u64>,
    pub last_mined_block_height: Option<u64>,
    pub last_mined_block_hash: Option<String>,
    pub last_error: Option<String>,
}

pub type ConsensusTelemetry = Arc<RwLock<ConsensusTelemetrySnapshot>>;

pub fn new_consensus_telemetry(mining_enabled: bool) -> ConsensusTelemetry {
    Arc::new(RwLock::new(ConsensusTelemetrySnapshot {
        mining_enabled,
        ..ConsensusTelemetrySnapshot::default()
    }))
}

pub fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

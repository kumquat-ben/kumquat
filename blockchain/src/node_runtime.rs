use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use walkdir::WalkDir;

use crate::consensus::telemetry::{ConsensusTelemetry, ConsensusTelemetrySnapshot};
use crate::crypto::{decode_address, encode_address};
use crate::mempool::Mempool;
use crate::network::service::NetworkService;
use crate::network::sync::sync_service::SyncService;
use crate::storage::block_store::{Block, BlockStore, Hash};
use crate::storage::state::{AccountState, CoinInventory, ConversionOrder, Denomination};
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::{TransactionRecord, TransactionStatus, TxStore};

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

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerSummaryResponse {
    pub node: NodeStatusSnapshot,
    pub recent_blocks: Vec<ExplorerBlockSummary>,
    pub recent_transactions: Vec<ExplorerTransactionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerBlockSummary {
    pub height: u64,
    pub hash: String,
    pub prev_hash: String,
    pub timestamp: u64,
    pub miner_address: String,
    pub transaction_count: usize,
    pub reward_token_count: usize,
    pub difficulty: u64,
    pub total_difficulty: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerBlockDetail {
    pub block: ExplorerBlockSummary,
    pub state_root: String,
    pub tx_root: String,
    pub pre_reward_state_root: String,
    pub result_commitment: String,
    pub poh_seq: u64,
    pub poh_hash: String,
    pub nonce: u64,
    pub conversion_fulfillment_order_count: usize,
    pub transactions: Vec<ExplorerTransactionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerTransactionSummary {
    pub hash: String,
    pub block_height: u64,
    pub timestamp: u64,
    pub sender_address: String,
    pub recipient_address: String,
    pub value_cents: u64,
    pub gas_price: u64,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub nonce: u64,
    pub status: String,
    pub transfer_token_count: usize,
    pub coin_transfer_cents: u64,
    pub coin_fee_cents: u64,
    pub has_conversion_intent: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerTransactionDetail {
    pub transaction: ExplorerTransactionSummary,
    pub fee_token_id: Option<String>,
    pub transfer_token_ids: Vec<String>,
    pub conversion_intent: Option<String>,
    pub data_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerAddressResponse {
    pub address: String,
    pub account: Option<ExplorerAccountSummary>,
    pub transactions: Vec<ExplorerTransactionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerAccountSummary {
    pub address: String,
    pub account_type: String,
    pub balance_cents: u64,
    pub nonce: u64,
    pub last_updated: u64,
    pub bill_count: usize,
    pub bill_value_cents: u64,
    pub bill_breakdown: Vec<ExplorerDenominationCount>,
    pub coin_value_cents: u64,
    pub coin_breakdown: Vec<ExplorerDenominationCount>,
    pub compatibility_token_count: usize,
    pub compute_allocation_count: usize,
    pub has_code: bool,
    pub conversion_order: Option<ExplorerConversionOrder>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerDenominationCount {
    pub denomination: String,
    pub count: u64,
    pub value_cents: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplorerConversionOrder {
    pub order_id: String,
    pub kind: String,
    pub requested_value_cents: u64,
    pub status: String,
    pub created_at_block: u64,
    pub eligible_at_block: u64,
    pub cycle_end_block: u64,
    pub failure_reason: Option<String>,
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
    tx_store: Arc<TxStore<'static>>,
    state_store: Arc<StateStore<'static>>,
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
        tx_store: Arc<TxStore<'static>>,
        state_store: Arc<StateStore<'static>>,
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
            tx_store,
            state_store,
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

    pub async fn explorer_summary(
        &self,
        recent_block_limit: usize,
        recent_transaction_limit: usize,
    ) -> ExplorerSummaryResponse {
        let node = self.snapshot().await;
        let recent_blocks = self.recent_blocks(recent_block_limit);
        let recent_transactions = self.recent_transactions(recent_transaction_limit);
        ExplorerSummaryResponse {
            node,
            recent_blocks,
            recent_transactions,
        }
    }

    pub fn explorer_block(&self, identifier: &str) -> Result<Option<ExplorerBlockDetail>, String> {
        let block = self.lookup_block(identifier)?;
        let Some(block) = block else {
            return Ok(None);
        };
        let mut transactions = self
            .tx_store
            .get_transactions_by_block(block.height)
            .map_err(|err| err.to_string())?;
        sort_transactions_desc(&mut transactions);

        Ok(Some(ExplorerBlockDetail {
            block: block_summary_from_block(&block),
            state_root: hex::encode(block.state_root),
            tx_root: hex::encode(block.tx_root),
            pre_reward_state_root: hex::encode(block.pre_reward_state_root),
            result_commitment: hex::encode(block.result_commitment),
            poh_seq: block.poh_seq,
            poh_hash: hex::encode(block.poh_hash),
            nonce: block.nonce,
            conversion_fulfillment_order_count: block.conversion_fulfillment_order_ids.len(),
            transactions: transactions
                .iter()
                .map(transaction_summary_from_record)
                .collect(),
        }))
    }

    pub fn explorer_transaction(
        &self,
        hash_text: &str,
    ) -> Result<Option<ExplorerTransactionDetail>, String> {
        let tx_hash = decode_hash(hash_text)?;
        let transaction = self
            .tx_store
            .get_transaction(&tx_hash)
            .map_err(|err| err.to_string())?;
        Ok(transaction.map(|tx| ExplorerTransactionDetail {
            transaction: transaction_summary_from_record(&tx),
            fee_token_id: tx.fee_token_id.map(hex::encode),
            transfer_token_ids: tx.transfer_token_ids.iter().map(hex::encode).collect(),
            conversion_intent: tx
                .conversion_intent
                .as_ref()
                .map(|intent| format!("{intent:?}")),
            data_hex: tx.data.as_ref().map(hex::encode),
        }))
    }

    pub fn explorer_address(
        &self,
        address_text: &str,
        transaction_limit: usize,
    ) -> Result<ExplorerAddressResponse, String> {
        let address = decode_address(address_text).map_err(|err| err.to_string())?;
        let normalized_address = encode_address(&address);
        let account = self
            .state_store
            .get_account_state(&address)
            .map(|state| account_summary_from_state(&normalized_address, &state));
        let mut transactions = self.transactions_for_address(&address)?;
        if transactions.len() > transaction_limit {
            transactions.truncate(transaction_limit);
        }

        Ok(ExplorerAddressResponse {
            address: normalized_address,
            account,
            transactions: transactions
                .iter()
                .map(transaction_summary_from_record)
                .collect(),
        })
    }

    fn recent_blocks(&self, limit: usize) -> Vec<ExplorerBlockSummary> {
        let Some(latest_height) = self.block_store.get_latest_height() else {
            return Vec::new();
        };
        let count = limit.max(1) as u64;
        let start = latest_height.saturating_sub(count.saturating_sub(1));
        self.block_store
            .get_blocks_by_height_range(start, latest_height)
            .map(|mut blocks| {
                blocks.sort_by(|left, right| right.height.cmp(&left.height));
                blocks
                    .into_iter()
                    .take(limit)
                    .map(|block| block_summary_from_block(&block))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn recent_transactions(&self, limit: usize) -> Vec<ExplorerTransactionSummary> {
        let mut transactions = Vec::new();
        let mut seen = HashSet::new();
        for block in self.recent_blocks(limit.max(8)).iter() {
            let Ok(block_transactions) = self.tx_store.get_transactions_by_block(block.height)
            else {
                continue;
            };
            for transaction in block_transactions {
                let hash = hex::encode(transaction.tx_id);
                if seen.insert(hash) {
                    transactions.push(transaction);
                }
            }
            if transactions.len() >= limit {
                break;
            }
        }
        sort_transactions_desc(&mut transactions);
        transactions.truncate(limit);
        transactions
            .iter()
            .map(transaction_summary_from_record)
            .collect()
    }

    fn lookup_block(&self, identifier: &str) -> Result<Option<Block>, String> {
        let trimmed = identifier.trim();
        if trimmed.is_empty() {
            return Err("block identifier is required".to_string());
        }
        if let Ok(height) = trimmed.parse::<u64>() {
            return self
                .block_store
                .get_block_by_height(height)
                .map_err(|err| err.to_string());
        }

        let hash = decode_hash(trimmed)?;
        self.block_store
            .get_block_by_hash(&hash)
            .map_err(|err| err.to_string())
    }

    fn transactions_for_address(&self, address: &Hash) -> Result<Vec<TransactionRecord>, String> {
        let mut combined = self
            .tx_store
            .get_transactions_by_sender(address)
            .map_err(|err| err.to_string())?;
        combined.extend(
            self.tx_store
                .get_transactions_by_recipient(address)
                .map_err(|err| err.to_string())?,
        );

        let mut seen = HashSet::new();
        combined.retain(|transaction| seen.insert(hex::encode(transaction.tx_id)));
        sort_transactions_desc(&mut combined);
        Ok(combined)
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

fn decode_hash(value: &str) -> Result<Hash, String> {
    let bytes = hex::decode(value.trim())
        .map_err(|_| "identifier must be 64 hex characters".to_string())?;
    if bytes.len() != 32 {
        return Err("identifier must be 64 hex characters".to_string());
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes);
    Ok(hash)
}

fn block_summary_from_block(block: &Block) -> ExplorerBlockSummary {
    ExplorerBlockSummary {
        height: block.height,
        hash: hex::encode(block.hash),
        prev_hash: hex::encode(block.prev_hash),
        timestamp: block.timestamp,
        miner_address: encode_address(&block.miner),
        transaction_count: block.transactions.len(),
        reward_token_count: block.reward_token_ids.len(),
        difficulty: block.difficulty,
        total_difficulty: block.total_difficulty.to_string(),
    }
}

fn transaction_summary_from_record(transaction: &TransactionRecord) -> ExplorerTransactionSummary {
    ExplorerTransactionSummary {
        hash: hex::encode(transaction.tx_id),
        block_height: transaction.block_height,
        timestamp: transaction.timestamp,
        sender_address: encode_address(&transaction.sender),
        recipient_address: encode_address(&transaction.recipient),
        value_cents: transaction.value,
        gas_price: transaction.gas_price,
        gas_limit: transaction.gas_limit,
        gas_used: transaction.gas_used,
        nonce: transaction.nonce,
        status: transaction_status_label(transaction.status),
        transfer_token_count: transaction.transfer_token_ids.len(),
        coin_transfer_cents: transaction.coin_transfer.total_value_cents(),
        coin_fee_cents: transaction.coin_fee.total_value_cents(),
        has_conversion_intent: transaction.conversion_intent.is_some(),
    }
}

fn transaction_status_label(status: TransactionStatus) -> String {
    match status {
        TransactionStatus::Pending => "pending".to_string(),
        TransactionStatus::Included => "included".to_string(),
        TransactionStatus::Confirmed => "confirmed".to_string(),
        TransactionStatus::Failed(error) => format!("failed:{error:?}").to_lowercase(),
    }
}

fn account_summary_from_state(address: &str, state: &AccountState) -> ExplorerAccountSummary {
    let bill_value_cents = state
        .bills
        .iter()
        .map(|bill| bill.value_cents())
        .sum::<u64>();
    ExplorerAccountSummary {
        address: address.to_string(),
        account_type: state.account_type.to_string().to_lowercase(),
        balance_cents: state.balance,
        nonce: state.nonce,
        last_updated: state.last_updated,
        bill_count: state.bills.len(),
        bill_value_cents,
        bill_breakdown: bill_breakdown(state),
        coin_value_cents: state.coin_inventory.total_value_cents(),
        coin_breakdown: coin_breakdown(&state.coin_inventory),
        compatibility_token_count: state.tokens.len(),
        compute_allocation_count: state.compute_allocations.len(),
        has_code: state.code.is_some(),
        conversion_order: state
            .conversion_order
            .as_ref()
            .map(conversion_order_summary),
    }
}

fn bill_breakdown(state: &AccountState) -> Vec<ExplorerDenominationCount> {
    Denomination::all_descending()
        .iter()
        .copied()
        .filter(|denomination| denomination.is_bill())
        .filter_map(|denomination| {
            let count = state
                .bills
                .iter()
                .filter(|bill| bill.denomination == denomination)
                .count() as u64;
            if count == 0 {
                None
            } else {
                Some(ExplorerDenominationCount {
                    denomination: denomination.label().to_string(),
                    count,
                    value_cents: denomination.value_cents() * count,
                })
            }
        })
        .collect()
}

fn coin_breakdown(inventory: &CoinInventory) -> Vec<ExplorerDenominationCount> {
    Denomination::all_descending()
        .iter()
        .copied()
        .filter(|denomination| denomination.is_coin())
        .filter_map(|denomination| {
            let count = inventory.count(denomination);
            if count == 0 {
                None
            } else {
                Some(ExplorerDenominationCount {
                    denomination: denomination.label().to_string(),
                    count,
                    value_cents: denomination.value_cents() * count,
                })
            }
        })
        .collect()
}

fn conversion_order_summary(order: &ConversionOrder) -> ExplorerConversionOrder {
    ExplorerConversionOrder {
        order_id: hex::encode(order.order_id),
        kind: format!("{:?}", order.kind).to_lowercase(),
        requested_value_cents: order.requested_value_cents,
        status: format!("{:?}", order.status).to_lowercase(),
        created_at_block: order.created_at_block,
        eligible_at_block: order.eligible_at_block,
        cycle_end_block: order.cycle_end_block,
        failure_reason: order.failure_reason.clone(),
    }
}

fn sort_transactions_desc(transactions: &mut [TransactionRecord]) {
    transactions.sort_by(|left, right| {
        right
            .block_height
            .cmp(&left.block_height)
            .then_with(|| right.timestamp.cmp(&left.timestamp))
            .then_with(|| hex::encode(right.tx_id).cmp(&hex::encode(left.tx_id)))
    });
}

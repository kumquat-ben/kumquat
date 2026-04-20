use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

use crate::consensus::block_processor::BlockProcessingResult;
use crate::consensus::engine::ConsensusEngine;
use crate::network::events::event_bus::EventBus;
use crate::network::events::event_types::{NetworkEvent, SyncResult};
use crate::network::peer::advanced_registry::AdvancedPeerRegistry;
use crate::network::peer::broadcaster::PeerBroadcaster;
use crate::network::peer::registry::PeerRegistry;
use crate::network::peer::reputation::{ReputationEvent, ReputationSystem};
use crate::network::service::advanced_router::SyncRequest;
use crate::network::types::message::NetMessage;
use crate::storage::block_store::{Block, BlockStore};

/// Sync error
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// State store error
    #[error("State store error: {0}")]
    StateStoreError(String),

    /// Other error
    #[error("{0}")]
    Other(String),
}

/// Sync state
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Whether a sync is in progress
    pub in_progress: bool,

    /// The current sync target height
    pub target_height: u64,

    /// The current sync height
    pub current_height: u64,

    /// Whether the sync service is still waiting to learn the remote tip height.
    pub awaiting_latest_height: bool,

    /// Whether at least one sync/tip-discovery round has completed.
    pub probe_completed: bool,

    /// The peer we're syncing from
    pub sync_peer: Option<String>,

    /// When the sync started
    pub start_time: Option<Instant>,

    /// The number of blocks synced
    pub blocks_synced: u64,

    /// The number of failed block requests
    pub failed_requests: u64,

    /// When sync progress last moved forward
    pub last_progress_at: Option<Instant>,
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            in_progress: false,
            target_height: 0,
            current_height: 0,
            awaiting_latest_height: false,
            probe_completed: false,
            sync_peer: None,
            start_time: None,
            blocks_synced: 0,
            failed_requests: 0,
            last_progress_at: None,
        }
    }
}

/// Configuration for the sync service
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of blocks to request at once
    pub max_blocks_per_request: u64,

    /// Timeout for sync requests
    pub request_timeout: Duration,

    /// Maximum number of retries for a request
    pub max_retries: u32,

    /// Interval between sync attempts
    pub sync_interval: Duration,

    /// Whether to automatically sync on startup
    pub auto_sync: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_blocks_per_request: 100,
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            sync_interval: Duration::from_secs(10),
            auto_sync: true,
        }
    }
}

/// Sync service for blockchain synchronization
pub struct SyncService {
    /// Block store
    block_store: Arc<BlockStore<'static>>,

    /// Consensus engine used to process synced blocks when available.
    consensus: Option<Arc<ConsensusEngine>>,

    /// Peer registry
    peer_registry: Arc<PeerRegistry>,

    /// Advanced peer registry
    advanced_registry: Option<Arc<AdvancedPeerRegistry>>,

    /// Broadcaster for sending messages to peers
    broadcaster: Arc<PeerBroadcaster>,

    /// Event bus for publishing events
    event_bus: Option<Arc<EventBus>>,

    /// Reputation system for tracking peer behavior
    reputation: Option<Arc<ReputationSystem>>,

    /// Sync state
    sync_state: Arc<RwLock<SyncState>>,

    /// Configuration
    config: SyncConfig,

    /// Channel for block responses
    block_rx: Arc<RwLock<Option<mpsc::Receiver<(Block, String)>>>>,

    /// Channel for block range responses
    block_range_rx: Arc<RwLock<Option<mpsc::Receiver<(Vec<Block>, String)>>>>,

    /// Whether the service is running
    running: Arc<RwLock<bool>>,
}

impl SyncService {
    fn complete_sync_state(state: &mut SyncState) {
        state.in_progress = false;
        state.awaiting_latest_height = false;
        state.probe_completed = true;
        state.sync_peer = None;
        state.start_time = None;
        state.last_progress_at = None;
    }

    async fn process_synced_block(
        block_store: &BlockStore<'static>,
        consensus: Option<&Arc<ConsensusEngine>>,
        block: &Block,
    ) -> Result<BlockProcessingResult, String> {
        if let Some(consensus) = consensus {
            if let Ok(Some(existing)) = block_store.get_block_by_height(block.height) {
                if existing.hash != block.hash {
                    let rollback_height = block.height.saturating_sub(1);
                    warn!(
                        "Detected divergent block at height {}; rolling back to {} before retrying sync",
                        block.height, rollback_height
                    );
                    consensus.rollback_to_height(rollback_height).await?;
                }
            }

            Ok(consensus.process_network_block(block.clone()).await)
        } else {
            block_store
                .put_block(block)
                .map_err(|err| format!("Failed to store synced block {}: {}", block.height, err))?;
            Ok(BlockProcessingResult::Success)
        }
    }

    async fn reconcile_sync_state(
        block_store: &BlockStore<'static>,
        sync_state: &Arc<RwLock<SyncState>>,
    ) -> bool {
        let latest_height = block_store.get_latest_height().unwrap_or(0);
        let mut state = sync_state.write().await;

        if state.awaiting_latest_height {
            if state.current_height != latest_height {
                state.current_height = latest_height;
            }
            return false;
        }

        if state.in_progress && latest_height >= state.target_height {
            state.current_height = latest_height;
            if latest_height > state.target_height {
                state.target_height = latest_height;
            }
            Self::complete_sync_state(&mut state);
            return true;
        }

        if state.current_height != latest_height {
            state.current_height = latest_height;
        }

        false
    }

    async fn retry_stale_sync(
        block_store: &BlockStore<'static>,
        broadcaster: &PeerBroadcaster,
        sync_state: &Arc<RwLock<SyncState>>,
        reputation: Option<&Arc<ReputationSystem>>,
        config: &SyncConfig,
    ) -> Result<bool, String> {
        let latest_height = block_store.get_latest_height().unwrap_or(0);
        let (sync_peer, start_height, end_height, failed_requests, stale_for, awaiting_latest_height) = {
            let state = sync_state.read().await;

            if !state.in_progress {
                return Ok(false);
            }

            let Some(last_progress_at) = state.last_progress_at.or(state.start_time) else {
                return Ok(false);
            };

            let stale_for = last_progress_at.elapsed();
            if stale_for < config.request_timeout {
                return Ok(false);
            }

            let Some(sync_peer) = state.sync_peer.clone() else {
                return Ok(false);
            };

            let start_height = latest_height;
            let end_height = state.target_height.max(latest_height);

            (
                sync_peer,
                start_height,
                end_height,
                state.failed_requests,
                stale_for,
                state.awaiting_latest_height,
            )
        };

        if failed_requests >= u64::from(config.max_retries) {
            warn!(
                "Sync with peer {} stalled for {:?} after {} retries; resetting sync state",
                sync_peer, stale_for, failed_requests
            );
            let mut state = sync_state.write().await;
            state.current_height = latest_height;
            Self::complete_sync_state(&mut state);
            return Ok(false);
        }

        let message = if awaiting_latest_height {
            NetMessage::RequestBlock(u64::MAX)
        } else if start_height <= end_height {
            NetMessage::RequestBlockRange {
                start_height,
                end_height,
            }
        } else {
            NetMessage::RequestBlock(u64::MAX)
        };

        warn!(
            "Sync with peer {} stalled for {:?}; retrying request {} of {}",
            sync_peer,
            stale_for,
            failed_requests + 1,
            config.max_retries
        );

        match broadcaster.send_to_peer(&sync_peer, message).await {
            Ok(true) => {
                let mut state = sync_state.write().await;
                state.failed_requests += 1;
                state.current_height = latest_height;
                state.last_progress_at = Some(Instant::now());
                if !awaiting_latest_height && start_height <= end_height {
                    state.target_height = end_height;
                }
                Ok(true)
            }
            Ok(false) => {
                if let Some(reputation) = reputation {
                    reputation.update_score(&sync_peer, ReputationEvent::Timeout);
                }
                let mut state = sync_state.write().await;
                state.failed_requests += 1;
                state.current_height = latest_height;
                state.last_progress_at = Some(Instant::now());
                Ok(true)
            }
            Err(err) => {
                if let Some(reputation) = reputation {
                    reputation.update_score(&sync_peer, ReputationEvent::Timeout);
                }
                let mut state = sync_state.write().await;
                state.failed_requests += 1;
                state.current_height = latest_height;
                state.last_progress_at = Some(Instant::now());
                Err(err)
            }
        }
    }

    /// Create a new sync service
    pub fn new(
        block_store: Arc<BlockStore<'static>>,
        peer_registry: Arc<PeerRegistry>,
        broadcaster: Arc<PeerBroadcaster>,
    ) -> Self {
        Self {
            block_store,
            consensus: None,
            peer_registry,
            advanced_registry: None,
            broadcaster,
            event_bus: None,
            reputation: None,
            sync_state: Arc::new(RwLock::new(SyncState::default())),
            config: SyncConfig::default(),
            block_rx: Arc::new(RwLock::new(None)),
            block_range_rx: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Set the advanced peer registry
    pub fn with_advanced_registry(mut self, registry: Arc<AdvancedPeerRegistry>) -> Self {
        self.advanced_registry = Some(registry);
        self
    }

    /// Set the event bus
    pub fn with_event_bus(mut self, event_bus: Arc<EventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Set the reputation system
    pub fn with_reputation(mut self, reputation: Arc<ReputationSystem>) -> Self {
        self.reputation = Some(reputation);
        self
    }

    /// Route synced blocks through consensus so state is updated alongside blocks.
    pub fn with_consensus(mut self, consensus: Arc<ConsensusEngine>) -> Self {
        self.consensus = Some(consensus);
        self
    }

    pub fn with_consensus_opt(mut self, consensus: Option<Arc<ConsensusEngine>>) -> Self {
        self.consensus = consensus;
        self
    }

    /// Set the configuration
    pub fn with_config(mut self, config: SyncConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the block response channel
    pub fn with_block_channel(mut self, rx: mpsc::Receiver<(Block, String)>) -> Self {
        // Since we can't use async in a constructor, we'll just create a new RwLock
        self.block_rx = Arc::new(RwLock::new(Some(rx)));
        self
    }

    /// Set the block range response channel
    pub fn with_block_range_channel(mut self, rx: mpsc::Receiver<(Vec<Block>, String)>) -> Self {
        // Since we can't use async in a constructor, we'll just create a new RwLock
        self.block_range_rx = Arc::new(RwLock::new(Some(rx)));
        self
    }

    /// Start the sync service
    pub async fn start(&self) -> Result<(), String> {
        // Check if we're already running
        {
            let running = self.running.read().await;
            if *running {
                return Err("Sync service already running".to_string());
            }
        }

        // Set running flag
        {
            let mut running = self.running.write().await;
            *running = true;
        }

        // Start the sync loop
        let block_store = self.block_store.clone();
        let peer_registry = self.peer_registry.clone();
        let broadcaster = self.broadcaster.clone();
        let sync_state = self.sync_state.clone();
        let config = self.config.clone();
        let running = self.running.clone();
        let event_bus = self.event_bus.clone();
        let reputation = self.reputation.clone();
        let advanced_registry = self.advanced_registry.clone();

        tokio::spawn(async move {
            info!("Starting sync service");

            // Auto-sync on startup if enabled
            if config.auto_sync {
                if let Err(e) = Self::sync_with_network(
                    block_store.clone(),
                    peer_registry.clone(),
                    broadcaster.clone(),
                    sync_state.clone(),
                    event_bus.clone(),
                    reputation.clone(),
                    advanced_registry.clone(),
                    &config,
                )
                .await
                {
                    warn!("Sync failed: {}", e);
                }
            }

            // Main sync loop
            let mut interval = tokio::time::interval(config.sync_interval);

            while {
                let is_running = *running.read().await;
                is_running
            } {
                interval.tick().await;

                if Self::reconcile_sync_state(&block_store, &sync_state).await {
                    debug!("Reconciled stale sync state against committed tip");
                }

                if let Err(err) = Self::retry_stale_sync(
                    &block_store,
                    &broadcaster,
                    &sync_state,
                    reputation.as_ref(),
                    &config,
                )
                .await
                {
                    warn!("Retrying stale sync failed: {}", err);
                }

                // Check if we're already syncing
                let is_syncing = {
                    let state = sync_state.read().await;
                    state.in_progress
                };

                if !is_syncing {
                    if let Err(e) = Self::sync_with_network(
                        block_store.clone(),
                        peer_registry.clone(),
                        broadcaster.clone(),
                        sync_state.clone(),
                        event_bus.clone(),
                        reputation.clone(),
                        advanced_registry.clone(),
                        &config,
                    )
                    .await
                    {
                        warn!("Sync failed: {}", e);
                    }
                }
            }

            info!("Sync service stopped");
        });

        // Start the block response handler
        let block_rx_option = {
            let mut block_rx_guard = self.block_rx.write().await;
            block_rx_guard.take()
        };

        if let Some(mut block_rx) = block_rx_option {
            let block_store = self.block_store.clone();
            let consensus = self.consensus.clone();
            let sync_state = self.sync_state.clone();
            let running = self.running.clone();
            let event_bus = self.event_bus.clone();
            let reputation = self.reputation.clone();
            let broadcaster = self.broadcaster.clone();

            tokio::spawn(async move {
                info!("Starting block response handler");

                while {
                    let is_running = *running.read().await;
                    is_running
                } {
                    match block_rx.recv().await {
                        Some((block, peer_id)) => {
                            debug!(
                                "Received block from peer {}: height={}",
                                peer_id, block.height
                            );

                            let processing_result = match Self::process_synced_block(
                                &block_store,
                                consensus.as_ref(),
                                &block,
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(err) => {
                                    error!(
                                        "Failed to process synced block {} from peer {}: {}",
                                        block.height, peer_id, err
                                    );
                                    let mut state = sync_state.write().await;
                                    if state.in_progress {
                                        state.failed_requests += 1;
                                        state.last_progress_at = Some(Instant::now());
                                    }
                                    continue;
                                }
                            };

                            // Update sync state
                            {
                                let mut state = sync_state.write().await;
                                if state.in_progress {
                                    let awaiting_latest_height = state.awaiting_latest_height;

                                    if awaiting_latest_height && block.height > state.current_height
                                    {
                                        let start_height = state.current_height;
                                        let end_height = block.height;
                                        match broadcaster
                                            .send_to_peer(
                                                &peer_id,
                                                NetMessage::RequestBlockRange {
                                                    start_height,
                                                    end_height,
                                                },
                                            )
                                            .await
                                        {
                                            Ok(true) => {
                                                info!(
                                                    "Requested blocks {}..{} from peer {} after latest-height discovery",
                                                    start_height, end_height, peer_id
                                                );
                                                state.awaiting_latest_height = false;
                                                state.target_height = end_height;
                                                state.sync_peer = Some(peer_id.clone());
                                                state.last_progress_at = Some(Instant::now());
                                            }
                                            Ok(false) => {
                                                warn!(
                                                    "Peer {} did not accept follow-up block range request {}..{}",
                                                    peer_id, start_height, end_height
                                                );
                                                Self::complete_sync_state(&mut state);
                                            }
                                            Err(err) => {
                                                warn!(
                                                    "Failed to request block range {}..{} from peer {}: {}",
                                                    start_height, end_height, peer_id, err
                                                );
                                                Self::complete_sync_state(&mut state);
                                            }
                                        }
                                        continue;
                                    }

                                    if awaiting_latest_height {
                                        state.awaiting_latest_height = false;
                                    }

                                    if !matches!(
                                        processing_result,
                                        BlockProcessingResult::Success
                                            | BlockProcessingResult::AlreadyKnown
                                    ) {
                                        warn!(
                                            "Rejected synced block {} from peer {} with result {:?}",
                                            block.height, peer_id, processing_result
                                        );
                                        state.failed_requests += 1;
                                        state.last_progress_at = Some(Instant::now());
                                        continue;
                                    }

                                    let start_height = state.current_height;
                                    let end_height = state.target_height.max(block.height);
                                    state.current_height = block.height;
                                    if block.height > state.target_height {
                                        state.target_height = block.height;
                                    }
                                    state.blocks_synced += 1;
                                    state.last_progress_at = Some(Instant::now());

                                    // Check if we've reached the target height
                                    if block.height >= state.target_height {
                                        Self::complete_sync_state(&mut state);

                                        // Publish sync completed event
                                        if let Some(event_bus) = &event_bus {
                                            let result = SyncResult {
                                                success: true,
                                                blocks_synced: state.blocks_synced,
                                                start_height,
                                                end_height,
                                                error: None,
                                            };

                                            event_bus
                                                .publish(NetworkEvent::SyncCompleted(result))
                                                .await;
                                        }
                                    }
                                }
                            }

                            // Update peer reputation (good block)
                            if let Some(reputation) = &reputation {
                                reputation.update_score(&peer_id, ReputationEvent::GoodBlock);
                            }
                        }
                        None => {
                            warn!("Block response channel closed");
                            break;
                        }
                    }
                }

                info!("Block response handler stopped");
            });
        }

        // Start the block range response handler
        let block_range_rx_option = {
            let mut block_range_rx_guard = self.block_range_rx.write().await;
            block_range_rx_guard.take()
        };

        if let Some(mut block_range_rx) = block_range_rx_option {
            let block_store = self.block_store.clone();
            let consensus = self.consensus.clone();
            let sync_state = self.sync_state.clone();
            let running = self.running.clone();
            let event_bus = self.event_bus.clone();
            let reputation = self.reputation.clone();

            tokio::spawn(async move {
                info!("Starting block range response handler");

                while {
                    let is_running = *running.read().await;
                    is_running
                } {
                    match block_range_rx.recv().await {
                        Some((blocks, peer_id)) => {
                            debug!("Received {} blocks from peer {}", blocks.len(), peer_id);

                            let mut applied_blocks = 0u64;
                            let mut last_applied_height = None;

                            for block in &blocks {
                                match Self::process_synced_block(
                                    &block_store,
                                    consensus.as_ref(),
                                    block,
                                )
                                .await
                                {
                                    Ok(BlockProcessingResult::Success)
                                    | Ok(BlockProcessingResult::AlreadyKnown) => {
                                        applied_blocks += 1;
                                        last_applied_height = Some(block.height);
                                    }
                                    Ok(result) => {
                                        warn!(
                                            "Rejected synced block {} from peer {} with result {:?}",
                                            block.height, peer_id, result
                                        );
                                        break;
                                    }
                                    Err(err) => {
                                        error!(
                                            "Failed to process synced block {} from peer {}: {}",
                                            block.height, peer_id, err
                                        );
                                        break;
                                    }
                                }
                            }

                            // Update sync state
                            {
                                let mut state = sync_state.write().await;
                                if state.in_progress {
                                    if applied_blocks == 0 {
                                        state.failed_requests += 1;
                                        state.last_progress_at = Some(Instant::now());
                                        continue;
                                    }

                                    let start_height = state.current_height;
                                    let end_height = state
                                        .target_height
                                        .max(last_applied_height.unwrap_or(state.current_height));

                                    if let Some(last_height) = last_applied_height {
                                        state.current_height = last_height;
                                        if last_height > state.target_height {
                                            state.target_height = last_height;
                                        }
                                    }
                                    state.blocks_synced += applied_blocks;
                                    state.last_progress_at = Some(Instant::now());

                                    // Check if we've reached the target height
                                    if let Some(last_height) = last_applied_height {
                                        if last_height >= state.target_height {
                                            Self::complete_sync_state(&mut state);

                                            // Publish sync completed event
                                            if let Some(event_bus) = &event_bus {
                                                let result = SyncResult {
                                                    success: true,
                                                    blocks_synced: state.blocks_synced,
                                                    start_height,
                                                    end_height,
                                                    error: None,
                                                };

                                                event_bus
                                                    .publish(NetworkEvent::SyncCompleted(result))
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }

                            // Update peer reputation (good blocks)
                            if let Some(reputation) = &reputation {
                                reputation.update_score(&peer_id, ReputationEvent::GoodBlock);
                            }
                        }
                        None => {
                            warn!("Block range response channel closed");
                            break;
                        }
                    }
                }

                info!("Block range response handler stopped");
            });
        }

        Ok(())
    }

    /// Stop the sync service
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
    }

    /// Sync with the network
    async fn sync_with_network(
        block_store: Arc<BlockStore<'static>>,
        peer_registry: Arc<PeerRegistry>,
        broadcaster: Arc<PeerBroadcaster>,
        sync_state: Arc<RwLock<SyncState>>,
        event_bus: Option<Arc<EventBus>>,
        reputation: Option<Arc<ReputationSystem>>,
        advanced_registry: Option<Arc<AdvancedPeerRegistry>>,
        _config: &SyncConfig,
    ) -> Result<(), String> {
        // Get our current height
        let current_height = block_store.get_latest_height().unwrap_or(0);

        // Find the best peer to sync from
        let sync_peer = if let Some(registry) = &advanced_registry {
            // Prefer the advanced registry when it actually knows about peers,
            // otherwise fall back to the basic shared registry used by the
            // live peer handlers.
            let best_peers = registry.get_best_sync_peers(1);
            best_peers.first().cloned().or_else(|| {
                let active_peers = peer_registry.get_active_peers();
                active_peers.first().map(|p| p.node_id.clone())
            })
        } else {
            let active_peers = peer_registry.get_active_peers();
            active_peers.first().map(|p| p.node_id.clone())
        };

        let sync_peer = match sync_peer {
            Some(peer) => peer,
            None => {
                warn!("No peers available for sync");
                return Err("No peers available for sync".to_string());
            }
        };

        // Request the latest block from the peer
        match broadcaster
            .send_to_peer(
                &sync_peer,
                NetMessage::RequestBlock(u64::MAX), // Special value to request the latest block
            )
            .await
        {
            Ok(true) => {
                debug!("Requested latest block from peer {}", sync_peer);

                // Update sync state
                {
                    let mut state = sync_state.write().await;
                    state.in_progress = true;
                    state.target_height = current_height;
                    state.current_height = current_height;
                    state.awaiting_latest_height = true;
                    state.probe_completed = false;
                    state.sync_peer = Some(sync_peer.clone());
                    state.start_time = Some(Instant::now());
                    state.blocks_synced = 0;
                    state.failed_requests = 0;
                    state.last_progress_at = state.start_time;
                }

                // Publish sync requested event
                if let Some(event_bus) = &event_bus {
                    event_bus
                        .publish(NetworkEvent::SyncRequested(
                            SyncRequest::GetLatestBlock,
                            sync_peer.clone(),
                        ))
                        .await;
                }

                Ok(())
            }
            Ok(false) => {
                error!("Failed to request latest block from peer {}", sync_peer);

                // Update peer reputation (timeout)
                if let Some(reputation) = &reputation {
                    reputation.update_score(&sync_peer, ReputationEvent::Timeout);
                }

                Err(format!("Failed to send request to peer {}", sync_peer))
            }
            Err(e) => {
                error!("Error sending request to peer {}: {}", sync_peer, e);

                // Update peer reputation (timeout)
                if let Some(reputation) = &reputation {
                    reputation.update_score(&sync_peer, ReputationEvent::Timeout);
                }

                Err(e)
            }
        }
    }

    /// Sync to a specific height
    pub async fn sync_to_height(&self, target_height: u64) -> Result<(), String> {
        // Get our current height
        let current_height = self.block_store.get_latest_height().unwrap_or(0);

        // Check if we're already at or beyond the target height
        if current_height >= target_height {
            debug!(
                "Already at or beyond target height: {} >= {}",
                current_height, target_height
            );
            return Ok(());
        }

        // Find the best peer to sync from
        let sync_peer = if let Some(registry) = &self.advanced_registry {
            let best_peers = registry.get_best_sync_peers(1);
            best_peers.first().cloned().or_else(|| {
                let active_peers = self.peer_registry.get_active_peers();
                active_peers.first().map(|p| p.node_id.clone())
            })
        } else {
            let active_peers = self.peer_registry.get_active_peers();
            active_peers.first().map(|p| p.node_id.clone())
        };

        let sync_peer = match sync_peer {
            Some(peer) => peer,
            None => {
                warn!("No peers available for sync");
                return Err("No peers available for sync".to_string());
            }
        };

        // Request blocks from the peer
        let start_height = current_height;
        let end_height = target_height;

        match self
            .broadcaster
            .send_to_peer(
                &sync_peer,
                NetMessage::RequestBlockRange {
                    start_height,
                    end_height,
                },
            )
            .await?
        {
            true => {
                info!(
                    "Requested blocks {}..{} from peer {}",
                    start_height, end_height, sync_peer
                );

                // Update sync state
                {
                    let mut state = self.sync_state.write().await;
                    state.in_progress = true;
                    state.target_height = target_height;
                    state.current_height = current_height;
                    state.awaiting_latest_height = false;
                    state.probe_completed = false;
                    state.sync_peer = Some(sync_peer.clone());
                    state.start_time = Some(Instant::now());
                    state.blocks_synced = 0;
                    state.failed_requests = 0;
                    state.last_progress_at = state.start_time;
                }

                // Publish sync requested event
                if let Some(event_bus) = &self.event_bus {
                    event_bus
                        .publish(NetworkEvent::SyncRequested(
                            SyncRequest::GetBlocks(start_height, end_height),
                            sync_peer.clone(),
                        ))
                        .await;
                }

                Ok(())
            }
            false => {
                error!("Failed to request blocks from peer {}", sync_peer);

                // Update peer reputation (timeout)
                if let Some(reputation) = &self.reputation {
                    reputation.update_score(&sync_peer, ReputationEvent::Timeout);
                }

                Err(format!("Failed to send request to peer {}", sync_peer))
            }
        }
    }

    /// Get the current sync state
    pub async fn get_sync_state(&self) -> SyncState {
        Self::reconcile_sync_state(&self.block_store, &self.sync_state).await;
        let state = self.sync_state.read().await;
        state.clone()
    }

    /// Check if a sync is in progress
    pub async fn is_syncing(&self) -> bool {
        let state = self.sync_state.read().await;
        state.in_progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::peer::state::ConnectionState;
    use crate::storage::kv_store::RocksDBStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sync_service() {
        // Create dependencies
        let temp_dir = tempdir().unwrap();
        let kv_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let block_store = Arc::new(BlockStore::new(kv_store));
        let peer_registry = Arc::new(PeerRegistry::new());
        let broadcaster = Arc::new(PeerBroadcaster::new());

        // Create sync service
        let service = SyncService::new(
            block_store.clone(),
            peer_registry.clone(),
            broadcaster.clone(),
        );

        // Check initial state
        let state = service.get_sync_state().await;
        assert!(!state.in_progress);
        assert_eq!(state.blocks_synced, 0);

        // Register a peer
        let addr: std::net::SocketAddr = "127.0.0.1:8000".parse().unwrap();
        peer_registry.register_peer("peer1", addr, true);
        peer_registry.update_peer_state("peer1", ConnectionState::Ready);

        // We can't fully test syncing without a network, but we can check that
        // the service starts and stops correctly
        let result = service.start().await;
        assert!(result.is_ok());

        // Stop the service
        service.stop().await;
    }

    #[tokio::test]
    async fn get_sync_state_reconciles_stale_target_against_local_tip() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let block_store = Arc::new(BlockStore::new(kv_store));
        let peer_registry = Arc::new(PeerRegistry::new());
        let broadcaster = Arc::new(PeerBroadcaster::new());
        let service = SyncService::new(block_store.clone(), peer_registry, broadcaster);

        let block = Block {
            height: 100,
            hash: [7u8; 32],
            prev_hash: [6u8; 32],
            timestamp: 12345,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0u8; 32],
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            result_commitment: [0u8; 32],
            state_root: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 100,
        };
        block_store.put_block(&block).unwrap();

        {
            let mut state = service.sync_state.write().await;
            state.in_progress = true;
            state.target_height = 62;
            state.current_height = 62;
            state.awaiting_latest_height = false;
            state.probe_completed = false;
            state.sync_peer = Some("peer1".to_string());
            state.start_time = Some(Instant::now() - Duration::from_secs(60));
            state.last_progress_at = state.start_time;
        }

        let state = service.get_sync_state().await;
        assert!(!state.in_progress);
        assert_eq!(state.current_height, 100);
        assert_eq!(state.target_height, 100);
        assert!(state.probe_completed);
        assert!(state.sync_peer.is_none());
    }

    #[tokio::test]
    async fn get_sync_state_does_not_complete_while_waiting_for_remote_tip() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let block_store = Arc::new(BlockStore::new(kv_store));
        let peer_registry = Arc::new(PeerRegistry::new());
        let broadcaster = Arc::new(PeerBroadcaster::new());
        let service = SyncService::new(block_store, peer_registry, broadcaster);

        {
            let mut state = service.sync_state.write().await;
            state.in_progress = true;
            state.target_height = 15;
            state.current_height = 15;
            state.awaiting_latest_height = true;
            state.probe_completed = false;
            state.sync_peer = Some("peer1".to_string());
            state.start_time = Some(Instant::now() - Duration::from_secs(1));
            state.last_progress_at = state.start_time;
        }

        let state = service.get_sync_state().await;
        assert!(state.in_progress);
        assert!(state.awaiting_latest_height);
        assert!(!state.probe_completed);
        assert_eq!(state.current_height, 0);
        assert_eq!(state.target_height, 15);
        assert_eq!(state.sync_peer.as_deref(), Some("peer1"));
    }

    #[tokio::test]
    async fn complete_sync_state_marks_probe_completed_even_at_genesis() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let block_store = Arc::new(BlockStore::new(kv_store));
        let peer_registry = Arc::new(PeerRegistry::new());
        let broadcaster = Arc::new(PeerBroadcaster::new());
        let service = SyncService::new(block_store, peer_registry, broadcaster);

        {
            let mut state = service.sync_state.write().await;
            state.in_progress = true;
            state.target_height = 0;
            state.current_height = 0;
            state.awaiting_latest_height = false;
            state.probe_completed = false;
            state.sync_peer = Some("peer1".to_string());
            state.start_time = Some(Instant::now() - Duration::from_secs(1));
            state.last_progress_at = state.start_time;
        }

        let state = service.get_sync_state().await;
        assert!(!state.in_progress);
        assert_eq!(state.current_height, 0);
        assert_eq!(state.target_height, 0);
        assert!(state.probe_completed);
        assert!(state.sync_peer.is_none());
    }
}

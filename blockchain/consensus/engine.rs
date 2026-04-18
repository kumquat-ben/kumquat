use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time;

use crate::consensus::config::ConsensusConfig;
use crate::consensus::telemetry::{unix_timestamp_now, ConsensusTelemetry};
use crate::network::types::message::NetMessage;
use crate::storage::block_store::{result_commitment, Block, BlockStore};
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::{TransactionRecord, TxStore};
use crate::storage::{BatchOperationManager, KVStore};

/// Create a genesis block
fn create_genesis_block(config: &ConsensusConfig) -> Block {
    Block {
        height: 0,
        hash: [0u8; 32],
        prev_hash: [0u8; 32],
        timestamp: 0,
        transactions: vec![],
        conversion_fulfillment_order_ids: vec![],
        miner: [0u8; 32],
        pre_reward_state_root: [0u8; 32],
        reward_token_ids: vec![],
        result_commitment: result_commitment(&[0u8; 32], &[0u8; 32], &[], &[]),
        state_root: [0u8; 32],
        tx_root: [0u8; 32],
        nonce: 0,
        poh_seq: 0,
        poh_hash: [0u8; 32],
        difficulty: config.initial_difficulty,
        total_difficulty: config.initial_difficulty as u128,
    }
}
use crate::consensus::block_processor::{BlockProcessingResult, BlockProcessor};
use crate::consensus::mining::block_producer::BlockProducer;
use crate::consensus::mining::mempool::Mempool;
use crate::consensus::poh::generator::PoHGenerator;
use crate::consensus::pow::difficulty::calculate_next_target;
use crate::consensus::types::ChainState;
use crate::consensus::validation::block_validator::BlockValidator;
use crate::consensus::validation::transaction_validator::TransactionValidator;

/// Main consensus engine
pub struct ConsensusEngine {
    /// Consensus configuration
    config: ConsensusConfig,

    /// Block store
    block_store: Arc<BlockStore<'static>>,

    /// Transaction store
    tx_store: Arc<TxStore<'static>>,

    /// State store
    state_store: Arc<StateStore<'static>>,

    /// Mempool
    mempool: Arc<Mempool>,

    /// Block validator
    block_validator: Arc<BlockValidator<'static>>,

    /// Transaction validator
    tx_validator: Arc<TransactionValidator<'static>>,

    /// PoH generator
    poh_generator: Arc<Mutex<PoHGenerator>>,

    /// Block producer
    block_producer: Arc<Mutex<BlockProducer<'static>>>,

    /// Block processor
    block_processor: Arc<BlockProcessor<'static>>,

    /// Batch operation manager
    batch_manager: Arc<BatchOperationManager<'static>>,

    /// Chain state
    chain_state: Arc<Mutex<ChainState>>,

    /// Network sender
    network_tx: mpsc::Sender<NetMessage>,

    /// Channel for new blocks
    block_rx: mpsc::Receiver<Block>,

    /// Channel for new transactions
    tx_rx: mpsc::Receiver<TransactionRecord>,

    /// Runtime telemetry for consensus and mining.
    telemetry: ConsensusTelemetry,

    /// Shared runtime gate for whether mining is currently allowed.
    mining_eligible: Arc<AtomicBool>,
}

impl ConsensusEngine {
    pub async fn process_network_block(&self, block: Block) -> BlockProcessingResult {
        let chain_state = self.chain_state.lock().await.clone();
        let result = self
            .block_processor
            .process_block(&block, &chain_state.current_target, &chain_state)
            .await;

        if result == BlockProcessingResult::Success {
            self.update_chain_state(&block).await;
            self.refresh_mining_eligibility_from_chain_tip().await;
        }

        result
    }

    pub async fn rollback_to_height(&self, height: u64) -> Result<(), String> {
        let latest_height = self.block_store.get_latest_height().unwrap_or(0);
        if latest_height <= height {
            return Ok(());
        }

        for rollback_height in (height + 1..=latest_height).rev() {
            self.block_processor.rollback_block(rollback_height).await?;
        }

        self.sync_chain_state_with_committed_tip().await?;
        self.refresh_mining_eligibility_from_chain_tip().await;
        Ok(())
    }

    fn committed_chain_state(&self) -> Result<ChainState, String> {
        let Some(tip_height) = self.block_store.get_latest_height() else {
            return Err("No committed tip found in block store".to_string());
        };

        let tip_block = self
            .block_store
            .get_block_by_height(tip_height)
            .map_err(|err| {
                format!(
                    "Failed to load committed tip at height {}: {}",
                    tip_height, err
                )
            })?
            .ok_or_else(|| {
                format!(
                    "Committed tip height {} missing from block store",
                    tip_height
                )
            })?;

        let persisted_root = self
            .state_store
            .get_state_root_at_height(tip_height)
            .map_err(|err| {
                format!(
                    "Failed to load persisted state root at height {}: {}",
                    tip_height, err
                )
            })?;
        let calculated_root = self
            .state_store
            .calculate_state_root(tip_height, tip_block.timestamp)
            .map_err(|err| {
                format!(
                    "Failed to calculate state root at height {}: {}",
                    tip_height, err
                )
            })?;

        let selected_root = match persisted_root {
            Some(root) => {
                if root.root_hash != calculated_root.root_hash {
                    return Err(format!(
                        "Committed state root mismatch at height {}: persisted={}, calculated={}",
                        tip_height,
                        hex::encode(root.root_hash),
                        hex::encode(calculated_root.root_hash),
                    ));
                }
                root
            }
            None => calculated_root,
        };

        if tip_block.state_root != selected_root.root_hash {
            return Err(format!(
                "Committed tip block root mismatch at height {}: block_store={}, state_store={}",
                tip_height,
                hex::encode(tip_block.state_root),
                hex::encode(selected_root.root_hash),
            ));
        }

        let mut chain_state = ChainState::new(
            tip_block.height,
            tip_block.hash,
            selected_root,
            tip_block.total_difficulty as u64,
            0,
            [0u8; 32],
        );
        chain_state.current_target =
            crate::consensus::types::Target::from_difficulty(tip_block.difficulty);
        chain_state.latest_timestamp = tip_block.timestamp;

        Ok(chain_state)
    }

    async fn sync_chain_state_with_committed_tip(&self) -> Result<(), String> {
        let committed_state = self.committed_chain_state()?;

        {
            let mut chain_state = self.chain_state.lock().await;
            *chain_state = committed_state.clone();
        }

        let mut block_producer = self.block_producer.lock().await;
        block_producer.update_chain_state(committed_state);
        Ok(())
    }

    fn apply_mining_eligibility(&self, eligible: bool) {
        self.mining_eligible.store(eligible, Ordering::SeqCst);
    }

    async fn refresh_mining_eligibility_from_chain_tip(&self) {
        if !self.config.enable_mining {
            self.apply_mining_eligibility(false);
            return;
        }

        let tip_height = self.block_store.get_latest_height().unwrap_or(0);
        self.apply_mining_eligibility(tip_height > 0);
    }

    pub fn is_mining_eligible(&self) -> bool {
        self.mining_eligible.load(Ordering::SeqCst)
    }

    /// Create a new consensus engine
    pub fn new(
        config: ConsensusConfig,
        kv_store: Arc<dyn KVStore + 'static>,
        block_store: Arc<BlockStore<'static>>,
        tx_store: Arc<TxStore<'static>>,
        state_store: Arc<StateStore<'static>>,
        network_tx: mpsc::Sender<NetMessage>,
        telemetry: ConsensusTelemetry,
    ) -> Self {
        Self::new_with_shared_mining_gate(
            config,
            kv_store,
            block_store,
            tx_store,
            state_store,
            network_tx,
            telemetry,
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub fn new_with_shared_mining_gate(
        config: ConsensusConfig,
        kv_store: Arc<dyn KVStore + 'static>,
        block_store: Arc<BlockStore<'static>>,
        tx_store: Arc<TxStore<'static>>,
        state_store: Arc<StateStore<'static>>,
        network_tx: mpsc::Sender<NetMessage>,
        telemetry: ConsensusTelemetry,
        mining_eligible: Arc<AtomicBool>,
    ) -> Self {
        // Create channels
        let (_block_tx, block_rx) = mpsc::channel(100);
        let (_tx_tx, tx_rx) = mpsc::channel(1000);

        // Create the mempool
        let mempool = Arc::new(Mempool::new(10000));

        // Create validators
        let block_validator = Arc::new(BlockValidator::new(
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            config.poh_tick_rate,
            config.hybrid_activation_height,
        ));

        let tx_validator = Arc::new(TransactionValidator::new(
            tx_store.clone(),
            state_store.clone(),
        ));

        // Create the PoH generator
        let poh_generator = Arc::new(Mutex::new(PoHGenerator::new(&config)));

        // Get the latest block
        let latest_block = match block_store.get_latest_height() {
            Some(height) => match block_store.get_block_by_height(height) {
                Ok(Some(block)) => block,
                _ => {
                    // Create a genesis block if we can't get the latest block
                    let genesis = create_genesis_block(&config);
                    block_store.put_block(&genesis).unwrap();
                    genesis
                }
            },
            None => {
                // Create a genesis block
                let genesis = create_genesis_block(&config);

                // Store the genesis block
                let _ = block_store.put_block(&genesis);

                genesis
            }
        };

        let persisted_state_root = state_store
            .get_state_root_at_height(latest_block.height)
            .ok()
            .flatten()
            .or_else(|| {
                state_store
                    .calculate_state_root(latest_block.height, latest_block.timestamp)
                    .ok()
            });

        let selected_state_root = if let Some(root) = persisted_state_root {
            if root.root_hash != latest_block.state_root {
                warn!(
                    "Consensus bootstrap root mismatch at height {}: block_store={}, state_store={}. Preferring state store root.",
                    latest_block.height,
                    hex::encode(latest_block.state_root),
                    hex::encode(root.root_hash),
                );
            }
            root
        } else {
            crate::storage::state::StateRoot::new(
                latest_block.state_root,
                latest_block.height,
                latest_block.timestamp,
            )
        };

        // Create the chain state
        let mut initial_chain_state = ChainState::new(
            latest_block.height,
            latest_block.hash,
            selected_state_root,
            latest_block.total_difficulty as u64,
            0,         // finalized_height
            [0u8; 32], // finalized_hash
        );
        initial_chain_state.current_target =
            crate::consensus::types::Target::from_difficulty(latest_block.difficulty);
        initial_chain_state.latest_timestamp = latest_block.timestamp;
        let chain_state = Arc::new(Mutex::new(initial_chain_state));

        // Create the batch operation manager
        let batch_manager = Arc::new(BatchOperationManager::new(
            kv_store.clone(),
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
        ));

        // Create the block processor
        let block_processor = Arc::new(BlockProcessor::new(
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            batch_manager.clone(),
            block_validator.clone(),
            Some(mempool.clone()),
            config.hybrid_activation_height,
        ));

        // Create the block producer
        let block_producer = Arc::new(Mutex::new(BlockProducer::new(
            // Get a clone of the chain state
            chain_state
                .try_lock()
                .expect("Failed to lock chain state")
                .clone(),
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            mempool.clone(),
            poh_generator.clone(),
            network_tx.clone(),
            config.clone(),
        )));

        let engine = Self {
            config,
            block_store,
            tx_store,
            state_store,
            mempool,
            block_validator,
            tx_validator,
            poh_generator,
            block_producer,
            block_processor,
            batch_manager,
            chain_state,
            network_tx,
            block_rx,
            tx_rx,
            telemetry,
            mining_eligible,
        };

        engine.refresh_mining_gate_from_local_tip();
        engine
    }

    fn refresh_mining_gate_from_local_tip(&self) {
        if !self.config.enable_mining {
            self.apply_mining_eligibility(false);
            return;
        }

        let tip_height = self.block_store.get_latest_height().unwrap_or(0);
        self.apply_mining_eligibility(
            tip_height > 0 || self.mining_eligible.load(Ordering::SeqCst),
        );
    }

    pub fn telemetry(&self) -> ConsensusTelemetry {
        self.telemetry.clone()
    }

    /// Run the consensus engine
    pub async fn run(&mut self) {
        {
            let mut telemetry = self.telemetry.write().await;
            telemetry.consensus_started = true;
        }
        // Start the PoH generator
        {
            let mut poh_gen = self.poh_generator.lock().await;
            poh_gen.start().await;
        } // Release the lock before proceeding

        // Register network handlers
        self.register_network_handlers().await;

        // Start the main loop
        self.main_loop().await;
    }

    /// Get a channel for sending blocks to the consensus engine
    pub fn block_channel(self: &Arc<Self>) -> mpsc::Sender<Block> {
        // Create a new channel
        let (tx, rx) = mpsc::channel::<Block>(100);

        // Clone self as Arc to extend lifetime to 'static
        let engine = self.clone();

        // Spawn a task with 'static lifetime
        tokio::spawn(async move {
            let mut block_rx = rx;
            while let Some(block) = block_rx.recv().await {
                debug!("Received block from network: height={}", block.height);
                let result = engine.process_network_block(block).await;
                match result {
                    BlockProcessingResult::Success => {
                        debug!("Block processed: Success");
                    }
                    BlockProcessingResult::AlreadyKnown => {
                        debug!("Block processed: Already Known");
                    }
                    BlockProcessingResult::UnknownParent => {
                        debug!("Block processed: Unknown Parent");
                    }
                    BlockProcessingResult::Invalid(reason) => {
                        debug!("Block processed: Invalid - {}", reason);
                    }
                    BlockProcessingResult::Error(error) => {
                        debug!("Block processed: Error - {}", error);
                    }
                }
            }
        });

        tx
    }

    /// Register handlers for network messages
    async fn register_network_handlers(&mut self) {
        // TODO: Register handlers for network messages
    }

    /// Main consensus loop
    async fn main_loop(&mut self) {
        // Mining interval
        let mining_interval = self.config.target_block_time_duration();
        info!(
            "Setting mining interval to {} seconds",
            mining_interval.as_secs()
        );
        let mut mining_timer = time::interval(mining_interval);
        // Make sure the first tick happens immediately
        mining_timer.tick().await;

        // Difficulty adjustment interval
        let difficulty_interval = Duration::from_secs(60);
        info!(
            "Setting difficulty adjustment interval to {} seconds",
            difficulty_interval.as_secs()
        );
        let mut difficulty_timer = time::interval(difficulty_interval);

        loop {
            tokio::select! {
                // Handle new blocks
                Some(block) = self.block_rx.recv() => {
                    self.handle_new_block(block).await;
                }

                // Handle new transactions
                Some(tx) = self.tx_rx.recv() => {
                    self.handle_new_transaction(tx).await;
                }

                // Mining timer
                _ = mining_timer.tick() => {
                    let mining_enabled = self.config.enable_mining && self.is_mining_eligible();
                    info!(
                        "Mining timer fired. Config enabled: {}, runtime eligible: {}",
                        self.config.enable_mining,
                        self.is_mining_eligible()
                    );
                    if mining_enabled {
                        self.mine_block().await;
                    }
                }

                // Difficulty adjustment timer
                _ = difficulty_timer.tick() => {
                    self.adjust_difficulty().await;
                }
            }
        }
    }

    /// Handle a new block
    async fn handle_new_block(&self, block: Block) {
        info!("Received new block at height {}", block.height);

        let result = self.process_network_block(block.clone()).await;

        match result {
            BlockProcessingResult::Success => {
                info!("Block processed successfully at height {}", block.height);
            }
            BlockProcessingResult::AlreadyKnown => {
                debug!("Block already known at height {}", block.height);
            }
            BlockProcessingResult::UnknownParent => {
                warn!("Block with unknown parent at height {}", block.height);

                // TODO: Request the parent block from the network
            }
            BlockProcessingResult::Invalid(reason) => {
                warn!("Invalid block at height {}: {}", block.height, reason);
            }
            BlockProcessingResult::Error(error) => {
                error!(
                    "Error processing block at height {}: {}",
                    block.height, error
                );
            }
        }
    }

    /// Update the chain state with a new block
    async fn update_chain_state(&self, block: &Block) {
        let previous_state = self.chain_state.lock().await.clone();
        info!(
            "Synchronizing chain state after block {} (previous height={}, previous tip={})",
            block.height,
            previous_state.height,
            hex::encode(previous_state.tip_hash),
        );

        match self.sync_chain_state_with_committed_tip().await {
            Ok(()) => {
                let chain_state = self.chain_state.lock().await.clone();
                info!(
                    "Chain state synchronized to committed tip: height={}, tip_hash={}, total_difficulty={}",
                    chain_state.height,
                    hex::encode(chain_state.tip_hash),
                    chain_state.total_difficulty
                );
            }
            Err(err) => {
                error!(
                    "Failed to synchronize chain state after block {}: {}",
                    block.height, err
                );
            }
        }
    }

    /// Handle a new transaction
    async fn handle_new_transaction(&self, tx: TransactionRecord) {
        debug!("Received new transaction: {:?}", tx.tx_id);

        // Add the transaction to the mempool
        if self.mempool.add_transaction(tx.clone()) {
            debug!("Added transaction to mempool: {:?}", tx.tx_id);
        } else {
            debug!("Transaction already in mempool: {:?}", tx.tx_id);
        }
    }

    /// Mine a new block
    async fn mine_block(&self) {
        info!("Attempting to mine a new block");
        {
            let mut telemetry = self.telemetry.write().await;
            telemetry.mining_attempts += 1;
            telemetry.last_mining_attempt_at = Some(unix_timestamp_now());
            telemetry.last_error = None;
        }

        if let Err(err) = self.sync_chain_state_with_committed_tip().await {
            error!(
                "Refusing to mine with inconsistent committed tip state: {}",
                err
            );
            let mut telemetry = self.telemetry.write().await;
            telemetry.failed_mining_attempts += 1;
            telemetry.last_error = Some(err);
            return;
        }

        // Get the current chain state height
        let (current_height, current_hash) = {
            let chain_state = self.chain_state.lock().await;
            (chain_state.height, chain_state.tip_hash)
        };

        info!(
            "Mining block at height {} on top of block {}",
            current_height + 1,
            hex::encode(&current_hash)
        );

        let mined_block = {
            // Lock only long enough to snapshot producer state and mine the next block.
            let block_producer = self.block_producer.lock().await;

            // Check the block producer's chain state
            let producer_height = block_producer.get_chain_state_height();
            let producer_hash = block_producer.get_chain_state_tip_hash();
            info!(
                "Block producer chain state: height={}, tip_hash={}",
                producer_height,
                hex::encode(&producer_hash)
            );

            block_producer.mine_block().await
        };

        // Mine a block
        if let Some(block) = mined_block {
            info!("Mined new block at height {}", block.height);
            {
                let mut telemetry = self.telemetry.write().await;
                telemetry.mined_blocks += 1;
                telemetry.last_mining_success_at = Some(unix_timestamp_now());
                telemetry.last_mined_block_height = Some(block.height);
                telemetry.last_mined_block_hash = Some(hex::encode(block.hash));
            }

            // Handle the new block
            self.handle_new_block(block).await;
        } else {
            warn!("Failed to mine a new block");
            let mut telemetry = self.telemetry.write().await;
            telemetry.failed_mining_attempts += 1;
            telemetry.last_error = Some("Mining attempt did not produce a block".to_string());
        }
    }

    /// Adjust the difficulty
    async fn adjust_difficulty(&self) {
        // Get the current chain state
        let mut chain_state = self.chain_state.lock().await;

        // Calculate the next target
        let next_target = calculate_next_target(
            &self.config,
            &self.block_store,
            &self.state_store,
            &self.tx_store,
            chain_state.height,
        );

        // Update the chain state
        chain_state.current_target = next_target;

        // Update the block producer
        let mut block_producer = self.block_producer.lock().await;
        block_producer.update_chain_state(chain_state.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::telemetry::new_consensus_telemetry;
    use crate::consensus::validation::BlockValidationResult;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::kv_store::{KVStore, KVStoreError, WriteBatchOperation};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    struct SharedTestStore(&'static RocksDBStore);

    impl KVStore for SharedTestStore {
        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KVStoreError> {
            self.0.put(key, value)
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KVStoreError> {
            self.0.get(key)
        }

        fn delete(&self, key: &[u8]) -> Result<(), KVStoreError> {
            self.0.delete(key)
        }

        fn exists(&self, key: &[u8]) -> Result<bool, KVStoreError> {
            self.0.exists(key)
        }

        fn write_batch(&self, operations: Vec<WriteBatchOperation>) -> Result<(), KVStoreError> {
            self.0.write_batch(operations)
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KVStoreError> {
            self.0.scan_prefix(prefix)
        }

        fn flush(&self) -> Result<(), KVStoreError> {
            self.0.flush()
        }
    }

    #[tokio::test]
    async fn test_consensus_engine_creation() {
        // Create a temporary directory for the database
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        // Create the stores
        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));

        // Create a network channel
        let (network_tx, _network_rx) = mpsc::channel(100);

        // Create a config
        let config = ConsensusConfig::default();
        let telemetry = new_consensus_telemetry(false);

        // Create the consensus engine
        let engine = ConsensusEngine::new(
            config,
            manager_store,
            block_store,
            tx_store,
            state_store,
            network_tx,
            telemetry,
        );

        // Check that the engine was created successfully
        assert_eq!(engine.config.target_block_time, 15);
    }

    #[tokio::test]
    async fn mined_blocks_advance_chain_without_invalidating_previous_state() {
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));
        let (network_tx, _network_rx) = mpsc::channel(100);

        let mut config = ConsensusConfig::default();
        config.enable_mining = true;
        config.initial_difficulty = 1;
        config.target_block_time = 1;
        config.mining_threads = 1;
        let telemetry = new_consensus_telemetry(false);

        let engine = ConsensusEngine::new(
            config,
            manager_store,
            block_store.clone(),
            tx_store,
            state_store,
            network_tx,
            telemetry,
        );

        let block = {
            let producer = engine.block_producer.lock().await;
            producer.mine_block().await.expect("expected mined block")
        };
        let chain_state = engine.chain_state.lock().await.clone();
        let validation = engine
            .block_validator
            .validate_block(&block, &chain_state.current_target);
        assert_eq!(
            validation,
            BlockValidationResult::Valid,
            "mined block should validate against its projected state root",
        );
        let result = engine
            .block_processor
            .process_block(&block, &chain_state.current_target, &chain_state)
            .await;
        assert_eq!(result, BlockProcessingResult::Success);
        engine.update_chain_state(&block).await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(block_store.get_latest_height(), Some(1));

        engine.mine_block().await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(block_store.get_latest_height(), Some(2));
    }

    #[tokio::test]
    async fn bootstrap_node_starts_with_mining_ineligible_at_genesis() {
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));
        let (network_tx, _network_rx) = mpsc::channel(100);

        let mut config = ConsensusConfig::default();
        config.enable_mining = true;
        let telemetry = new_consensus_telemetry(false);

        let engine = ConsensusEngine::new_with_shared_mining_gate(
            config,
            manager_store,
            block_store,
            tx_store,
            state_store,
            network_tx,
            telemetry,
            Arc::new(AtomicBool::new(false)),
        );

        assert!(
            !engine.is_mining_eligible(),
            "fresh bootstrapping node should not mine before syncing beyond genesis",
        );
    }

    #[tokio::test]
    async fn synced_block_unlocks_shared_mining_gate_after_advancing_past_genesis() {
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));
        let (network_tx, _network_rx) = mpsc::channel(100);

        let mut config = ConsensusConfig::default();
        config.enable_mining = true;
        config.initial_difficulty = 1;
        config.target_block_time = 1;
        config.mining_threads = 1;
        let shared_gate = Arc::new(AtomicBool::new(false));

        let producer_engine = ConsensusEngine::new_with_shared_mining_gate(
            config.clone(),
            manager_store.clone(),
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            network_tx.clone(),
            new_consensus_telemetry(false),
            shared_gate.clone(),
        );
        let sync_engine = ConsensusEngine::new_with_shared_mining_gate(
            config,
            manager_store,
            block_store.clone(),
            tx_store,
            state_store,
            network_tx,
            new_consensus_telemetry(false),
            shared_gate,
        );

        assert!(!producer_engine.is_mining_eligible());
        assert!(!sync_engine.is_mining_eligible());

        let block = {
            let producer = producer_engine.block_producer.lock().await;
            producer.mine_block().await.expect("expected mined block")
        };

        assert_eq!(
            sync_engine.process_network_block(block).await,
            BlockProcessingResult::Success
        );
        assert!(producer_engine.is_mining_eligible());
        assert!(sync_engine.is_mining_eligible());
        assert_eq!(block_store.get_latest_height(), Some(1));
    }

    #[tokio::test]
    async fn nonempty_genesis_state_stays_consistent_across_single_node_mining() {
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));
        let (network_tx, _network_rx) = mpsc::channel(100);

        let genesis_address = [7u8; 32];
        state_store
            .create_account(&genesis_address, 500, crate::storage::AccountType::User)
            .unwrap();
        let genesis_root = state_store.calculate_state_root(0, 1).unwrap();
        state_store.put_state_root(&genesis_root).unwrap().unwrap();

        let genesis_block = Block {
            height: 0,
            hash: [9u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 1,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0u8; 32],
            pre_reward_state_root: genesis_root.root_hash,
            reward_token_ids: vec![],
            result_commitment: result_commitment(&[9u8; 32], &genesis_root.root_hash, &[], &[]),
            state_root: genesis_root.root_hash,
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0u8; 32],
            difficulty: 1,
            total_difficulty: 1,
        };
        block_store.put_block(&genesis_block).unwrap();

        let mut config = ConsensusConfig::default();
        config.enable_mining = true;
        config.initial_difficulty = 1;
        config.target_block_time = 1;
        config.mining_threads = 1;
        let telemetry = new_consensus_telemetry(false);

        let engine = ConsensusEngine::new(
            config,
            manager_store,
            block_store.clone(),
            tx_store,
            state_store.clone(),
            network_tx,
            telemetry,
        );

        let first_block = {
            let producer = engine.block_producer.lock().await;
            producer
                .mine_block()
                .await
                .expect("expected first mined block")
        };
        assert_eq!(first_block.pre_reward_state_root, genesis_root.root_hash);
        let first_state = engine.chain_state.lock().await.clone();
        assert_eq!(
            engine
                .block_validator
                .validate_block(&first_block, &first_state.current_target),
            BlockValidationResult::Valid
        );
        assert_eq!(
            engine
                .block_processor
                .process_block(&first_block, &first_state.current_target, &first_state)
                .await,
            BlockProcessingResult::Success
        );
        engine.update_chain_state(&first_block).await;

        let second_block = {
            let producer = engine.block_producer.lock().await;
            producer
                .mine_block()
                .await
                .expect("expected second mined block")
        };
        assert_eq!(second_block.pre_reward_state_root, first_block.state_root);
        let second_state = engine.chain_state.lock().await.clone();
        assert_eq!(
            engine
                .block_validator
                .validate_block(&second_block, &second_state.current_target),
            BlockValidationResult::Valid
        );
        assert_eq!(
            engine
                .block_processor
                .process_block(&second_block, &second_state.current_target, &second_state)
                .await,
            BlockProcessingResult::Success
        );
        engine.update_chain_state(&second_block).await;
        assert_eq!(block_store.get_latest_height(), Some(2));
    }

    #[tokio::test]
    async fn generated_genesis_state_stays_consistent_across_second_mined_block() {
        let temp_dir = tempdir().unwrap();
        let shared_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let manager_store: Arc<dyn KVStore> = Arc::new(SharedTestStore(shared_store));

        let block_store = Arc::new(BlockStore::new(shared_store));
        let tx_store = Arc::new(TxStore::new(shared_store));
        let state_store = Arc::new(StateStore::new(shared_store));
        let (network_tx, _network_rx) = mpsc::channel(100);

        let genesis_path = temp_dir.path().join("genesis.toml");
        let mut genesis_config = crate::tools::genesis::GenesisConfig::default();
        genesis_config.chain_id = 1337;
        genesis_config.timestamp = 1_744_067_299;
        genesis_config.initial_difficulty = 100;
        genesis_config.save(&genesis_path).unwrap();

        let (mut genesis_block, genesis_accounts) =
            crate::tools::genesis::generate_genesis(&genesis_path).unwrap();
        for (address, state) in genesis_accounts {
            let mut state = state;
            state.assign_token_owner(address);
            state.sync_balance_from_tokens();
            state_store.set_account_state(&address, &state).unwrap();
        }
        let genesis_root = state_store
            .calculate_state_root(0, genesis_block.timestamp)
            .unwrap();
        state_store.put_state_root(&genesis_root).unwrap().unwrap();
        genesis_block.pre_reward_state_root = genesis_root.root_hash;
        genesis_block.state_root = genesis_root.root_hash;
        genesis_block.hash =
            crate::storage::block_store::pow_hash(&genesis_block.canonical_header());
        genesis_block.result_commitment = result_commitment(
            &genesis_block.hash,
            &genesis_block.state_root,
            &genesis_block.reward_token_ids,
            &genesis_block.conversion_fulfillment_order_ids,
        );
        block_store.put_block(&genesis_block).unwrap();

        let mut config = ConsensusConfig::default();
        config.enable_mining = true;
        config.initial_difficulty = 100;
        config.target_block_time = 1;
        config.mining_threads = 1;
        let telemetry = new_consensus_telemetry(false);

        let engine = ConsensusEngine::new(
            config,
            manager_store,
            block_store.clone(),
            tx_store,
            state_store.clone(),
            network_tx,
            telemetry,
        );

        let first_block = {
            let producer = engine.block_producer.lock().await;
            producer
                .mine_block()
                .await
                .expect("expected first mined block")
        };
        let first_state = engine.chain_state.lock().await.clone();
        assert_eq!(
            engine
                .block_processor
                .process_block(&first_block, &first_state.current_target, &first_state)
                .await,
            BlockProcessingResult::Success
        );
        engine.update_chain_state(&first_block).await;

        let second_block = {
            let producer = engine.block_producer.lock().await;
            producer
                .mine_block()
                .await
                .expect("expected second mined block")
        };
        let second_state = engine.chain_state.lock().await.clone();
        assert_eq!(
            engine
                .block_processor
                .process_block(&second_block, &second_state.current_target, &second_state)
                .await,
            BlockProcessingResult::Success
        );
        engine.update_chain_state(&second_block).await;

        let committed_state = engine.committed_chain_state();
        assert!(
            committed_state.is_ok(),
            "committed chain state should remain consistent after second block: {:?}",
            committed_state,
        );
    }
}

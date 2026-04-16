use log::{debug, error, info, warn};
use std::sync::Arc;

use crate::consensus::mining::mempool::Mempool;
use crate::consensus::types::{ChainState, Target};
use crate::consensus::validation::fork_choice::{choose_fork, resolve_fork, ForkChoice};
use crate::consensus::validation::{BlockValidationResult, BlockValidator};
use crate::storage::{
    AccountState, BatchOperationManager, Block, BlockStore, Hash, StateStore, TransactionRecord,
    TxStore,
};

/// Result of block processing
#[derive(Debug, Clone, PartialEq)]
pub enum BlockProcessingResult {
    /// Block was successfully processed
    Success,

    /// Block was already known
    AlreadyKnown,

    /// Block has an unknown parent
    UnknownParent,

    /// Block is invalid
    Invalid(String),

    /// Error occurred during processing
    Error(String),
}

/// Block processor for handling new blocks
pub struct BlockProcessor<'a> {
    /// Block store
    block_store: Arc<BlockStore<'a>>,

    /// Transaction store
    tx_store: Arc<TxStore<'a>>,

    /// State store
    state_store: Arc<StateStore<'a>>,

    /// Batch operation manager
    batch_manager: Arc<BatchOperationManager<'a>>,

    /// Block validator
    validator: Arc<BlockValidator<'a>>,

    /// Mempool
    mempool: Option<Arc<Mempool>>,

    /// Block height where hybrid cash rules activate.
    hybrid_activation_height: u64,
}

impl<'a> BlockProcessor<'a> {
    /// Create a new block processor
    pub fn new(
        block_store: Arc<BlockStore<'a>>,
        tx_store: Arc<TxStore<'a>>,
        state_store: Arc<StateStore<'a>>,
        batch_manager: Arc<BatchOperationManager<'a>>,
        validator: Arc<BlockValidator<'a>>,
        mempool: Option<Arc<Mempool>>,
        hybrid_activation_height: u64,
    ) -> Self {
        Self {
            block_store,
            tx_store,
            state_store,
            batch_manager,
            validator,
            mempool,
            hybrid_activation_height,
        }
    }

    fn hybrid_active_at(&self, block_height: u64) -> bool {
        block_height >= self.hybrid_activation_height
    }

    fn prepare_hybrid_state_for_block(&self, block_height: u64) -> Result<(), String> {
        if !self.hybrid_active_at(block_height) {
            return Ok(());
        }

        if self.hybrid_activation_height > 0 && block_height == self.hybrid_activation_height {
            self.state_store
                .migrate_legacy_state_to_hybrid(block_height)
                .map_err(|e| format!("Failed to migrate legacy hybrid state: {}", e))?;
        }

        self.state_store
            .sweep_conversion_order_lifecycle(block_height)
            .map_err(|e| format!("Failed to sweep conversion lifecycle: {}", e))?;
        Ok(())
    }

    /// Process a new block
    pub async fn process_block(
        &self,
        block: &Block,
        target: &Target,
        chain_state: &ChainState,
    ) -> BlockProcessingResult {
        info!("Processing block at height {}", block.height);

        if let Err(err) = self.prepare_hybrid_state_for_block(block.height) {
            error!("{}", err);
            return BlockProcessingResult::Error(err);
        }

        // Validate the block
        match self.validator.validate_block(block, target) {
            BlockValidationResult::Valid => {
                debug!("Block is valid");

                // Check if this block is part of the main chain or a fork
                let fork_choice = choose_fork(&self.block_store, chain_state, block);

                match fork_choice {
                    ForkChoice::Accept => {
                        // Block builds on the current chain, continue processing
                        debug!("Block builds on current chain");
                    }
                    ForkChoice::Reject => {
                        // Block is not part of the best chain, reject it
                        warn!(
                            "Block rejected by fork choice rule: height={}",
                            block.height
                        );
                        return BlockProcessingResult::Invalid(
                            "Rejected by fork choice rule".to_string(),
                        );
                    }
                    ForkChoice::Fork => {
                        // We have a fork, need to resolve it
                        warn!("Fork detected at height {}", block.height);

                        // Get the current tip
                        let current_tip =
                            match self.block_store.get_block_by_hash(&chain_state.tip_hash) {
                                Ok(Some(tip)) => tip,
                                Ok(None) => {
                                    error!("Current tip block not found");
                                    return BlockProcessingResult::Error(
                                        "Current tip block not found".to_string(),
                                    );
                                }
                                Err(e) => {
                                    error!("Failed to get current tip: {}", e);
                                    return BlockProcessingResult::Error(format!(
                                        "Failed to get current tip: {}",
                                        e
                                    ));
                                }
                            };

                        // Resolve the fork
                        match resolve_fork(&self.block_store, &current_tip, block) {
                            Ok(canonical_tip) => {
                                if canonical_tip.hash != block.hash {
                                    // The current chain is still the best chain
                                    info!(
                                        "Current chain is the canonical tip, rejecting new block"
                                    );
                                    return BlockProcessingResult::Invalid(
                                        "Not the canonical tip".to_string(),
                                    );
                                }

                                // The new block is the canonical tip, continue processing
                                info!("New block is the canonical tip, accepting it");
                            }
                            Err(e) => {
                                error!("Failed to resolve fork: {}", e);
                                return BlockProcessingResult::Error(format!(
                                    "Failed to resolve fork: {}",
                                    e
                                ));
                            }
                        }
                    }
                    ForkChoice::Replace => {
                        // Replace the current chain with this block
                        debug!("Replacing current chain with this block");
                    }
                }
            }
            BlockValidationResult::AlreadyKnown => {
                debug!("Block already known: height={}", block.height);
                return BlockProcessingResult::AlreadyKnown;
            }
            BlockValidationResult::UnknownParent => {
                warn!("Block has unknown parent: height={}", block.height);
                return BlockProcessingResult::UnknownParent;
            }
            BlockValidationResult::Invalid(reason) => {
                warn!("Invalid block: {}", reason);
                return BlockProcessingResult::Invalid(reason);
            }
        }

        // Get the transactions for this block
        let transactions = match self.get_block_transactions(block) {
            Ok(txs) => txs,
            Err(e) => {
                error!("Failed to get block transactions: {}", e);
                return BlockProcessingResult::Error(format!("Failed to get transactions: {}", e));
            }
        };

        // Apply the transactions to get state changes
        let state_changes = match self.apply_transactions(block, &transactions) {
            Ok(changes) => changes,
            Err(e) => {
                error!("Failed to apply transactions: {}", e);
                return BlockProcessingResult::Error(format!(
                    "Failed to apply transactions: {}",
                    e
                ));
            }
        };

        // Commit the block, transactions, and state changes atomically
        match self
            .batch_manager
            .commit_block(block, &transactions, &state_changes)
        {
            Ok(_) => {
                info!("Block committed successfully: height={}", block.height);

                // Update mempool to remove included transactions
                if let Some(mempool) = &self.mempool {
                    self.update_mempool(block, mempool).await;
                }

                BlockProcessingResult::Success
            }
            Err(e) => {
                error!("Failed to commit block: {}", e);
                BlockProcessingResult::Error(format!("Failed to commit block: {}", e))
            }
        }
    }

    /// Get the transactions for a block
    fn get_block_transactions(&self, block: &Block) -> Result<Vec<TransactionRecord>, String> {
        let mut transactions = Vec::new();

        for tx_hash in &block.transactions {
            match self.tx_store.get_transaction(tx_hash) {
                Ok(Some(tx)) => {
                    transactions.push(tx);
                }
                Ok(None) => {
                    // Transaction not found, try to get it from the mempool
                    if let Some(mempool) = &self.mempool {
                        if let Some(tx) = mempool
                            .get_pending_transactions(100)
                            .iter()
                            .find(|tx| tx.tx_id == *tx_hash)
                        {
                            // Create a confirmed transaction record
                            let mut tx_record = tx.clone();
                            tx_record.block_height = block.height;
                            tx_record.status = crate::storage::TransactionStatus::Confirmed;
                            transactions.push(tx_record);
                        } else {
                            return Err(format!("Transaction not found: {}", hex::encode(tx_hash)));
                        }
                    } else {
                        return Err(format!("Transaction not found: {}", hex::encode(tx_hash)));
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to get transaction {}: {}",
                        hex::encode(tx_hash),
                        e
                    ));
                }
            }
        }

        Ok(transactions)
    }

    /// Apply transactions to get state changes
    fn apply_transactions(
        &self,
        block: &Block,
        transactions: &[TransactionRecord],
    ) -> Result<Vec<(Hash, AccountState)>, String> {
        self.state_store
            .project_state_changes(
                block.height,
                transactions,
                &block.miner,
                &block.conversion_fulfillment_order_ids,
                Some(&block.hash),
            )
            .map_err(|e| format!("Failed to project state changes: {}", e))
    }

    /// Update mempool to remove transactions included in the block
    async fn update_mempool(&self, block: &Block, mempool: &Mempool) {
        // Mark transactions as included in a block
        for tx_hash in &block.transactions {
            mempool.mark_included(tx_hash);
        }
    }

    /// Rollback a block
    pub async fn rollback_block(&self, height: u64) -> Result<(), String> {
        info!("Rolling back block at height {}", height);

        match self.batch_manager.rollback_block(height) {
            Ok(()) => {
                info!("Block rolled back successfully: height={}", height);
                Ok(())
            }
            Err(e) => {
                error!("Failed to rollback block: {}", e);
                Err(format!("Failed to rollback block: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RocksDBStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_block_processor_creation() {
        // Create a temporary directory for the database
        let temp_dir = tempdir().unwrap();
        let kv_store = Arc::new(RocksDBStore::new(temp_dir.path()).unwrap());

        // Create stores
        let block_store = Arc::new(BlockStore::new(kv_store.as_ref()));
        let tx_store = Arc::new(TxStore::new(kv_store.as_ref()));
        let state_store = Arc::new(StateStore::new(kv_store.as_ref()));

        // Create batch operation manager
        let batch_manager = Arc::new(BatchOperationManager::new(
            kv_store.clone(),
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
        ));

        // Create validator
        let validator = Arc::new(BlockValidator::new(
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            100,
            0,
        ));

        // Create block processor
        let processor = BlockProcessor::new(
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            batch_manager.clone(),
            validator.clone(),
            None,
            0,
        );
        let chain_state = ChainState::new(
            0,
            [0u8; 32],
            crate::storage::StateRoot::new([0u8; 32], 0, 0),
            0,
            0,
            [0u8; 32],
        );

        assert_eq!(chain_state.height, 0);
        let _ = processor;
    }
}

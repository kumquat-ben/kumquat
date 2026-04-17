use log::{error, info};
use std::sync::Arc;

use crate::storage::block_store::Hash;
use crate::storage::block_store::{Block, BlockStore, BlockStoreError};
use crate::storage::kv_store::{KVStore, KVStoreError, WriteBatchOperation};
use crate::storage::state::AccountState;
use crate::storage::state_store::{StateStore, StateStoreError};
use crate::storage::tx_store::{TransactionRecord, TxStore, TxStoreError};

/// Error type for batch operations
#[derive(Debug, thiserror::Error)]
pub enum BatchOperationError {
    /// Block store error
    #[error("Block store error: {0}")]
    BlockStoreError(#[from] BlockStoreError),

    /// Transaction store error
    #[error("Transaction store error: {0}")]
    TxStoreError(#[from] TxStoreError),

    /// State store error
    #[error("State store error: {0}")]
    StateStoreError(#[from] StateStoreError),

    /// KV store error
    #[error("KV store error: {0}")]
    KVStoreError(#[from] KVStoreError),

    /// Other error
    #[error("Other error: {0}")]
    Other(String),
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::{AccountType, TransactionStatus};
    use tempfile::tempdir;

    #[test]
    fn commit_block_persists_account_states_under_state_store_keys() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Arc::new(RocksDBStore::new(temp_dir.path()).unwrap());
        let block_store = Arc::new(BlockStore::new(kv_store.as_ref()));
        let tx_store = Arc::new(TxStore::new(kv_store.as_ref()));
        let state_store = Arc::new(StateStore::new(kv_store.as_ref()));
        let batch_manager = BatchOperationManager::new(
            kv_store.clone(),
            block_store,
            tx_store,
            state_store.clone(),
        );

        let address = [7; 32];
        let mut account = AccountState::new_user(100, 1);
        account.assign_token_owner(address);
        account.nonce = 1;
        account.last_updated = 1;

        let block = Block {
            height: 1,
            hash: [1; 32],
            prev_hash: [0; 32],
            timestamp: 12345,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [8; 32],
            pre_reward_state_root: [0; 32],
            reward_token_ids: vec![],
            result_commitment: [0; 32],
            state_root: [0; 32],
            tx_root: crate::crypto::hash::sha256(b"empty_tx_root"),
            nonce: 42,
            poh_seq: 1,
            poh_hash: [0; 32],
            difficulty: 1,
            total_difficulty: 1,
        };

        let txs: Vec<TransactionRecord> = vec![];
        let state_changes = vec![(address, account.clone())];

        batch_manager
            .commit_block(&block, &txs, &state_changes)
            .unwrap();

        let stored_account = state_store.get_account_state(&address).unwrap();
        assert_eq!(stored_account, account);

        let stored_root = state_store.get_state_root_at_height(block.height).unwrap();
        assert!(stored_root.is_some());
        assert_eq!(
            batch_manager.block_store.get_latest_height(),
            Some(block.height)
        );
    }

    #[test]
    fn projected_root_with_reward_matches_override_root() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let state_store = StateStore::new(&kv_store);

        let sender = [1; 32];
        let recipient = [2; 32];
        let miner = [3; 32];
        let block_hash = [4; 32];

        state_store
            .create_account(&sender, 101, AccountType::User)
            .unwrap();
        let sender_state = state_store.get_account_state(&sender).unwrap();
        let tx = TransactionRecord {
            tx_id: [5; 32],
            sender,
            recipient,
            transfer_token_ids: vec![sender_state.tokens[0].token_id],
            fee_token_id: Some(sender_state.tokens[1].token_id),
            coin_transfer: crate::storage::CoinInventory::default(),
            coin_fee: crate::storage::CoinInventory::default(),
            value: 100,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 21_000,
            nonce: 0,
            timestamp: 12345,
            block_height: 1,
            data: None,
            conversion_intent: None,
            status: TransactionStatus::Pending,
        };

        let projected = state_store
            .calculate_projected_state_root_with_block_reward(
                1,
                12345,
                &[tx.clone()],
                &miner,
                &[],
                &block_hash,
            )
            .unwrap();
        let overrides = state_store
            .project_state_changes(1, &[tx], &miner, &[], Some(&block_hash))
            .unwrap();
        let recomputed = state_store
            .calculate_state_root_with_overrides(1, 12345, &overrides)
            .unwrap();

        assert_eq!(projected.root_hash, recomputed.root_hash);
    }

    #[test]
    fn reward_only_commit_root_matches_recalculated_persisted_root() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Arc::new(RocksDBStore::new(temp_dir.path()).unwrap());
        let block_store = Arc::new(BlockStore::new(kv_store.as_ref()));
        let tx_store = Arc::new(TxStore::new(kv_store.as_ref()));
        let state_store = Arc::new(StateStore::new(kv_store.as_ref()));
        let batch_manager = BatchOperationManager::new(
            kv_store.clone(),
            block_store.clone(),
            tx_store,
            state_store.clone(),
        );

        let miner = [9; 32];
        let block_hash = [4; 32];
        let timestamp = 12345;
        let reward_token_ids = crate::storage::block_store::reward_outcome(miner, 1, &block_hash)
            .into_iter()
            .map(|token| token.token_id)
            .collect::<Vec<_>>();

        let projected = state_store
            .calculate_projected_state_root_with_block_reward(
                1,
                timestamp,
                &[],
                &miner,
                &[],
                &block_hash,
            )
            .unwrap();
        let state_changes = state_store
            .project_state_changes(1, &[], &miner, &[], Some(&block_hash))
            .unwrap();

        let block = Block {
            height: 1,
            hash: block_hash,
            prev_hash: [0; 32],
            timestamp,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: state_store
                .calculate_projected_state_root(1, timestamp, &[], &miner, &[])
                .unwrap()
                .root_hash,
            reward_token_ids: reward_token_ids.clone(),
            result_commitment: crate::storage::block_store::result_commitment(
                &block_hash,
                &projected.root_hash,
                &reward_token_ids,
                &[],
            ),
            state_root: projected.root_hash,
            tx_root: crate::crypto::hash::sha256(b"empty_tx_root"),
            nonce: 1,
            poh_seq: 1,
            poh_hash: [0; 32],
            difficulty: 1,
            total_difficulty: 1,
        };

        batch_manager
            .commit_block(&block, &[], &state_changes)
            .unwrap();

        let persisted_root = state_store
            .get_state_root_at_height(block.height)
            .unwrap()
            .unwrap();
        let recalculated_root = state_store.calculate_state_root(block.height, timestamp).unwrap();

        assert_eq!(persisted_root.root_hash, block.state_root);
        assert_eq!(recalculated_root.root_hash, block.state_root);
        assert_eq!(block_store.get_latest_height(), Some(block.height));
    }

    #[test]
    fn second_reward_only_commit_stays_consistent_with_realistic_genesis_accounts() {
        let temp_dir = tempdir().unwrap();
        let kv_store = Arc::new(RocksDBStore::new(temp_dir.path()).unwrap());
        let block_store = Arc::new(BlockStore::new(kv_store.as_ref()));
        let tx_store = Arc::new(TxStore::new(kv_store.as_ref()));
        let state_store = Arc::new(StateStore::new(kv_store.as_ref()));
        let batch_manager = BatchOperationManager::new(
            kv_store.clone(),
            block_store.clone(),
            tx_store,
            state_store.clone(),
        );

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
        block_store.put_block(&genesis_block).unwrap();

        let miner = [9; 32];
        let first_hash = [4; 32];
        let second_hash = [5; 32];
        let first_timestamp = genesis_block.timestamp + 5;
        let second_timestamp = first_timestamp + 5;

        let first_projected = state_store
            .calculate_projected_state_root_with_block_reward(
                1,
                first_timestamp,
                &[],
                &miner,
                &[],
                &first_hash,
            )
            .unwrap();
        let first_state_changes = state_store
            .project_state_changes(1, &[], &miner, &[], Some(&first_hash))
            .unwrap();
        let first_state_changes_again = state_store
            .project_state_changes(1, &[], &miner, &[], Some(&first_hash))
            .unwrap();
        let first_recomputed_again = state_store
            .calculate_state_root_with_overrides(1, first_timestamp, &first_state_changes_again)
            .unwrap();
        let first_recomputed = state_store
            .calculate_state_root_with_overrides(1, first_timestamp, &first_state_changes)
            .unwrap();
        assert_eq!(
            first_recomputed.root_hash,
            first_recomputed_again.root_hash,
            "equivalent first-block projections should yield the same canonical root"
        );
        assert_eq!(
            first_recomputed.root_hash,
            first_projected.root_hash,
            "direct override root should match projected root before first commit"
        );
        let first_reward_token_ids =
            crate::storage::block_store::reward_outcome(miner, 1, &first_hash)
                .into_iter()
                .map(|token| token.token_id)
                .collect::<Vec<_>>();
        let first_block = Block {
            height: 1,
            hash: first_hash,
            prev_hash: genesis_block.hash,
            timestamp: first_timestamp,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: state_store
                .calculate_projected_state_root(1, first_timestamp, &[], &miner, &[])
                .unwrap()
                .root_hash,
            reward_token_ids: first_reward_token_ids.clone(),
            result_commitment: crate::storage::block_store::result_commitment(
                &first_hash,
                &first_projected.root_hash,
                &first_reward_token_ids,
                &[],
            ),
            state_root: first_projected.root_hash,
            tx_root: crate::crypto::hash::sha256(b"empty_tx_root"),
            nonce: 1,
            poh_seq: 1,
            poh_hash: [0; 32],
            difficulty: 100,
            total_difficulty: 200,
        };
        batch_manager
            .commit_block(&first_block, &[], &first_state_changes)
            .unwrap();
        assert_eq!(
            state_store
                .get_state_root_at_height(first_block.height)
                .unwrap()
                .unwrap()
                .root_hash,
            first_block.state_root
        );

        let second_projected = state_store
            .calculate_projected_state_root_with_block_reward(
                2,
                second_timestamp,
                &[],
                &miner,
                &[],
                &second_hash,
            )
            .unwrap();
        let second_state_changes = state_store
            .project_state_changes(2, &[], &miner, &[], Some(&second_hash))
            .unwrap();
        let second_reward_token_ids =
            crate::storage::block_store::reward_outcome(miner, 2, &second_hash)
                .into_iter()
                .map(|token| token.token_id)
                .collect::<Vec<_>>();
        let second_block = Block {
            height: 2,
            hash: second_hash,
            prev_hash: first_block.hash,
            timestamp: second_timestamp,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: state_store
                .calculate_projected_state_root(2, second_timestamp, &[], &miner, &[])
                .unwrap()
                .root_hash,
            reward_token_ids: second_reward_token_ids.clone(),
            result_commitment: crate::storage::block_store::result_commitment(
                &second_hash,
                &second_projected.root_hash,
                &second_reward_token_ids,
                &[],
            ),
            state_root: second_projected.root_hash,
            tx_root: crate::crypto::hash::sha256(b"empty_tx_root"),
            nonce: 2,
            poh_seq: 2,
            poh_hash: [0; 32],
            difficulty: 100,
            total_difficulty: 300,
        };
        batch_manager
            .commit_block(&second_block, &[], &second_state_changes)
            .unwrap();

        let second_persisted_root = state_store
            .get_state_root_at_height(second_block.height)
            .unwrap()
            .unwrap();
        let second_recalculated_root = state_store
            .calculate_state_root(second_block.height, second_timestamp)
            .unwrap();

        assert_eq!(
            second_persisted_root.root_hash,
            second_block.state_root,
            "persisted root should match second block root"
        );
        assert_eq!(
            second_recalculated_root.root_hash,
            second_block.state_root,
            "recalculated root should match second block root"
        );
    }
}

/// Batch operations manager for atomic updates
pub struct BatchOperationManager<'a> {
    /// KV store
    store: Arc<dyn KVStore + 'a>,
    /// Block store
    block_store: Arc<BlockStore<'a>>,
    /// Transaction store
    tx_store: Arc<TxStore<'a>>,
    /// State store
    state_store: Arc<StateStore<'a>>,
}

impl<'a> BatchOperationManager<'a> {
    /// Create a new batch operation manager
    pub fn new(
        store: Arc<dyn KVStore + 'a>,
        block_store: Arc<BlockStore<'a>>,
        tx_store: Arc<TxStore<'a>>,
        state_store: Arc<StateStore<'a>>,
    ) -> Self {
        Self {
            store,
            block_store,
            tx_store,
            state_store,
        }
    }

    /// Commit a block with all its transactions and state changes atomically
    pub fn commit_block(
        &self,
        block: &Block,
        transactions: &[TransactionRecord],
        state_changes: &[(Hash, AccountState)],
    ) -> Result<(), BatchOperationError> {
        // Create a batch operation
        let mut batch = Vec::new();

        // Add block to batch
        let block_key = format!("block:{}", block.height);
        let block_value = bincode::serialize(block)
            .map_err(|e| BatchOperationError::Other(format!("Failed to serialize block: {}", e)))?;

        batch.push(WriteBatchOperation::Put {
            key: block_key.as_bytes().to_vec(),
            value: block_value,
        });

        // Add block hash index
        let hash_key = format!("block_hash:{}", hex::encode(&block.hash));
        batch.push(WriteBatchOperation::Put {
            key: hash_key.as_bytes().to_vec(),
            value: block.height.to_be_bytes().to_vec(),
        });

        // Update latest block height metadata
        batch.push(WriteBatchOperation::Put {
            key: b"meta:latest_block_height".to_vec(),
            value: block.height.to_be_bytes().to_vec(),
        });

        // Add transactions to batch
        for tx in transactions {
            // Primary index by transaction hash
            let tx_key = format!("tx:{}", hex::encode(&tx.tx_id));
            let tx_value = bincode::serialize(tx).map_err(|e| {
                BatchOperationError::Other(format!("Failed to serialize transaction: {}", e))
            })?;

            batch.push(WriteBatchOperation::Put {
                key: tx_key.as_bytes().to_vec(),
                value: tx_value,
            });

            // Secondary index: transactions by block
            let block_tx_key = format!("tx_block:{}:{}", tx.block_height, hex::encode(&tx.tx_id));
            batch.push(WriteBatchOperation::Put {
                key: block_tx_key.as_bytes().to_vec(),
                value: tx.tx_id.to_vec(),
            });

            // Secondary index: transactions by sender
            let sender_tx_key = format!(
                "tx_sender:{}:{}",
                hex::encode(&tx.sender),
                hex::encode(&tx.tx_id)
            );
            batch.push(WriteBatchOperation::Put {
                key: sender_tx_key.as_bytes().to_vec(),
                value: tx.tx_id.to_vec(),
            });

            // Secondary index: transactions by recipient
            let recipient_tx_key = format!(
                "tx_recipient:{}:{}",
                hex::encode(&tx.recipient),
                hex::encode(&tx.tx_id)
            );
            batch.push(WriteBatchOperation::Put {
                key: recipient_tx_key.as_bytes().to_vec(),
                value: tx.tx_id.to_vec(),
            });

            // Secondary index: latest nonce for sender
            let sender_nonce_key = format!("tx_sender_nonce:{}", hex::encode(&tx.sender));

            // Only update if this nonce is higher than any previously stored
            match self.store.get(sender_nonce_key.as_bytes()) {
                Ok(Some(bytes)) => {
                    if bytes.len() == 8 {
                        let mut nonce_arr = [0u8; 8];
                        nonce_arr.copy_from_slice(&bytes);
                        let stored_nonce = u64::from_be_bytes(nonce_arr);

                        if tx.nonce > stored_nonce {
                            batch.push(WriteBatchOperation::Put {
                                key: sender_nonce_key.as_bytes().to_vec(),
                                value: tx.nonce.to_be_bytes().to_vec(),
                            });
                        }
                    } else {
                        // Invalid format, overwrite
                        batch.push(WriteBatchOperation::Put {
                            key: sender_nonce_key.as_bytes().to_vec(),
                            value: tx.nonce.to_be_bytes().to_vec(),
                        });
                    }
                }
                _ => {
                    // No existing nonce, store this one
                    batch.push(WriteBatchOperation::Put {
                        key: sender_nonce_key.as_bytes().to_vec(),
                        value: tx.nonce.to_be_bytes().to_vec(),
                    });
                }
            }
        }

        // Add state changes to batch
        for (address, state) in state_changes {
            let normalized_state =
                StateStore::normalize_account_state(state.clone(), Some(*address));
            let state_key = format!("state:account:{}", hex::encode(address));
            let state_value = bincode::serialize(&normalized_state).map_err(|e| {
                BatchOperationError::Other(format!("Failed to serialize state: {}", e))
            })?;

            batch.push(WriteBatchOperation::Put {
                key: state_key.as_bytes().to_vec(),
                value: state_value,
            });
        }

        // Calculate and store state root
        let state_root = self
            .state_store
            .calculate_state_root_with_overrides(block.height, block.timestamp, state_changes)
            .map_err(|e| BatchOperationError::StateStoreError(e))?;

        let state_root_key = format!("state_root:{}", block.height);
        let state_root_value = bincode::serialize(&state_root).map_err(|e| {
            BatchOperationError::Other(format!("Failed to serialize state root: {}", e))
        })?;

        batch.push(WriteBatchOperation::Put {
            key: state_root_key.as_bytes().to_vec(),
            value: state_root_value,
        });

        // Execute the batch
        self.store
            .write_batch(batch)
            .map_err(|e| BatchOperationError::KVStoreError(e))?;

        self.block_store.note_committed_height(block.height);
        self.state_store.clear_cache();
        self.state_store.set_state_root(state_root);

        info!(
            "Committed block {} with {} transactions and {} state changes",
            block.height,
            transactions.len(),
            state_changes.len()
        );

        Ok(())
    }

    /// Rollback a block and all its effects
    pub fn rollback_block(&self, block_height: u64) -> Result<(), BatchOperationError> {
        // Get the block
        let block = match self.block_store.get_block_by_height(block_height)? {
            Some(block) => block,
            None => {
                return Err(BatchOperationError::Other(format!(
                    "Block not found: {}",
                    block_height
                )))
            }
        };

        // Get all transactions in the block
        let transactions = self.tx_store.get_transactions_by_block(block_height)?;

        // Create a batch operation
        let mut batch = Vec::new();

        // Remove block
        let block_key = format!("block:{}", block.height);
        batch.push(WriteBatchOperation::Delete {
            key: block_key.as_bytes().to_vec(),
        });

        // Remove block hash index
        let hash_key = format!("block_hash:{}", hex::encode(&block.hash));
        batch.push(WriteBatchOperation::Delete {
            key: hash_key.as_bytes().to_vec(),
        });

        // Update latest block height metadata to previous block
        if block_height > 0 {
            batch.push(WriteBatchOperation::Put {
                key: b"meta:latest_block_height".to_vec(),
                value: (block_height - 1).to_be_bytes().to_vec(),
            });
        } else {
            batch.push(WriteBatchOperation::Delete {
                key: b"meta:latest_block_height".to_vec(),
            });
        }

        // Remove transactions
        for tx in &transactions {
            // Remove primary index
            let tx_key = format!("tx:{}", hex::encode(&tx.tx_id));
            batch.push(WriteBatchOperation::Delete {
                key: tx_key.as_bytes().to_vec(),
            });

            // Remove block index
            let block_tx_key = format!("tx_block:{}:{}", tx.block_height, hex::encode(&tx.tx_id));
            batch.push(WriteBatchOperation::Delete {
                key: block_tx_key.as_bytes().to_vec(),
            });

            // Remove sender index
            let sender_tx_key = format!(
                "tx_sender:{}:{}",
                hex::encode(&tx.sender),
                hex::encode(&tx.tx_id)
            );
            batch.push(WriteBatchOperation::Delete {
                key: sender_tx_key.as_bytes().to_vec(),
            });

            // Remove recipient index
            let recipient_tx_key = format!(
                "tx_recipient:{}:{}",
                hex::encode(&tx.recipient),
                hex::encode(&tx.tx_id)
            );
            batch.push(WriteBatchOperation::Delete {
                key: recipient_tx_key.as_bytes().to_vec(),
            });

            // Note: We don't remove the sender nonce as it would be complex to determine the previous value
        }

        // Remove state root
        let state_root_key = format!("state_root:{}", block.height);
        batch.push(WriteBatchOperation::Delete {
            key: state_root_key.as_bytes().to_vec(),
        });

        // Execute the batch
        self.store
            .write_batch(batch)
            .map_err(|e| BatchOperationError::KVStoreError(e))?;

        self.block_store
            .note_rolled_back_height(if block_height > 0 {
                Some(block_height - 1)
            } else {
                None
            });

        info!(
            "Rolled back block {} with {} transactions",
            block_height,
            transactions.len()
        );

        Ok(())
    }
}

#[cfg(all(test, feature = "legacy-test-compat"))]
mod tests {
    use super::*;
    use crate::storage::kv_store::RocksDBStore;
    use tempfile::tempdir;

    #[test]
    fn test_commit_and_rollback_block() {
        // Create a temporary directory for the database
        let temp_dir = tempdir().unwrap();
        let kv_store = Arc::new(RocksDBStore::new(temp_dir.path()).unwrap());

        // Create stores
        let block_store = Arc::new(BlockStore::new(&kv_store));
        let tx_store = Arc::new(TxStore::new(kv_store.as_ref()));
        let state_store = Arc::new(StateStore::new(kv_store.as_ref()));

        // Create batch operation manager
        let batch_manager = BatchOperationManager::new(
            kv_store.clone(),
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
        );

        // Create a test block
        let block = Block {
            height: 1,
            hash: [1; 32],
            prev_hash: [0; 32],
            timestamp: 12345,
            transactions: vec![[2; 32], [3; 32]],
            miner: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [4; 32],
            tx_root: [5; 32],
            nonce: 42,
            poh_seq: 100,
            poh_hash: [6; 32],
            difficulty: 1000,
            total_difficulty: 1000,
        };

        // Create test transactions
        let tx1 = TransactionRecord {
            tx_id: [2; 32],
            sender: [10; 32],
            recipient: [11; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            value: 100,
            gas_price: 1,
            gas_limit: 21000,
            gas_used: 21000,
            nonce: 0,
            timestamp: 12345,
            block_height: 1,
            data: None,
            status: crate::storage::tx_store::TransactionStatus::Confirmed,
        };

        let tx2 = TransactionRecord {
            tx_id: [3; 32],
            sender: [12; 32],
            recipient: [13; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            value: 200,
            gas_price: 1,
            gas_limit: 21000,
            gas_used: 21000,
            nonce: 0,
            timestamp: 12345,
            block_height: 1,
            data: None,
            status: crate::storage::tx_store::TransactionStatus::Confirmed,
        };

        // Create test state changes
        let state1 = AccountState {
            balance: 900,
            nonce: 1,
            account_type: crate::storage::AccountType::User,
        };

        let state2 = AccountState {
            balance: 200,
            nonce: 0,
            account_type: crate::storage::AccountType::User,
        };

        let state_changes = vec![([10; 32], state1), ([11; 32], state2)];

        // Commit the block
        batch_manager
            .commit_block(&block, &[tx1, tx2], &state_changes)
            .unwrap();

        // Verify the block was stored
        let stored_block = block_store.get_block_by_height(1).unwrap().unwrap();
        assert_eq!(stored_block.hash, block.hash);

        // Verify transactions were stored
        let stored_tx1 = tx_store.get_transaction(&[2; 32]).unwrap().unwrap();
        assert_eq!(stored_tx1.value, 100);

        let stored_tx2 = tx_store.get_transaction(&[3; 32]).unwrap().unwrap();
        assert_eq!(stored_tx2.value, 200);

        // Verify state changes were stored
        let stored_state1 = state_store.get_account_state(&[10; 32]).unwrap().unwrap();
        assert_eq!(stored_state1.balance, 900);

        let stored_state2 = state_store.get_account_state(&[11; 32]).unwrap().unwrap();
        assert_eq!(stored_state2.balance, 200);

        // Rollback the block
        batch_manager.rollback_block(1).unwrap();

        // Verify the block was removed
        assert!(block_store.get_block_by_height(1).unwrap().is_none());

        // Verify transactions were removed
        assert!(tx_store.get_transaction(&[2; 32]).unwrap().is_none());
        assert!(tx_store.get_transaction(&[3; 32]).unwrap().is_none());

        // Note: State changes are not automatically rolled back as that would require
        // knowing the previous state. In a real implementation, you would need to
        // store and restore the previous state.
    }
}

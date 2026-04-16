use log::{error, warn};
use sha2::Digest;
use std::sync::Arc;

use crate::consensus::poh::verifier::PoHVerifier;
use crate::consensus::types::Target;
use crate::crypto::hash::{sha256, Hash};
use crate::storage::block_store::{pow_hash, result_commitment, reward_outcome, Block, BlockStore};
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::TxStore;
use crate::storage::TransactionRecord;

/// Result of block validation
#[derive(Debug, PartialEq)]
pub enum BlockValidationResult {
    /// Block is valid
    Valid,

    /// Block is invalid
    Invalid(String),

    /// Block is already known
    AlreadyKnown,

    /// Block's parent is unknown
    UnknownParent,
}

/// Validator for blocks
pub struct BlockValidator<'a> {
    /// Block store
    block_store: Arc<BlockStore<'a>>,

    /// Transaction store
    tx_store: Arc<TxStore<'a>>,

    /// State store
    state_store: Arc<StateStore<'a>>,

    /// PoH verifier
    poh_verifier: PoHVerifier,

    /// Expected PoH tick rate used for warning thresholds.
    poh_tick_rate: u64,

    /// Block height where hybrid cash rules activate.
    hybrid_activation_height: u64,
}

impl<'a> BlockValidator<'a> {
    /// Create a new block validator
    pub fn new(
        block_store: Arc<BlockStore<'a>>,
        tx_store: Arc<TxStore<'a>>,
        state_store: Arc<StateStore<'a>>,
        poh_tick_rate: u64,
        hybrid_activation_height: u64,
    ) -> Self {
        Self {
            block_store,
            tx_store,
            state_store,
            poh_verifier: PoHVerifier::new(),
            poh_tick_rate,
            hybrid_activation_height,
        }
    }

    fn hybrid_active_at(&self, block_height: u64) -> bool {
        block_height >= self.hybrid_activation_height
    }

    fn transaction_uses_hybrid_features(tx: &TransactionRecord) -> bool {
        !tx.coin_transfer.is_empty()
            || !tx.coin_fee.is_empty()
            || tx.conversion_intent.is_some()
    }

    /// Validate a block
    pub fn validate_block(&self, block: &Block, target: &Target) -> BlockValidationResult {
        // Check if the block is already known
        if let Ok(Some(_)) = self.block_store.get_block_by_hash(&block.hash) {
            return BlockValidationResult::AlreadyKnown;
        }

        // Check if the parent block exists
        if block.height > 0 {
            if let Ok(None) = self.block_store.get_block_by_hash(&block.prev_hash) {
                return BlockValidationResult::UnknownParent;
            }
        }

        // Validate transactions
        if !self.validate_transactions(block) {
            return BlockValidationResult::Invalid("Invalid transactions".to_string());
        }

        // Validate block hash
        if !self.validate_block_hash(block) {
            return BlockValidationResult::Invalid("Invalid block hash".to_string());
        }

        if !self.validate_reward_token_ids(block) {
            return BlockValidationResult::Invalid("Invalid reward token ids".to_string());
        }

        if !self.validate_result_commitment(block) {
            return BlockValidationResult::Invalid("Invalid result commitment".to_string());
        }

        // Validate PoW
        if !self.validate_pow(block, target) {
            return BlockValidationResult::Invalid("Invalid proof of work".to_string());
        }

        // Validate state root
        if !self.validate_state_root(block) {
            return BlockValidationResult::Invalid("Invalid state root".to_string());
        }

        // Validate Proof of History
        if !self.validate_poh(block) {
            return BlockValidationResult::Invalid("Invalid Proof of History".to_string());
        }

        // All checks passed
        BlockValidationResult::Valid
    }

    fn extends_committed_tip(&self, block: &Block) -> bool {
        if block.height == 0 {
            return true;
        }

        match self.block_store.get_latest_block() {
            Ok(Some(latest_block)) => {
                latest_block.hash == block.prev_hash && latest_block.height + 1 == block.height
            }
            Ok(None) => block.height == 0,
            Err(err) => {
                warn!(
                    "Failed to load committed tip while validating block {}: {}",
                    block.height, err
                );
                false
            }
        }
    }

    /// Validate the block hash
    fn validate_block_hash(&self, block: &Block) -> bool {
        if block.height == 0 {
            return true;
        }

        let header_hash = pow_hash(&block.canonical_header());
        if header_hash != block.hash {
            error!(
                "Block hash mismatch at height {}: expected {}, got {}",
                block.height,
                hex::encode(header_hash),
                hex::encode(block.hash),
            );
            return false;
        }

        if !self.extends_committed_tip(block) {
            warn!(
                "Skipping pre-reward state root recomputation for block {} because it does not extend the committed tip",
                block.height
            );
            return true;
        }

        let transactions = match self.load_block_transactions(block) {
            Ok(transactions) => transactions,
            Err(err) => {
                error!(
                    "Failed to load block transactions for hash validation: {}",
                    err
                );
                return false;
            }
        };

        let pre_reward_state_root = match self.state_store.calculate_projected_state_root(
            block.height,
            block.timestamp,
            &transactions,
            &block.miner,
            &block.conversion_fulfillment_order_ids,
        ) {
            Ok(root) => root.root_hash,
            Err(err) => {
                error!(
                    "Failed to calculate pre-reward state root for block {}: {}",
                    block.height, err
                );
                return false;
            }
        };

        if pre_reward_state_root != block.pre_reward_state_root {
            error!(
                "Pre-reward state root mismatch at height {}: expected {}, got {}",
                block.height,
                hex::encode(pre_reward_state_root),
                hex::encode(block.pre_reward_state_root),
            );
            return false;
        }

        true
    }

    /// Validate the proof of work
    fn validate_pow(&self, block: &Block, target: &Target) -> bool {
        // For genesis block or the first block after genesis, we accept any PoW
        if block.height == 0 || block.height == 1 {
            return true;
        }

        // Check if the block hash meets the target
        target.is_met_by(&block.hash)
    }

    fn validate_reward_token_ids(&self, block: &Block) -> bool {
        if block.height == 0 {
            return true;
        }

        let expected_reward_token_ids = reward_outcome(block.miner, block.height, &block.hash)
            .into_iter()
            .map(|token| token.token_id)
            .collect::<Vec<_>>();

        if block.reward_token_ids != expected_reward_token_ids {
            error!(
                "Reward token id mismatch at height {}: expected {} ids, got {}",
                block.height,
                expected_reward_token_ids.len(),
                block.reward_token_ids.len(),
            );
            return false;
        }

        true
    }

    fn validate_result_commitment(&self, block: &Block) -> bool {
        let expected_commitment =
            result_commitment(
                &block.hash,
                &block.state_root,
                &block.reward_token_ids,
                &block.conversion_fulfillment_order_ids,
            );

        if block.result_commitment != expected_commitment {
            error!(
                "Result commitment mismatch at height {}: expected {}, got {}",
                block.height,
                hex::encode(expected_commitment),
                hex::encode(block.result_commitment),
            );
            return false;
        }

        true
    }

    /// Validate the transactions in the block
    fn validate_transactions(&self, block: &Block) -> bool {
        // Check that all transactions exist and are valid
        let transactions = match self.load_block_transactions(block) {
            Ok(transactions) => transactions,
            Err(err) => {
                error!("{}", err);
                return false;
            }
        };

        // Validate the transaction root
        // Convert [u8; 32] array to Hash type
        let tx_hashes: Vec<Hash> = transactions.iter().map(|tx| Hash::new(tx.tx_id)).collect();
        let calculated_tx_root = self.calculate_tx_root(&tx_hashes);
        if calculated_tx_root != Hash::new(block.tx_root) {
            error!(
                "Transaction root mismatch: expected {}, calculated {}",
                hex::encode(&block.tx_root),
                hex::encode(&calculated_tx_root)
            );
            return false;
        }

        if !self.hybrid_active_at(block.height) {
            if !block.conversion_fulfillment_order_ids.is_empty()
                || transactions
                    .iter()
                    .any(Self::transaction_uses_hybrid_features)
            {
                error!(
                    "Hybrid transaction features are not active at height {}",
                    block.height
                );
                return false;
            }
            return true;
        }

        if let Err(err) = self.state_store.validate_conversion_fulfillment_order_ids(
            block.height,
            &transactions,
            &block.miner,
            &block.conversion_fulfillment_order_ids,
        ) {
            error!(
                "Invalid conversion fulfillment list at height {}: {}",
                block.height, err
            );
            return false;
        }

        true
    }

    fn load_block_transactions(&self, block: &Block) -> Result<Vec<TransactionRecord>, String> {
        let mut transactions = Vec::with_capacity(block.transactions.len());

        for tx_hash in &block.transactions {
            match self.tx_store.get_transaction(tx_hash) {
                Ok(Some(tx)) => {
                    if tx.block_height != block.height {
                        return Err(format!(
                            "Transaction {} has incorrect block height: expected {}, got {}",
                            hex::encode(tx_hash),
                            block.height,
                            tx.block_height,
                        ));
                    }
                    transactions.push(tx);
                }
                Ok(None) => {
                    return Err(format!(
                        "Transaction not found in transaction store: {}",
                        hex::encode(tx_hash)
                    ));
                }
                Err(err) => {
                    return Err(format!(
                        "Failed to load transaction {}: {}",
                        hex::encode(tx_hash),
                        err
                    ));
                }
            }
        }

        Ok(transactions)
    }

    /// Calculate the transaction root (Merkle root of transactions)
    fn calculate_tx_root(&self, tx_hashes: &[Hash]) -> Hash {
        if tx_hashes.is_empty() {
            // Empty transaction list has a special hash
            return Hash::new(sha256(b"empty_tx_root"));
        }

        // Create leaf nodes from transaction hashes
        let mut nodes: Vec<Hash> = tx_hashes.to_vec();

        // Build the Merkle tree bottom-up
        while nodes.len() > 1 {
            let mut next_level = Vec::new();

            // Process pairs of nodes
            for chunk in nodes.chunks(2) {
                let mut hasher = sha2::Sha256::new();

                // Add the first hash
                hasher.update(&chunk[0]);

                // Add the second hash if it exists, otherwise duplicate the first
                if chunk.len() > 1 {
                    hasher.update(&chunk[1]);
                } else {
                    hasher.update(&chunk[0]); // Duplicate the node if we have an odd number
                }

                // Create the parent node
                let result = hasher.finalize();
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&result);
                next_level.push(Hash::new(hash));
            }

            // Move to the next level
            nodes = next_level;
        }

        // The root is the only remaining node
        nodes[0]
    }

    /// Validate the state root
    fn validate_state_root(&self, block: &Block) -> bool {
        // For genesis block, we accept any state root
        if block.height == 0 {
            return true;
        }

        if !self.extends_committed_tip(block) {
            warn!(
                "Skipping final state root recomputation for block {} because it does not extend the committed tip",
                block.height
            );
            return true;
        }

        // Get the previous block to start from its state
        let _prev_block = match self.block_store.get_block_by_hash(&block.prev_hash) {
            Ok(Some(b)) => b,
            Ok(None) => {
                error!("Previous block {} not found", hex::encode(&block.prev_hash));
                return false;
            }
            Err(e) => {
                error!("Error retrieving previous block: {}", e);
                return false;
            }
        };

        let transactions = match self.load_block_transactions(block) {
            Ok(transactions) => transactions,
            Err(err) => {
                error!("Failed to load block transactions for state root validation: {}", err);
                return false;
            }
        };

        let calculated_state_root = match self
            .state_store
            .calculate_projected_state_root_with_block_reward(
                block.height,
                block.timestamp,
                &transactions,
                &block.miner,
                &block.conversion_fulfillment_order_ids,
                &block.hash,
            ) {
            Ok(root) => root.root_hash,
            Err(e) => {
                error!("Failed to calculate state root: {}", e);
                return false;
            }
        };

        // Compare the calculated state root with the one in the block
        if calculated_state_root != block.state_root {
            error!(
                "State root mismatch: expected {}, calculated {}",
                hex::encode(&block.state_root),
                hex::encode(&calculated_state_root)
            );
            return false;
        }

        true
    }

    /// Validate the Proof of History
    fn validate_poh(&self, block: &Block) -> bool {
        // For genesis block or the first block after genesis, we accept any PoH
        if block.height == 0 || block.height == 1 {
            return true;
        }

        // Get the previous block
        let prev_block = match self.block_store.get_block_by_hash(&block.prev_hash) {
            Ok(Some(b)) => b,
            Ok(None) => {
                error!("Previous block {} not found", hex::encode(&block.prev_hash));
                return false;
            }
            Err(e) => {
                error!("Error retrieving previous block: {}", e);
                return false;
            }
        };

        // Calculate the expected PoH hash
        let seq_diff = block.poh_seq - prev_block.poh_seq;
        let event_data = seq_diff.to_be_bytes();
        let combined = [&prev_block.poh_hash[..], &event_data[..]].concat();
        let expected_poh_hash = crate::crypto::hash::sha256(&combined);

        // Compare with the block's PoH hash
        if block.poh_hash != expected_poh_hash {
            error!(
                "PoH hash mismatch: expected {}, got {}",
                hex::encode(&expected_poh_hash),
                hex::encode(&block.poh_hash)
            );
            return false;
        }

        // Create a PoH entry for verification (for additional checks)
        let poh_entry = crate::storage::poh_store::PoHEntry {
            hash: block.poh_hash,
            sequence: block.poh_seq,
            timestamp: block.timestamp,
        };

        // Verify the PoH sequence using the verifier
        if !self
            .poh_verifier
            .verify_event(&poh_entry, &prev_block.poh_hash, &event_data)
        {
            error!(
                "Invalid PoH sequence: prev_seq={}, curr_seq={}",
                prev_block.poh_seq, block.poh_seq
            );
            return false;
        }

        // Verify that the PoH sequence number is increasing
        if block.poh_seq <= prev_block.poh_seq {
            error!(
                "PoH sequence not increasing: prev_seq={}, curr_seq={}",
                prev_block.poh_seq, block.poh_seq
            );
            return false;
        }

        // Verify that the PoH sequence is within reasonable bounds
        let expected_ticks = (block.timestamp - prev_block.timestamp) * self.poh_tick_rate;
        let actual_ticks = block.poh_seq - prev_block.poh_seq;

        // Allow for some variance (±20%)
        let min_ticks = expected_ticks * 8 / 10;
        let max_ticks = expected_ticks * 12 / 10;

        if actual_ticks < min_ticks || actual_ticks > max_ticks {
            warn!(
                "PoH sequence count unusual: expected ~{}, got {}",
                expected_ticks, actual_ticks
            );
            // This is just a warning, not a validation failure
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_store::{
        pow_hash, result_commitment, reward_outcome, CanonicalBlockHeader,
    };
    use crate::storage::Denomination;
    use crate::storage::kv_store::RocksDBStore;
    use tempfile::tempdir;

    fn setup_validator() -> (
        Arc<BlockStore<'static>>,
        Arc<TxStore<'static>>,
        Arc<StateStore<'static>>,
        BlockValidator<'static>,
    ) {
        let temp_dir = tempdir().unwrap();
        let kv_store = Box::leak(Box::new(RocksDBStore::new(temp_dir.path()).unwrap()));
        let block_store = Arc::new(BlockStore::new(kv_store));
        let tx_store = Arc::new(TxStore::new(kv_store));
        let state_store = Arc::new(StateStore::new(kv_store));
        let validator =
            BlockValidator::new(
                block_store.clone(),
                tx_store.clone(),
                state_store.clone(),
                100,
                0,
            );
        std::mem::forget(temp_dir);
        (block_store, tx_store, state_store, validator)
    }

    fn genesis_block() -> Block {
        let empty_tx_root = sha256(b"empty_tx_root");
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
            tx_root: empty_tx_root,
            poh_hash: [0u8; 32],
            poh_seq: 0,
            nonce: 0,
            difficulty: 1,
            total_difficulty: 1,
        }
    }

    fn build_block(
        state_store: &StateStore<'static>,
        tx_hashes: Vec<[u8; 32]>,
        transactions: &[TransactionRecord],
        prev_hash: [u8; 32],
        height: u64,
        timestamp: u64,
        miner: [u8; 32],
    ) -> Block {
        let tx_root = if tx_hashes.is_empty() {
            sha256(b"empty_tx_root")
        } else {
            let mut nodes: Vec<Hash> = tx_hashes.iter().copied().map(Hash::new).collect();
            while nodes.len() > 1 {
                let mut next_level = Vec::new();
                for chunk in nodes.chunks(2) {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(chunk[0].as_bytes());
                    hasher.update(chunk.get(1).unwrap_or(&chunk[0]).as_bytes());
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&hasher.finalize());
                    next_level.push(Hash::new(hash));
                }
                nodes = next_level;
            }
            *nodes[0].as_bytes()
        };

        let pre_reward_state_root = state_store
            .calculate_projected_state_root(height, timestamp, transactions, &miner, &[])
            .unwrap()
            .root_hash;
        let header = CanonicalBlockHeader {
            height,
            prev_hash,
            timestamp,
            miner,
            pre_reward_state_root,
            tx_root,
            nonce: 42,
            poh_seq: timestamp,
            poh_hash: [0u8; 32],
            difficulty: 1,
            total_difficulty: 2,
        };
        let hash = pow_hash(&header);
        let reward_token_ids = reward_outcome(miner, height, &hash)
            .into_iter()
            .map(|token| token.token_id)
            .collect::<Vec<_>>();
        let state_root = state_store
            .calculate_projected_state_root_with_block_reward(
                height,
                timestamp,
                transactions,
                &miner,
                &[],
                &hash,
            )
            .unwrap()
            .root_hash;

        Block {
            height,
            hash,
            prev_hash,
            timestamp,
            transactions: tx_hashes,
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root,
            reward_token_ids: reward_token_ids.clone(),
            result_commitment: result_commitment(&hash, &state_root, &reward_token_ids, &[]),
            state_root,
            tx_root,
            poh_hash: crate::crypto::hash::sha256(&(timestamp.to_be_bytes())),
            poh_seq: timestamp,
            nonce: 42,
            difficulty: 1,
            total_difficulty: 2,
        }
    }

    #[test]
    fn test_block_validation_and_unknown_parent() {
        let (block_store, _tx_store, state_store, validator) = setup_validator();
        let genesis = genesis_block();
        let target = Target::from_difficulty(1);
        assert_eq!(
            validator.validate_block(&genesis, &target),
            BlockValidationResult::Valid
        );

        block_store.put_block(&genesis).unwrap();
        let valid_block = build_block(&state_store, vec![], &[], genesis.hash, 1, 10, [7u8; 32]);
        block_store.put_block(&valid_block).unwrap();
        assert_eq!(
            validator.validate_block(&valid_block, &target),
            BlockValidationResult::AlreadyKnown
        );

        let orphan_block = build_block(&state_store, vec![], &[], [99u8; 32], 2, 20, [8u8; 32]);
        assert_eq!(
            validator.validate_block(&orphan_block, &target),
            BlockValidationResult::UnknownParent
        );
    }

    #[test]
    fn test_rejects_tampered_reward_claim() {
        let (block_store, _tx_store, state_store, validator) = setup_validator();
        let genesis = genesis_block();
        block_store.put_block(&genesis).unwrap();

        let mut block = build_block(&state_store, vec![], &[], genesis.hash, 1, 10, [9u8; 32]);
        block.reward_token_ids.push([77u8; 32]);
        block.result_commitment =
            result_commitment(
                &block.hash,
                &block.state_root,
                &block.reward_token_ids,
                &block.conversion_fulfillment_order_ids,
            );

        let target = Target::from_difficulty(1);
        assert!(matches!(
            validator.validate_block(&block, &target),
            BlockValidationResult::Invalid(_)
        ));
    }

    #[test]
    fn test_rejects_tampered_result_commitment() {
        let (block_store, _tx_store, state_store, validator) = setup_validator();
        let genesis = genesis_block();
        block_store.put_block(&genesis).unwrap();

        let mut block = build_block(&state_store, vec![], &[], genesis.hash, 1, 10, [14u8; 32]);
        block.result_commitment = [255u8; 32];

        let target = Target::from_difficulty(1);
        assert!(matches!(
            validator.validate_block(&block, &target),
            BlockValidationResult::Invalid(_)
        ));
    }

    #[test]
    fn test_accepts_replayed_height_one_block_after_tip_advances() {
        let (block_store, tx_store, state_store, validator) = setup_validator();
        let genesis = genesis_block();
        block_store.put_block(&genesis).unwrap();

        let block_one = build_block(&state_store, vec![], &[], genesis.hash, 1, 10, [21u8; 32]);
        block_store.put_block(&block_one).unwrap();
        state_store.apply_block(&block_one, &tx_store).unwrap();

        let competing_block_one =
            build_block(&state_store, vec![], &[], genesis.hash, 1, 11, [22u8; 32]);

        let target = Target::from_difficulty(1);
        assert_eq!(
            validator.validate_block(&competing_block_one, &target),
            BlockValidationResult::Valid
        );
    }

    #[test]
    fn test_rejects_hybrid_features_before_activation_height() {
        let (block_store, tx_store, state_store, _validator) = setup_validator();
        let validator = BlockValidator::new(
            block_store.clone(),
            tx_store.clone(),
            state_store.clone(),
            100,
            10,
        );
        let genesis = genesis_block();
        block_store.put_block(&genesis).unwrap();

        let mut coin_transfer = crate::storage::CoinInventory::default();
        coin_transfer.add(Denomination::Cents25, 1).unwrap();

        let tx = TransactionRecord {
            tx_id: [44; 32],
            sender: [1; 32],
            recipient: [2; 32],
            transfer_token_ids: vec![],
            fee_token_id: Some([9; 32]),
            coin_transfer,
            coin_fee: crate::storage::CoinInventory::default(),
            value: 25,
            gas_price: 1,
            gas_limit: 1,
            gas_used: 0,
            nonce: 0,
            timestamp: 1,
            block_height: 1,
            data: None,
            conversion_intent: None,
            status: crate::storage::TransactionStatus::Confirmed,
        };
        tx_store.put_transaction(&tx).unwrap();
        let block = build_block(
            &state_store,
            vec![tx.tx_id],
            &[tx],
            genesis.hash,
            1,
            10,
            [7u8; 32],
        );

        let target = Target::from_difficulty(1);
        assert!(matches!(
            validator.validate_block(&block, &target),
            BlockValidationResult::Invalid(_)
        ));
    }

    #[test]
    fn test_accepts_hybrid_features_from_genesis_activation() {
        let (block_store, tx_store, state_store, validator) = setup_validator();
        let genesis = genesis_block();
        block_store.put_block(&genesis).unwrap();

        let mut coin_transfer = crate::storage::CoinInventory::default();
        coin_transfer.add(Denomination::Cents25, 1).unwrap();

        let tx = TransactionRecord {
            tx_id: [45; 32],
            sender: [1; 32],
            recipient: [2; 32],
            transfer_token_ids: vec![],
            fee_token_id: Some([9; 32]),
            coin_transfer,
            coin_fee: crate::storage::CoinInventory::default(),
            value: 25,
            gas_price: 1,
            gas_limit: 1,
            gas_used: 0,
            nonce: 0,
            timestamp: 1,
            block_height: 1,
            data: None,
            conversion_intent: None,
            status: crate::storage::TransactionStatus::Confirmed,
        };
        tx_store.put_transaction(&tx).unwrap();
        let block = build_block(
            &state_store,
            vec![tx.tx_id],
            &[tx],
            genesis.hash,
            1,
            10,
            [8u8; 32],
        );

        let target = Target::from_difficulty(1);
        assert_eq!(
            validator.validate_block(&block, &target),
            BlockValidationResult::Valid
        );
    }
}

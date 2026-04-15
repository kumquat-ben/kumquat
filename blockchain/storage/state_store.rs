use hex;
use log::{debug, error, info, warn};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::storage::block_store::{Block, Hash};
use crate::executor::{execute_transaction_batch, ExecutionRejection, ExecutionStatus};
use crate::storage::kv_store::{
    KVStore, KVStoreError, WriteBatchOperation, WriteBatchOperationExt,
};
use crate::storage::state::{
    AccountState, AccountType, CoinInventory, ConversionOrder, ConversionOrderStatus,
    ConversionTransaction, Denomination, StateResult, StateRoot,
};
use crate::storage::trie::mpt::MerklePatriciaTrie;
use crate::storage::tx_store::TransactionRecord;
use crate::storage::tx_store::{TransactionError, TransactionStatus, TxStore};

// Note: AccountState, AccountType, and StateRoot are now imported from the state module

/// Error type for StateStore operations
#[derive(Debug, thiserror::Error)]
pub enum StateStoreError {
    /// KVStore error
    #[error("KVStore error: {0}")]
    KVStoreError(#[from] KVStoreError),

    /// TxStore error
    #[error("TxStore error: {0}")]
    TxStoreError(#[from] crate::storage::tx_store::TxStoreError),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Account not found
    #[error("Account not found: {0}")]
    AccountNotFound(String),

    /// Insufficient balance
    #[error("Insufficient balance: required {0}, available {1}")]
    InsufficientBalance(u64, u64),

    /// Invalid nonce
    #[error("Invalid nonce: expected {0}, got {1}")]
    InvalidNonce(u64, u64),

    /// Storage key not found
    #[error("Storage key not found: {0}")]
    StorageKeyNotFound(String),

    /// Other error
    #[error("Other error: {0}")]
    Other(String),

    /// Account already exists
    #[error("Account already exists: {0}")]
    AccountAlreadyExists(String),

    /// Balance overflow
    #[error("Balance overflow for account: {0}")]
    BalanceOverflow(String),
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::{
        CoinInventory, ConversionOrderKind, ConversionOrderRequest, ConversionTransaction,
        Denomination,
    };
    use crate::storage::TransactionStatus;
    use tempfile::tempdir;

    fn sample_tx(
        sender: Hash,
        recipient: Hash,
        transfer_token_ids: Vec<Hash>,
        fee_token_id: Hash,
        block_height: u64,
    ) -> TransactionRecord {
        TransactionRecord {
            tx_id: [9; 32],
            sender,
            recipient,
            transfer_token_ids,
            fee_token_id: Some(fee_token_id),
            coin_transfer: crate::storage::CoinInventory::default(),
            coin_fee: crate::storage::CoinInventory::default(),
            value: 100,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 21_000,
            nonce: 0,
            timestamp: 1_234_567,
            block_height,
            data: None,
            conversion_intent: None,
            status: TransactionStatus::Pending,
        }
    }

    #[test]
    fn projected_state_root_does_not_mutate_canonical_state() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let state_store = StateStore::new(&kv_store);

        let sender = [1; 32];
        let recipient = [2; 32];
        let miner = [3; 32];

        state_store
            .create_account(&sender, 101, AccountType::User)
            .unwrap();
        let before_sender = state_store.get_account_state(&sender).unwrap();
        let transfer_token_id = before_sender.tokens[0].token_id;
        let fee_token_id = before_sender.tokens[1].token_id;

        let tx = sample_tx(sender, recipient, vec![transfer_token_id], fee_token_id, 1);

        let _ = state_store
            .calculate_projected_state_root(1, 1_234_567, &[tx], &miner, &[])
            .unwrap();

        let after_sender = state_store.get_account_state(&sender).unwrap();
        assert_eq!(after_sender, before_sender);
        assert!(state_store.get_account_state(&recipient).is_none());
        assert!(state_store.get_account_state(&miner).is_none());
    }

    #[test]
    fn apply_conversion_transaction_create_and_cancel() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let state_store = StateStore::new(&kv_store);

        let sender = [7; 32];
        let miner = [8; 32];
        state_store.create_account(&sender, 100, AccountType::User).unwrap();

        let mut sender_state = state_store.get_account_state(&sender).unwrap();
        sender_state
            .coin_inventory
            .add(crate::storage::Denomination::Cents1, 5)
            .unwrap();
        sender_state.sync_balance_from_hybrid();
        state_store.set_account_state(&sender, &sender_state).unwrap();

        let mut create_fee = CoinInventory::default();
        create_fee
            .add(crate::storage::Denomination::Cents1, 1)
            .unwrap();

        let create_tx = TransactionRecord {
            tx_id: [9; 32],
            sender,
            recipient: [0; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: create_fee,
            value: 0,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 21_000,
            nonce: 0,
            timestamp: 1_000,
            block_height: 1,
            data: None,
            conversion_intent: Some(ConversionTransaction::Create(ConversionOrderRequest {
                kind: ConversionOrderKind::BillToCoins,
                requested_value_cents: 25,
                requested_coin_inventory: CoinInventory::default(),
                requested_bill_denominations: Vec::new(),
            })),
            status: TransactionStatus::Pending,
        };

        state_store
            .apply_conversion_transaction(
                &create_tx,
                &miner,
                1,
                create_tx.conversion_intent.as_ref().unwrap(),
            )
            .unwrap();

        let created_state = state_store.get_account_state(&sender).unwrap();
        let order_id = created_state.conversion_order.as_ref().unwrap().order_id;
        assert_eq!(
            created_state.conversion_order.as_ref().unwrap().eligible_at_block,
            70
        );

        let mut cancel_fee = CoinInventory::default();
        cancel_fee
            .add(crate::storage::Denomination::Cents1, 1)
            .unwrap();
        let cancel_tx = TransactionRecord {
            tx_id: [10; 32],
            sender,
            recipient: [0; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: cancel_fee,
            value: 0,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 21_000,
            nonce: 1,
            timestamp: 1_001,
            block_height: 2,
            data: None,
            conversion_intent: Some(ConversionTransaction::Cancel { order_id }),
            status: TransactionStatus::Pending,
        };

        state_store
            .apply_conversion_transaction(
                &cancel_tx,
                &miner,
                2,
                cancel_tx.conversion_intent.as_ref().unwrap(),
            )
            .unwrap();

        let cancelled_state = state_store.get_account_state(&sender).unwrap();
        assert!(cancelled_state.conversion_order.is_none());
    }

    #[test]
    fn project_state_changes_fulfills_eligible_bill_to_coins_order() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let state_store = StateStore::new(&kv_store);

        let requester = [4; 32];
        let miner = [5; 32];
        state_store
            .create_account(&requester, 100, AccountType::User)
            .unwrap();
        state_store.create_account(&miner, 0, AccountType::User).unwrap();

        let mut requester_state = state_store.get_account_state(&requester).unwrap();
        let mut requested_coins = CoinInventory::default();
        requested_coins.add(Denomination::Cents50, 2).unwrap();
        requester_state.conversion_order = Some(ConversionOrder::new(
            [6; 32],
            requester,
            ConversionOrderRequest {
                kind: ConversionOrderKind::BillToCoins,
                requested_value_cents: 100,
                requested_coin_inventory: requested_coins.clone(),
                requested_bill_denominations: vec![Denomination::Dollars1],
            },
            1,
        ));
        state_store
            .set_account_state(&requester, &requester_state)
            .unwrap();

        let mut miner_state = state_store.get_account_state(&miner).unwrap();
        miner_state.coin_inventory = requested_coins.clone();
        miner_state.sync_balance_from_hybrid();
        state_store.set_account_state(&miner, &miner_state).unwrap();
        let projected = state_store
            .project_state_changes(70, &[], &miner, &[[6; 32]], None)
            .unwrap()
            .into_iter()
            .collect::<HashMap<_, _>>();

        let requester_after = projected.get(&requester).unwrap();
        assert!(requester_after.conversion_order.is_none());
        assert_eq!(
            requester_after.coin_inventory.count(Denomination::Cents50),
            2
        );
        assert!(requester_after.tokens.is_empty());

        let miner_after = projected.get(&miner).unwrap();
        assert_eq!(miner_after.coin_inventory.count(Denomination::Cents50), 0);
        assert_eq!(
            miner_after
                .tokens
                .iter()
                .filter(|token| token.denomination() == Denomination::Dollars1)
                .count(),
            1
        );
    }
}

// StateRoot is now imported from the state module

/// Store for account states
pub struct StateStore<'a> {
    /// The underlying key-value store
    store: &'a dyn KVStore,

    /// Current state root
    state_root: std::sync::RwLock<Option<StateRoot>>,

    /// Cache of recently accessed accounts
    account_cache: dashmap::DashMap<String, AccountState>,

    /// Maximum number of accounts to cache
    max_cache_size: usize,
}

impl<'a> StateStore<'a> {
    /// Create a new StateStore with the given KVStore implementation
    pub fn new(store: &'a dyn KVStore) -> Self {
        Self {
            store,
            state_root: std::sync::RwLock::new(None),
            account_cache: dashmap::DashMap::new(),
            max_cache_size: 10000, // Default cache size
        }
    }

    /// Create a new StateStore with custom cache size
    pub fn with_cache_size(store: &'a dyn KVStore, max_cache_size: usize) -> Self {
        Self {
            store,
            state_root: std::sync::RwLock::new(None),
            account_cache: dashmap::DashMap::new(),
            max_cache_size,
        }
    }

    fn normalize_account_state(mut state: AccountState, fallback_owner: Option<Hash>) -> AccountState {
        if let Some(owner) = fallback_owner {
            state.assign_token_owner(owner);
        }
        if state.bills.is_empty() && state.coin_inventory.is_empty() {
            state.sync_hybrid_from_tokens();
        } else {
            state.sync_balance_from_hybrid();
        }
        state
    }

    /// Get the state of an account
    pub fn get_account_state(&self, address: &Hash) -> Option<AccountState> {
        let addr_str = hex::encode(address);

        // Check the cache first
        if let Some(cached) = self.account_cache.get(&addr_str) {
            return Some(Self::normalize_account_state(cached.clone(), Some(*address)));
        }

        // If not in cache, get from store
        let key = format!("state:account:{}", addr_str);
        match self.store.get(key.as_bytes()) {
            Ok(Some(bytes)) => {
                match bincode::deserialize::<AccountState>(&bytes) {
                    Ok(state) => {
                        let state = Self::normalize_account_state(state, Some(*address));
                        // Add to cache
                        self.add_to_cache(addr_str, state.clone());
                        Some(state)
                    }
                    Err(e) => {
                        error!(
                            "Failed to deserialize account state for {}: {}",
                            addr_str, e
                        );
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to get account state for {}: {}", addr_str, e);
                None
            }
        }
    }

    /// Set the state of an account
    pub fn set_account_state(
        &self,
        address: &Hash,
        state: &AccountState,
    ) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);
        let key = format!("state:account:{}", addr_str);
        let normalized_state = Self::normalize_account_state(state.clone(), Some(*address));

        let value = bincode::serialize(&normalized_state)
            .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

        // Update the store
        self.store.put(key.as_bytes(), &value)?;

        // Update the cache
        self.add_to_cache(addr_str, normalized_state);

        // Invalidate the state root
        let mut state_root = self.state_root.write().unwrap();
        *state_root = None;

        Ok(())
    }

    /// Update account state (alias for set_account_state for compatibility)
    pub fn update_account(
        &self,
        address: &Hash,
        state: &AccountState,
    ) -> Result<(), StateStoreError> {
        self.set_account_state(address, state)
    }

    /// Put an account at a specific block height
    pub fn put_account(
        &self,
        address: &[u8],
        account: &AccountState,
        height: u64,
    ) -> Result<StateResult<()>, StateStoreError> {
        let addr_str = hex::encode(address);
        let key = format!("state:account:{}:{}", addr_str, height);
        let mut owner = [0u8; 32];
        if address.len() == owner.len() {
            owner.copy_from_slice(address);
        }
        let normalized_account = Self::normalize_account_state(account.clone(), Some(owner));

        let value = bincode::serialize(&normalized_account)
            .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

        // Update the store
        self.store.put(key.as_bytes(), &value)?;

        // Update the cache
        self.add_to_cache(addr_str, normalized_account);

        // Invalidate the state root
        let mut state_root = self.state_root.write().unwrap();
        *state_root = None;

        Ok(Ok(()))
    }

    /// Add an account state to the cache
    fn add_to_cache(&self, address: String, state: AccountState) {
        // If cache is full, remove a random entry
        if self.account_cache.len() >= self.max_cache_size {
            if let Some(entry) = self.account_cache.iter().next() {
                self.account_cache.remove(entry.key());
            }
        }

        // Add to cache
        self.account_cache.insert(address, state);
    }

    /// Get an account at a specific block height
    pub fn get_account(
        &self,
        address: &[u8],
        height: u64,
    ) -> Result<Option<AccountState>, StateStoreError> {
        let addr_str = hex::encode(address);
        let key = format!("state:account:{}:{}", addr_str, height);

        match self.store.get(key.as_bytes())? {
            Some(value) => match bincode::deserialize(&value) {
                Ok(state) => Ok(Some(Self::normalize_account_state(state, Some({
                    let mut owner = [0u8; 32];
                    owner.copy_from_slice(address);
                    owner
                })))),
                Err(e) => Err(StateStoreError::SerializationError(e.to_string())),
            },
            None => Ok(None),
        }
    }

    /// Get the latest account state
    pub fn get_latest_account(
        &self,
        address: &[u8],
    ) -> Result<Option<AccountState>, StateStoreError> {
        let addr_str = hex::encode(address);

        // First check the cache
        if let Some(state) = self.account_cache.get(&addr_str) {
            return Ok(Some(state.clone()));
        }

        // If not in cache, scan the database for the latest version
        let prefix = format!("state:account:{}:", addr_str);
        let entries = self.store.scan_prefix(prefix.as_bytes())?;

        if entries.is_empty() {
            return Ok(None);
        }

        // Find the entry with the highest height
        let mut latest_height = 0;
        let mut latest_value = None;

        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 4 {
                if let Ok(height) = parts[3].parse::<u64>() {
                    if height > latest_height {
                        latest_height = height;
                        latest_value = Some(value);
                    }
                }
            }
        }

        match latest_value {
            Some(value) => {
                match bincode::deserialize::<AccountState>(&value) {
                    Ok(state) => {
                        let mut address = [0u8; 32];
                        if let Ok(bytes) = hex::decode(&addr_str) {
                            if bytes.len() == address.len() {
                                address.copy_from_slice(&bytes);
                            }
                        }
                        let state = Self::normalize_account_state(state, Some(address));
                        // Add to cache
                        let state_clone = state.clone();
                        self.add_to_cache(addr_str, state_clone);
                        Ok(Some(state))
                    }
                    Err(e) => Err(StateStoreError::SerializationError(e.to_string())),
                }
            }
            None => Ok(None),
        }
    }

    /// Compatibility-only balance reset helper.
    ///
    /// This rebuilds an account's token inventory from an aggregate balance and must not be used
    /// by token execution paths.
    pub fn update_balance_compat(
        &self,
        address: &Hash,
        new_balance: u64,
    ) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);

        if let Some(mut state) = self.get_account_state(address) {
            state.balance = new_balance;
            state.last_updated = self.get_current_block_height().unwrap_or(0);
            state
                .rebuild_tokens_from_balance_compat(
                    *address,
                    state.last_updated,
                    crate::storage::TokenMintSource::TransferChange,
                )
                .map_err(|e| StateStoreError::Other(e.to_string()))?;
            self.set_account_state(address, &state)?;
            debug!("Updated balance for account {}: {}", addr_str, new_balance);
            Ok(())
        } else {
            Err(StateStoreError::AccountNotFound(addr_str))
        }
    }

    pub fn get_coin_inventory(&self, address: &Hash) -> Result<CoinInventory, StateStoreError> {
        self.get_account_state(address)
            .map(|state| state.coin_inventory)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(address)))
    }

    pub fn get_conversion_order(
        &self,
        address: &Hash,
    ) -> Result<Option<ConversionOrder>, StateStoreError> {
        self.get_account_state(address)
            .map(|state| state.conversion_order)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(address)))
    }

    pub fn set_conversion_order(
        &self,
        address: &Hash,
        conversion_order: ConversionOrder,
        block_height: u64,
    ) -> Result<(), StateStoreError> {
        let mut state = self
            .get_account_state(address)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(address)))?;
        state.conversion_order = Some(conversion_order);
        state.last_updated = block_height;
        self.set_account_state(address, &state)
    }

    pub fn clear_conversion_order(
        &self,
        address: &Hash,
        block_height: u64,
    ) -> Result<(), StateStoreError> {
        let mut state = self
            .get_account_state(address)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(address)))?;
        state.conversion_order = None;
        state.last_updated = block_height;
        self.set_account_state(address, &state)
    }

    /// Transfer balance between accounts
    pub fn transfer_balance(
        &self,
        from: &Hash,
        to: &Hash,
        amount: u64,
        block_height: u64,
    ) -> Result<(), StateStoreError> {
        let from_str = hex::encode(from);
        let to_str = hex::encode(to);

        // Get sender account
        let mut sender = self
            .get_account_state(from)
            .ok_or_else(|| StateStoreError::AccountNotFound(from_str.clone()))?;

        if sender.total_token_value() < amount {
            return Err(StateStoreError::InsufficientBalance(
                amount,
                sender.total_token_value(),
            ));
        }

        let transfer_token_ids = sender.token_ids_for_amount(amount).ok_or_else(|| {
            StateStoreError::Other(format!(
                "sender does not own an exact token set for {} cents",
                amount
            ))
        })?;

        // Get recipient account
        let mut recipient = self.get_account_state(to).unwrap_or_else(|| {
            // Create new account if it doesn't exist
            AccountState::new_user(0, block_height)
        });

        let moved_tokens = sender
            .remove_tokens_by_id(&transfer_token_ids)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;

        sender.last_updated = block_height;
        recipient.last_updated = block_height;
        sender.assign_token_owner(*from);
        sender.sync_balance_from_tokens();
        recipient.deposit_tokens(*to, moved_tokens);

        // Create a batch operation
        let mut batch = WriteBatchOperation::new();

        // Serialize accounts
        let sender_key = format!("state:account:{}", from_str);
        let sender_value = bincode::serialize(&sender)
            .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

        let recipient_key = format!("state:account:{}", to_str);
        let recipient_value = bincode::serialize(&recipient)
            .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

        // Add to batch
        batch.put(sender_key.as_bytes().to_vec(), sender_value);
        batch.put(recipient_key.as_bytes().to_vec(), recipient_value);

        // Execute the batch
        self.store.write_batch(batch)?;

        // Update the cache
        self.add_to_cache(from_str.clone(), sender);
        self.add_to_cache(to_str.clone(), recipient);

        // Invalidate the state root
        let mut state_root = self.state_root.write().unwrap();
        *state_root = None;

        info!("Transferred {} from {} to {}", amount, from_str, to_str);
        Ok(())
    }

    /// Increment account nonce
    pub fn increment_nonce(&self, address: &Hash) -> Result<u64, StateStoreError> {
        let addr_str = hex::encode(address);

        if let Some(mut state) = self.get_account_state(address) {
            state.nonce += 1;
            state.last_updated = self.get_current_block_height().unwrap_or(0);
            self.set_account_state(address, &state)?;
            debug!(
                "Incremented nonce for account {}: {}",
                addr_str, state.nonce
            );
            Ok(state.nonce)
        } else {
            Err(StateStoreError::AccountNotFound(addr_str))
        }
    }

    /// Create a new account with default values
    pub fn create_account(
        &self,
        address: &Hash,
        initial_balance: u64,
        account_type: AccountType,
    ) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);

        // Check if account already exists
        if self.get_account_state(address).is_some() {
            debug!("Account {} already exists", addr_str);
            return Ok(());
        }

        let block_height = self.get_current_block_height().unwrap_or(0);
        let mut state = match account_type {
            AccountType::User => AccountState::new_user(initial_balance, block_height),
            AccountType::Contract => {
                AccountState::new_contract(initial_balance, Vec::new(), block_height)
            }
            AccountType::System => AccountState::new_system(initial_balance, block_height),
            AccountType::Validator => AccountState::new_validator(initial_balance, 0, block_height),
        };
        state.assign_token_owner(*address);

        self.set_account_state(address, &state)?;
        info!(
            "Created new account {} with balance {}",
            addr_str, initial_balance
        );
        Ok(())
    }

    /// Set contract code for an account
    pub fn set_contract_code(&self, address: &Hash, code: Vec<u8>) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);

        if let Some(mut state) = self.get_account_state(address) {
            state.code = Some(code);
            state.account_type = AccountType::Contract;
            state.last_updated = self.get_current_block_height().unwrap_or(0);
            self.set_account_state(address, &state)?;
            info!("Set contract code for account {}", addr_str);
            Ok(())
        } else {
            Err(StateStoreError::AccountNotFound(addr_str))
        }
    }

    /// Get contract code for an account
    pub fn get_contract_code(&self, address: &Hash) -> Option<Vec<u8>> {
        self.get_account_state(address).and_then(|state| state.code)
    }

    /// Set storage value for a contract
    pub fn set_storage_value(
        &self,
        address: &Hash,
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);

        if let Some(mut state) = self.get_account_state(address) {
            state.storage.insert(key.to_vec(), value);
            state.last_updated = self.get_current_block_height().unwrap_or(0);
            self.set_account_state(address, &state)?;
            debug!(
                "Set storage value for account {} key {}",
                addr_str,
                hex::encode(key)
            );
            Ok(())
        } else {
            Err(StateStoreError::AccountNotFound(addr_str))
        }
    }

    /// Get storage value for a contract
    pub fn get_storage_value(&self, address: &Hash, key: &[u8]) -> Option<Vec<u8>> {
        self.get_account_state(address)
            .and_then(|state| state.storage.get(key).cloned())
    }

    /// Delete storage value for a contract
    pub fn delete_storage_value(&self, address: &Hash, key: &[u8]) -> Result<(), StateStoreError> {
        let addr_str = hex::encode(address);

        if let Some(mut state) = self.get_account_state(address) {
            if state.storage.remove(key).is_none() {
                return Err(StateStoreError::StorageKeyNotFound(hex::encode(key)));
            }
            state.last_updated = self.get_current_block_height().unwrap_or(0);
            self.set_account_state(address, &state)?;
            debug!(
                "Deleted storage value for account {} key {}",
                addr_str,
                hex::encode(key)
            );
            Ok(())
        } else {
            Err(StateStoreError::AccountNotFound(addr_str))
        }
    }

    /// Get all accounts
    pub fn get_all_accounts(&self) -> Vec<(Hash, AccountState)> {
        let prefix = b"state:account:";
        match self.store.scan_prefix(prefix) {
            Ok(entries) => {
                entries
                    .iter()
                    .filter_map(|(key, value)| {
                        // Extract address from key
                        let key_str = std::str::from_utf8(key).ok()?;
                        let addr_hex = key_str.strip_prefix("state:account:")?;
                        let addr_bytes = hex::decode(addr_hex).ok()?;

                        // Convert to Hash
                        let mut addr = [0u8; 32];
                        if addr_bytes.len() == 32 {
                            addr.copy_from_slice(&addr_bytes);
                        } else {
                            return None;
                        }

                        // Deserialize account state
                        match bincode::deserialize(value) {
                            Ok(state) => Some((addr, Self::normalize_account_state(state, Some(addr)))),
                            Err(e) => {
                                error!("Failed to deserialize account state: {}", e);
                                None
                            }
                        }
                    })
                    .collect()
            }
            Err(e) => {
                error!("Failed to scan accounts: {}", e);
                Vec::new()
            }
        }
    }

    /// Get accounts updated since a specific block height
    pub fn get_accounts_updated_since(&self, block_height: u64) -> Vec<(Hash, AccountState)> {
        self.get_all_accounts()
            .into_iter()
            .filter(|(_, state)| state.last_updated > block_height)
            .collect()
    }

    /// Get the current block height
    fn get_current_block_height(&self) -> Option<u64> {
        // This would normally come from the blockchain
        // For now, we'll just return None
        None
    }

    /// Calculate the state root hash using a Merkle Patricia Trie
    pub fn calculate_state_root(
        &self,
        block_height: u64,
        timestamp: u64,
    ) -> Result<StateRoot, StateStoreError> {
        // Check if we already have a cached state root
        {
            let state_root = self.state_root.read().unwrap();
            if let Some(root) = &*state_root {
                return Ok(root.clone());
            }
        }

        // Get all accounts
        let accounts = self.get_all_accounts();

        // Build a Merkle Patricia Trie with all accounts
        let mut trie = MerklePatriciaTrie::new();

        // Sort accounts by address for deterministic ordering
        let mut sorted_accounts = accounts;
        sorted_accounts.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Insert each account into the trie
        for (addr, state) in sorted_accounts {
            // Serialize the account state
            let state_bytes = bincode::serialize(&state)
                .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

            // Insert into the trie (address -> state)
            trie.insert(&addr, state_bytes);
        }

        // Calculate the root hash
        let root_hash = trie.root_hash();

        // Create state root
        let state_root = StateRoot {
            root_hash,
            block_height,
            timestamp,
        };

        // Cache the state root
        let mut root = self.state_root.write().unwrap();
        *root = Some(state_root.clone());

        Ok(state_root)
    }

    /// Get the current state root
    pub fn get_state_root(&self) -> Option<StateRoot> {
        let state_root = self.state_root.read().unwrap();
        state_root.clone()
    }

    /// Get the state root at a specific height
    pub fn get_state_root_at_height(
        &self,
        height: u64,
    ) -> Result<Option<StateRoot>, StateStoreError> {
        let key = format!("state_root:{}", height);

        match self.store.get(key.as_bytes())? {
            Some(value) => match bincode::deserialize(&value) {
                Ok(root) => Ok(Some(root)),
                Err(e) => Err(StateStoreError::SerializationError(e.to_string())),
            },
            None => Ok(None),
        }
    }

    /// Put a state root
    pub fn put_state_root(&self, root: &StateRoot) -> Result<StateResult<()>, StateStoreError> {
        let key = format!("state_root:{}", root.block_height);

        let value = bincode::serialize(root)
            .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

        self.store.put(key.as_bytes(), &value)?;

        // Update the cached state root
        let mut state_root = self.state_root.write().unwrap();
        *state_root = Some(root.clone());

        Ok(Ok(()))
    }

    /// Set the state root
    pub fn set_state_root(&self, root: StateRoot) {
        let mut state_root = self.state_root.write().unwrap();
        *state_root = Some(root);
    }

    /// Generate a proof for an account
    ///
    /// This method generates a proof that can be used by light clients to verify
    /// the state of an account without having to download the entire state trie.
    pub fn generate_account_proof(
        &self,
        address: &Hash,
    ) -> Result<crate::storage::trie::mpt::Proof, StateStoreError> {
        // Get all accounts
        let accounts = self.get_all_accounts();

        // Build a Merkle Patricia Trie with all accounts
        let mut trie = MerklePatriciaTrie::new();

        // Sort accounts by address for deterministic ordering
        let mut sorted_accounts = accounts;
        sorted_accounts.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Insert each account into the trie
        for (addr, state) in sorted_accounts {
            // Serialize the account state
            let state_bytes = bincode::serialize(&state)
                .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

            // Insert into the trie (address -> state)
            trie.insert(&addr, state_bytes);
        }

        // Generate the proof
        let proof = trie.generate_proof(address);

        Ok(proof)
    }

    /// Verify an account proof
    ///
    /// This method verifies a proof generated by `generate_account_proof`.
    /// It can be used by light clients to verify the state of an account
    /// without having to download the entire state trie.
    pub fn verify_account_proof(
        proof: &crate::storage::trie::mpt::Proof,
        expected_root: &Hash,
    ) -> bool {
        // Check that the proof's root hash matches the expected root
        if proof.root_hash != *expected_root {
            return false;
        }

        // Verify the proof
        MerklePatriciaTrie::verify_proof_with_root(proof, expected_root)
    }

    /// Flush all pending writes to disk
    pub fn flush(&self) -> Result<(), StateStoreError> {
        self.store.flush().map_err(|e| e.into())
    }

    /// Clear the account cache
    pub fn clear_cache(&self) {
        self.account_cache.clear();
    }

    /// Clone the state store for validation purposes
    ///
    /// This creates a new StateStore that shares the same underlying KVStore
    /// but has its own cache and state root. This is useful for validating
    /// blocks without modifying the main state.
    pub fn clone_for_validation(&self) -> Self {
        Self {
            store: self.store,
            state_root: std::sync::RwLock::new(None),
            account_cache: dashmap::DashMap::new(),
            max_cache_size: self.max_cache_size,
        }
    }

    pub fn apply_block_reward(
        &self,
        miner: &Hash,
        block_height: u64,
        block_hash: &Hash,
    ) -> Result<Vec<Hash>, StateStoreError> {
        let mut miner_state = self
            .get_account_state(miner)
            .unwrap_or_else(|| AccountState::new_user(0, block_height));

        let minted_tokens =
            crate::storage::block_store::reward_outcome(*miner, block_height, block_hash);
        let reward_token_ids = minted_tokens
            .iter()
            .map(|token| token.token_id)
            .collect::<Vec<_>>();

        miner_state.tokens.extend(minted_tokens);
        miner_state.sync_balance_from_tokens();
        miner_state.last_updated = block_height;
        miner_state.assign_token_owner(*miner);

        self.set_account_state(miner, &miner_state)?;
        Ok(reward_token_ids)
    }

    pub fn calculate_projected_state_root(
        &self,
        block_height: u64,
        timestamp: u64,
        transactions: &[TransactionRecord],
        miner: &Hash,
        conversion_fulfillment_order_ids: &[Hash],
    ) -> Result<StateRoot, StateStoreError> {
        let projected_accounts = self.project_state_changes(
            block_height,
            transactions,
            miner,
            conversion_fulfillment_order_ids,
            None,
        )?;
        self.calculate_state_root_with_overrides(block_height, timestamp, &projected_accounts)
    }

    pub fn calculate_projected_state_root_with_block_reward(
        &self,
        block_height: u64,
        timestamp: u64,
        transactions: &[TransactionRecord],
        miner: &Hash,
        conversion_fulfillment_order_ids: &[Hash],
        block_hash: &Hash,
    ) -> Result<StateRoot, StateStoreError> {
        let projected_accounts = self.project_state_changes(
            block_height,
            transactions,
            miner,
            conversion_fulfillment_order_ids,
            Some(block_hash),
        )?;
        self.calculate_state_root_with_overrides(block_height, timestamp, &projected_accounts)
    }

    pub fn project_state_changes(
        &self,
        block_height: u64,
        transactions: &[TransactionRecord],
        miner: &Hash,
        conversion_fulfillment_order_ids: &[Hash],
        block_hash: Option<&Hash>,
    ) -> Result<Vec<(Hash, AccountState)>, StateStoreError> {
        let accounts = self.get_all_accounts();
        let original_accounts = accounts.iter().cloned().collect::<HashMap<Hash, AccountState>>();
        let execution =
            execute_transaction_batch(&accounts, transactions, miner, block_height, block_hash);
        let mut projected_accounts = execution
            .accounts
            .into_iter()
            .collect::<HashMap<Hash, AccountState>>();
        let untouched_accounts = touched_accounts_for_projection(transactions, miner, block_hash);
        for (address, original_state) in &original_accounts {
            if !untouched_accounts.contains(address) {
                projected_accounts.insert(*address, original_state.clone());
            }
        }
        self.apply_conversion_fulfillments(
            &mut projected_accounts,
            miner,
            block_height,
            conversion_fulfillment_order_ids,
        )?;
        let mut accounts = projected_accounts.into_iter().collect::<Vec<_>>();
        accounts.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(accounts)
    }

    fn apply_conversion_fulfillments(
        &self,
        accounts: &mut HashMap<Hash, AccountState>,
        miner: &Hash,
        block_height: u64,
        order_ids: &[Hash],
    ) -> Result<(), StateStoreError> {
        for account in accounts.values_mut() {
            if let Some(order) = account.conversion_order.as_mut() {
                refresh_conversion_order_status(order, block_height);
            }
        }

        if order_ids.is_empty() {
            return Ok(());
        }

        let mut duplicate_ids = std::collections::HashSet::new();
        let mut seen_ids = std::collections::HashSet::new();
        for order_id in order_ids {
            if !seen_ids.insert(*order_id) {
                duplicate_ids.insert(*order_id);
            }
        }

        let mut selected_requesters = Vec::with_capacity(order_ids.len());
        let mut invalid_reason = None;
        for order_id in order_ids {
            let maybe_requester = accounts.iter().find_map(|(address, account)| {
                account
                    .conversion_order
                    .as_ref()
                    .filter(|order| order.order_id == *order_id)
                    .map(|_| *address)
            });

            let requester = match maybe_requester {
                Some(requester) => requester,
                None => {
                    invalid_reason = Some(format!(
                        "conversion order {} was already filled or does not exist",
                        hex::encode(order_id)
                    ));
                    break;
                }
            };

            let order = accounts
                .get(&requester)
                .and_then(|account| account.conversion_order.as_ref())
                .ok_or_else(|| {
                    StateStoreError::Other(format!(
                        "conversion order {} disappeared during fulfillment",
                        hex::encode(order_id)
                    ))
                })?;

            if duplicate_ids.contains(order_id) {
                invalid_reason = Some(format!(
                    "conversion order {} was duplicated in the fulfillment list",
                    hex::encode(order_id)
                ));
                break;
            }

            if order.status != ConversionOrderStatus::Eligible {
                invalid_reason = Some(format!(
                    "conversion order {} is not eligible for fulfillment",
                    hex::encode(order_id)
                ));
                break;
            }

            selected_requesters.push(requester);
        }

        if let Some(reason) = invalid_reason {
            for requester in selected_requesters {
                if let Some(order) = accounts
                    .get_mut(&requester)
                    .and_then(|account| account.conversion_order.as_mut())
                {
                    order.status = ConversionOrderStatus::Failed;
                    order.failure_reason = Some(reason.clone());
                }
            }
            return Ok(());
        }

        let miner_account = accounts
            .get(miner)
            .cloned()
            .unwrap_or_else(|| AccountState::new_user(0, block_height));
        let mut staged_accounts = accounts.clone();
        staged_accounts.insert(*miner, miner_account);

        for requester in &selected_requesters {
            let order = staged_accounts
                .get(requester)
                .and_then(|account| account.conversion_order.clone())
                .ok_or_else(|| {
                    StateStoreError::Other(format!(
                        "conversion requester {} has no active order",
                        hex::encode(requester)
                    ))
                })?;

            match order.kind {
                crate::storage::ConversionOrderKind::BillToCoins => {
                    let requested_bills = conversion_bill_denominations(&order)?;
                    let requested_coins = conversion_coin_inventory(&order)?;

                    let requester_account =
                        staged_accounts.get(requester).ok_or_else(|| {
                            StateStoreError::AccountNotFound(hex::encode(requester))
                        })?;
                    let miner_account = staged_accounts.get(miner).ok_or_else(|| {
                        StateStoreError::AccountNotFound(hex::encode(miner))
                    })?;

                    let requester_token_ids = match select_bill_token_ids_for_denominations(
                        requester_account,
                        &requested_bills,
                    ) {
                        Ok(token_ids) => token_ids,
                        Err(_) => {
                            return self.fail_selected_orders(
                                accounts,
                                &selected_requesters,
                                "requester no longer has required bill inventory",
                            );
                        }
                    };
                    if !miner_account.coin_inventory.can_cover(&requested_coins) {
                        return self.fail_selected_orders(
                            accounts,
                            &selected_requesters,
                            "miner inventory cannot satisfy conversion order",
                        );
                    }

                    let requester_account = staged_accounts.get_mut(requester).unwrap();
                    let moved_tokens = requester_account
                        .remove_tokens_by_id(&requester_token_ids)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    requester_account
                        .coin_inventory
                        .add_inventory(&requested_coins)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    requester_account.last_updated = block_height;
                    requester_account.conversion_order = None;
                    requester_account.sync_balance_from_hybrid();

                    let miner_account = staged_accounts.get_mut(miner).unwrap();
                    miner_account
                        .coin_inventory
                        .remove_inventory(&requested_coins)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    miner_account.deposit_tokens(*miner, moved_tokens);
                    miner_account.last_updated = block_height;
                    miner_account.sync_balance_from_hybrid();
                }
                crate::storage::ConversionOrderKind::CoinsToBill => {
                    let requested_coins = conversion_coin_inventory(&order)?;
                    let requested_bills = conversion_bill_denominations(&order)?;

                    let requester_account =
                        staged_accounts.get(requester).ok_or_else(|| {
                            StateStoreError::AccountNotFound(hex::encode(requester))
                        })?;
                    let miner_account = staged_accounts.get(miner).ok_or_else(|| {
                        StateStoreError::AccountNotFound(hex::encode(miner))
                    })?;

                    if !requester_account.coin_inventory.can_cover(&requested_coins) {
                        return self.fail_selected_orders(
                            accounts,
                            &selected_requesters,
                            "requester no longer has required coin inventory",
                        );
                    }
                    let miner_token_ids = match select_bill_token_ids_for_denominations(
                        miner_account,
                        &requested_bills,
                    ) {
                        Ok(token_ids) => token_ids,
                        Err(_) => {
                            return self.fail_selected_orders(
                                accounts,
                                &selected_requesters,
                                "miner inventory cannot satisfy conversion order",
                            );
                        }
                    };

                    let requester_account = staged_accounts.get_mut(requester).unwrap();
                    requester_account
                        .coin_inventory
                        .remove_inventory(&requested_coins)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    requester_account.last_updated = block_height;
                    requester_account.sync_balance_from_hybrid();

                    let miner_account = staged_accounts.get_mut(miner).unwrap();
                    let moved_tokens = miner_account
                        .remove_tokens_by_id(&miner_token_ids)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    miner_account
                        .coin_inventory
                        .add_inventory(&requested_coins)
                        .map_err(|e| StateStoreError::Other(e.to_string()))?;
                    miner_account.last_updated = block_height;
                    miner_account.sync_balance_from_hybrid();

                    let requester_account = staged_accounts.get_mut(requester).unwrap();
                    requester_account.deposit_tokens(*requester, moved_tokens);
                    requester_account.conversion_order = None;
                    requester_account.last_updated = block_height;
                    requester_account.sync_balance_from_hybrid();
                }
            }
        }

        *accounts = staged_accounts;
        Ok(())
    }

    fn fail_selected_orders(
        &self,
        accounts: &mut HashMap<Hash, AccountState>,
        selected_requesters: &[Hash],
        reason: &str,
    ) -> Result<(), StateStoreError> {
        for requester in selected_requesters {
            if let Some(order) = accounts
                .get_mut(requester)
                .and_then(|account| account.conversion_order.as_mut())
            {
                order.status = ConversionOrderStatus::Failed;
                order.failure_reason = Some(reason.to_string());
            }
        }
        Ok(())
    }

    pub fn calculate_state_root_with_overrides(
        &self,
        block_height: u64,
        timestamp: u64,
        overrides: &[(Hash, AccountState)],
    ) -> Result<StateRoot, StateStoreError> {
        let mut accounts: HashMap<Hash, AccountState> = self.get_all_accounts().into_iter().collect();
        for (address, state) in overrides {
            accounts.insert(*address, state.clone());
        }

        let mut trie = MerklePatriciaTrie::new();
        let mut sorted_accounts = accounts.into_iter().collect::<Vec<_>>();
        sorted_accounts.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (addr, state) in sorted_accounts {
            let state_bytes = bincode::serialize(&state)
                .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;
            trie.insert(&addr, state_bytes);
        }

        Ok(StateRoot {
            root_hash: trie.root_hash(),
            block_height,
            timestamp,
        })
    }

    pub fn apply_token_transaction(
        &self,
        tx: &TransactionRecord,
        fee_recipient: &Hash,
        block_height: u64,
    ) -> Result<Vec<(Hash, AccountState)>, StateStoreError> {
        if let Some(intent) = &tx.conversion_intent {
            return self.apply_conversion_transaction(tx, fee_recipient, block_height, intent);
        }

        if tx.transfer_token_ids.is_empty() && tx.coin_transfer.is_empty() {
            return Err(StateStoreError::Other(format!(
                "transaction {} has no transfer inputs",
                hex::encode(tx.tx_id)
            )));
        }

        if tx.fee_token_id.is_none() && tx.coin_fee.is_empty() {
            return Err(StateStoreError::Other(format!(
                "transaction {} is missing fee inputs",
                hex::encode(tx.tx_id)
            )));
        }

        if let Some(fee_token_id) = tx.fee_token_id {
            if tx
                .transfer_token_ids
                .iter()
                .any(|token_id| *token_id == fee_token_id)
            {
                return Err(StateStoreError::Other(format!(
                    "transaction {} reuses fee token as payment token",
                    hex::encode(tx.tx_id)
                )));
            }
        }

        let mut sender = self
            .get_account_state(&tx.sender)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(tx.sender)))?;

        let payment_total = tx
            .transfer_token_ids
            .iter()
            .try_fold(0u64, |acc, token_id| -> Result<u64, StateStoreError> {
                let token = sender
                    .tokens
                    .iter()
                    .find(|token| token.token_id == *token_id && token.denomination().is_bill())
                    .ok_or_else(|| {
                        StateStoreError::Other(format!(
                            "sender does not own bill token {}",
                            hex::encode(token_id)
                        ))
                    })?;
                Ok(acc + token.value_cents())
            })?;
        let coin_transfer_total = tx.coin_transfer.total_value_cents();

        let fee_value = if let Some(fee_token_id) = tx.fee_token_id {
            sender
                .tokens
                .iter()
                .find(|token| token.token_id == fee_token_id && token.denomination().is_bill())
                .map(|token| token.value_cents())
                .ok_or_else(|| {
                    StateStoreError::Other(format!(
                        "sender does not own fee bill token {}",
                        hex::encode(fee_token_id)
                    ))
                })?
        } else {
            0
        };

        let mut recipient = self
            .get_account_state(&tx.recipient)
            .unwrap_or_else(|| AccountState::new_user(0, block_height));
        let mut fee_account = if *fee_recipient == tx.recipient {
            recipient.clone()
        } else {
            self.get_account_state(fee_recipient)
                .unwrap_or_else(|| AccountState::new_user(0, block_height))
        };

        let moved_tokens = sender
            .remove_tokens_by_id(&tx.transfer_token_ids)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;
        let fee_tokens = if let Some(fee_token_id) = tx.fee_token_id {
            sender
                .remove_tokens_by_id(&[fee_token_id])
                .map_err(|e| StateStoreError::Other(e.to_string()))?
        } else {
            Vec::new()
        };

        sender
            .coin_inventory
            .remove_inventory(&tx.coin_transfer)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;
        sender
            .coin_inventory
            .remove_inventory(&tx.coin_fee)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;

        sender.nonce += 1;
        sender.last_updated = block_height;
        sender.assign_token_owner(tx.sender);
        sender.sync_balance_from_hybrid();

        recipient.deposit_tokens(tx.recipient, moved_tokens);
        recipient
            .coin_inventory
            .add_inventory(&tx.coin_transfer)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;
        recipient.last_updated = block_height;
        recipient.sync_balance_from_hybrid();

        if !fee_tokens.is_empty() {
            fee_account.deposit_tokens(*fee_recipient, fee_tokens);
        }
        fee_account
            .coin_inventory
            .add_inventory(&tx.coin_fee)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;
        fee_account.last_updated = block_height;
        fee_account.sync_balance_from_hybrid();

        if payment_total + coin_transfer_total != tx.value {
            return Err(StateStoreError::Other(format!(
                "transaction {} has mismatched value mirror",
                hex::encode(tx.tx_id)
            )));
        }

        self.set_account_state(&tx.sender, &sender)?;
        self.set_account_state(&tx.recipient, &recipient)?;
        if *fee_recipient != tx.recipient {
            self.set_account_state(fee_recipient, &fee_account)?;
        } else {
            self.set_account_state(fee_recipient, &fee_account)?;
        }

        let mut changed_accounts = vec![(tx.sender, sender), (tx.recipient, recipient)];
        if *fee_recipient != tx.recipient {
            changed_accounts.push((*fee_recipient, fee_account));
        } else {
            changed_accounts.pop();
            changed_accounts.push((*fee_recipient, fee_account));
        }

        debug!(
            "Applied token transaction {} transferring {} cents and fee {} cents",
            hex::encode(tx.tx_id),
            payment_total + coin_transfer_total,
            fee_value + tx.coin_fee.total_value_cents(),
        );

        Ok(changed_accounts)
    }

    pub fn apply_conversion_transaction(
        &self,
        tx: &TransactionRecord,
        fee_recipient: &Hash,
        block_height: u64,
        intent: &ConversionTransaction,
    ) -> Result<Vec<(Hash, AccountState)>, StateStoreError> {
        if !tx.transfer_token_ids.is_empty() || !tx.coin_transfer.is_empty() || tx.fee_token_id.is_some() {
            return Err(StateStoreError::Other(format!(
                "conversion transaction {} must not carry normal transfer inputs",
                hex::encode(tx.tx_id)
            )));
        }
        if tx.coin_fee.is_empty() {
            return Err(StateStoreError::Other(format!(
                "conversion transaction {} is missing coin fee",
                hex::encode(tx.tx_id)
            )));
        }

        let mut sender = self
            .get_account_state(&tx.sender)
            .ok_or_else(|| StateStoreError::AccountNotFound(hex::encode(tx.sender)))?;
        if sender.nonce != tx.nonce {
            return Err(StateStoreError::InvalidNonce(tx.nonce, sender.nonce));
        }
        sender
            .coin_inventory
            .remove_inventory(&tx.coin_fee)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;

        match intent {
            ConversionTransaction::Create(request) => {
                if sender.conversion_order.is_some() || request.requested_value_cents == 0 {
                    return Err(StateStoreError::Other(format!(
                        "conversion transaction {} cannot create a new order",
                        hex::encode(tx.tx_id)
                    )));
                }
                sender.conversion_order = Some(ConversionOrder::new(
                    derive_conversion_order_id(&tx.tx_id, tx.sender),
                    tx.sender,
                    request.clone(),
                    block_height,
                ));
            }
            ConversionTransaction::Cancel { order_id } => {
                let existing = sender.conversion_order.as_ref().ok_or_else(|| {
                    StateStoreError::Other(format!(
                        "conversion transaction {} has no active order to cancel",
                        hex::encode(tx.tx_id)
                    ))
                })?;
                if existing.order_id != *order_id {
                    return Err(StateStoreError::Other(format!(
                        "conversion transaction {} order id mismatch",
                        hex::encode(tx.tx_id)
                    )));
                }
                sender.conversion_order = None;
            }
        }

        sender.nonce += 1;
        sender.last_updated = block_height;
        sender.sync_balance_from_hybrid();

        let mut fee_account = self
            .get_account_state(fee_recipient)
            .unwrap_or_else(|| AccountState::new_user(0, block_height));
        fee_account
            .coin_inventory
            .add_inventory(&tx.coin_fee)
            .map_err(|e| StateStoreError::Other(e.to_string()))?;
        fee_account.last_updated = block_height;
        fee_account.sync_balance_from_hybrid();

        self.set_account_state(&tx.sender, &sender)?;
        self.set_account_state(fee_recipient, &fee_account)?;

        let mut changed_accounts = vec![(tx.sender, sender)];
        if *fee_recipient != tx.sender {
            changed_accounts.push((*fee_recipient, fee_account));
        } else {
            changed_accounts[0] = (*fee_recipient, fee_account);
        }
        Ok(changed_accounts)
    }

    /// Apply a block's transactions to the state store
    ///
    /// This method processes all transactions in a block and updates the account states accordingly.
    /// It ensures that transactions are applied in the correct order and validates each transaction
    /// before applying it.
    ///
    /// # Arguments
    ///
    /// * `block` - The block containing transactions to apply
    /// * `tx_store` - The transaction store to retrieve transaction details
    ///
    /// # Returns
    ///
    /// * `Result<(), StateStoreError>` - Success or error
    pub fn apply_block(&self, block: &Block, tx_store: &TxStore) -> Result<(), StateStoreError> {
        info!(
            "Applying block {} with {} transactions",
            block.height,
            block.transactions.len()
        );

        let mut block_transactions = Vec::with_capacity(block.transactions.len());
        for tx_hash in &block.transactions {
            let tx = match tx_store.get_transaction(tx_hash)? {
                Some(tx) => tx,
                None => {
                    error!("Transaction {} not found in tx_store", hex::encode(tx_hash));
                    return Err(StateStoreError::Other(format!(
                        "Transaction {} not found",
                        hex::encode(tx_hash)
                    )));
                }
            };
            if tx.block_height != block.height {
                warn!(
                    "Transaction {} has mismatched block height: {} vs {}",
                    hex::encode(&tx.tx_id),
                    tx.block_height,
                    block.height
                );
            }
            block_transactions.push(tx);
        }

        let accounts = self.get_all_accounts();
        let execution = execute_transaction_batch(
            &accounts,
            &block_transactions,
            &block.miner,
            block.height,
            Some(&block.hash),
        );
        let original_accounts = accounts.iter().cloned().collect::<HashMap<Hash, AccountState>>();
        let mut projected_accounts = execution
            .accounts
            .into_iter()
            .collect::<HashMap<Hash, AccountState>>();
        let touched_accounts =
            touched_accounts_for_projection(&block_transactions, &block.miner, Some(&block.hash));
        for (address, original_state) in &original_accounts {
            if !touched_accounts.contains(address) {
                projected_accounts.insert(*address, original_state.clone());
            }
        }
        self.apply_conversion_fulfillments(
            &mut projected_accounts,
            &block.miner,
            block.height,
            &block.conversion_fulfillment_order_ids,
        )?;

        let expected_reward_token_ids = crate::storage::block_store::reward_outcome(
            block.miner,
            block.height,
            &block.hash,
        )
        .into_iter()
        .map(|token| token.token_id)
        .collect::<Vec<_>>();
        if !block.reward_token_ids.is_empty() && block.reward_token_ids != expected_reward_token_ids
        {
            return Err(StateStoreError::Other(format!(
                "reward token ids mismatch for block {}",
                block.height
            )));
        }

        let mut batch = WriteBatchOperation::new();
        for (tx, outcome) in block_transactions.iter().zip(execution.outcomes.iter()) {
            match outcome.status {
                ExecutionStatus::Applied => {
                    tx_store.update_transaction_status(&tx.tx_id, TransactionStatus::Confirmed)?;
                }
                ExecutionStatus::Rejected(reason) => {
                    tx_store.update_transaction_status(
                        &tx.tx_id,
                        TransactionStatus::Failed(map_rejection_to_tx_error(reason)),
                    )?;
                }
            }
        }

        let mut materialized_accounts = projected_accounts.into_iter().collect::<Vec<_>>();
        materialized_accounts.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (address, state) in materialized_accounts {
            let addr_str = hex::encode(&address);
            let key = format!("state:account:{}", addr_str);

            let value = bincode::serialize(&state)
                .map_err(|e| StateStoreError::SerializationError(e.to_string()))?;

            batch.put(key.as_bytes().to_vec(), value);
            self.add_to_cache(addr_str, state);
        }

        // Execute the batch
        self.store.write_batch(batch)?;

        // Invalidate the state root since we've modified the state
        let mut state_root = self.state_root.write().unwrap();
        *state_root = None;

        // Calculate the new state root
        drop(state_root); // Release the write lock before calculating
        let new_root = self.calculate_state_root(block.height, block.timestamp)?;

        // Verify that the calculated state root matches the block's state root
        if new_root.root_hash != block.state_root {
            warn!(
                "Calculated state root {} does not match block's state root {}",
                hex::encode(&new_root.root_hash),
                hex::encode(&block.state_root)
            );
        }

        info!(
            "Successfully applied block {} with {} transactions",
            block.height,
            block.transactions.len()
        );
        Ok(())
    }
}

fn map_rejection_to_tx_error(reason: ExecutionRejection) -> TransactionError {
    match reason {
        ExecutionRejection::InvalidNonce => TransactionError::InvalidNonce,
        ExecutionRejection::InsufficientBalance | ExecutionRejection::MissingToken => {
            TransactionError::InsufficientBalance
        }
        ExecutionRejection::LockConflict | ExecutionRejection::StaleVersion => {
            TransactionError::ExecutionError
        }
        _ => TransactionError::Other,
    }
}

fn derive_conversion_order_id(tx_id: &Hash, sender: Hash) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(tx_id);
    hasher.update(sender);
    let digest = hasher.finalize();
    let mut order_id = [0u8; 32];
    order_id.copy_from_slice(&digest[..32]);
    order_id
}

fn canonical_coin_inventory_for_amount(amount_cents: u64) -> Result<CoinInventory, StateStoreError> {
    let mut remaining = amount_cents;
    let mut inventory = CoinInventory::default();

    for denomination in [
        Denomination::Cents50,
        Denomination::Cents25,
        Denomination::Cents10,
        Denomination::Cents5,
        Denomination::Cents1,
    ] {
        let value = denomination.value_cents();
        let count = remaining / value;
        if count > 0 {
            inventory
                .add(denomination, count)
                .map_err(|e| StateStoreError::Other(e.to_string()))?;
            remaining -= count * value;
        }
    }

    if remaining != 0 {
        return Err(StateStoreError::Other(format!(
            "cannot represent {} cents as canonical coin inventory",
            amount_cents
        )));
    }

    Ok(inventory)
}

fn canonical_bill_denominations_for_amount(
    amount_cents: u64,
) -> Result<Vec<Denomination>, StateStoreError> {
    let mut remaining = amount_cents;
    let mut denominations = Vec::new();

    for denomination in [
        Denomination::Dollars100,
        Denomination::Dollars50,
        Denomination::Dollars20,
        Denomination::Dollars10,
        Denomination::Dollars5,
        Denomination::Dollars2,
        Denomination::Dollars1,
    ] {
        let value = denomination.value_cents();
        let count = remaining / value;
        for _ in 0..count {
            denominations.push(denomination);
        }
        remaining -= count * value;
    }

    if remaining != 0 {
        return Err(StateStoreError::Other(format!(
            "cannot represent {} cents as canonical bill denominations",
            amount_cents
        )));
    }

    Ok(denominations)
}

fn conversion_bill_denominations(order: &ConversionOrder) -> Result<Vec<Denomination>, StateStoreError> {
    if order.requested_bill_denominations.is_empty() {
        canonical_bill_denominations_for_amount(order.requested_value_cents)
    } else {
        let requested_total = order
            .requested_bill_denominations
            .iter()
            .map(|denomination| denomination.value_cents())
            .sum::<u64>();
        if requested_total != order.requested_value_cents {
            return Err(StateStoreError::Other(format!(
                "conversion order {} bill denominations do not match requested value",
                hex::encode(order.order_id)
            )));
        }
        Ok(order.requested_bill_denominations.clone())
    }
}

fn conversion_coin_inventory(order: &ConversionOrder) -> Result<CoinInventory, StateStoreError> {
    if order.requested_coin_inventory.is_empty() {
        canonical_coin_inventory_for_amount(order.requested_value_cents)
    } else if order.requested_coin_inventory.total_value_cents() != order.requested_value_cents {
        Err(StateStoreError::Other(format!(
            "conversion order {} coin inventory does not match requested value",
            hex::encode(order.order_id)
        )))
    } else {
        Ok(order.requested_coin_inventory.clone())
    }
}

fn select_bill_token_ids_for_denominations(
    account: &AccountState,
    requested_denominations: &[Denomination],
) -> Result<Vec<Hash>, StateStoreError> {
    let mut token_ids = Vec::with_capacity(requested_denominations.len());
    let mut used_indexes = Vec::with_capacity(requested_denominations.len());

    for denomination in requested_denominations {
        let maybe_index = account
            .tokens
            .iter()
            .enumerate()
            .find(|(index, token)| {
                token.denomination() == *denomination
                    && token.denomination().is_bill()
                    && !used_indexes.contains(index)
            })
            .map(|(index, token)| (index, token.token_id));

        let (index, token_id) = maybe_index.ok_or_else(|| {
            StateStoreError::Other(format!(
                "account is missing required {} bill for conversion",
                denomination
            ))
        })?;
        used_indexes.push(index);
        token_ids.push(token_id);
    }

    Ok(token_ids)
}

fn refresh_conversion_order_status(order: &mut ConversionOrder, block_height: u64) {
    if matches!(
        order.status,
        ConversionOrderStatus::Failed
            | ConversionOrderStatus::Expired
            | ConversionOrderStatus::Fulfilled
    ) {
        return;
    }

    if block_height >= order.cycle_end_block {
        order.status = ConversionOrderStatus::Expired;
        order.failure_reason = Some("conversion cycle expired".to_string());
    } else if block_height >= order.eligible_at_block {
        order.status = ConversionOrderStatus::Eligible;
        order.failure_reason = None;
    } else {
        order.status = ConversionOrderStatus::Pending;
        order.failure_reason = None;
    }
}

fn touched_accounts_for_projection(
    transactions: &[TransactionRecord],
    miner: &Hash,
    block_hash: Option<&Hash>,
) -> HashSet<Hash> {
    let mut touched = HashSet::new();
    if block_hash.is_some() {
        touched.insert(*miner);
    }

    for tx in transactions {
        touched.insert(tx.sender);
        touched.insert(tx.recipient);
        touched.insert(*miner);
    }

    touched
}

#[cfg(all(test, feature = "legacy-test-compat"))]
mod tests {
    use super::*;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::state::{CoinInventory, ConversionOrderKind, ConversionOrderStatus};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    #[test]
    fn test_state_store() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        let address = [1; 32];

        // Create a new account
        state_store
            .create_account(&address, 1000, AccountType::User)
            .unwrap();

        // Get the account state
        let state = state_store.get_account_state(&address).unwrap();
        assert_eq!(state.balance, 1000);
        assert_eq!(state.nonce, 0);
        assert_eq!(state.code, None);
        assert_eq!(state.account_type, AccountType::User);

        // Update balance
        state_store.update_balance_compat(&address, 2000).unwrap();
        let state = state_store.get_account_state(&address).unwrap();
        assert_eq!(state.balance, 2000);

        // Increment nonce
        let new_nonce = state_store.increment_nonce(&address).unwrap();
        assert_eq!(new_nonce, 1);
        let state = state_store.get_account_state(&address).unwrap();
        assert_eq!(state.nonce, 1);

        // Set contract code
        let contract_code = vec![1, 2, 3, 4];
        state_store
            .set_contract_code(&address, contract_code.clone())
            .unwrap();
        let state = state_store.get_account_state(&address).unwrap();
        assert_eq!(state.code, Some(contract_code));
        assert_eq!(state.account_type, AccountType::Contract);

        // Test contract storage
        let storage_key = b"test_key";
        let storage_value = b"test_value".to_vec();
        state_store
            .set_storage_value(&address, storage_key, storage_value.clone())
            .unwrap();

        let retrieved_value = state_store
            .get_storage_value(&address, storage_key)
            .unwrap();
        assert_eq!(retrieved_value, storage_value);

        // Test delete storage value
        state_store
            .delete_storage_value(&address, storage_key)
            .unwrap();
        assert!(state_store
            .get_storage_value(&address, storage_key)
            .is_none());

        // Test flush
        state_store.flush().unwrap();
    }

    #[test]
    fn test_transfer_balance() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        let sender = [1; 32];
        let recipient = [2; 32];

        // Create accounts
        state_store
            .create_account(&sender, 1500, AccountType::User)
            .unwrap();
        state_store
            .create_account(&recipient, 500, AccountType::User)
            .unwrap();

        let sender_before = state_store.get_account_state(&sender).unwrap();
        let moved_token_id = sender_before
            .token_ids_for_amount(500)
            .expect("sender should have an exact 500-cent token")[0];

        // Transfer balance
        state_store
            .transfer_balance(&sender, &recipient, 500, 1)
            .unwrap();

        // Check balances
        let sender_state = state_store.get_account_state(&sender).unwrap();
        let recipient_state = state_store.get_account_state(&recipient).unwrap();

        assert_eq!(sender_state.balance, 1000);
        assert_eq!(recipient_state.balance, 1000);
        assert!(!sender_state.owns_token(&moved_token_id));
        assert!(recipient_state.owns_token(&moved_token_id));

        // Exact-token transfers fail when the sender lacks a matching token bundle.
        let result = state_store.transfer_balance(&sender, &recipient, 300, 2);
        assert!(matches!(
            result,
            Err(StateStoreError::Other(_))
        ));
    }

    #[test]
    fn test_get_account_state_normalizes_legacy_hybrid_fields() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        let address = [7; 32];
        let key = format!("state:account:{}", hex::encode(address));

        let mut legacy_state = AccountState::new_user(136, 5);
        legacy_state.bills.clear();
        legacy_state.coin_inventory = CoinInventory::default();

        let bytes = bincode::serialize(&legacy_state).unwrap();
        kv_store.put(key.as_bytes(), &bytes).unwrap();

        let loaded = state_store.get_account_state(&address).unwrap();
        assert_eq!(loaded.total_bill_value(), 100);
        assert_eq!(loaded.total_coin_value(), 36);
        assert_eq!(loaded.bills.len(), 1);
        assert_eq!(loaded.coin_inventory.count(crate::storage::Denomination::Cents25), 1);
    }

    #[test]
    fn test_conversion_order_round_trip() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        let address = [8; 32];
        state_store
            .create_account(&address, 500, AccountType::User)
            .unwrap();

        let order = ConversionOrder {
            order_id: [9; 32],
            requester: address,
            kind: ConversionOrderKind::BillToCoins,
            requested_value_cents: 125,
            requested_coin_inventory: CoinInventory::default(),
            requested_bill_denominations: Vec::new(),
            created_at_block: 10,
            eligible_at_block: 79,
            cycle_end_block: 420,
            status: ConversionOrderStatus::Pending,
            failure_reason: None,
        };

        state_store
            .set_conversion_order(&address, order.clone(), 10)
            .unwrap();

        let stored = state_store.get_conversion_order(&address).unwrap();
        assert_eq!(stored, Some(order));

        state_store.clear_conversion_order(&address, 11).unwrap();
        let cleared = state_store.get_conversion_order(&address).unwrap();
        assert_eq!(cleared, None);
    }

    #[test]
    fn test_state_root() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        // Create some accounts
        let addr1 = [1; 32];
        let addr2 = [2; 32];

        state_store
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();

        // Calculate state root
        let state_root = state_store.calculate_state_root(1, 12345).unwrap();

        // Verify state root is not zero
        assert_ne!(state_root.root_hash, [0; 32]);
        assert_eq!(state_root.block_height, 1);
        assert_eq!(state_root.timestamp, 12345);

        // Get state root
        let retrieved_root = state_store.get_state_root().unwrap();
        assert_eq!(retrieved_root, state_root);

        // Modify state and check that root is invalidated
        state_store.update_balance_compat(&addr1, 1500).unwrap();

        // Calculate new root
        let new_root = state_store.calculate_state_root(2, 12346).unwrap();
        assert_ne!(new_root.root_hash, state_root.root_hash);
    }

    #[test]
    fn test_get_all_accounts() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        // Create some accounts
        let addr1 = [1; 32];
        let addr2 = [2; 32];
        let addr3 = [3; 32];

        state_store
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr3, 3000, AccountType::Contract)
            .unwrap();

        // Get all accounts
        let accounts = state_store.get_all_accounts();
        assert_eq!(accounts.len(), 3);

        // Verify account data
        let mut found_addr1 = false;
        let mut found_addr2 = false;
        let mut found_addr3 = false;

        for (addr, state) in accounts {
            if addr == addr1 {
                assert_eq!(state.balance, 1000);
                found_addr1 = true;
            } else if addr == addr2 {
                assert_eq!(state.balance, 2000);
                found_addr2 = true;
            } else if addr == addr3 {
                assert_eq!(state.balance, 3000);
                assert_eq!(state.account_type, AccountType::Contract);
                found_addr3 = true;
            }
        }

        assert!(found_addr1 && found_addr2 && found_addr3);
    }

    #[test]
    fn test_account_cache() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::with_cache_size(&kv_store, 2); // Small cache for testing

        // Create some accounts
        let addr1 = [1; 32];
        let addr2 = [2; 32];
        let addr3 = [3; 32];

        state_store
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();

        // Access accounts to populate cache
        state_store.get_account_state(&addr1);
        state_store.get_account_state(&addr2);

        // Add a third account, which should evict one from the cache
        state_store
            .create_account(&addr3, 3000, AccountType::User)
            .unwrap();
        state_store.get_account_state(&addr3);

        // Clear cache
        state_store.clear_cache();

        // Cache should be empty now
        assert_eq!(state_store.account_cache.len(), 0);
    }

    #[test]
    fn test_apply_block() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);
        let tx_store = TxStore::new(&kv_store);

        // Create sender and recipient accounts
        let sender = [1; 32];
        let recipient = [2; 32];

        state_store
            .create_account(&sender, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&recipient, 500, AccountType::User)
            .unwrap();

        // Create a transaction
        let tx = TransactionRecord {
            tx_id: [10; 32],
            sender,
            recipient,
            value: 300,
            gas_price: 5,
            gas_limit: 21000,
            gas_used: 10,
            nonce: 0, // Matches the sender's initial nonce
            timestamp: 12345,
            block_height: 1, // Will be included in block 1
            data: None,
            status: TransactionStatus::Included,
        };

        // Store the transaction
        tx_store.put_transaction(&tx).unwrap();

        // Create a block with this transaction
        let block = Block {
            height: 1,
            hash: [20; 32],
            prev_hash: [0; 32],
            timestamp: 12345,
            transactions: vec![tx.tx_id],
            miner: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0; 32], // This would normally be calculated
            nonce: 42,
            poh_seq: 100,
            poh_hash: [30; 32],
            difficulty: 1000,
            total_difficulty: 1000,
        };

        // Apply the block
        state_store.apply_block(&block, &tx_store).unwrap();

        // Verify account states
        let sender_state = state_store.get_account_state(&sender).unwrap();
        let recipient_state = state_store.get_account_state(&recipient).unwrap();

        // Sender should have 1000 - 300 - (10 * 5) = 650 balance and nonce incremented
        assert_eq!(sender_state.balance, 650);
        assert_eq!(sender_state.nonce, 1);

        // Recipient should have 500 + 300 = 800 balance
        assert_eq!(recipient_state.balance, 800);

        // Transaction status should be updated to Confirmed
        let updated_tx = tx_store.get_transaction(&tx.tx_id).unwrap();
        assert_eq!(updated_tx.status, TransactionStatus::Confirmed);
    }

    #[test]
    fn test_apply_block_with_invalid_transactions() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);
        let tx_store = TxStore::new(&kv_store);

        // Create sender and recipient accounts
        let sender = [1; 32];
        let recipient = [2; 32];

        state_store
            .create_account(&sender, 100, AccountType::User)
            .unwrap(); // Only 100 balance
        state_store
            .create_account(&recipient, 500, AccountType::User)
            .unwrap();

        // Create a transaction with insufficient balance
        let tx1 = TransactionRecord {
            tx_id: [10; 32],
            sender,
            recipient,
            value: 300, // More than sender's balance
            gas_price: 5,
            gas_limit: 21000,
            gas_used: 10,
            nonce: 0,
            timestamp: 12345,
            block_height: 1,
            data: None,
            status: TransactionStatus::Included,
        };

        // Create a transaction with invalid nonce
        let tx2 = TransactionRecord {
            tx_id: [11; 32],
            sender,
            recipient,
            value: 50, // Valid amount
            gas_price: 5,
            gas_limit: 21000,
            gas_used: 10,
            nonce: 5, // Invalid nonce (should be 0)
            timestamp: 12345,
            block_height: 1,
            data: None,
            status: TransactionStatus::Included,
        };

        // Store the transactions
        tx_store.put_transaction(&tx1).unwrap();
        tx_store.put_transaction(&tx2).unwrap();

        // Create a block with these transactions
        let block = Block {
            height: 1,
            hash: [20; 32],
            prev_hash: [0; 32],
            timestamp: 12345,
            transactions: vec![tx1.tx_id, tx2.tx_id],
            miner: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0; 32],
            nonce: 42,
            poh_seq: 100,
            poh_hash: [30; 32],
            difficulty: 1000,
            total_difficulty: 1000,
        };

        // Apply the block
        state_store.apply_block(&block, &tx_store).unwrap();

        // Verify account states - should be unchanged since both transactions failed
        let sender_state = state_store.get_account_state(&sender).unwrap();
        let recipient_state = state_store.get_account_state(&recipient).unwrap();

        assert_eq!(sender_state.balance, 100); // Unchanged
        assert_eq!(sender_state.nonce, 0); // Unchanged
        assert_eq!(recipient_state.balance, 500); // Unchanged

        // Transaction statuses should be updated to Failed
        let updated_tx1 = tx_store.get_transaction(&tx1.tx_id).unwrap();
        match updated_tx1.status {
            TransactionStatus::Failed(TransactionError::InsufficientBalance) => {} // Expected
            _ => panic!("Expected InsufficientBalance error"),
        }

        let updated_tx2 = tx_store.get_transaction(&tx2.tx_id).unwrap();
        match updated_tx2.status {
            TransactionStatus::Failed(TransactionError::InvalidNonce) => {} // Expected
            _ => panic!("Expected InvalidNonce error"),
        }
    }

    #[test]
    fn test_state_root_calculation() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        // Create some accounts
        let addr1 = [1; 32];
        let addr2 = [2; 32];
        let addr3 = [3; 32];

        state_store
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr3, 3000, AccountType::User)
            .unwrap();

        // Calculate the state root
        let state_root = state_store.calculate_state_root(1, 12345).unwrap();

        // The root hash should not be all zeros
        assert_ne!(state_root.root_hash, [0; 32]);

        // Create a second state store with the same accounts
        let state_store2 = StateStore::new(&kv_store);

        // Create the same accounts
        state_store2
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store2
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();
        state_store2
            .create_account(&addr3, 3000, AccountType::User)
            .unwrap();

        // Calculate the state root again
        let state_root2 = state_store2.calculate_state_root(1, 12345).unwrap();

        // The root hashes should be the same
        assert_eq!(state_root.root_hash, state_root2.root_hash);

        // Modify an account
        state_store2.update_balance_compat(&addr1, 1500).unwrap();

        // Calculate the state root again
        let state_root3 = state_store2.calculate_state_root(1, 12345).unwrap();

        // The root hash should be different now
        assert_ne!(state_root.root_hash, state_root3.root_hash);
    }

    #[test]
    fn test_account_proof() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path());
        let state_store = StateStore::new(&kv_store);

        // Create some accounts
        let addr1 = [1; 32];
        let addr2 = [2; 32];
        let addr3 = [3; 32];

        state_store
            .create_account(&addr1, 1000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr2, 2000, AccountType::User)
            .unwrap();
        state_store
            .create_account(&addr3, 3000, AccountType::User)
            .unwrap();

        // Calculate the state root
        let state_root = state_store.calculate_state_root(1, 12345).unwrap();

        // Generate a proof for addr2
        let proof = state_store.generate_account_proof(&addr2).unwrap();

        // Verify the proof
        assert!(StateStore::verify_account_proof(
            &proof,
            &state_root.root_hash
        ));

        // The proof should contain the account state
        assert!(proof.value.is_some());

        // Deserialize the account state
        let account_state: AccountState = bincode::deserialize(&proof.value.unwrap()).unwrap();

        // Verify the account state
        assert_eq!(account_state.balance, 2000);
        assert_eq!(account_state.account_type, AccountType::User);

        // Generate a proof for a non-existent account
        let non_existent = [99; 32];
        let proof = state_store.generate_account_proof(&non_existent).unwrap();

        // Verify the proof
        assert!(StateStore::verify_account_proof(
            &proof,
            &state_root.root_hash
        ));

        // The proof should not contain an account state
        assert!(proof.value.is_none());

        // Tamper with the proof
        let mut tampered_proof = state_store.generate_account_proof(&addr2).unwrap();
        if let Some(ref mut value) = tampered_proof.value {
            // Change the balance
            let mut account_state: AccountState = bincode::deserialize(value).unwrap();
            account_state.balance = 9999;
            *value = bincode::serialize(&account_state).unwrap();
        }

        // Verify the tampered proof (should fail)
        assert!(!StateStore::verify_account_proof(
            &tampered_proof,
            &state_root.root_hash
        ));
    }
}

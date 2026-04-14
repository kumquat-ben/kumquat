use log::error;
use std::sync::Arc;

use crate::crypto::keys::VibePublicKey;
use crate::crypto::signer::VibeSignature;
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::{TransactionRecord, TxStore};
// use crate::mempool::types::TransactionRecord as MempoolTransactionRecord;

/// Result of transaction validation
#[derive(Debug, PartialEq)]
pub enum TransactionValidationResult {
    /// Transaction is valid
    Valid,

    /// Transaction is invalid
    Invalid(String),

    /// Transaction is already known
    AlreadyKnown,
}

/// Validator for transactions
pub struct TransactionValidator<'a> {
    /// Transaction store
    tx_store: Arc<TxStore<'a>>,

    /// State store
    state_store: Arc<StateStore<'a>>,
}

impl<'a> TransactionValidator<'a> {
    /// Create a new transaction validator
    pub fn new(tx_store: Arc<TxStore<'a>>, state_store: Arc<StateStore<'a>>) -> Self {
        Self {
            tx_store,
            state_store,
        }
    }

    /// Validate a transaction
    pub fn validate_transaction(
        &self,
        tx: &TransactionRecord,
        signature: &VibeSignature,
        sender_pubkey: &VibePublicKey,
    ) -> TransactionValidationResult {
        // Check if the transaction is already known
        if let Ok(Some(_)) = self.tx_store.get_transaction(&tx.tx_id) {
            return TransactionValidationResult::AlreadyKnown;
        }

        // Verify the signature
        if !self.verify_signature(tx, signature, sender_pubkey) {
            return TransactionValidationResult::Invalid("Invalid signature".to_string());
        }

        // Check sender balance
        if !self.check_balance(tx, &sender_pubkey.address()) {
            return TransactionValidationResult::Invalid("Insufficient balance".to_string());
        }

        // Check nonce
        if !self.check_nonce(tx, &sender_pubkey.address()) {
            return TransactionValidationResult::Invalid("Invalid nonce".to_string());
        }

        // All checks passed
        TransactionValidationResult::Valid
    }

    /// Serialize a transaction for signature verification
    fn serialize_for_signing(&self, tx: &TransactionRecord) -> Vec<u8> {
        // Create a canonical representation for signing
        let mut data = Vec::new();

        // Add all transaction fields except signature
        data.extend_from_slice(&tx.sender);
        data.extend_from_slice(&tx.recipient);
        for token_id in &tx.transfer_token_ids {
            data.extend_from_slice(token_id);
        }
        if let Some(fee_token_id) = tx.fee_token_id {
            data.extend_from_slice(&fee_token_id);
        }
        data.extend_from_slice(&tx.value.to_be_bytes());
        data.extend_from_slice(&tx.gas_price.to_be_bytes());
        data.extend_from_slice(&tx.gas_limit.to_be_bytes());
        data.extend_from_slice(&tx.nonce.to_be_bytes());
        data.extend_from_slice(&tx.timestamp.to_be_bytes());

        // Add optional data if present
        if let Some(tx_data) = &tx.data {
            data.extend_from_slice(tx_data);
        }

        data
    }

    /// Verify the transaction signature
    fn verify_signature(
        &self,
        tx: &TransactionRecord,
        signature: &VibeSignature,
        sender_pubkey: &VibePublicKey,
    ) -> bool {
        // Get the serialized transaction data for signing
        let tx_data = self.serialize_for_signing(tx);

        // Convert VibePublicKey to ed25519_dalek PublicKey
        match sender_pubkey.to_dalek_pubkey() {
            Ok(pubkey) => {
                // Verify the signature using the crypto module
                crate::crypto::signer::verify_signature(&tx_data, signature, &pubkey)
            }
            Err(e) => {
                error!("Failed to convert public key: {:?}", e);
                false
            }
        }
    }

    /// Check if the sender has sufficient balance
    fn check_balance(&self, tx: &TransactionRecord, sender_address: &[u8; 32]) -> bool {
        // Get the sender's account state
        let sender_state = match self.state_store.get_account_state(sender_address) {
            Some(state) => state,
            None => {
                // If the account doesn't exist, it has zero balance
                return false;
            }
        };

        if tx.fee_token_id.is_none() {
            return false;
        }

        if tx.transfer_token_ids.is_empty() {
            return false;
        }

        if let Some(fee_token_id) = tx.fee_token_id {
            if tx
                .transfer_token_ids
                .iter()
                .any(|token_id| *token_id == fee_token_id)
            {
                return false;
            }

            if !sender_state.owns_token(&fee_token_id) {
                return false;
            }
        }

        let transfer_tokens_owned = tx
            .transfer_token_ids
            .iter()
            .all(|token_id| sender_state.owns_token(token_id));
        if !transfer_tokens_owned {
            return false;
        }

        let payment_total = tx
            .transfer_token_ids
            .iter()
            .filter_map(|token_id| sender_state.token_value(token_id))
            .sum::<u64>();
        let fee_total = tx
            .fee_token_id
            .and_then(|token_id| sender_state.token_value(&token_id))
            .unwrap_or(0);

        payment_total == tx.value && sender_state.total_token_value() >= payment_total + fee_total
    }

    /// Check if the transaction nonce is valid
    fn check_nonce(&self, tx: &TransactionRecord, sender_address: &[u8; 32]) -> bool {
        // Get the sender's account state
        let sender_state = match self.state_store.get_account_state(sender_address) {
            Some(state) => state,
            None => {
                // If the account doesn't exist, only nonce 0 is valid
                return false;
            }
        };

        tx.nonce == sender_state.nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::VibeKeypair;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::{AccountType, TransactionStatus};
    use tempfile::tempdir;

    #[test]
    fn test_transaction_validation() {
        // Create a temporary directory for the database
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();

        // Create the stores
        let tx_store = Arc::new(TxStore::new(&kv_store));
        let state_store = Arc::new(StateStore::new(&kv_store));

        // Create a validator
        let validator = TransactionValidator::new(tx_store.clone(), state_store.clone());

        // Create a keypair
        let keypair = VibeKeypair::generate();
        let pubkey = VibePublicKey::from(keypair.public);

        // Create an account with some balance
        let address = keypair.address();
        state_store
            .create_account(&address, 1000, AccountType::User)
            .unwrap();

        // Create a transaction
        let tx = TransactionRecord {
            tx_id: [1u8; 32],
            sender: address,
            recipient: [2u8; 32],
            transfer_token_ids: vec![[4u8; 32]],
            fee_token_id: Some([5u8; 32]),
            value: 100,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 10,
            nonce: 0,
            timestamp: 1,
            block_height: 0,
            data: None,
            status: TransactionStatus::Pending,
        };

        // Create a dummy signature
        let signature = VibeSignature::new([0u8; 64]);

        // The dummy signature should fail.
        let result = validator.validate_transaction(&tx, &signature, &pubkey);
        assert!(matches!(result, TransactionValidationResult::Invalid(_)));

        // Store the transaction
        tx_store.put_transaction(&tx).unwrap();

        // Try to validate the same transaction again
        let result = validator.validate_transaction(&tx, &signature, &pubkey);

        // The transaction should be already known
        assert_eq!(result, TransactionValidationResult::AlreadyKnown);
    }
}

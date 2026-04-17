use log::error;
use std::sync::Arc;

use crate::crypto::keys::VibePublicKey;
use crate::crypto::signer::VibeSignature;
use crate::storage::state::{ConversionOrderRequest, ConversionTransaction, Denomination};
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
        for denomination in Denomination::all_descending() {
            data.extend_from_slice(&tx.coin_transfer.count(*denomination).to_be_bytes());
        }
        for denomination in Denomination::all_descending() {
            data.extend_from_slice(&tx.coin_fee.count(*denomination).to_be_bytes());
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

        serialize_conversion_intent(&mut data, tx.conversion_intent.as_ref());

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

        if tx.fee_token_id.is_none() && tx.coin_fee.is_empty() {
            return false;
        }

        if let Some(intent) = &tx.conversion_intent {
            return self.check_conversion_balance(tx, sender_state, intent);
        }

        if tx.transfer_token_ids.is_empty() && tx.coin_transfer.is_empty() {
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

        if !sender_state.coin_inventory.can_cover(&tx.coin_transfer)
            || !sender_state.coin_inventory.can_cover(&tx.coin_fee)
        {
            return false;
        }

        let payment_total = tx
            .transfer_token_ids
            .iter()
            .filter_map(|token_id| sender_state.token_value(token_id))
            .sum::<u64>();
        let coin_total = tx.coin_transfer.total_value_cents();
        let fee_total = tx
            .fee_token_id
            .and_then(|token_id| sender_state.token_value(&token_id))
            .unwrap_or(0);
        let coin_fee_total = tx.coin_fee.total_value_cents();

        payment_total + coin_total == tx.value
            && sender_state.total_account_value()
                >= payment_total + coin_total + fee_total + coin_fee_total
    }

    fn check_conversion_balance(
        &self,
        tx: &TransactionRecord,
        sender_state: crate::storage::AccountState,
        intent: &ConversionTransaction,
    ) -> bool {
        if !tx.transfer_token_ids.is_empty() || !tx.coin_transfer.is_empty() {
            return false;
        }

        if !sender_state.coin_inventory.can_cover(&tx.coin_fee) {
            return false;
        }

        if let Some(existing) = &sender_state.conversion_order {
            match intent {
                ConversionTransaction::Create(_) => return false,
                ConversionTransaction::Cancel { order_id } => {
                    if existing.order_id != *order_id {
                        return false;
                    }
                }
                ConversionTransaction::ClearDead { order_id } => {
                    let clearable = matches!(
                        existing.status,
                        crate::storage::ConversionOrderStatus::Expired
                            | crate::storage::ConversionOrderStatus::Failed
                    ) || tx.block_height >= existing.cycle_end_block;
                    if existing.order_id != *order_id || !clearable {
                        return false;
                    }
                }
            }
        } else if matches!(
            intent,
            ConversionTransaction::Cancel { .. } | ConversionTransaction::ClearDead { .. }
        ) {
            return false;
        }

        match intent {
            ConversionTransaction::Create(request) => {
                if request.requested_value_cents == 0 {
                    return false;
                }
            }
            ConversionTransaction::Cancel { .. } | ConversionTransaction::ClearDead { .. } => {}
        }

        let fee_total = tx.coin_fee.total_value_cents()
            + tx.fee_token_id
                .and_then(|token_id| sender_state.token_value(&token_id))
                .unwrap_or(0);
        sender_state.total_account_value() >= fee_total
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

fn serialize_conversion_intent(data: &mut Vec<u8>, intent: Option<&ConversionTransaction>) {
    match intent {
        None => data.push(0),
        Some(ConversionTransaction::Create(request)) => {
            data.push(1);
            serialize_conversion_request(data, request);
        }
        Some(ConversionTransaction::Cancel { order_id }) => {
            data.push(2);
            data.extend_from_slice(order_id);
        }
        Some(ConversionTransaction::ClearDead { order_id }) => {
            data.push(3);
            data.extend_from_slice(order_id);
        }
    }
}

fn serialize_conversion_request(data: &mut Vec<u8>, request: &ConversionOrderRequest) {
    data.push(match request.kind {
        crate::storage::ConversionOrderKind::BillToCoins => 1,
        crate::storage::ConversionOrderKind::CoinsToBill => 2,
    });
    data.extend_from_slice(&request.requested_value_cents.to_be_bytes());
    for denomination in Denomination::all_descending() {
        data.extend_from_slice(
            &request
                .requested_coin_inventory
                .count(*denomination)
                .to_be_bytes(),
        );
    }
    data.extend_from_slice(&(request.requested_bill_denominations.len() as u64).to_be_bytes());
    for denomination in &request.requested_bill_denominations {
        data.extend_from_slice(&denomination.value_cents().to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::VibeKeypair;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::{
        AccountType, CoinInventory, ConversionOrderKind, ConversionOrderRequest,
        ConversionTransaction, TransactionStatus,
    };
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
            coin_transfer: crate::storage::CoinInventory::default(),
            coin_fee: crate::storage::CoinInventory::default(),
            value: 100,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 10,
            nonce: 0,
            timestamp: 1,
            block_height: 0,
            data: None,
            conversion_intent: None,
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

    #[test]
    fn test_conversion_transaction_validation() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let tx_store = Arc::new(TxStore::new(&kv_store));
        let state_store = Arc::new(StateStore::new(&kv_store));
        let validator = TransactionValidator::new(tx_store.clone(), state_store.clone());

        let keypair = VibeKeypair::generate();
        let address = keypair.address();
        state_store
            .create_account(&address, 100, AccountType::User)
            .unwrap();

        let mut state = state_store.get_account_state(&address).unwrap();
        state
            .coin_inventory
            .add(crate::storage::Denomination::Cents1, 3)
            .unwrap();
        state.sync_balance_from_hybrid();
        state_store.set_account_state(&address, &state).unwrap();

        let mut fee = CoinInventory::default();
        fee.add(crate::storage::Denomination::Cents1, 1).unwrap();

        let tx = TransactionRecord {
            tx_id: [12u8; 32],
            sender: address,
            recipient: [0u8; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: fee,
            value: 0,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 10,
            nonce: 0,
            timestamp: 2,
            block_height: 0,
            data: None,
            conversion_intent: Some(ConversionTransaction::Create(ConversionOrderRequest {
                kind: ConversionOrderKind::BillToCoins,
                requested_value_cents: 25,
                requested_coin_inventory: CoinInventory::default(),
                requested_bill_denominations: Vec::new(),
            })),
            status: TransactionStatus::Pending,
        };

        assert!(validator.check_balance(&tx, &address));
        assert!(validator.check_nonce(&tx, &address));
    }
}

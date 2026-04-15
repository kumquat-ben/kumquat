use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::crypto::keys::VibeKeypair;
use crate::crypto::signer::{sign_message, VibeSignature};
use crate::storage::state::{CoinInventory, ConversionOrderRequest, ConversionTransaction, Denomination};

/// Type alias for address (public key hash)
pub type Address = [u8; 32];

/// Type alias for transaction hash
pub type Hash = [u8; 32];

/// Transaction record structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionRecord {
    /// Unique transaction ID (hash)
    pub tx_id: Hash,

    /// Sender address
    pub sender: Address,

    /// Recipient address
    pub recipient: Address,

    /// Exact bill object IDs that move from sender to recipient.
    pub transfer_token_ids: Vec<Hash>,

    /// Exact bill object ID selected as the validator/miner fee.
    pub fee_token_id: Option<Hash>,

    /// Fungible coin inventory that moves from sender to recipient.
    #[serde(default)]
    pub coin_transfer: CoinInventory,

    /// Fungible coin inventory consumed as fees.
    #[serde(default)]
    pub coin_fee: CoinInventory,

    /// Compatibility mirror of the transferred value in cents.
    pub value: u64,

    /// Gas price (fee per gas unit)
    pub gas_price: u64,

    /// Gas limit (maximum gas units)
    pub gas_limit: u64,

    /// Account nonce (prevents replay attacks)
    pub nonce: u64,

    /// Timestamp when the transaction was created
    pub timestamp: u64,

    /// Transaction signature
    pub signature: VibeSignature,

    /// Optional data payload
    pub data: Option<Vec<u8>>,

    /// Optional conversion-order intent handled separately from normal transfers.
    #[serde(default)]
    pub conversion_intent: Option<ConversionTransaction>,
}

impl TransactionRecord {
    /// Create a new transaction record
    pub fn new(
        sender: Address,
        recipient: Address,
        value: u64,
        gas_price: u64,
        gas_limit: u64,
        nonce: u64,
        data: Option<Vec<u8>>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create a placeholder signature
        let signature = VibeSignature::new([0u8; 64]);

        // Create the transaction without a valid tx_id
        let mut tx = Self {
            tx_id: [0u8; 32],
            sender,
            recipient,
            transfer_token_ids: Vec::new(),
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: CoinInventory::default(),
            value,
            gas_price,
            gas_limit,
            nonce,
            timestamp,
            signature,
            data,
            conversion_intent: None,
        };

        // Compute the transaction ID based on the contents
        tx.tx_id = tx.compute_tx_id();

        tx
    }

    /// Serialize the transaction for signing (excluding the signature)
    pub fn serialize_for_signing(&self) -> Vec<u8> {
        // Create a canonical representation for signing
        let mut data = Vec::new();

        // Add all transaction fields except signature
        data.extend_from_slice(&self.sender);
        data.extend_from_slice(&self.recipient);
        for token_id in &self.transfer_token_ids {
            data.extend_from_slice(token_id);
        }
        if let Some(fee_token_id) = self.fee_token_id {
            data.extend_from_slice(&fee_token_id);
        }
        serialize_coin_inventory(&mut data, &self.coin_transfer);
        serialize_coin_inventory(&mut data, &self.coin_fee);
        data.extend_from_slice(&self.value.to_be_bytes());
        data.extend_from_slice(&self.gas_price.to_be_bytes());
        data.extend_from_slice(&self.gas_limit.to_be_bytes());
        data.extend_from_slice(&self.nonce.to_be_bytes());
        data.extend_from_slice(&self.timestamp.to_be_bytes());

        // Add optional data if present
        if let Some(tx_data) = &self.data {
            data.extend_from_slice(tx_data);
        }

        serialize_conversion_intent(&mut data, self.conversion_intent.as_ref());

        data
    }

    /// Compute the transaction ID (hash of all fields except signature)
    pub fn compute_tx_id(&self) -> Hash {
        use crate::crypto::hash::sha256;

        // Get the serialized transaction data
        let data = self.serialize_for_signing();

        // Hash the data to create the transaction ID
        sha256(&data)
    }

    /// Sign the transaction with the given keypair
    pub fn sign(&mut self, keypair: &VibeKeypair) {
        // Make sure the transaction ID is computed
        self.tx_id = self.compute_tx_id();

        // Get the data to sign (serialized transaction)
        let data = self.serialize_for_signing();

        // Sign the data
        self.signature = sign_message(keypair, &data);
    }

    /// Create a signed transaction
    pub fn create_signed(
        keypair: &VibeKeypair,
        recipient: Address,
        value: u64,
        gas_price: u64,
        gas_limit: u64,
        nonce: u64,
        data: Option<Vec<u8>>,
    ) -> Self {
        // Get the sender's address
        let sender = keypair.address();

        // Create an unsigned transaction
        let mut tx = Self::new(sender, recipient, value, gas_price, gas_limit, nonce, data);

        // Sign the transaction
        tx.sign(keypair);

        tx
    }

    /// Get the total gas cost (gas_price * gas_limit)
    pub fn gas_cost(&self) -> u64 {
        self.gas_price * self.gas_limit
    }

    /// Get the total transaction cost (value + gas_cost)
    pub fn total_cost(&self) -> u64 {
        self.value + self.coin_fee.total_value_cents() + self.gas_cost()
    }

    pub fn has_bill_transfers(&self) -> bool {
        !self.transfer_token_ids.is_empty()
    }

    pub fn has_coin_transfers(&self) -> bool {
        !self.coin_transfer.is_empty()
    }

    pub fn has_any_transfers(&self) -> bool {
        self.has_bill_transfers() || self.has_coin_transfers()
    }

    pub fn has_any_fee_inputs(&self) -> bool {
        self.fee_token_id.is_some() || !self.coin_fee.is_empty()
    }

    pub fn has_conversion_intent(&self) -> bool {
        self.conversion_intent.is_some()
    }

    /// Check if the transaction is expired
    pub fn is_expired(&self, max_age_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        now - self.timestamp > max_age_secs
    }
}

/// Transaction status in the mempool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction is pending in the mempool
    Pending,

    /// Transaction is included in a block
    Included,

    /// Transaction is rejected
    Rejected,

    /// Transaction is expired
    Expired,
}

/// Implement Ord for TransactionRecord to enable priority queue
impl Ord for TransactionRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by gas price (higher gas price has higher priority)
        other
            .gas_price
            .cmp(&self.gas_price)
            // Then compare by timestamp (older transactions have higher priority)
            .then(self.timestamp.cmp(&other.timestamp))
    }
}

impl PartialOrd for TransactionRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TransactionRecord {
    fn eq(&self, other: &Self) -> bool {
        self.tx_id == other.tx_id
    }
}

impl Eq for TransactionRecord {}

fn serialize_coin_inventory(data: &mut Vec<u8>, inventory: &CoinInventory) {
    for denomination in Denomination::all_descending() {
        data.extend_from_slice(&inventory.count(*denomination).to_be_bytes());
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
    serialize_coin_inventory(data, &request.requested_coin_inventory);
    data.extend_from_slice(&(request.requested_bill_denominations.len() as u64).to_be_bytes());
    for denomination in &request.requested_bill_denominations {
        data.extend_from_slice(&denomination.value_cents().to_be_bytes());
    }
}

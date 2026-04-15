//! Core state structures and abstractions for Kumquat blockchain
//!
//! This module defines the fundamental state structures used throughout the blockchain,
//! including account states, global state, and state transitions. It provides a clear
//! abstraction layer for representing and manipulating blockchain state.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::fmt;
use thiserror::Error;

static DENOMINATION_INDEX: Lazy<DenominationIndexDocument> = Lazy::new(|| {
    serde_json::from_str(include_str!("../kumquat_denominations.json"))
        .expect("denomination index JSON must be valid")
});

static DENOMINATION_RANGES_ASC: Lazy<Vec<DenominationRange>> = Lazy::new(|| {
    let ascending = [
        Denomination::Cents1,
        Denomination::Cents5,
        Denomination::Cents10,
        Denomination::Cents25,
        Denomination::Cents50,
        Denomination::Dollars1,
        Denomination::Dollars2,
        Denomination::Dollars5,
        Denomination::Dollars10,
        Denomination::Dollars20,
        Denomination::Dollars50,
        Denomination::Dollars100,
    ];

    let mut cursor = 0u64;
    ascending
        .into_iter()
        .filter_map(|denomination| {
            let count = DENOMINATION_INDEX
                .denominations
                .iter()
                .find_map(|entry| {
                    let parsed = Denomination::parse(entry.denomination.trim_start_matches('$'))?;
                    (parsed == denomination).then_some(entry.count)
                })
                .unwrap_or(0);

            if count == 0 {
                return None;
            }

            let range = DenominationRange {
                denomination,
                start_index: cursor,
                count,
            };
            cursor += count;
            Some(range)
        })
        .collect()
});

#[derive(Debug, Deserialize)]
struct DenominationIndexDocument {
    total_units: u64,
    denominations: Vec<DenominationIndexEntry>,
}

#[derive(Debug, Deserialize)]
struct DenominationIndexEntry {
    denomination: String,
    count: u64,
}

#[derive(Debug, Clone, Copy)]
struct DenominationRange {
    denomination: Denomination,
    start_index: u64,
    count: u64,
}

impl DenominationRange {
    fn end_exclusive(&self) -> u64 {
        self.start_index + self.count
    }
}

/// Error type for state operations
#[derive(Debug, Error)]
pub enum StateError {
    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Invalid state transition
    #[error("Invalid state transition: {0}")]
    InvalidStateTransition(String),

    /// Invalid account
    #[error("Invalid account: {0}")]
    InvalidAccount(String),

    /// Invalid storage key
    #[error("Invalid storage key: {0}")]
    InvalidStorageKey(String),

    /// Invalid storage value
    #[error("Invalid storage value: {0}")]
    InvalidStorageValue(String),

    /// Insufficient balance
    #[error("Insufficient balance: required {0}, available {1}")]
    InsufficientBalance(u64, u64),

    /// Invalid nonce
    #[error("Invalid nonce: expected {0}, got {1}")]
    InvalidNonce(u64, u64),

    /// Other error
    #[error("Other error: {0}")]
    Other(String),
}

/// Result type for state operations
pub type StateResult<T> = Result<T, StateError>;

/// Monetary amount in USD cents.
pub type AmountCents = u64;

/// Account type
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountType {
    /// Regular user account
    User,

    /// Smart contract account
    Contract,

    /// System account (for special operations)
    System,

    /// Validator account (for consensus participation)
    Validator,
}

impl fmt::Display for AccountType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountType::User => write!(f, "User"),
            AccountType::Contract => write!(f, "Contract"),
            AccountType::System => write!(f, "System"),
            AccountType::Validator => write!(f, "Validator"),
        }
    }
}

/// Fixed USD denominations supported by the ledger.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Denomination {
    Dollars100,
    Dollars50,
    Dollars20,
    Dollars10,
    Dollars5,
    Dollars2,
    Dollars1,
    Cents50,
    Cents25,
    Cents10,
    Cents5,
    Cents1,
}

impl Denomination {
    pub fn value_cents(&self) -> AmountCents {
        match self {
            Denomination::Dollars100 => 10_000,
            Denomination::Dollars50 => 5_000,
            Denomination::Dollars20 => 2_000,
            Denomination::Dollars10 => 1_000,
            Denomination::Dollars5 => 500,
            Denomination::Dollars2 => 200,
            Denomination::Dollars1 => 100,
            Denomination::Cents50 => 50,
            Denomination::Cents25 => 25,
            Denomination::Cents10 => 10,
            Denomination::Cents5 => 5,
            Denomination::Cents1 => 1,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Denomination::Dollars100 => "100",
            Denomination::Dollars50 => "50",
            Denomination::Dollars20 => "20",
            Denomination::Dollars10 => "10",
            Denomination::Dollars5 => "5",
            Denomination::Dollars2 => "2",
            Denomination::Dollars1 => "1",
            Denomination::Cents50 => "0.5",
            Denomination::Cents25 => "0.25",
            Denomination::Cents10 => "0.1",
            Denomination::Cents5 => "0.05",
            Denomination::Cents1 => "0.01",
        }
    }

    pub fn all_descending() -> &'static [Denomination] {
        &[
            Denomination::Dollars100,
            Denomination::Dollars50,
            Denomination::Dollars20,
            Denomination::Dollars10,
            Denomination::Dollars5,
            Denomination::Dollars2,
            Denomination::Dollars1,
            Denomination::Cents50,
            Denomination::Cents25,
            Denomination::Cents10,
            Denomination::Cents5,
            Denomination::Cents1,
        ]
    }

    pub fn reward_set() -> Vec<Denomination> {
        Self::all_descending().to_vec()
    }

    pub fn from_assignment_index(assignment_index: u64) -> Option<Self> {
        DENOMINATION_RANGES_ASC
            .iter()
            .find(|range| {
                assignment_index >= range.start_index && assignment_index < range.end_exclusive()
            })
            .map(|range| range.denomination)
    }

    pub fn assignment_range(&self) -> Option<(u64, u64)> {
        DENOMINATION_RANGES_ASC
            .iter()
            .find(|range| range.denomination == *self)
            .map(|range| (range.start_index, range.count))
    }

    pub fn total_assignment_count() -> u64 {
        DENOMINATION_INDEX.total_units
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim() {
            "100" | "100.0" | "100.00" => Some(Denomination::Dollars100),
            "50" | "50.0" | "50.00" => Some(Denomination::Dollars50),
            "20" | "20.0" | "20.00" => Some(Denomination::Dollars20),
            "10" | "10.0" | "10.00" => Some(Denomination::Dollars10),
            "5" | "5.0" | "5.00" => Some(Denomination::Dollars5),
            "2" | "2.0" | "2.00" => Some(Denomination::Dollars2),
            "1" | "1.0" | "1.00" => Some(Denomination::Dollars1),
            "0.5" | "0.50" => Some(Denomination::Cents50),
            "0.25" | ".25" => Some(Denomination::Cents25),
            "0.1" | "0.10" | ".1" => Some(Denomination::Cents10),
            "0.05" | ".05" => Some(Denomination::Cents5),
            "0.01" | ".01" => Some(Denomination::Cents1),
            _ => None,
        }
    }

    pub fn is_bill(&self) -> bool {
        matches!(
            self,
            Denomination::Dollars100
                | Denomination::Dollars50
                | Denomination::Dollars20
                | Denomination::Dollars10
                | Denomination::Dollars5
                | Denomination::Dollars2
                | Denomination::Dollars1
        )
    }

    pub fn is_coin(&self) -> bool {
        matches!(
            self,
            Denomination::Cents50
                | Denomination::Cents25
                | Denomination::Cents10
                | Denomination::Cents5
                | Denomination::Cents1
        )
    }
}

impl fmt::Display for Denomination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Why a token was minted.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenMintSource {
    Genesis,
    BlockReward,
    LegacyBalanceBootstrap,
    TransferChange,
}

/// A unique owned denomination token.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DenominationToken {
    pub token_id: [u8; 32],
    pub version: u64,
    pub assignment_index: u64,
    pub owner: [u8; 32],
    pub minted_at_block: u64,
    pub mint_source: TokenMintSource,
    pub metadata: HashMap<String, String>,
}

impl DenominationToken {
    pub fn new(
        owner: [u8; 32],
        assignment_index: u64,
        minted_at_block: u64,
        mint_source: TokenMintSource,
    ) -> Self {
        assert!(
            Denomination::from_assignment_index(assignment_index).is_some(),
            "assignment index must map to a known denomination"
        );
        let token_id = assignment_index_to_token_id(assignment_index);

        Self {
            token_id,
            version: 0,
            assignment_index,
            owner,
            minted_at_block,
            mint_source,
            metadata: HashMap::new(),
        }
    }

    pub fn denomination(&self) -> Denomination {
        Denomination::from_assignment_index(self.assignment_index)
            .expect("stored assignment index must map to a denomination")
    }

    pub fn value_cents(&self) -> AmountCents {
        self.denomination().value_cents()
    }
}

/// A non-fungible bill tracked as a discrete ledger object.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BillToken {
    pub bill_id: [u8; 32],
    pub version: u64,
    pub denomination: Denomination,
    pub owner: [u8; 32],
    pub minted_at_block: u64,
    pub mint_source: TokenMintSource,
    pub metadata: HashMap<String, String>,
}

impl BillToken {
    pub fn new(
        owner: [u8; 32],
        denomination: Denomination,
        minted_at_block: u64,
        mint_source: TokenMintSource,
    ) -> StateResult<Self> {
        if !denomination.is_bill() {
            return Err(StateError::Other(format!(
                "denomination {} cannot be minted as a bill",
                denomination
            )));
        }

        let assignment_index = deterministic_assignment_index(
            owner,
            denomination,
            minted_at_block,
            mint_source,
            0,
        )?;

        Ok(Self {
            bill_id: assignment_index_to_token_id(assignment_index),
            version: 0,
            denomination,
            owner,
            minted_at_block,
            mint_source,
            metadata: HashMap::new(),
        })
    }

    pub fn value_cents(&self) -> AmountCents {
        self.denomination.value_cents()
    }
}

impl TryFrom<DenominationToken> for BillToken {
    type Error = StateError;

    fn try_from(token: DenominationToken) -> StateResult<Self> {
        let denomination = token.denomination();
        if !denomination.is_bill() {
            return Err(StateError::Other(format!(
                "token denomination {} cannot be represented as a bill",
                denomination
            )));
        }

        Ok(Self {
            bill_id: token.token_id,
            version: token.version,
            denomination,
            owner: token.owner,
            minted_at_block: token.minted_at_block,
            mint_source: token.mint_source,
            metadata: token.metadata,
        })
    }
}

impl From<BillToken> for DenominationToken {
    fn from(bill: BillToken) -> Self {
        let assignment_index = bill
            .denomination
            .assignment_range()
            .map(|(start_index, _)| start_index)
            .unwrap_or(0);

        Self {
            token_id: bill.bill_id,
            version: bill.version,
            assignment_index,
            owner: bill.owner,
            minted_at_block: bill.minted_at_block,
            mint_source: bill.mint_source,
            metadata: bill.metadata,
        }
    }
}

/// Fungible sub-dollar inventory tracked per denomination.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct CoinInventory {
    pub counts: HashMap<Denomination, u64>,
}

impl CoinInventory {
    pub fn is_empty(&self) -> bool {
        self.counts.values().all(|count| *count == 0)
    }

    pub fn total_value_cents(&self) -> AmountCents {
        self.counts
            .iter()
            .map(|(denomination, count)| denomination.value_cents() * count)
            .sum()
    }

    pub fn count(&self, denomination: Denomination) -> u64 {
        self.counts.get(&denomination).copied().unwrap_or(0)
    }

    pub fn add(&mut self, denomination: Denomination, count: u64) -> StateResult<()> {
        if !denomination.is_coin() {
            return Err(StateError::Other(format!(
                "denomination {} cannot be added to coin inventory",
                denomination
            )));
        }

        let entry = self.counts.entry(denomination).or_insert(0);
        *entry = entry
            .checked_add(count)
            .ok_or_else(|| StateError::Other("coin inventory overflow".to_string()))?;
        Ok(())
    }

    pub fn remove(&mut self, denomination: Denomination, count: u64) -> StateResult<()> {
        if !denomination.is_coin() {
            return Err(StateError::Other(format!(
                "denomination {} cannot be removed from coin inventory",
                denomination
            )));
        }

        let entry = self.counts.entry(denomination).or_insert(0);
        if *entry < count {
            return Err(StateError::InsufficientBalance(count, *entry));
        }

        *entry -= count;
        if *entry == 0 {
            self.counts.remove(&denomination);
        }
        Ok(())
    }

    pub fn can_cover(&self, required: &CoinInventory) -> bool {
        required
            .counts
            .iter()
            .all(|(denomination, count)| self.count(*denomination) >= *count)
    }

    pub fn add_inventory(&mut self, other: &CoinInventory) -> StateResult<()> {
        for (denomination, count) in &other.counts {
            self.add(*denomination, *count)?;
        }
        Ok(())
    }

    pub fn remove_inventory(&mut self, other: &CoinInventory) -> StateResult<()> {
        if !self.can_cover(other) {
            return Err(StateError::Other(
                "coin inventory cannot cover requested removal".to_string(),
            ));
        }

        for (denomination, count) in &other.counts {
            self.remove(*denomination, *count)?;
        }
        Ok(())
    }

    pub fn from_tokens(tokens: &[DenominationToken]) -> Self {
        let mut inventory = Self::default();
        for token in tokens {
            let denomination = token.denomination();
            if denomination.is_coin() {
                *inventory.counts.entry(denomination).or_insert(0) += 1;
            }
        }
        inventory
    }
}

/// How melted coin value can be consumed by the network.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComputeUseMode {
    ImmediateExecution,
    ReservedCapacity,
}

/// Granted compute use created by melting fungible coins.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ComputeAllocation {
    pub allocation_id: [u8; 32],
    pub value_cents: AmountCents,
    pub mode: ComputeUseMode,
    pub created_at_block: u64,
    pub expires_at_block: Option<u64>,
}

/// User-visible conversion order lifecycle states.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversionOrderStatus {
    Pending,
    Eligible,
    Fulfilled,
    Expired,
    Failed,
}

/// Direction of a cash-form conversion request.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversionOrderKind {
    BillToCoins,
    CoinsToBill,
}

pub const CONVERSION_ORDER_ELIGIBILITY_BLOCKS: u64 = 69;
pub const CONVERSION_ORDER_CYCLE_BLOCKS: u64 = 420;

/// User-specified request payload for opening a conversion order.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ConversionOrderRequest {
    pub kind: ConversionOrderKind,
    pub requested_value_cents: AmountCents,
    pub requested_coin_inventory: CoinInventory,
    pub requested_bill_denominations: Vec<Denomination>,
}

/// Transaction-level conversion intent.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ConversionTransaction {
    Create(ConversionOrderRequest),
    Cancel { order_id: [u8; 32] },
    ClearDead { order_id: [u8; 32] },
}

/// Protocol-tracked conversion order state for the hybrid cash model.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ConversionOrder {
    pub order_id: [u8; 32],
    pub requester: [u8; 32],
    pub kind: ConversionOrderKind,
    pub requested_value_cents: AmountCents,
    pub requested_coin_inventory: CoinInventory,
    pub requested_bill_denominations: Vec<Denomination>,
    pub created_at_block: u64,
    pub eligible_at_block: u64,
    pub cycle_end_block: u64,
    pub status: ConversionOrderStatus,
    pub failure_reason: Option<String>,
}

impl ConversionOrder {
    pub fn new(order_id: [u8; 32], requester: [u8; 32], request: ConversionOrderRequest, created_at_block: u64) -> Self {
        let cycle_start = created_at_block - (created_at_block % CONVERSION_ORDER_CYCLE_BLOCKS);
        let cycle_end_block = cycle_start + CONVERSION_ORDER_CYCLE_BLOCKS;

        Self {
            order_id,
            requester,
            kind: request.kind,
            requested_value_cents: request.requested_value_cents,
            requested_coin_inventory: request.requested_coin_inventory,
            requested_bill_denominations: request.requested_bill_denominations,
            created_at_block,
            eligible_at_block: created_at_block + CONVERSION_ORDER_ELIGIBILITY_BLOCKS,
            cycle_end_block,
            status: ConversionOrderStatus::Pending,
            failure_reason: None,
        }
    }
}

/// Account state structure
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AccountState {
    /// Compatibility mirror of the account value in cents.
    /// The source of truth during the migration is `tokens`.
    pub balance: u64,

    /// Account nonce (for transaction ordering)
    pub nonce: u64,

    /// Smart contract code (if this is a contract account)
    pub code: Option<Vec<u8>>,

    /// Smart contract storage (key-value pairs)
    pub storage: HashMap<Vec<u8>, Vec<u8>>,

    /// Last updated block height
    pub last_updated: u64,

    /// Account type
    pub account_type: AccountType,

    /// Staked amount (for validator accounts)
    pub staked_amount: Option<u64>,

    /// Delegation information (for validator accounts)
    pub delegations: Option<HashMap<[u8; 32], u64>>,

    /// Account metadata (arbitrary key-value pairs)
    pub metadata: HashMap<String, String>,

    /// Individually owned denomination tokens.
    pub tokens: Vec<DenominationToken>,

    /// Hybrid-cash bill objects tracked separately from fungible coin inventory.
    #[serde(default)]
    pub bills: Vec<BillToken>,

    /// Fungible sub-dollar inventory tracked by denomination.
    #[serde(default)]
    pub coin_inventory: CoinInventory,

    /// Active conversion order for the account. Current protocol design allows one slot.
    #[serde(default)]
    pub conversion_order: Option<ConversionOrder>,

    /// Compute-use claims obtained by melting coins back into network use.
    #[serde(default)]
    pub compute_allocations: Vec<ComputeAllocation>,
}

impl AccountState {
    /// Create a new user account
    pub fn new_user(balance: u64, block_height: u64) -> Self {
        let mut account = Self {
            balance,
            nonce: 0,
            code: None,
            storage: HashMap::new(),
            last_updated: block_height,
            account_type: AccountType::User,
            staked_amount: None,
            delegations: None,
            metadata: HashMap::new(),
            tokens: Vec::new(),
            bills: Vec::new(),
            coin_inventory: CoinInventory::default(),
            conversion_order: None,
            compute_allocations: Vec::new(),
        };
        account
            .rebuild_tokens_from_balance_compat(
                [0; 32],
                block_height,
                TokenMintSource::LegacyBalanceBootstrap,
            )
            .expect("balance decomposition should always succeed");
        account
    }

    /// Create a new contract account
    pub fn new_contract(balance: u64, code: Vec<u8>, block_height: u64) -> Self {
        let mut account = Self {
            balance,
            nonce: 0,
            code: Some(code),
            storage: HashMap::new(),
            last_updated: block_height,
            account_type: AccountType::Contract,
            staked_amount: None,
            delegations: None,
            metadata: HashMap::new(),
            tokens: Vec::new(),
            bills: Vec::new(),
            coin_inventory: CoinInventory::default(),
            conversion_order: None,
            compute_allocations: Vec::new(),
        };
        account
            .rebuild_tokens_from_balance_compat(
                [0; 32],
                block_height,
                TokenMintSource::LegacyBalanceBootstrap,
            )
            .expect("balance decomposition should always succeed");
        account
    }

    /// Create a new system account
    pub fn new_system(balance: u64, block_height: u64) -> Self {
        let mut account = Self {
            balance,
            nonce: 0,
            code: None,
            storage: HashMap::new(),
            last_updated: block_height,
            account_type: AccountType::System,
            staked_amount: None,
            delegations: None,
            metadata: HashMap::new(),
            tokens: Vec::new(),
            bills: Vec::new(),
            coin_inventory: CoinInventory::default(),
            conversion_order: None,
            compute_allocations: Vec::new(),
        };
        account
            .rebuild_tokens_from_balance_compat(
                [0; 32],
                block_height,
                TokenMintSource::LegacyBalanceBootstrap,
            )
            .expect("balance decomposition should always succeed");
        account
    }

    /// Create a new validator account
    pub fn new_validator(balance: u64, staked_amount: u64, block_height: u64) -> Self {
        let mut account = Self {
            balance,
            nonce: 0,
            code: None,
            storage: HashMap::new(),
            last_updated: block_height,
            account_type: AccountType::Validator,
            staked_amount: Some(staked_amount),
            delegations: Some(HashMap::new()),
            metadata: HashMap::new(),
            tokens: Vec::new(),
            bills: Vec::new(),
            coin_inventory: CoinInventory::default(),
            conversion_order: None,
            compute_allocations: Vec::new(),
        };
        account
            .rebuild_tokens_from_balance_compat(
                [0; 32],
                block_height,
                TokenMintSource::LegacyBalanceBootstrap,
            )
            .expect("balance decomposition should always succeed");
        account
    }

    pub fn from_tokens(
        owner: [u8; 32],
        account_type: AccountType,
        tokens: Vec<DenominationToken>,
        block_height: u64,
    ) -> Self {
        let balance = tokens.iter().map(|token| token.value_cents()).sum();
        let mut account = Self {
            balance,
            nonce: 0,
            code: None,
            storage: HashMap::new(),
            last_updated: block_height,
            account_type,
            staked_amount: (account_type == AccountType::Validator).then_some(0),
            delegations: (account_type == AccountType::Validator).then_some(HashMap::new()),
            metadata: HashMap::new(),
            tokens: tokens
                .into_iter()
                .map(|mut token| {
                    token.owner = owner;
                    token
                })
                .collect(),
            bills: Vec::new(),
            coin_inventory: CoinInventory::default(),
            conversion_order: None,
            compute_allocations: Vec::new(),
        };
        account.sync_hybrid_from_tokens();
        account
    }

    /// Check if the account has sufficient balance
    pub fn has_sufficient_balance(&self, amount: u64) -> bool {
        self.total_account_value() >= amount
    }

    /// Increment the account nonce
    pub fn increment_nonce(&mut self) {
        self.nonce += 1;
    }

    /// Compatibility helper for balance-first code paths.
    ///
    /// This should not be used by token execution. It exists only for bootstrap and
    /// legacy tests that still start from an aggregate balance.
    pub fn add_balance_compat(&mut self, amount: u64) -> StateResult<()> {
        let new_total = self
            .total_token_value()
            .checked_add(amount)
            .ok_or_else(|| StateError::Other("Balance overflow".to_string()))?;
        self.balance = new_total;
        self.rebuild_tokens_from_balance_compat(
            self.current_owner(),
            self.last_updated,
            TokenMintSource::TransferChange,
        )
    }

    /// Compatibility helper for balance-first code paths.
    ///
    /// This should not be used by token execution. It exists only for bootstrap and
    /// legacy tests that still start from an aggregate balance.
    pub fn subtract_balance_compat(&mut self, amount: u64) -> StateResult<()> {
        if !self.has_sufficient_balance(amount) {
            return Err(StateError::InsufficientBalance(
                amount,
                self.total_token_value(),
            ));
        }
        let new_total = self.total_token_value() - amount;
        self.balance = new_total;
        self.rebuild_tokens_from_balance_compat(
            self.current_owner(),
            self.last_updated,
            TokenMintSource::TransferChange,
        )
    }

    pub fn total_token_value(&self) -> AmountCents {
        self.tokens.iter().map(|token| token.value_cents()).sum()
    }

    pub fn total_bill_value(&self) -> AmountCents {
        self.bills.iter().map(|bill| bill.value_cents()).sum()
    }

    pub fn total_coin_value(&self) -> AmountCents {
        self.coin_inventory.total_value_cents()
    }

    pub fn total_account_value(&self) -> AmountCents {
        let hybrid_total = self.total_bill_value() + self.total_coin_value();
        hybrid_total.max(self.total_token_value())
    }

    pub fn sync_balance_from_tokens(&mut self) {
        self.balance = self.total_token_value();
        self.sync_hybrid_from_tokens();
    }

    pub fn sync_balance_from_hybrid(&mut self) {
        self.balance = self.total_bill_value() + self.total_coin_value();
    }

    pub fn sync_hybrid_from_tokens(&mut self) {
        self.bills = self
            .tokens
            .iter()
            .filter(|token| token.denomination().is_bill())
            .cloned()
            .filter_map(|token| BillToken::try_from(token).ok())
            .collect();
        self.coin_inventory = CoinInventory::from_tokens(&self.tokens);
        self.balance = self.total_account_value();
    }

    pub fn current_owner(&self) -> [u8; 32] {
        self.tokens
            .first()
            .map(|token| token.owner)
            .unwrap_or([0; 32])
    }

    pub fn assign_token_owner(&mut self, owner: [u8; 32]) {
        for token in &mut self.tokens {
            token.owner = owner;
        }
    }

    pub fn owns_token(&self, token_id: &[u8; 32]) -> bool {
        self.tokens.iter().any(|token| &token.token_id == token_id)
    }

    pub fn token_value(&self, token_id: &[u8; 32]) -> Option<AmountCents> {
        self.tokens
            .iter()
            .find(|token| &token.token_id == token_id)
            .map(|token| token.value_cents())
    }

    pub fn token_version(&self, token_id: &[u8; 32]) -> Option<u64> {
        self.tokens
            .iter()
            .find(|token| &token.token_id == token_id)
            .map(|token| token.version)
    }

    pub fn token_ids_for_amount(&self, amount_cents: AmountCents) -> Option<Vec<[u8; 32]>> {
        let mut selected = Vec::new();
        let mut running_total = 0;

        let mut tokens = self.tokens.clone();
        tokens.sort_by_key(|token| std::cmp::Reverse(token.value_cents()));

        for token in tokens {
            if running_total == amount_cents {
                break;
            }

            if running_total + token.value_cents() <= amount_cents {
                running_total += token.value_cents();
                selected.push(token.token_id);
            }
        }

        (running_total == amount_cents).then_some(selected)
    }

    pub fn remove_tokens_by_id(
        &mut self,
        token_ids: &[[u8; 32]],
    ) -> StateResult<Vec<DenominationToken>> {
        let requested_ids: HashSet<[u8; 32]> = token_ids.iter().copied().collect();
        if requested_ids.len() != token_ids.len() {
            return Err(StateError::Other(
                "duplicate token ids in request".to_string(),
            ));
        }

        let mut retained = Vec::with_capacity(self.tokens.len());
        let mut removed = Vec::new();

        for token in self.tokens.drain(..) {
            if requested_ids.contains(&token.token_id) {
                removed.push(token);
            } else {
                retained.push(token);
            }
        }

        if removed.len() != token_ids.len() {
            return Err(StateError::Other(
                "account does not own all requested token ids".to_string(),
            ));
        }

        self.tokens = retained;
        self.sync_balance_from_tokens();
        Ok(removed)
    }

    pub fn deposit_tokens(&mut self, owner: [u8; 32], mut tokens: Vec<DenominationToken>) {
        for token in &mut tokens {
            if token.owner != owner {
                token.version += 1;
            }
            token.owner = owner;
        }
        self.tokens.extend(tokens);
        self.sync_balance_from_tokens();
    }

    pub fn set_tokens(&mut self, owner: [u8; 32], tokens: Vec<DenominationToken>) {
        self.tokens = tokens
            .into_iter()
            .map(|mut token| {
                token.owner = owner;
                token
            })
            .collect();
        self.sync_balance_from_tokens();
    }

    pub fn mint_block_reward_set(
        owner: [u8; 32],
        block_height: u64,
        block_hash: &[u8; 32],
    ) -> Vec<DenominationToken> {
        crate::rewards::reward_tokens_for_block(owner, block_height, block_hash)
    }

    pub fn rebuild_tokens_from_balance_compat(
        &mut self,
        owner: [u8; 32],
        block_height: u64,
        mint_source: TokenMintSource,
    ) -> StateResult<()> {
        let tokens = mint_exact_amount(owner, self.balance, block_height, mint_source)?;
        self.tokens = tokens;
        self.sync_balance_from_tokens();
        Ok(())
    }

    /// Set a storage value
    pub fn set_storage(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.storage.insert(key, value);
    }

    /// Get a storage value
    pub fn get_storage(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.storage.get(key)
    }

    /// Delete a storage value
    pub fn delete_storage(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.storage.remove(key)
    }

    /// Add a delegation (for validator accounts)
    pub fn add_delegation(&mut self, delegator: [u8; 32], amount: u64) -> StateResult<()> {
        if self.account_type != AccountType::Validator {
            return Err(StateError::InvalidAccount(format!(
                "Cannot add delegation to non-validator account: {}",
                self.account_type
            )));
        }

        let delegations = self.delegations.get_or_insert_with(HashMap::new);
        let current_amount = delegations.get(&delegator).copied().unwrap_or(0);
        let new_amount = current_amount
            .checked_add(amount)
            .ok_or_else(|| StateError::Other("Delegation amount overflow".to_string()))?;

        delegations.insert(delegator, new_amount);
        Ok(())
    }

    /// Remove a delegation (for validator accounts)
    pub fn remove_delegation(&mut self, delegator: &[u8; 32], amount: u64) -> StateResult<()> {
        if self.account_type != AccountType::Validator {
            return Err(StateError::InvalidAccount(format!(
                "Cannot remove delegation from non-validator account: {}",
                self.account_type
            )));
        }

        let delegations = match &mut self.delegations {
            Some(d) => d,
            None => return Err(StateError::Other("No delegations found".to_string())),
        };

        let current_amount = match delegations.get(delegator) {
            Some(a) => *a,
            None => {
                return Err(StateError::Other(format!(
                    "No delegation found for {:?}",
                    delegator
                )))
            }
        };

        if current_amount < amount {
            return Err(StateError::InsufficientBalance(amount, current_amount));
        }

        let new_amount = current_amount - amount;
        if new_amount == 0 {
            delegations.remove(delegator);
        } else {
            delegations.insert(*delegator, new_amount);
        }

        Ok(())
    }

    /// Get the total delegated amount (for validator accounts)
    pub fn get_total_delegated(&self) -> u64 {
        match &self.delegations {
            Some(delegations) => delegations.values().sum(),
            None => 0,
        }
    }

    /// Set a metadata value
    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get a metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Delete a metadata value
    pub fn delete_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }
}

fn mint_exact_amount(
    owner: [u8; 32],
    amount_cents: AmountCents,
    block_height: u64,
    mint_source: TokenMintSource,
) -> StateResult<Vec<DenominationToken>> {
    let mut remaining = amount_cents;
    let mut tokens = Vec::new();

    for denomination in Denomination::all_descending() {
        while remaining >= denomination.value_cents() {
            let assignment_index = deterministic_assignment_index(
                owner,
                *denomination,
                block_height,
                mint_source,
                tokens.len() as u64,
            )?;
            tokens.push(DenominationToken::new(
                owner,
                assignment_index,
                block_height,
                mint_source,
            ));
            remaining -= denomination.value_cents();
        }
    }

    if remaining != 0 {
        return Err(StateError::Other(format!(
            "could not represent remaining amount in cents: {}",
            remaining
        )));
    }

    Ok(tokens)
}

pub fn assignment_index_to_token_id(assignment_index: u64) -> [u8; 32] {
    let mut token_id = [0u8; 32];
    token_id[24..].copy_from_slice(&assignment_index.to_be_bytes());
    token_id
}

pub fn assignment_index_for_denomination(
    denomination: Denomination,
    ordinal_in_denomination: u64,
) -> StateResult<u64> {
    let (start_index, count) = denomination.assignment_range().ok_or_else(|| {
        StateError::Other(format!("missing assignment range for denomination {}", denomination))
    })?;

    if ordinal_in_denomination >= count {
        return Err(StateError::Other(format!(
            "assignment ordinal {} exceeds supply for denomination {}",
            ordinal_in_denomination, denomination
        )));
    }

    Ok(start_index + ordinal_in_denomination)
}

fn deterministic_assignment_index(
    owner: [u8; 32],
    denomination: Denomination,
    block_height: u64,
    mint_source: TokenMintSource,
    sequence: u64,
) -> StateResult<u64> {
    let (start_index, count) = denomination.assignment_range().ok_or_else(|| {
        StateError::Other(format!("missing assignment range for denomination {}", denomination))
    })?;

    let mut hasher = Sha256::new();
    hasher.update(owner);
    hasher.update(denomination.label().as_bytes());
    hasher.update(block_height.to_be_bytes());
    hasher.update([mint_source as u8]);
    hasher.update(sequence.to_be_bytes());
    let digest = hasher.finalize();

    let mut word = [0u8; 8];
    word.copy_from_slice(&digest[..8]);
    let offset = u64::from_be_bytes(word) % count.max(1);
    Ok(start_index + offset)
}

/// State root hash
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRoot {
    /// Root hash of the state trie
    pub root_hash: [u8; 32],

    /// Block height at which this state root was calculated
    pub block_height: u64,

    /// Timestamp when this state root was calculated
    pub timestamp: u64,
}

impl StateRoot {
    /// Create a new state root
    pub fn new(root_hash: [u8; 32], block_height: u64, timestamp: u64) -> Self {
        Self {
            root_hash,
            block_height,
            timestamp,
        }
    }

    /// Convert the root hash to a hex string
    pub fn root_hash_hex(&self) -> String {
        hex::encode(self.root_hash)
    }
}

/// Global state structure
#[derive(Debug, Clone)]
pub struct GlobalState {
    /// Current state root
    pub state_root: StateRoot,

    /// Total supply of coins
    pub total_supply: u64,

    /// Total staked amount
    pub total_staked: u64,

    /// Total number of accounts
    pub total_accounts: u64,

    /// Total number of transactions
    pub total_transactions: u64,

    /// Chain parameters
    pub chain_params: ChainParameters,
}

impl GlobalState {
    /// Create a new global state
    pub fn new(state_root: StateRoot, chain_params: ChainParameters) -> Self {
        Self {
            state_root,
            total_supply: chain_params.initial_supply,
            total_staked: 0,
            total_accounts: 0,
            total_transactions: 0,
            chain_params,
        }
    }

    /// Update the state root
    pub fn update_state_root(&mut self, state_root: StateRoot) {
        self.state_root = state_root;
    }

    /// Update the total supply
    pub fn update_total_supply(&mut self, total_supply: u64) {
        self.total_supply = total_supply;
    }

    /// Update the total staked amount
    pub fn update_total_staked(&mut self, total_staked: u64) {
        self.total_staked = total_staked;
    }

    /// Update the total number of accounts
    pub fn update_total_accounts(&mut self, total_accounts: u64) {
        self.total_accounts = total_accounts;
    }

    /// Update the total number of transactions
    pub fn update_total_transactions(&mut self, total_transactions: u64) {
        self.total_transactions = total_transactions;
    }

    /// Get the current block reward
    pub fn get_block_reward(&self) -> u64 {
        self.chain_params.block_reward
    }

    /// Get the current transaction fee
    pub fn get_transaction_fee(&self) -> u64 {
        self.chain_params.transaction_fee
    }

    /// Get the current staking reward rate
    pub fn get_staking_reward_rate(&self) -> f64 {
        self.chain_params.staking_reward_rate
    }
}

/// Chain parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainParameters {
    /// Chain ID
    pub chain_id: u64,

    /// Initial supply of coins
    pub initial_supply: u64,

    /// Block reward
    pub block_reward: u64,

    /// Transaction fee
    pub transaction_fee: u64,

    /// Staking reward rate (annual percentage)
    pub staking_reward_rate: f64,

    /// Minimum stake amount
    pub min_stake_amount: u64,

    /// Maximum validators
    pub max_validators: u64,

    /// Block time target (in seconds)
    pub block_time_target: u64,

    /// Difficulty adjustment period (in blocks)
    pub difficulty_adjustment_period: u64,
}

impl Default for ChainParameters {
    fn default() -> Self {
        Self {
            chain_id: 1,
            initial_supply: 1_000_000_000,
            block_reward: 50,
            transaction_fee: 1,
            staking_reward_rate: 0.05, // 5% annual
            min_stake_amount: 1000,
            max_validators: 100,
            block_time_target: 10,              // 10 seconds
            difficulty_adjustment_period: 2016, // ~2 weeks with 10-second blocks
        }
    }
}

/// State transition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// Previous state root
    pub prev_state_root: [u8; 32],

    /// New state root
    pub new_state_root: [u8; 32],

    /// Block height
    pub block_height: u64,

    /// Timestamp
    pub timestamp: u64,

    /// Account changes
    pub account_changes: Vec<AccountChange>,
}

/// Account change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountChange {
    /// Account address
    pub address: [u8; 32],

    /// Previous account state
    pub prev_state: Option<AccountState>,

    /// New account state
    pub new_state: Option<AccountState>,

    /// Change type
    pub change_type: AccountChangeType,
}

/// Account change type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AccountChangeType {
    /// Account created
    Created,

    /// Account updated
    Updated,

    /// Account deleted
    Deleted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_state_new_user() {
        let account = AccountState::new_user(1000, 100);
        assert_eq!(account.balance, 1000);
        assert_eq!(account.nonce, 0);
        assert_eq!(account.account_type, AccountType::User);
        assert_eq!(account.last_updated, 100);
        assert!(account.code.is_none());
        assert!(account.storage.is_empty());
        assert!(account.staked_amount.is_none());
        assert!(account.delegations.is_none());
        assert!(account.metadata.is_empty());
        assert_eq!(account.total_bill_value(), 1000);
        assert_eq!(account.total_coin_value(), 0);
    }

    #[test]
    fn test_account_state_new_contract() {
        let code = vec![1, 2, 3, 4];
        let account = AccountState::new_contract(500, code.clone(), 200);
        assert_eq!(account.balance, 500);
        assert_eq!(account.nonce, 0);
        assert_eq!(account.account_type, AccountType::Contract);
        assert_eq!(account.last_updated, 200);
        assert_eq!(account.code, Some(code));
        assert!(account.storage.is_empty());
        assert!(account.staked_amount.is_none());
        assert!(account.delegations.is_none());
        assert!(account.metadata.is_empty());
        assert_eq!(account.total_bill_value(), 500);
        assert_eq!(account.total_coin_value(), 0);
    }

    #[test]
    fn test_account_state_new_validator() {
        let account = AccountState::new_validator(2000, 1000, 300);
        assert_eq!(account.balance, 2000);
        assert_eq!(account.nonce, 0);
        assert_eq!(account.account_type, AccountType::Validator);
        assert_eq!(account.last_updated, 300);
        assert!(account.code.is_none());
        assert!(account.storage.is_empty());
        assert_eq!(account.staked_amount, Some(1000));
        assert!(account.delegations.as_ref().unwrap().is_empty());
        assert!(account.metadata.is_empty());
        assert_eq!(account.total_bill_value(), 2000);
        assert_eq!(account.total_coin_value(), 0);
    }

    #[test]
    fn test_sync_hybrid_from_tokens_splits_bills_and_coins() {
        let account = AccountState::new_user(136, 42);

        assert_eq!(account.total_bill_value(), 100);
        assert_eq!(account.total_coin_value(), 36);
        assert_eq!(account.coin_inventory.count(Denomination::Cents25), 1);
        assert_eq!(account.coin_inventory.count(Denomination::Cents10), 1);
        assert_eq!(account.coin_inventory.count(Denomination::Cents1), 1);
        assert_eq!(account.bills.len(), 1);
        assert_eq!(account.bills[0].denomination, Denomination::Dollars1);
    }

    #[test]
    fn test_account_state_balance_operations() {
        let mut account = AccountState::new_user(1000, 100);

        // Test has_sufficient_balance
        assert!(account.has_sufficient_balance(1000));
        assert!(account.has_sufficient_balance(500));
        assert!(!account.has_sufficient_balance(1001));

        // Test balance-first compatibility helpers
        account.add_balance_compat(500).unwrap();
        assert_eq!(account.balance, 1500);

        // Test subtract_balance_compat
        account.subtract_balance_compat(300).unwrap();
        assert_eq!(account.balance, 1200);

        // Test insufficient balance
        let result = account.subtract_balance_compat(1500);
        assert!(result.is_err());
        if let Err(StateError::InsufficientBalance(required, available)) = result {
            assert_eq!(required, 1500);
            assert_eq!(available, 1200);
        } else {
            panic!("Expected InsufficientBalance error");
        }
    }

    #[test]
    fn test_deposit_tokens_bumps_version_on_transfer() {
        let mut sender = AccountState::new_user(500, 1);
        let token_id = sender.token_ids_for_amount(500).unwrap()[0];
        let moved = sender.remove_tokens_by_id(&[token_id]).unwrap();

        let mut recipient = AccountState::new_user(0, 1);
        recipient.deposit_tokens([9; 32], moved);

        let received = recipient
            .tokens
            .iter()
            .find(|token| token.token_id == token_id)
            .unwrap();
        assert_eq!(received.owner, [9; 32]);
        assert_eq!(received.version, 1);
    }

    #[test]
    fn test_account_state_storage_operations() {
        let mut account = AccountState::new_user(1000, 100);

        // Test set_storage and get_storage
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        account.set_storage(key.clone(), value.clone());
        assert_eq!(account.get_storage(&key), Some(&value));

        // Test delete_storage
        let removed = account.delete_storage(&key);
        assert_eq!(removed, Some(value));
        assert_eq!(account.get_storage(&key), None);
    }

    #[test]
    fn test_account_state_delegation_operations() {
        let mut account = AccountState::new_validator(2000, 1000, 300);
        let delegator = [1; 32];

        // Test add_delegation
        account.add_delegation(delegator, 500).unwrap();
        let delegations = account.delegations.as_ref().unwrap();
        assert_eq!(delegations.get(&delegator), Some(&500));

        // Test get_total_delegated
        assert_eq!(account.get_total_delegated(), 500);

        // Test add more delegation
        account.add_delegation(delegator, 300).unwrap();
        let delegations = account.delegations.as_ref().unwrap();
        assert_eq!(delegations.get(&delegator), Some(&800));
        assert_eq!(account.get_total_delegated(), 800);

        // Test remove_delegation
        account.remove_delegation(&delegator, 300).unwrap();
        let delegations = account.delegations.as_ref().unwrap();
        assert_eq!(delegations.get(&delegator), Some(&500));
        assert_eq!(account.get_total_delegated(), 500);

        // Test remove all delegation
        account.remove_delegation(&delegator, 500).unwrap();
        let delegations = account.delegations.as_ref().unwrap();
        assert!(delegations.is_empty());
        assert_eq!(account.get_total_delegated(), 0);
    }

    #[test]
    fn test_account_state_metadata_operations() {
        let mut account = AccountState::new_user(1000, 100);

        // Test set_metadata and get_metadata
        account.set_metadata("name".to_string(), "Alice".to_string());
        assert_eq!(account.get_metadata("name"), Some(&"Alice".to_string()));

        // Test delete_metadata
        let removed = account.delete_metadata("name");
        assert_eq!(removed, Some("Alice".to_string()));
        assert_eq!(account.get_metadata("name"), None);
    }

    #[test]
    fn test_state_root() {
        let root_hash = [1; 32];
        let state_root = StateRoot::new(root_hash, 100, 12345);

        assert_eq!(state_root.root_hash, root_hash);
        assert_eq!(state_root.block_height, 100);
        assert_eq!(state_root.timestamp, 12345);
        assert_eq!(
            state_root.root_hash_hex(),
            "0101010101010101010101010101010101010101010101010101010101010101"
        );
    }

    #[test]
    fn test_global_state() {
        let root_hash = [1; 32];
        let state_root = StateRoot::new(root_hash, 100, 12345);
        let chain_params = ChainParameters::default();

        let mut global_state = GlobalState::new(state_root.clone(), chain_params.clone());

        assert_eq!(global_state.state_root, state_root);
        assert_eq!(global_state.total_supply, chain_params.initial_supply);
        assert_eq!(global_state.total_staked, 0);
        assert_eq!(global_state.total_accounts, 0);
        assert_eq!(global_state.total_transactions, 0);

        // Test update methods
        let new_root_hash = [2; 32];
        let new_state_root = StateRoot::new(new_root_hash, 101, 12346);
        global_state.update_state_root(new_state_root.clone());
        global_state.update_total_supply(2_000_000_000);
        global_state.update_total_staked(500_000);
        global_state.update_total_accounts(1000);
        global_state.update_total_transactions(5000);

        assert_eq!(global_state.state_root, new_state_root);
        assert_eq!(global_state.total_supply, 2_000_000_000);
        assert_eq!(global_state.total_staked, 500_000);
        assert_eq!(global_state.total_accounts, 1000);
        assert_eq!(global_state.total_transactions, 5000);

        // Test getter methods
        assert_eq!(global_state.get_block_reward(), chain_params.block_reward);
        assert_eq!(
            global_state.get_transaction_fee(),
            chain_params.transaction_fee
        );
        assert_eq!(
            global_state.get_staking_reward_rate(),
            chain_params.staking_reward_rate
        );
    }

    #[test]
    fn test_chain_parameters_default() {
        let params = ChainParameters::default();

        assert_eq!(params.chain_id, 1);
        assert_eq!(params.initial_supply, 1_000_000_000);
        assert_eq!(params.block_reward, 50);
        assert_eq!(params.transaction_fee, 1);
        assert_eq!(params.staking_reward_rate, 0.05);
        assert_eq!(params.min_stake_amount, 1000);
        assert_eq!(params.max_validators, 100);
        assert_eq!(params.block_time_target, 10);
        assert_eq!(params.difficulty_adjustment_period, 2016);
    }
}

//! Storage module for Kumquat blockchain
//!
//! This module provides a comprehensive storage layer for the blockchain,
//! including block storage, transaction storage, account state storage, and PoH entries.
//!
//! The storage module is built on top of RocksDB, a high-performance key-value store.
//! It provides a modular design with separate components for different types of data:
//!
//! - `kv_store`: Low-level key-value store interface and RocksDB implementation
//! - `block_store`: Storage for blockchain blocks
//! - `tx_store`: Storage for transactions
//! - `state_store`: Storage for account states
//! - `poh_store`: Storage for Proof of History entries
//! - `trie`: Merkle Patricia Trie implementation for state verification (optional)
//! - `object`: Sui-style object model implementation
//! - `object_store`: Storage for Sui-style objects
//! - `object_transaction`: Transaction types for object operations
//! - `object_processor`: Processor for object transactions

pub mod batch_operations;
pub mod block_store;
pub mod kv_store;
pub mod mempool;
pub mod object;
pub mod object_processor;
pub mod object_store;
pub mod object_transaction;
pub mod poh_store;
pub mod rocksdb_schema;
pub mod snapshot;
pub mod state;
pub mod state_indexing;
pub mod state_manager;
pub mod state_pruning;
pub mod state_sharding;
pub mod state_store;
pub mod state_sync;
pub mod state_validation;
pub mod trie;
pub mod tx_store;

// Re-export common types
pub use batch_operations::{BatchOperationError, BatchOperationManager};
pub use block_store::{Block, BlockStore, Hash};
pub use kv_store::{KVStore, KVStoreError, RocksDBStore, WriteBatchOperation};
pub use mempool::{MempoolStorageError, MempoolStore, MempoolTransactionMetadata};
pub use object::{Object, ObjectError, ObjectId, Ownership};
pub use object_processor::{ObjectProcessor, ObjectProcessorError};
pub use object_store::{ObjectStore, ObjectStoreError};
pub use object_transaction::{ObjectTransactionKind, ObjectTransactionRecord};
pub use poh_store::{PoHEntry, PoHStore};
pub use rocksdb_schema::{DatabaseStats, KeyType, RocksDBManager, Schema};
pub use snapshot::{
    CompressionType, SnapshotConfig, SnapshotError, SnapshotManager, SnapshotMetadata, SnapshotType,
};
pub use state::{
    AccountChange, AccountChangeType, AccountState, AccountType, AmountCents, ChainParameters,
    BillToken, CoinInventory, ComputeAllocation, ComputeUseMode, ConversionOrder,
    ConversionOrderKind, ConversionOrderRequest, ConversionOrderStatus,
    ConversionTransaction, CONVERSION_ORDER_CYCLE_BLOCKS, CONVERSION_ORDER_ELIGIBILITY_BLOCKS,
    Denomination, DenominationToken, GlobalState, StateError, StateResult, StateRoot,
    StateTransition, TokenMintSource,
};
pub use state_indexing::{
    IndexConfig, IndexType, IndexingError, IndexingProgress, IndexingResult, IndexingStatus,
    StateIndex, StateIndexingManager,
};
pub use state_manager::StateManager;
pub use state_pruning::{
    PrunerConfig, PruningError, PruningMode, PruningResult, PruningStats, StatePruner,
};
pub use state_sharding::{
    ShardConfig, ShardingError, ShardingResult, ShardingStrategy, StateShard, StateShardingManager,
};
pub use state_store::StateStore;
pub use state_sync::{
    NetworkClient, StateSynchronizer, SyncConfig, SyncError, SyncMode, SyncProgress, SyncResult,
    SyncStatus,
};
pub use state_validation::{StateValidator, ValidationError, ValidationResult};
pub use trie::mpt::{MerklePatriciaTrie, Proof, ProofItem};
pub use trie::node::Node;
pub use tx_store::{TransactionError, TransactionRecord, TransactionStatus, TxStore};

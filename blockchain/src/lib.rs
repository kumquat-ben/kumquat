// Kumquat - A next-generation blockchain combining Proof-of-Work with Solana-style Proof of History

// Export modules
pub mod api;
pub mod config;
pub mod consensus;
pub mod crypto;
pub mod mempool;
pub mod network;
pub mod node_runtime;
pub mod rewards;
pub mod storage;
pub mod tools;

// Initialize logging
pub fn init_logger() {
    env_logger::init();
}

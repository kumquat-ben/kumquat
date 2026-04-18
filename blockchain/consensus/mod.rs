// Kumquat Consensus Module
//
// This module implements the hybrid Proof-of-Work (PoW) and Proof-of-History (PoH)
// consensus mechanism for the Kumquat blockchain.

pub mod block_processor;
pub mod config;
pub mod engine;
pub mod engine_runner;
pub mod mining;
pub mod poh;
pub mod pow;
pub mod telemetry;
pub mod types;
pub mod validation;

use log::info;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::consensus::config::ConsensusConfig;
use crate::consensus::engine::ConsensusEngine;
use crate::consensus::engine_runner::ConsensusEngineRunner;
use crate::consensus::telemetry::{new_consensus_telemetry, ConsensusTelemetry};
use crate::network::types::message::NetMessage;
use crate::storage::block_store::BlockStore;
use crate::storage::kv_store::KVStore;
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::TxStore;

/// Start the consensus engine
pub async fn start_consensus<S: KVStore + 'static>(
    config: ConsensusConfig,
    kv_store: Arc<S>,
    block_store: Arc<BlockStore<'static>>,
    tx_store: Arc<TxStore<'static>>,
    state_store: Arc<StateStore<'static>>,
    network_tx: mpsc::Sender<NetMessage>,
    initial_mining_eligible: bool,
) -> Arc<ConsensusEngine> {
    let telemetry: ConsensusTelemetry = new_consensus_telemetry(config.enable_mining);
    let mining_gate = Arc::new(AtomicBool::new(initial_mining_eligible));

    // Create the consensus engine
    let engine = ConsensusEngine::new_with_shared_mining_gate(
        config.clone(),
        kv_store.clone(),
        block_store.clone(),
        tx_store.clone(),
        state_store.clone(),
        network_tx.clone(),
        telemetry.clone(),
        mining_gate.clone(),
    );

    // Create a reference to the engine for returning
    let engine_arc = Arc::new(engine);

    // Create a clone of the engine for the runner
    let engine_for_runner = ConsensusEngine::new_with_shared_mining_gate(
        config,
        kv_store,
        block_store,
        tx_store,
        state_store,
        network_tx,
        telemetry,
        mining_gate,
    );

    // Create the engine runner
    let runner = ConsensusEngineRunner::new(engine_for_runner);

    // Start the engine in a separate task
    tokio::spawn(async move {
        info!("Starting consensus engine...");
        runner.run().await;
    });

    // Return a reference to the engine
    engine_arc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::config::ConsensusConfig;

    #[test]
    fn test_consensus_config() {
        let config = ConsensusConfig::default();
        assert!(config.target_block_time > 0);
        assert!(config.initial_difficulty > 0);
        assert!(config.difficulty_adjustment_window > 0);
    }
}

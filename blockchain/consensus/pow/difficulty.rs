use log::{info, warn};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

use crate::consensus::config::ConsensusConfig;
use crate::consensus::types::Target;
use crate::storage::block_store::BlockStore;
use crate::storage::state_store::StateStore;
use crate::storage::tx_store::TxStore;
use crate::storage::{
    ConversionMarketSnapshot, ConversionOrderKind, ConversionTransaction,
    CONVERSION_ORDER_CYCLE_BLOCKS, CONVERSION_ORDER_ELIGIBILITY_BLOCKS,
};

const CONVERSION_DIFFICULTY_CLAMP: f64 = 0.10;

#[derive(Debug, Clone, Copy, Default)]
struct ConversionFlowStats {
    bill_to_coins_requested_cents: u64,
    coins_to_bill_requested_cents: u64,
    bill_to_coins_fulfilled_cents: u64,
    coins_to_bill_fulfilled_cents: u64,
}

fn normalized_delta(positive: u64, negative: u64) -> f64 {
    let total = positive.saturating_add(negative);
    if total == 0 {
        0.0
    } else {
        (positive as f64 - negative as f64) / total as f64
    }
}

fn collect_tracked_miner_addresses(
    block_store: &BlockStore<'_>,
    current_height: u64,
    window: u64,
) -> HashSet<[u8; 32]> {
    let mut miners = HashSet::new();
    let start_height = current_height.saturating_sub(window.saturating_sub(1));
    for height in start_height..=current_height {
        if let Ok(Some(block)) = block_store.get_block_by_height(height) {
            miners.insert(block.miner);
        }
    }
    miners
}

fn collect_conversion_flow_stats(
    block_store: &BlockStore<'_>,
    tx_store: &TxStore<'_>,
    create_scan_start: u64,
    flow_start: u64,
    end_height: u64,
) -> ConversionFlowStats {
    let mut stats = ConversionFlowStats::default();
    let mut order_requests = std::collections::HashMap::new();

    for height in create_scan_start..=end_height {
        let block = match block_store.get_block_by_height(height) {
            Ok(Some(block)) => block,
            _ => continue,
        };

        for tx_hash in &block.transactions {
            let tx = match tx_store.get_transaction(tx_hash) {
                Ok(Some(tx)) => tx,
                _ => continue,
            };

            if let Some(ConversionTransaction::Create(request)) = tx.conversion_intent {
                let mut hasher = Sha256::new();
                hasher.update(tx.tx_id);
                hasher.update(tx.sender);
                let digest = hasher.finalize();
                let mut order_id = [0u8; 32];
                order_id.copy_from_slice(&digest[..32]);
                order_requests.insert(order_id, (request.kind, request.requested_value_cents));

                if height < flow_start {
                    continue;
                }
                match request.kind {
                    ConversionOrderKind::BillToCoins => {
                        stats.bill_to_coins_requested_cents = stats
                            .bill_to_coins_requested_cents
                            .saturating_add(request.requested_value_cents);
                    }
                    ConversionOrderKind::CoinsToBill => {
                        stats.coins_to_bill_requested_cents = stats
                            .coins_to_bill_requested_cents
                            .saturating_add(request.requested_value_cents);
                    }
                }
            }
        }

        if height < flow_start {
            continue;
        }
        for order_id in &block.conversion_fulfillment_order_ids {
            if let Some((kind, value)) = order_requests.get(order_id) {
                match kind {
                    ConversionOrderKind::BillToCoins => {
                        stats.bill_to_coins_fulfilled_cents =
                            stats.bill_to_coins_fulfilled_cents.saturating_add(*value);
                    }
                    ConversionOrderKind::CoinsToBill => {
                        stats.coins_to_bill_fulfilled_cents =
                            stats.coins_to_bill_fulfilled_cents.saturating_add(*value);
                    }
                }
            }
        }
    }

    stats
}

fn conversion_market_signal(
    snapshot: &ConversionMarketSnapshot,
    cycle_flow: ConversionFlowStats,
    rolling_flow: ConversionFlowStats,
) -> f64 {
    let demand_signal = normalized_delta(
        snapshot.bill_to_coins_demand_cents,
        snapshot.coins_to_bill_demand_cents,
    );
    let dominant_bill_to_coins =
        snapshot.bill_to_coins_demand_cents >= snapshot.coins_to_bill_demand_cents;
    let dominant_demand = if dominant_bill_to_coins {
        snapshot.bill_to_coins_demand_cents
    } else {
        snapshot.coins_to_bill_demand_cents
    };
    let dominant_inventory = if dominant_bill_to_coins {
        snapshot.tracked_miner_coin_inventory_cents
    } else {
        snapshot.tracked_miner_bill_inventory_cents
    };
    let inventory_signal = if dominant_demand == 0 && dominant_inventory == 0 {
        0.0
    } else {
        ((dominant_demand as f64 - dominant_inventory as f64)
            / dominant_demand.max(dominant_inventory).max(1) as f64)
            .clamp(-1.0, 1.0)
    };
    let backlog_ratio =
        (snapshot.open_order_count as f64 / CONVERSION_ORDER_CYCLE_BLOCKS as f64).clamp(0.0, 1.0);
    let cycle_signal = normalized_delta(
        cycle_flow.bill_to_coins_requested_cents,
        cycle_flow.coins_to_bill_requested_cents,
    );
    let rolling_signal = normalized_delta(
        rolling_flow.bill_to_coins_requested_cents,
        rolling_flow.coins_to_bill_requested_cents,
    );
    let fulfillment_signal = normalized_delta(
        cycle_flow.bill_to_coins_fulfilled_cents,
        cycle_flow.coins_to_bill_fulfilled_cents,
    );
    let dominant_requested_cycle = if dominant_bill_to_coins {
        cycle_flow.bill_to_coins_requested_cents
    } else {
        cycle_flow.coins_to_bill_requested_cents
    };
    let dominant_fulfilled_cycle = if dominant_bill_to_coins {
        cycle_flow.bill_to_coins_fulfilled_cents
    } else {
        cycle_flow.coins_to_bill_fulfilled_cents
    };
    let settlement_gap_signal = if dominant_requested_cycle == 0 {
        0.0
    } else {
        ((dominant_requested_cycle.saturating_sub(dominant_fulfilled_cycle)) as f64
            / dominant_requested_cycle as f64)
            .clamp(0.0, 1.0)
    };
    let smoothed_flow_signal = 0.7 * cycle_signal + 0.3 * rolling_signal;

    (0.28 * demand_signal.abs()
        + 0.27 * inventory_signal
        + 0.20 * settlement_gap_signal
        + 0.15 * (backlog_ratio * demand_signal.abs())
        + 0.05 * smoothed_flow_signal.abs()
        + 0.05 * fulfillment_signal.abs())
    .clamp(-1.0, 1.0)
}

/// Calculate the next target difficulty
pub fn calculate_next_target(
    config: &ConsensusConfig,
    block_store: &BlockStore<'_>,
    state_store: &StateStore<'_>,
    tx_store: &TxStore<'_>,
    current_height: u64,
) -> Target {
    // If we're at the genesis block or below the adjustment window, use the initial difficulty
    if current_height < config.difficulty_adjustment_window {
        return Target::from_difficulty(config.initial_difficulty);
    }

    // Get the block at the start of the window
    let start_height = current_height - config.difficulty_adjustment_window;
    let start_block = match block_store.get_block_by_height(start_height) {
        Ok(Some(block)) => block,
        Ok(None) => {
            warn!(
                "Could not find block at height {}, using initial difficulty",
                start_height
            );
            return Target::from_difficulty(config.initial_difficulty);
        }
        Err(_e) => {
            warn!(
                "Could not find block at height {}, using initial difficulty",
                start_height
            );
            return Target::from_difficulty(config.initial_difficulty);
        }
    };

    // Get the latest block
    let latest_block = match block_store.get_block_by_height(current_height) {
        Ok(Some(block)) => block,
        Ok(None) => {
            warn!(
                "Could not find block at height {}, using initial difficulty",
                current_height
            );
            return Target::from_difficulty(config.initial_difficulty);
        }
        Err(_e) => {
            warn!(
                "Could not find block at height {}, using initial difficulty",
                current_height
            );
            return Target::from_difficulty(config.initial_difficulty);
        }
    };

    // Calculate the time taken for the window
    let time_taken = latest_block.timestamp - start_block.timestamp;

    // Expected time for the window
    let expected_time = config.target_block_time * config.difficulty_adjustment_window;

    // If time_taken is zero, something is wrong, use the initial difficulty
    if time_taken == 0 {
        warn!("Time taken for difficulty window is zero, using initial difficulty");
        return Target::from_difficulty(config.initial_difficulty);
    }

    // Calculate the adjustment factor
    let adjustment_factor = expected_time as f64 / time_taken as f64;

    // Clamp the adjustment factor to prevent too rapid changes
    let clamped_factor = adjustment_factor.max(0.25).min(4.0);

    // Get the current difficulty
    let current_target = get_target_at_height(block_store, current_height);
    let current_difficulty = current_target.to_difficulty();

    // Calculate the new difficulty
    let time_adjusted_difficulty = (current_difficulty as f64 * clamped_factor) as u64;

    if !config.hybrid_active_at(current_height + 1) {
        let new_difficulty = time_adjusted_difficulty.max(config.initial_difficulty);
        info!(
            "Difficulty adjustment: {} -> {} (time_factor: {:.2}, hybrid_inactive_until_height: {})",
            current_difficulty,
            new_difficulty,
            clamped_factor,
            config.hybrid_activation_height,
        );
        return Target::from_difficulty(new_difficulty);
    }

    let tracked_miners = collect_tracked_miner_addresses(
        block_store,
        current_height,
        CONVERSION_ORDER_ELIGIBILITY_BLOCKS,
    );
    let snapshot = state_store.conversion_market_snapshot(current_height, &tracked_miners);
    let cycle_start =
        current_height.saturating_sub(CONVERSION_ORDER_CYCLE_BLOCKS.saturating_sub(1));
    let rolling_start =
        current_height.saturating_sub(CONVERSION_ORDER_ELIGIBILITY_BLOCKS.saturating_sub(1));
    let cycle_flow = collect_conversion_flow_stats(
        block_store,
        tx_store,
        cycle_start,
        cycle_start,
        current_height,
    );
    let rolling_flow = collect_conversion_flow_stats(
        block_store,
        tx_store,
        cycle_start,
        rolling_start,
        current_height,
    );
    let market_signal = conversion_market_signal(&snapshot, cycle_flow, rolling_flow);
    let bounded_factor = (1.0 - (market_signal * CONVERSION_DIFFICULTY_CLAMP)).clamp(
        1.0 - CONVERSION_DIFFICULTY_CLAMP,
        1.0 + CONVERSION_DIFFICULTY_CLAMP,
    );
    let new_difficulty = (time_adjusted_difficulty as f64 * bounded_factor).round() as u64;

    // Ensure the difficulty doesn't go below the initial difficulty
    let new_difficulty = new_difficulty.max(config.initial_difficulty);

    info!(
        "Difficulty adjustment: {} -> {} (time_factor: {:.2}, market_signal: {:.2}, tracked_miner_inventory: {}, open_orders: {})",
        current_difficulty,
        new_difficulty,
        clamped_factor,
        market_signal,
        snapshot.tracked_miner_coin_inventory_cents,
        snapshot.open_order_count,
    );

    Target::from_difficulty(new_difficulty)
}

/// Get the target at a specific height
pub fn get_target_at_height(block_store: &BlockStore<'_>, height: u64) -> Target {
    // Get the block at the specified height
    let block = match block_store.get_block_by_height(height) {
        Ok(Some(block)) => block,
        Ok(None) => {
            warn!(
                "Could not find block at height {}, using default target",
                height
            );
            return Target::from_difficulty(1);
        }
        Err(_e) => {
            warn!(
                "Could not find block at height {}, using default target",
                height
            );
            return Target::from_difficulty(1);
        }
    };

    Target::from_difficulty(block.difficulty.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        Block, CoinInventory, ConversionOrder, ConversionOrderKind, ConversionOrderRequest,
        RocksDBStore, StateStore, TransactionRecord, TransactionStatus, TxStore,
    };
    use tempfile::tempdir;

    #[cfg(feature = "legacy-test-compat")]
    #[test]
    fn test_difficulty_adjustment() {
        // Create a temporary directory for the database
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let block_store = BlockStore::new(&kv_store);
        let state_store = StateStore::new(&kv_store);
        let tx_store = TxStore::new(&kv_store);

        // Create a config
        let config = ConsensusConfig::default()
            .with_target_block_time(10)
            .with_difficulty_adjustment_window(2);

        // Create some test blocks
        // Block 0 (genesis) at time 0
        let block0 = Block {
            height: 0,
            hash: [0u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 0,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0u8; 32],
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0u8; 32],
            difficulty: 1,
            total_difficulty: 1,
        };

        // Block 1 at time 10 (on target)
        let block1 = Block {
            height: 1,
            hash: [1u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 10,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0u8; 32],
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 1,
            poh_hash: [0u8; 32],
            difficulty: config.initial_difficulty,
            total_difficulty: 2,
        };

        // Block 2 at time 30 (slower than target)
        let block2 = Block {
            height: 2,
            hash: [2u8; 32],
            prev_hash: [1u8; 32],
            timestamp: 30,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0u8; 32],
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 2,
            poh_hash: [0u8; 32],
            difficulty: config.initial_difficulty,
            total_difficulty: 3,
        };

        // Store the blocks
        block_store.put_block(&block0);
        block_store.put_block(&block1);
        block_store.put_block(&block2);

        // Calculate the next target
        let target = calculate_next_target(&config, &block_store, &state_store, &tx_store, 2);

        // The blocks took longer than expected, so difficulty should decrease
        // Expected time: 10 * 2 = 20
        // Actual time: 30 - 0 = 30
        // Adjustment factor: 20 / 30 = 0.67
        // New difficulty: initial_difficulty * 0.67
        let expected_difficulty = (config.initial_difficulty as f64 * 0.67) as u64;

        // Allow for some floating point imprecision
        let actual_difficulty = target.to_difficulty();
        let ratio = if actual_difficulty > expected_difficulty {
            actual_difficulty as f64 / expected_difficulty as f64
        } else {
            expected_difficulty as f64 / actual_difficulty as f64
        };

        assert!(ratio < 1.01, "Difficulty adjustment error too large");
    }

    #[test]
    fn test_conversion_market_pressure_eases_difficulty() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let block_store = BlockStore::new(&kv_store);
        let state_store = StateStore::new(&kv_store);
        let tx_store = TxStore::new(&kv_store);

        let config = ConsensusConfig::default()
            .with_target_block_time(10)
            .with_initial_difficulty(100)
            .with_difficulty_adjustment_window(2);

        let miner = [7u8; 32];
        let requester = [8u8; 32];

        state_store
            .create_account(&requester, 100, crate::storage::AccountType::User)
            .unwrap();
        state_store
            .create_account(&miner, 0, crate::storage::AccountType::User)
            .unwrap();

        let mut requester_state = state_store.get_account_state(&requester).unwrap();
        requester_state.conversion_order = Some(ConversionOrder::new(
            [9u8; 32],
            requester,
            ConversionOrderRequest {
                kind: ConversionOrderKind::BillToCoins,
                requested_value_cents: 100,
                requested_coin_inventory: CoinInventory::default(),
                requested_bill_denominations: vec![],
            },
            1,
        ));
        state_store
            .set_account_state(&requester, &requester_state)
            .unwrap();

        let block0 = Block {
            height: 0,
            hash: [0u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 0,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 100,
        };
        let block1 = Block {
            height: 1,
            hash: [1u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 10,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 1,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 200,
        };
        let create_tx = TransactionRecord {
            tx_id: [2u8; 32],
            sender: requester,
            recipient: [0u8; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: CoinInventory::default(),
            value: 0,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 10,
            nonce: 0,
            timestamp: 20,
            block_height: 2,
            data: None,
            conversion_intent: Some(ConversionTransaction::Create(ConversionOrderRequest {
                kind: ConversionOrderKind::BillToCoins,
                requested_value_cents: 100,
                requested_coin_inventory: CoinInventory::default(),
                requested_bill_denominations: vec![],
            })),
            status: TransactionStatus::Confirmed,
        };
        tx_store.put_transaction(&create_tx).unwrap();
        let block2 = Block {
            height: 2,
            hash: [3u8; 32],
            prev_hash: [1u8; 32],
            timestamp: 20,
            transactions: vec![create_tx.tx_id],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 2,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 300,
        };

        block_store.put_block(&block0).unwrap();
        block_store.put_block(&block1).unwrap();
        block_store.put_block(&block2).unwrap();

        let target = calculate_next_target(&config, &block_store, &state_store, &tx_store, 2);
        assert!(
            target.to_difficulty() < 100,
            "expected shortage-driven conversion pressure to ease difficulty"
        );
    }

    #[test]
    fn test_coins_to_bill_shortage_also_eases_difficulty() {
        let temp_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(temp_dir.path()).unwrap();
        let block_store = BlockStore::new(&kv_store);
        let state_store = StateStore::new(&kv_store);
        let tx_store = TxStore::new(&kv_store);

        let config = ConsensusConfig::default()
            .with_target_block_time(10)
            .with_initial_difficulty(100)
            .with_difficulty_adjustment_window(2);

        let miner = [17u8; 32];
        let requester = [18u8; 32];

        state_store
            .create_account(&requester, 100, crate::storage::AccountType::User)
            .unwrap();
        state_store
            .create_account(&miner, 0, crate::storage::AccountType::User)
            .unwrap();

        let mut requester_state = state_store.get_account_state(&requester).unwrap();
        let mut requested_coins = CoinInventory::default();
        requested_coins
            .add(crate::storage::Denomination::Cents50, 2)
            .unwrap();
        requester_state.conversion_order = Some(ConversionOrder::new(
            [19u8; 32],
            requester,
            ConversionOrderRequest {
                kind: ConversionOrderKind::CoinsToBill,
                requested_value_cents: 100,
                requested_coin_inventory: requested_coins,
                requested_bill_denominations: vec![crate::storage::Denomination::Dollars1],
            },
            1,
        ));
        state_store
            .set_account_state(&requester, &requester_state)
            .unwrap();

        let block0 = Block {
            height: 0,
            hash: [20u8; 32],
            prev_hash: [0u8; 32],
            timestamp: 0,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 100,
        };
        let block1 = Block {
            height: 1,
            hash: [21u8; 32],
            prev_hash: [20u8; 32],
            timestamp: 10,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 1,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 200,
        };
        let create_tx = TransactionRecord {
            tx_id: [22u8; 32],
            sender: requester,
            recipient: [0u8; 32],
            transfer_token_ids: vec![],
            fee_token_id: None,
            coin_transfer: CoinInventory::default(),
            coin_fee: CoinInventory::default(),
            value: 0,
            gas_price: 1,
            gas_limit: 21_000,
            gas_used: 10,
            nonce: 0,
            timestamp: 20,
            block_height: 2,
            data: None,
            conversion_intent: Some(ConversionTransaction::Create(ConversionOrderRequest {
                kind: ConversionOrderKind::CoinsToBill,
                requested_value_cents: 100,
                requested_coin_inventory: {
                    let mut inventory = CoinInventory::default();
                    inventory
                        .add(crate::storage::Denomination::Cents50, 2)
                        .unwrap();
                    inventory
                },
                requested_bill_denominations: vec![crate::storage::Denomination::Dollars1],
            })),
            status: TransactionStatus::Confirmed,
        };
        tx_store.put_transaction(&create_tx).unwrap();
        let block2 = Block {
            height: 2,
            hash: [23u8; 32],
            prev_hash: [21u8; 32],
            timestamp: 20,
            transactions: vec![create_tx.tx_id],
            conversion_fulfillment_order_ids: vec![],
            miner,
            pre_reward_state_root: [0u8; 32],
            reward_token_ids: vec![],
            state_root: [0u8; 32],
            result_commitment: [0u8; 32],
            tx_root: [0u8; 32],
            nonce: 0,
            poh_seq: 2,
            poh_hash: [0u8; 32],
            difficulty: 100,
            total_difficulty: 300,
        };

        block_store.put_block(&block0).unwrap();
        block_store.put_block(&block1).unwrap();
        block_store.put_block(&block2).unwrap();

        let target = calculate_next_target(&config, &block_store, &state_store, &tx_store, 2);
        assert!(
            target.to_difficulty() < 100,
            "expected bill-shortage conversion pressure to ease difficulty"
        );
    }
}

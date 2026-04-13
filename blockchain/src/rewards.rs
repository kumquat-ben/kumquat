use once_cell::sync::Lazy;
use serde::Deserialize;

use crate::storage::block_store::Hash;
use crate::storage::state::{Denomination, DenominationToken, TokenMintSource};

const MAX_MULTI_UNIT_REWARD: u32 = 8;

static MINING_SCHEDULE: Lazy<MiningScheduleDocument> = Lazy::new(|| {
    serde_json::from_str(include_str!("../kumquat_mining_schedule.json"))
        .expect("mining schedule JSON must be valid")
});

#[derive(Debug, Deserialize)]
struct MiningScheduleDocument {
    mining_schedule: MiningScheduleMetadata,
    eras: Vec<EraSchedule>,
}

#[derive(Debug, Deserialize)]
struct MiningScheduleMetadata {
    total_blocks: u64,
    blocks_per_era: u64,
}

#[derive(Debug, Deserialize)]
struct EraSchedule {
    era: u8,
    avg_units_per_block: f64,
    multi_unit_blocks_possible: bool,
    denominations: Vec<EraDenomination>,
}

#[derive(Debug, Deserialize)]
struct EraDenomination {
    denomination: String,
    units_in_era: u64,
}

#[derive(Debug, Clone)]
struct EraWeights {
    avg_units_per_block: f64,
    multi_unit_blocks_possible: bool,
    weights: Vec<(Denomination, f64)>,
}

pub fn reward_tokens_for_block(
    owner: Hash,
    block_height: u64,
    block_hash: &Hash,
) -> Vec<DenominationToken> {
    if block_height == 0 || block_height >= MINING_SCHEDULE.mining_schedule.total_blocks {
        return Vec::new();
    }

    let Some(era) = era_weights_for_height(block_height) else {
        return Vec::new();
    };

    let reward_count = sample_reward_count(&era, block_hash);
    if reward_count == 0 {
        return Vec::new();
    }

    (0..reward_count)
        .filter_map(|index| {
            sample_denomination(&era, block_hash, index as usize + 1).map(|denomination| {
                DenominationToken::new(
                    owner,
                    denomination,
                    block_height,
                    TokenMintSource::BlockReward,
                    index as u64,
                )
            })
        })
        .collect()
}

pub fn reward_token_ids_for_block(owner: Hash, block_height: u64, block_hash: &Hash) -> Vec<Hash> {
    reward_tokens_for_block(owner, block_height, block_hash)
        .into_iter()
        .map(|token| token.token_id)
        .collect()
}

fn era_weights_for_height(block_height: u64) -> Option<EraWeights> {
    let blocks_per_era = MINING_SCHEDULE.mining_schedule.blocks_per_era;
    let era_index = ((block_height - 1) / blocks_per_era) as usize;
    let era = MINING_SCHEDULE.eras.get(era_index)?;

    let total_units = era
        .denominations
        .iter()
        .map(|item| item.units_in_era)
        .sum::<u64>() as f64;
    if total_units == 0.0 {
        return Some(EraWeights {
            avg_units_per_block: era.avg_units_per_block,
            multi_unit_blocks_possible: era.multi_unit_blocks_possible,
            weights: Vec::new(),
        });
    }

    let mut weights = Vec::new();
    for item in &era.denominations {
        if item.units_in_era == 0 {
            continue;
        }

        let Some(denomination) = Denomination::parse(item.denomination.trim_start_matches('$'))
        else {
            continue;
        };

        weights.push((denomination, item.units_in_era as f64 / total_units));
    }

    Some(EraWeights {
        avg_units_per_block: era.avg_units_per_block,
        multi_unit_blocks_possible: era.multi_unit_blocks_possible,
        weights,
    })
}

fn sample_reward_count(era: &EraWeights, block_hash: &Hash) -> u32 {
    if era.avg_units_per_block <= 0.0 {
        return 0;
    }

    let unit_entropy = uniform_from_word(entropy_word(block_hash, 0));
    if era.multi_unit_blocks_possible {
        sample_poisson(era.avg_units_per_block, unit_entropy).min(MAX_MULTI_UNIT_REWARD)
    } else if unit_entropy < era.avg_units_per_block {
        1
    } else {
        0
    }
}

fn sample_denomination(
    era: &EraWeights,
    block_hash: &Hash,
    window_index: usize,
) -> Option<Denomination> {
    if era.weights.is_empty() {
        return None;
    }

    let roll = uniform_from_word(entropy_word(block_hash, window_index));
    let mut cumulative = 0.0;
    for (denomination, weight) in &era.weights {
        cumulative += *weight;
        if roll <= cumulative {
            return Some(*denomination);
        }
    }

    era.weights.last().map(|(denomination, _)| *denomination)
}

fn sample_poisson(lambda: f64, uniform: f64) -> u32 {
    let mut probability = (-lambda).exp();
    let mut cumulative = probability;
    let mut k = 0u32;

    while uniform > cumulative && k < MAX_MULTI_UNIT_REWARD {
        k += 1;
        probability *= lambda / k as f64;
        cumulative += probability;
    }

    k
}

fn entropy_word(block_hash: &Hash, window_index: usize) -> u32 {
    let byte_offset = window_index * 4;
    if byte_offset + 4 <= block_hash.len() {
        return u32::from_be_bytes([
            block_hash[byte_offset],
            block_hash[byte_offset + 1],
            block_hash[byte_offset + 2],
            block_hash[byte_offset + 3],
        ]);
    }

    let mut data = Vec::with_capacity(block_hash.len() + 8);
    data.extend_from_slice(block_hash);
    data.extend_from_slice(&(window_index as u64).to_be_bytes());
    let expanded = crate::crypto::hash::sha256(&data);
    u32::from_be_bytes([expanded[0], expanded[1], expanded[2], expanded[3]])
}

fn uniform_from_word(word: u32) -> f64 {
    (word as f64 + 0.5) / (u32::MAX as f64 + 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_second_era_at_boundary_height() {
        let blocks_per_era = MINING_SCHEDULE.mining_schedule.blocks_per_era;
        let era_one = era_weights_for_height(1).unwrap();
        let era_two = era_weights_for_height(blocks_per_era + 1).unwrap();

        assert!(era_one.avg_units_per_block > era_two.avg_units_per_block);
    }

    #[test]
    fn reward_generation_is_deterministic() {
        let owner = [7u8; 32];
        let block_hash = [3u8; 32];

        let a = reward_token_ids_for_block(owner, 1, &block_hash);
        let b = reward_token_ids_for_block(owner, 1, &block_hash);

        assert_eq!(a, b);
    }

    #[test]
    fn extinct_hundreds_do_not_reappear_after_era_five() {
        let blocks_per_era = MINING_SCHEDULE.mining_schedule.blocks_per_era;
        let block_height = blocks_per_era * 4 + 1;

        for nonce in 0u8..=64 {
            let block_hash = [nonce; 32];
            let minted = reward_tokens_for_block([9u8; 32], block_height, &block_hash);
            assert!(minted
                .iter()
                .all(|token| token.denomination != Denomination::Dollars100));
        }
    }

    #[test]
    fn non_multi_unit_eras_never_emit_more_than_one_unit() {
        let blocks_per_era = MINING_SCHEDULE.mining_schedule.blocks_per_era;
        let block_height = blocks_per_era * 2 + 1;

        for nonce in 0u8..=64 {
            let block_hash = [nonce; 32];
            let minted = reward_tokens_for_block([5u8; 32], block_height, &block_hash);
            assert!(minted.len() <= 1);
        }
    }
}

use once_cell::sync::Lazy;
use serde::Deserialize;

use crate::storage::block_store::Hash;
use crate::storage::state::{
    assignment_index_to_token_id, Denomination, DenominationToken, TokenMintSource,
};

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
}

#[derive(Debug, Clone)]
struct EraWeights {
    avg_units_per_block: f64,
    multi_unit_blocks_possible: bool,
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
            sample_assignment_index(block_height, block_hash, index as usize + 1).map(
                |assignment_index| {
                    DenominationToken::new(
                        owner,
                        assignment_index,
                        block_height,
                        TokenMintSource::BlockReward,
                    )
                },
            )
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

    Some(EraWeights {
        avg_units_per_block: era.avg_units_per_block,
        multi_unit_blocks_possible: era.multi_unit_blocks_possible,
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

fn sample_assignment_index(
    block_height: u64,
    block_hash: &Hash,
    window_index: usize,
) -> Option<u64> {
    let total = Denomination::total_assignment_count();
    if total == 0 {
        return None;
    }

    let mut data = Vec::with_capacity(block_hash.len() + 16);
    data.extend_from_slice(block_hash);
    data.extend_from_slice(&block_height.to_be_bytes());
    data.extend_from_slice(&(window_index as u64).to_be_bytes());
    let digest = crate::crypto::hash::sha256(&data);
    let mut word = [0u8; 8];
    word.copy_from_slice(&digest[..8]);
    Some(u64::from_be_bytes(word) % total)
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
    fn reward_token_ids_encode_assignment_indices() {
        let block_hash = [11u8; 32];
        let minted = reward_tokens_for_block([9u8; 32], 42, &block_hash);
        for token in minted {
            assert_eq!(token.token_id, assignment_index_to_token_id(token.assignment_index));
            assert!(Denomination::from_assignment_index(token.assignment_index).is_some());
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

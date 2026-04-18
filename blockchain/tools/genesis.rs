use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::crypto::{decode_address, encode_address};
use crate::storage::block_store::{pow_hash, result_commitment, Block, Hash};
use crate::storage::state::{
    assignment_index_for_denomination, AccountState, AccountType, Denomination, DenominationToken,
    TokenMintSource,
};
use crate::storage::MerklePatriciaTrie;

/// Genesis configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisConfig {
    /// Chain ID
    pub chain_id: u64,

    /// Genesis timestamp
    pub timestamp: u64,

    /// Initial difficulty
    pub initial_difficulty: u64,

    /// Initial accounts
    pub initial_accounts: HashMap<String, GenesisAccount>,
}

/// Genesis account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisAccount {
    /// Legacy compatibility field in cents.
    pub balance: Option<u64>,

    /// Exact owned denominations to mint at genesis.
    pub denominations: Option<Vec<String>>,

    /// Account type
    pub account_type: String,
}

/// Auditable genesis ceremony artifact derived from a genesis config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisCeremonyRecord {
    /// Chain ID for the resulting chain identity.
    pub chain_id: u64,

    /// Genesis config path used to derive the ceremony record.
    pub genesis_config_path: String,

    /// Genesis block hash that operators must pin.
    pub genesis_hash: String,

    /// Fully-qualified chain identity.
    pub chain_identity: String,

    /// Finalized genesis state root.
    pub state_root: String,

    /// Genesis timestamp from the config.
    pub timestamp: u64,

    /// Genesis difficulty from the config.
    pub initial_difficulty: u64,

    /// Deterministically sorted account summaries included in the genesis state.
    pub accounts: Vec<GenesisCeremonyAccount>,
}

/// Account summary included in a genesis ceremony record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenesisCeremonyAccount {
    pub address: String,
    pub balance: u64,
    pub token_count: usize,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        let mut initial_accounts = HashMap::new();

        // Add some initial accounts
        initial_accounts.insert(
            encode_address(&{
                let mut address = [0u8; 32];
                address[31] = 1;
                address
            }),
            GenesisAccount {
                balance: None,
                denominations: Some(
                    Denomination::reward_set()
                        .into_iter()
                        .map(|denomination| denomination.to_string())
                        .collect(),
                ),
                account_type: "User".to_string(),
            },
        );

        initial_accounts.insert(
            encode_address(&{
                let mut address = [0u8; 32];
                address[31] = 2;
                address
            }),
            GenesisAccount {
                balance: None,
                denominations: Some(
                    Denomination::reward_set()
                        .into_iter()
                        .map(|denomination| denomination.to_string())
                        .collect(),
                ),
                account_type: "User".to_string(),
            },
        );

        Self {
            chain_id: 1,
            timestamp: 1609459200, // 2021-01-01 00:00:00 UTC
            initial_difficulty: 1000,
            initial_accounts,
        }
    }
}

impl GenesisConfig {
    /// Load genesis configuration from a file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let config_str = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read genesis config file: {}", e))?;

        let config: GenesisConfig = toml::from_str(&config_str)
            .map_err(|e| format!("Failed to parse genesis config file: {}", e))?;

        Ok(config)
    }

    /// Save genesis configuration to a file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), String> {
        let config_str = toml::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize genesis config: {}", e))?;

        fs::write(path, config_str)
            .map_err(|e| format!("Failed to write genesis config file: {}", e))?;

        Ok(())
    }

    /// Generate a default genesis configuration file if it doesn't exist
    pub fn generate_default<P: AsRef<Path>>(path: P) -> Result<(), String> {
        let path = path.as_ref();

        if path.exists() {
            info!("Genesis config file already exists at {:?}", path);
            return Ok(());
        }

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create genesis config directory: {}", e))?;
            }
        }

        // Create default config
        let config = GenesisConfig::default();

        // Save config
        config.save(path)?;

        info!("Generated default genesis config at {:?}", path);
        Ok(())
    }

    /// Generate a genesis block from the configuration
    pub fn generate_block(&self) -> Block {
        // Create an empty block
        let mut block = Block {
            height: 0,
            hash: [0; 32],
            prev_hash: [0; 32],
            timestamp: self.timestamp,
            transactions: vec![],
            conversion_fulfillment_order_ids: vec![],
            miner: [0; 32],
            pre_reward_state_root: [0; 32],
            reward_token_ids: vec![],
            result_commitment: [0; 32],
            state_root: [0; 32], // Will be calculated later
            tx_root: [0; 32],    // Empty transaction root
            nonce: 0,
            poh_seq: 0,
            poh_hash: [0; 32],
            difficulty: self.initial_difficulty,
            total_difficulty: self.initial_difficulty as u128,
        };

        // Calculate the block hash
        block.hash = pow_hash(&block.canonical_header());
        block.result_commitment = result_commitment(
            &block.hash,
            &block.state_root,
            &block.reward_token_ids,
            &block.conversion_fulfillment_order_ids,
        );

        block
    }

    pub fn finalize_genesis_block(
        &self,
        mut block: Block,
        account_states: &[(Hash, AccountState)],
    ) -> Result<Block, String> {
        let mut trie = MerklePatriciaTrie::new();
        let mut sorted_accounts = account_states.to_vec();
        sorted_accounts.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (address, state) in sorted_accounts {
            let mut state = state;
            state.assign_token_owner(address);
            state.sync_balance_from_tokens();

            let normalized = crate::storage::state_store::StateStore::normalize_account_state(
                state,
                Some(address),
            );
            let state_bytes =
                crate::storage::state_store::StateStore::canonical_account_state_bytes(&normalized)
                    .map_err(|e| format!("Failed to serialize genesis account state: {}", e))?;
            trie.insert(&address, state_bytes);
        }

        let genesis_root = trie.root_hash();
        block.pre_reward_state_root = genesis_root;
        block.state_root = genesis_root;

        block.hash = pow_hash(&block.canonical_header());
        block.result_commitment = result_commitment(
            &block.hash,
            &block.state_root,
            &block.reward_token_ids,
            &block.conversion_fulfillment_order_ids,
        );

        Ok(block)
    }

    /// Generate initial account states from the configuration
    pub fn generate_account_states(&self) -> Vec<(Hash, AccountState)> {
        let mut account_states = Vec::new();
        let mut denomination_ordinals = HashMap::new();
        let mut sorted_accounts = self.initial_accounts.iter().collect::<Vec<_>>();
        sorted_accounts.sort_by(|(address_a, _), (address_b, _)| address_a.cmp(address_b));

        for (address_text, account) in sorted_accounts {
            let address = decode_address(address_text)
                .unwrap_or_else(|_| panic!("Invalid address '{}' in genesis config", address_text));

            // Parse the account type
            let account_type = match account.account_type.as_str() {
                "User" => AccountType::User,
                "Contract" => AccountType::Contract,
                "System" => AccountType::System,
                _ => {
                    warn!(
                        "Unknown account type: {}, defaulting to User",
                        account.account_type
                    );
                    AccountType::User
                }
            };

            let tokens = self.generate_account_tokens(address, account, &mut denomination_ordinals);

            // Create the account state
            let state = if !tokens.is_empty() {
                AccountState::from_tokens(address, account_type, tokens, 0)
            } else {
                let bootstrap_balance = account.balance.unwrap_or(0);
                match account_type {
                    AccountType::User => AccountState::new_user(bootstrap_balance, 0),
                    AccountType::Contract => {
                        AccountState::new_contract(bootstrap_balance, Vec::new(), 0)
                    }
                    AccountType::System => AccountState::new_system(bootstrap_balance, 0),
                    AccountType::Validator => {
                        AccountState::new_validator(bootstrap_balance, bootstrap_balance, 0)
                    }
                }
            };

            account_states.push((address, state));
        }

        account_states
    }

    fn generate_account_tokens(
        &self,
        address: Hash,
        account: &GenesisAccount,
        denomination_ordinals: &mut HashMap<Denomination, u64>,
    ) -> Vec<DenominationToken> {
        if let Some(denominations) = &account.denominations {
            return denominations
                .iter()
                .filter_map(|value| {
                    let denomination = Denomination::parse(value);
                    if denomination.is_none() {
                        warn!("Unknown denomination '{}' in genesis account", value);
                    }
                    denomination.and_then(|denomination| {
                        let next_ordinal = denomination_ordinals.entry(denomination).or_insert(0);
                        let assignment_index =
                            assignment_index_for_denomination(denomination, *next_ordinal).ok()?;
                        *next_ordinal += 1;
                        Some(DenominationToken::new(
                            address,
                            assignment_index,
                            0,
                            TokenMintSource::Genesis,
                        ))
                    })
                })
                .collect();
        }

        let mut remaining = account.balance.unwrap_or(0);
        let mut tokens = Vec::new();
        let mut local_ordinals = HashMap::new();
        for denomination in Denomination::all_descending() {
            while remaining >= denomination.value_cents() {
                let next_ordinal = local_ordinals.entry(*denomination).or_insert(0);
                let assignment_index =
                    match assignment_index_for_denomination(*denomination, *next_ordinal) {
                        Ok(index) => index,
                        Err(_) => break,
                    };
                tokens.push(DenominationToken::new(
                    address,
                    assignment_index,
                    0,
                    TokenMintSource::LegacyBalanceBootstrap,
                ));
                *next_ordinal += 1;
                remaining -= denomination.value_cents();
            }
        }
        tokens
    }
}

/// Generate a genesis block and initial account states
pub fn generate_genesis<P: AsRef<Path>>(
    config_path: P,
) -> Result<(Block, Vec<(Hash, AccountState)>), String> {
    // Load the genesis configuration
    let config = GenesisConfig::load(config_path)?;

    // Generate the genesis block
    // Generate the initial account states
    let account_states = config.generate_account_states();

    // Generate the finalized genesis block using the real initial state root.
    let block = config.finalize_genesis_block(config.generate_block(), &account_states)?;

    Ok((block, account_states))
}

/// Build a deterministic ceremony record for operator verification and signing.
pub fn build_genesis_ceremony_record<P: AsRef<Path>>(
    config_path: P,
) -> Result<GenesisCeremonyRecord, String> {
    let config_path = config_path.as_ref();
    let config = GenesisConfig::load(config_path)?;
    let (block, account_states) = generate_genesis(config_path)?;

    let mut accounts = account_states
        .iter()
        .map(|(address, state)| GenesisCeremonyAccount {
            address: encode_address(address),
            balance: state.balance,
            token_count: state.tokens.len(),
        })
        .collect::<Vec<_>>();
    accounts.sort_by(|a, b| a.address.cmp(&b.address));

    Ok(GenesisCeremonyRecord {
        chain_id: config.chain_id,
        genesis_config_path: config_path.display().to_string(),
        genesis_hash: hex::encode(block.hash),
        chain_identity: format!("chain-{}:{}", config.chain_id, hex::encode(block.hash)),
        state_root: hex::encode(block.state_root),
        timestamp: config.timestamp,
        initial_difficulty: config.initial_difficulty,
        accounts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_store::pow_hash;
    use crate::storage::kv_store::RocksDBStore;
    use crate::storage::state_store::StateStore;
    use crate::storage::Denomination;
    use tempfile::tempdir;

    #[test]
    fn genesis_derivation_is_deterministic_across_runs() {
        let dir = tempdir().unwrap();
        let genesis_path = dir.path().join("genesis.toml");
        GenesisConfig::default().save(&genesis_path).unwrap();

        let first = build_genesis_ceremony_record(&genesis_path).unwrap();
        let second = build_genesis_ceremony_record(&genesis_path).unwrap();

        assert_eq!(first.genesis_hash, second.genesis_hash);
        assert_eq!(first.state_root, second.state_root);
        assert_eq!(first.accounts, second.accounts);
    }

    #[test]
    fn genesis_account_states_start_in_hybrid_mode() {
        let config = GenesisConfig::default();
        let account_states = config.generate_account_states();

        assert!(!account_states.is_empty());
        for (_, state) in account_states {
            assert_eq!(state.total_account_value(), state.balance);
            assert_eq!(
                state.total_account_value(),
                state.total_bill_value() + state.total_coin_value()
            );
            assert!(!state.bills.is_empty());
            assert!(state.coin_inventory.count(Denomination::Cents25) > 0);
        }
    }

    #[test]
    fn generated_genesis_hash_matches_runtime_initialized_hash() {
        let dir = tempdir().unwrap();
        let genesis_path = dir.path().join("genesis.toml");
        GenesisConfig::default().save(&genesis_path).unwrap();

        let (generated_block, generated_accounts) = generate_genesis(&genesis_path).unwrap();

        let state_dir = tempdir().unwrap();
        let kv_store = RocksDBStore::new(state_dir.path()).unwrap();
        let state_store = StateStore::new(&kv_store);

        for (address, state) in generated_accounts {
            let mut state = state;
            state.assign_token_owner(address);
            state.sync_balance_from_tokens();
            state_store.set_account_state(&address, &state).unwrap();
        }

        let runtime_root = state_store
            .calculate_state_root(0, generated_block.timestamp)
            .unwrap();

        let mut runtime_block = generated_block.clone();
        runtime_block.pre_reward_state_root = runtime_root.root_hash;
        runtime_block.state_root = runtime_root.root_hash;
        runtime_block.hash = pow_hash(&runtime_block.canonical_header());
        runtime_block.result_commitment = result_commitment(
            &runtime_block.hash,
            &runtime_block.state_root,
            &runtime_block.reward_token_ids,
            &runtime_block.conversion_fulfillment_order_ids,
        );

        assert_eq!(generated_block.state_root, runtime_block.state_root);
        assert_eq!(generated_block.hash, runtime_block.hash);
        assert_eq!(generated_block.result_commitment, runtime_block.result_commitment);
    }
}

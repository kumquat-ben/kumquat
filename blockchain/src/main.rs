use log::{error, info, warn};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::time::Duration;

use kumquat::api;
use kumquat::config::Config;
use kumquat::consensus::config::ConsensusConfig;
use kumquat::consensus::start_consensus;
use kumquat::init_logger;
use kumquat::mempool::Mempool;
use kumquat::network::NetworkConfig;
use kumquat::network::start_enhanced_network;
use kumquat::node_runtime::NodeRuntime;
use kumquat::storage::state::AccountState;
use kumquat::storage::Block;
use kumquat::storage::{BatchOperationManager, BlockStore, RocksDBStore, StateRoot, StateStore, TxStore};
use kumquat::tools::genesis::generate_genesis;

fn parse_bootstrap_target(spec: &str) -> Option<(String, u16)> {
    let spec = spec.trim();
    if spec.is_empty() {
        return None;
    }

    if let Ok(addr) = spec.parse::<SocketAddr>() {
        return Some((addr.ip().to_string(), addr.port()));
    }

    let segments = spec
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() >= 4
        && matches!(segments[0], "dns" | "dns4" | "dns6" | "ip4" | "ip6")
        && segments[2] == "tcp"
    {
        if let Ok(port) = segments[3].parse::<u16>() {
            return Some((segments[1].to_string(), port));
        }
    }

    if let Some((host, port_text)) = spec.rsplit_once(':') {
        if let Ok(port) = port_text.parse::<u16>() {
            return Some((host.to_string(), port));
        }
    }

    None
}

fn resolve_bootstrap_addr(spec: &str) -> Option<SocketAddr> {
    let (host, port) = match parse_bootstrap_target(spec) {
        Some(target) => target,
        None => {
            warn!("Ignoring unsupported bootstrap node format: {}", spec);
            return None;
        }
    };

    match (host.as_str(), port).to_socket_addrs() {
        Ok(mut addresses) => addresses.next().or_else(|| {
            warn!("Bootstrap node resolved to no socket addresses: {}", spec);
            None
        }),
        Err(err) => {
            warn!("Failed to resolve bootstrap node {}: {}", spec, err);
            None
        }
    }
}

fn resolve_miner_address(node_id: Option<&str>, node_name: &str) -> [u8; 32] {
    if let Some(node_id) = node_id {
        if let Ok(address) = kumquat::crypto::decode_address(node_id) {
            return address;
        }
    }

    kumquat::crypto::hash::sha256(node_name.as_bytes())
}

fn parse_genesis_hash(hash_text: &str) -> [u8; 32] {
    let bytes = match hex::decode(hash_text) {
        Ok(bytes) => bytes,
        Err(err) => {
            error!("Invalid configured genesis hash '{}': {}", hash_text, err);
            std::process::exit(1);
        }
    };

    if bytes.len() != 32 {
        error!(
            "Invalid configured genesis hash '{}': expected 32 bytes, got {}",
            hash_text,
            bytes.len()
        );
        std::process::exit(1);
    }

    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes);
    hash
}

fn resolve_expected_genesis(
    genesis_path: &Path,
    configured_hash: Option<&str>,
) -> ([u8; 32], Block, Vec<([u8; 32], AccountState)>) {
    if !genesis_path.exists() {
        info!(
            "Genesis config missing at {:?}; generating default genesis config.",
            genesis_path
        );
        if let Err(err) = kumquat::tools::genesis::GenesisConfig::generate_default(genesis_path) {
            error!("Failed to generate default genesis config: {}", err);
            std::process::exit(1);
        }
    }

    let (genesis_block, genesis_accounts) = match generate_genesis(genesis_path) {
        Ok(result) => result,
        Err(err) => {
            error!(
                "Failed to build genesis block from {:?}: {}",
                genesis_path, err
            );
            std::process::exit(1);
        }
    };

    let generated_hash_text = hex::encode(genesis_block.hash);
    info!(
        "Resolved expected genesis block hash from {:?}: {}",
        genesis_path, generated_hash_text
    );

    if let Some(hash_text) = configured_hash {
        let configured_hash = parse_genesis_hash(hash_text);
        info!("Using configured expected genesis hash: {}", hash_text);

        if configured_hash != genesis_block.hash {
            error!(
                "Configured genesis hash {} does not match genesis config at {:?} (computed {}). Refusing to start.",
                hash_text,
                genesis_path,
                generated_hash_text
            );
            std::process::exit(1);
        }

        return (configured_hash, genesis_block, genesis_accounts);
    }

    (genesis_block.hash, genesis_block, genesis_accounts)
}

fn chain_identity(chain_id: u64, genesis_hash: [u8; 32]) -> String {
    format!("chain-{}:{}", chain_id, hex::encode(genesis_hash))
}

fn ensure_local_genesis(
    expected_genesis_hash: [u8; 32],
    genesis_block: Block,
    genesis_accounts: Vec<([u8; 32], AccountState)>,
    block_store: &BlockStore<'static>,
    state_store: &StateStore<'static>,
) {
    match block_store.get_block_by_height(0) {
        Ok(Some(existing_genesis)) => {
            if existing_genesis.hash != expected_genesis_hash {
                error!(
                    "Stored genesis hash {} does not match expected genesis hash {}. Refusing to start.",
                    hex::encode(existing_genesis.hash),
                    hex::encode(expected_genesis_hash)
                );
                std::process::exit(1);
            }

            let persisted_root = match state_store.get_state_root_at_height(0) {
                Ok(root) => root,
                Err(err) => {
                    error!("Failed to inspect persisted genesis state root: {}", err);
                    std::process::exit(1);
                }
            };

            let current_root = match state_store.calculate_state_root(0, existing_genesis.timestamp) {
                Ok(root) => root,
                Err(err) => {
                    error!("Failed to calculate current genesis state root: {}", err);
                    std::process::exit(1);
                }
            };

            let persisted_root_matches_block_store = persisted_root
                .as_ref()
                .is_some_and(|root| root.root_hash == existing_genesis.state_root);

            if existing_genesis.state_root != current_root.root_hash {
                if persisted_root_matches_block_store {
                    warn!(
                        "Local genesis root drift detected. block_store={} persisted_state_root={} calculated_state_root={}. Continuing with the persisted chain root because the block store and stored state root still agree.",
                        hex::encode(existing_genesis.state_root),
                        persisted_root
                            .as_ref()
                            .map(|root| hex::encode(root.root_hash))
                            .unwrap_or_else(|| "missing".to_string()),
                        hex::encode(current_root.root_hash),
                    );
                } else {
                    error!(
                        "Local genesis root mismatch. block_store={}, persisted_state_root={}, calculated_state_root={}. Refusing to start. Reinitialize local data to repair the chain root.",
                        hex::encode(existing_genesis.state_root),
                        persisted_root
                            .as_ref()
                            .map(|root| hex::encode(root.root_hash))
                            .unwrap_or_else(|| "missing".to_string()),
                        hex::encode(current_root.root_hash),
                    );
                    std::process::exit(1);
                }
            }
            info!(
                "Verified local genesis block hash matches expected chain root: {}",
                hex::encode(existing_genesis.hash)
            );
            info!(
                "Using existing local genesis block as chain root: {}",
                hex::encode(existing_genesis.hash)
            );
            return;
        }
        Ok(None) => {
            info!("No local genesis block found in storage; initializing block 0.");
        }
        Err(err) => {
            error!("Failed to inspect local chain root: {}", err);
            std::process::exit(1);
        }
    }

    let mut genesis_block = genesis_block;
    info!(
        "Initializing local genesis block with hash: {}",
        hex::encode(genesis_block.hash)
    );

    for (address, state) in &genesis_accounts {
        let mut state = state.clone();
        state.assign_token_owner(*address);
        state.sync_balance_from_tokens();
        match state_store.set_account_state(address, &state) {
            Ok(_) => {
                info!(
                    "Created genesis account: {} with {} cents across {} tokens",
                    hex::encode(address),
                    state.balance,
                    state.tokens.len()
                );
            }
            Err(err) => {
                error!("Failed to create genesis account: {}", err);
                std::process::exit(1);
            }
        }
    }

    let genesis_state_root = match state_store.calculate_state_root(0, genesis_block.timestamp) {
        Ok(root) => root,
        Err(err) => {
            error!("Failed to calculate genesis state root: {}", err);
            std::process::exit(1);
        }
    };
    if let Err(err) = state_store.put_state_root(&genesis_state_root) {
        error!("Failed to persist genesis state root: {}", err);
        std::process::exit(1);
    }

    genesis_block.pre_reward_state_root = genesis_state_root.root_hash;
    genesis_block.state_root = genesis_state_root.root_hash;
    genesis_block.result_commitment = kumquat::storage::block_store::result_commitment(
        &genesis_block.hash,
        &genesis_block.state_root,
        &genesis_block.reward_token_ids,
        &genesis_block.conversion_fulfillment_order_ids,
    );

    if let Err(err) = block_store.put_block(&genesis_block) {
        error!("Failed to store genesis block: {}", err);
        std::process::exit(1);
    }

    info!(
        "Stored local genesis block as chain root: {}",
        hex::encode(genesis_block.hash)
    );
}

fn ensure_tip_state_root_consistency(
    block_store: &BlockStore<'static>,
    state_store: &StateStore<'static>,
) {
    let Some(tip_height) = block_store.get_latest_height() else {
        return;
    };

    let tip_block = match block_store.get_block_by_height(tip_height) {
        Ok(Some(block)) => block,
        Ok(None) => {
            error!("Latest block height {} is missing from block store", tip_height);
            std::process::exit(1);
        }
        Err(err) => {
            error!("Failed to load latest block at height {}: {}", tip_height, err);
            std::process::exit(1);
        }
    };

    let persisted_root = match state_store.get_state_root_at_height(tip_height) {
        Ok(root) => root,
        Err(err) => {
            error!("Failed to load persisted state root at height {}: {}", tip_height, err);
            std::process::exit(1);
        }
    };

    let calculated_root = match state_store.calculate_state_root(tip_height, tip_block.timestamp) {
        Ok(root) => root,
        Err(err) => {
            error!("Failed to calculate state root at height {}: {}", tip_height, err);
            std::process::exit(1);
        }
    };

    let persisted_root_hash = persisted_root
        .as_ref()
        .map(|root| root.root_hash)
        .unwrap_or(calculated_root.root_hash);

    if tip_block.state_root != persisted_root_hash || tip_block.state_root != calculated_root.root_hash {
        error!(
            "Tip state root mismatch at height {}. block_store={}, persisted_state_root={}, calculated_state_root={}. Refusing to start.",
            tip_height,
            hex::encode(tip_block.state_root),
            hex::encode(persisted_root_hash),
            hex::encode(calculated_root.root_hash),
        );
        std::process::exit(1);
    }

    state_store.set_state_root(StateRoot::new(
        calculated_root.root_hash,
        tip_height,
        tip_block.timestamp,
    ));
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kumquat", about = "Kumquat blockchain node")]
struct Opt {
    /// Config file
    #[structopt(short, long, parse(from_os_str))]
    config: Option<PathBuf>,

    /// Genesis file
    #[structopt(short, long, parse(from_os_str))]
    genesis: Option<PathBuf>,

    /// Network (dev, testnet, mainnet)
    #[structopt(short, long)]
    network: Option<String>,

    /// Data directory
    #[structopt(short, long, parse(from_os_str))]
    data_dir: Option<PathBuf>,

    /// Bootstrap nodes
    #[structopt(short, long)]
    bootstrap: Option<String>,

    /// Enable mining
    #[structopt(long)]
    enable_mining: Option<bool>,

    /// Mining threads
    #[structopt(long)]
    mining_threads: Option<usize>,

    /// API port
    #[structopt(long)]
    api_port: Option<u16>,

    /// API host
    #[structopt(long)]
    api_host: Option<String>,

    /// Listen port
    #[structopt(long)]
    listen_port: Option<u16>,

    /// Listen address
    #[structopt(long)]
    listen_addr: Option<String>,
}

#[tokio::main]
async fn main() {
    // Initialize logger
    init_logger();

    // Parse command line arguments
    let opt = Opt::from_args();

    info!("Starting Kumquat node...");

    // Load or generate configuration
    let mut config = if let Some(config_path) = &opt.config {
        match Config::load(config_path) {
            Ok(config) => {
                info!("Loaded configuration from {:?}", config_path);
                config
            }
            Err(e) => {
                error!("Failed to load configuration: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        info!("Using default configuration");
        Config::default()
    };

    // Update config with command line arguments
    if let Some(network) = &opt.network {
        match network.as_str() {
            "dev" => {
                config.consensus.chain_id = 1337;
                config.consensus.target_block_time = 5;
                config.consensus.initial_difficulty = 100;
                config.consensus.enable_mining = true;
            }
            "testnet" => {
                config.consensus.chain_id = 2;
                config.consensus.target_block_time = 10;
                config.consensus.initial_difficulty = 1000;
                config.network.bootstrap_nodes = vec![
                    "/dns4/bootstrap1.kumquat.network/tcp/30333/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".to_string(),
                    "/dns4/bootstrap2.kumquat.network/tcp/30333/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9".to_string(),
                ];
            }
            "mainnet" => {
                config.consensus.chain_id = 1;
                config.consensus.target_block_time = 10;
                config.consensus.initial_difficulty = 10000;
                config.network.bootstrap_nodes = vec![
                    "/dns4/bootstrap1.kumquat.network/tcp/30333/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".to_string(),
                    "/dns4/bootstrap2.kumquat.network/tcp/30333/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9".to_string(),
                    "/dns4/bootstrap3.kumquat.network/tcp/30333/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9".to_string(),
                    "/dns4/bootstrap4.kumquat.network/tcp/30333/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9".to_string(),
                ];
            }
            _ => {
                warn!("Unknown network: {}, using default", network);
            }
        }
    }

    if let Some(data_dir) = &opt.data_dir {
        config.node.data_dir = data_dir.to_string_lossy().to_string();
    }

    if let Some(bootstrap) = &opt.bootstrap {
        config.network.bootstrap_nodes = bootstrap
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s.contains('/') || s.contains(':') {
                    s.to_string()
                } else {
                    format!("{}:30333", s)
                }
            })
            .collect();
    }

    if let Some(enable_mining) = opt.enable_mining {
        config.consensus.enable_mining = enable_mining;
    }

    if let Some(mining_threads) = opt.mining_threads {
        config.consensus.mining_threads = mining_threads;
    }

    if let Some(api_port) = opt.api_port {
        config.node.api_port = api_port;
    }

    if let Some(api_host) = opt.api_host {
        config.node.api_host = api_host;
    }

    if let Some(listen_port) = opt.listen_port {
        config.network.listen_port = listen_port;
    }

    if let Some(listen_addr) = opt.listen_addr {
        config.network.listen_addr = listen_addr;
    }

    // Create data directory if it doesn't exist
    let data_dir = Path::new(&config.node.data_dir);
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir).expect("Failed to create data directory");
    }

    // Determine genesis config path for local initialization if needed.
    let genesis_path = if let Some(genesis_path) = &opt.genesis {
        genesis_path.clone()
    } else {
        data_dir.join("genesis.toml")
    };

    // Initialize storage
    info!("Initializing storage...");
    let db_path = Path::new(&config.storage.db_path);
    if !db_path.exists() {
        std::fs::create_dir_all(db_path).expect("Failed to create database directory");
    }

    // Create a RocksDBStore with 'static lifetime
    let kv_store = Arc::new(RocksDBStore::new(db_path).expect("Failed to initialize RocksDB"));

    // Create a wrapper struct that implements KVStore and has a 'static lifetime
    struct StaticKVStore {
        inner: Arc<RocksDBStore>,
    }

    impl kumquat::storage::KVStore for StaticKVStore {
        fn put(
            &self,
            key: &[u8],
            value: &[u8],
        ) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.put(key, value)
        }

        fn get(
            &self,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, kumquat::storage::kv_store::KVStoreError> {
            self.inner.get(key)
        }

        fn delete(&self, key: &[u8]) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.delete(key)
        }

        fn exists(&self, key: &[u8]) -> Result<bool, kumquat::storage::kv_store::KVStoreError> {
            self.inner.exists(key)
        }

        fn write_batch(
            &self,
            operations: Vec<kumquat::storage::kv_store::WriteBatchOperation>,
        ) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.write_batch(operations)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, kumquat::storage::kv_store::KVStoreError> {
            self.inner.scan_prefix(prefix)
        }

        fn flush(&self) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.flush()
        }
    }

    // Create a static KVStore
    let static_kv_store = Arc::new(StaticKVStore {
        inner: kv_store.clone(),
    });

    // Create a static reference to the KVStore
    // This is safe because these stores will live for the entire program
    let static_kv_store_box = Box::new(StaticKVStore {
        inner: kv_store.clone(),
    });
    let kv_store_static = Box::leak(static_kv_store_box) as &'static StaticKVStore;

    let block_store = Arc::new(BlockStore::new(kv_store_static));
    let tx_store = Arc::new(TxStore::new(kv_store_static));
    let state_store = Arc::new(StateStore::new(kv_store_static));

    // Create batch operation manager
    let _batch_manager = Arc::new(BatchOperationManager::new(
        kv_store.clone(),
        block_store.clone(),
        tx_store.clone(),
        state_store.clone(),
    ));

    let (expected_genesis_hash, genesis_block, genesis_accounts) =
        resolve_expected_genesis(&genesis_path, config.consensus.genesis_hash.as_deref());
    let expected_genesis_hash_text = hex::encode(expected_genesis_hash);
    let configured_genesis_hash = config.consensus.genesis_hash.clone();

    info!("Genesis config path: {:?}", genesis_path);
    info!("Expected genesis hash: {}", expected_genesis_hash_text);
    if let Some(hash) = &configured_genesis_hash {
        info!("Configured genesis hash pin: {}", hash);
    } else {
        info!(
            "Configured genesis hash pin: none; node will trust the genesis config at {:?}",
            genesis_path
        );
    }
    info!(
        "Resolved chain identity: {}",
        chain_identity(config.consensus.chain_id, expected_genesis_hash)
    );

    ensure_local_genesis(
        expected_genesis_hash,
        genesis_block,
        genesis_accounts,
        &block_store,
        &state_store,
    );
    ensure_tip_state_root_consistency(&block_store, &state_store);

    // Initialize consensus
    info!("Initializing consensus...");
    let (network_tx, _network_rx) =
        mpsc::channel::<kumquat::network::types::message::NetMessage>(100);

    // Convert config.consensus to ConsensusConfig
    let consensus_config = ConsensusConfig {
        enable_mining: config.consensus.enable_mining,
        mining_threads: config.consensus.mining_threads,
        target_block_time: config.consensus.target_block_time,
        initial_difficulty: config.consensus.initial_difficulty,
        difficulty_adjustment_window: config.consensus.difficulty_adjustment_interval,
        max_transactions_per_block: config.consensus.max_transactions_per_block,
        poh_tick_rate: 400_000, // Default value
        miner_address: resolve_miner_address(
            config.node.node_id.as_deref(),
            &config.node.node_name,
        ),
        hybrid_activation_height: config.consensus.hybrid_activation_height,
    };

    let consensus = start_consensus(
        consensus_config,
        static_kv_store,
        block_store.clone(),
        tx_store.clone(),
        state_store.clone(),
        network_tx.clone(),
    )
    .await;

    info!("Initializing network...");
    let resolved_seed_peers = config
        .network
        .bootstrap_nodes
        .iter()
        .filter_map(|addr| resolve_bootstrap_addr(addr))
        .collect::<Vec<_>>();
    info!(
        "Configured {} bootstrap node specs; resolved {} at startup.",
        config.network.bootstrap_nodes.len(),
        resolved_seed_peers.len()
    );
    let network_config = NetworkConfig {
        bind_addr: format!(
            "{}:{}",
            config.network.listen_addr, config.network.listen_port
        )
        .parse()
        .unwrap(),
        seed_peers: resolved_seed_peers,
        seed_peer_specs: config.network.bootstrap_nodes.clone(),
        max_outbound: config.network.max_peers / 2,
        max_inbound: config.network.max_peers,
        node_id: config.node.node_name.clone(),
        connection_timeout: Duration::from_secs(config.network.connection_timeout),
        bootstrap_retry_interval: Duration::from_secs(config.network.discovery_interval.max(1)),
    };

    // Create a mempool
    let mempool = Arc::new(Mempool::new().with_state_store(state_store.clone()));

    // Start the network only after consensus exists so inbound blocks are routed
    // through validation and state application instead of bypassing consensus.
    let network = start_enhanced_network(
        network_config,
        Some(block_store.clone()),
        Some(tx_store.clone()),
        Some(mempool.clone()),
        Some(consensus.clone()),
    )
    .await;

    if config.node.enable_api {
        let api_bind_addr = format!("{}:{}", config.node.api_host, config.node.api_port)
            .parse()
            .expect("Invalid API bind address");
        let network_bind_addr = format!(
            "{}:{}",
            config.network.listen_addr, config.network.listen_port
        )
        .parse()
        .expect("Invalid network bind address");

        let runtime = Arc::new(NodeRuntime::new(
            config.node.node_name.clone(),
            config.consensus.chain_id,
            configured_genesis_hash,
            expected_genesis_hash_text,
            genesis_path.clone(),
            api_bind_addr,
            network_bind_addr,
            PathBuf::from(&config.node.data_dir),
            PathBuf::from(&config.storage.db_path),
            config.consensus.enable_mining,
            config.consensus.mining_threads,
            true,
            block_store.clone(),
            mempool.clone(),
            network.service.clone(),
            consensus.telemetry(),
            network.sync_service.clone(),
        ));

        tokio::spawn(async move {
            if let Err(e) = api::serve(runtime).await {
                error!("API server exited with error: {}", e);
            }
        });

        info!(
            "Node API dashboard available at http://{}:{}/dashboard",
            config.node.api_host, config.node.api_port
        );
    } else {
        info!("Node API is disabled in configuration");
    }

    info!("Kumquat node started successfully");

    // Keep the main thread alive
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");
    info!("Shutting down Kumquat node...");
}

#[cfg(test)]
mod tests {
    use super::parse_bootstrap_target;

    #[test]
    fn parses_dns_multiaddr_bootstrap_targets() {
        let target = parse_bootstrap_target("/dns4/genesis-peer.kumquat.svc.cluster.local/tcp/30380")
            .expect("expected bootstrap target");
        assert_eq!(target.0, "genesis-peer.kumquat.svc.cluster.local");
        assert_eq!(target.1, 30380);
    }

    #[test]
    fn parses_host_port_bootstrap_targets() {
        let target = parse_bootstrap_target("genesis-peer.kumquat.svc.cluster.local:30380")
            .expect("expected bootstrap target");
        assert_eq!(target.0, "genesis-peer.kumquat.svc.cluster.local");
        assert_eq!(target.1, 30380);
    }
}

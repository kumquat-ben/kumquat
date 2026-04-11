use std::sync::Arc;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use log::{info, error, warn};
use structopt::StructOpt;

use kumquat::init_logger;
use kumquat::api;
use kumquat::config::Config;
use kumquat::storage::{RocksDBStore, BlockStore, TxStore, StateStore, BatchOperationManager};
use kumquat::mempool::Mempool;
use kumquat::consensus::start_consensus;
use kumquat::consensus::config::ConsensusConfig;
use kumquat::network::start_enhanced_network;
use kumquat::network::NetworkConfig;
use kumquat::node_runtime::NodeRuntime;
use kumquat::tools::genesis::generate_genesis;

fn resolve_miner_address(node_id: Option<&str>, node_name: &str) -> [u8; 32] {
    if let Some(node_id) = node_id {
        if let Ok(bytes) = hex::decode(node_id) {
            if bytes.len() == 32 {
                let mut address = [0u8; 32];
                address.copy_from_slice(&bytes);
                return address;
            }
        }
    }

    kumquat::crypto::hash::sha256(node_name.as_bytes())
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
            },
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
                config.network.bootstrap_nodes = vec![];
            },
            "testnet" => {
                config.consensus.chain_id = 2;
                config.consensus.target_block_time = 10;
                config.consensus.initial_difficulty = 1000;
                config.network.bootstrap_nodes = vec![
                    "/dns4/bootstrap1.kumquat.network/tcp/30333/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp".to_string(),
                    "/dns4/bootstrap2.kumquat.network/tcp/30333/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9".to_string(),
                ];
            },
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
            },
            _ => {
                warn!("Unknown network: {}, using default", network);
            }
        }
    }

    if let Some(data_dir) = &opt.data_dir {
        config.node.data_dir = data_dir.to_string_lossy().to_string();
    }

    if let Some(bootstrap) = &opt.bootstrap {
        config.network.bootstrap_nodes = bootstrap.split(',').map(|s| format!("/dns4/{}/tcp/30333", s)).collect();
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

    // Load or generate genesis block
    let genesis_path = if let Some(genesis_path) = &opt.genesis {
        genesis_path.clone()
    } else {
        data_dir.join("genesis.toml")
    };

    let (genesis_block, genesis_accounts) = if genesis_path.exists() {
        match generate_genesis(&genesis_path) {
            Ok((block, accounts)) => {
                info!("Loaded genesis block with hash: {}", hex::encode(&block.hash));
                (block, accounts)
            },
            Err(e) => {
                error!("Failed to load genesis block: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        error!("Genesis file not found at {:?}", genesis_path);
        std::process::exit(1);
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
        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.put(key, value)
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, kumquat::storage::kv_store::KVStoreError> {
            self.inner.get(key)
        }

        fn delete(&self, key: &[u8]) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.delete(key)
        }

        fn exists(&self, key: &[u8]) -> Result<bool, kumquat::storage::kv_store::KVStoreError> {
            self.inner.exists(key)
        }

        fn write_batch(&self, operations: Vec<kumquat::storage::kv_store::WriteBatchOperation>) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.write_batch(operations)
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, kumquat::storage::kv_store::KVStoreError> {
            self.inner.scan_prefix(prefix)
        }

        fn flush(&self) -> Result<(), kumquat::storage::kv_store::KVStoreError> {
            self.inner.flush()
        }
    }

    // Create a static KVStore
    let static_kv_store = Arc::new(StaticKVStore { inner: kv_store.clone() });

    // Create a static reference to the KVStore
    // This is safe because these stores will live for the entire program
    let static_kv_store_box = Box::new(StaticKVStore { inner: kv_store.clone() });
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

    // Initialize genesis state if needed
    if let Ok(None) = block_store.get_block_by_height(0) {
        info!("Initializing genesis state...");

        // Create initial accounts
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
                },
                Err(e) => {
                    error!("Failed to create genesis account: {}", e);
                    std::process::exit(1);
                }
            }
        }

        // Store genesis block
        match block_store.put_block(&genesis_block) {
            Ok(_) => {
                info!("Stored genesis block");
            },
            Err(e) => {
                error!("Failed to store genesis block: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        info!("Genesis state already initialized");
    }

    // Initialize network
    info!("Initializing network...");
    let (network_tx, _network_rx) = mpsc::channel::<kumquat::network::types::message::NetMessage>(100);

    // Convert config.network to NetworkConfig
    let network_config = NetworkConfig {
        bind_addr: format!("{}:{}", config.network.listen_addr, config.network.listen_port).parse().unwrap(),
        seed_peers: config.network.bootstrap_nodes.iter()
            .filter_map(|addr| addr.parse().ok())
            .collect(),
        max_outbound: config.network.max_peers / 2,
        max_inbound: config.network.max_peers,
        node_id: config.node.node_name.clone(),
    };

    // Create a mempool
    let mempool = Arc::new(Mempool::new().with_state_store(state_store.clone()));

    // Use enhanced network service with block synchronization
    let network = start_enhanced_network(
        network_config,
        Some(block_store.clone()),
        Some(tx_store.clone()),
        Some(mempool.clone()),
        None, // No consensus yet
    ).await;

    // Initialize consensus
    info!("Initializing consensus...");
    // Convert config.consensus to ConsensusConfig
    let consensus_config = ConsensusConfig {
        enable_mining: config.consensus.enable_mining,
        mining_threads: config.consensus.mining_threads,
        target_block_time: config.consensus.target_block_time,
        initial_difficulty: config.consensus.initial_difficulty,
        difficulty_adjustment_window: config.consensus.difficulty_adjustment_interval,
        max_transactions_per_block: config.consensus.max_transactions_per_block,
        poh_tick_rate: 400_000, // Default value
        miner_address: resolve_miner_address(config.node.node_id.as_deref(), &config.node.node_name),
    };

    let _consensus = start_consensus(
        consensus_config,
        static_kv_store,
        block_store.clone(),
        tx_store.clone(),
        state_store.clone(),
        network_tx.clone(),
    ).await;

    if config.node.enable_api {
        let api_bind_addr = format!("{}:{}", config.node.api_host, config.node.api_port)
            .parse()
            .expect("Invalid API bind address");
        let network_bind_addr = format!("{}:{}", config.network.listen_addr, config.network.listen_port)
            .parse()
            .expect("Invalid network bind address");

        let runtime = Arc::new(NodeRuntime::new(
            config.node.node_name.clone(),
            config.consensus.chain_id,
            api_bind_addr,
            network_bind_addr,
            PathBuf::from(&config.node.data_dir),
            PathBuf::from(&config.storage.db_path),
            config.consensus.enable_mining,
            config.consensus.mining_threads,
            true,
            block_store.clone(),
            mempool.clone(),
            network.clone(),
        ));

        tokio::spawn(async move {
            if let Err(e) = api::serve(runtime).await {
                error!("API server exited with error: {}", e);
            }
        });

        info!(
            "Node API dashboard available at http://{}:{}/dashboard",
            config.node.api_host,
            config.node.api_port
        );
    } else {
        info!("Node API is disabled in configuration");
    }

    info!("Kumquat node started successfully");

    // Keep the main thread alive
    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    info!("Shutting down Kumquat node...");
}

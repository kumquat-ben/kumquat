use kumquat::crypto::encode_address;
use kumquat::init_logger;
use kumquat::tools::genesis::{build_genesis_ceremony_record, generate_genesis, GenesisConfig};
use log::{error, info, warn};
use std::fs;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kumquat-genesis", about = "Kumquat genesis block generator")]
struct Opt {
    /// Generate a default genesis configuration
    #[structopt(long)]
    generate: bool,

    /// Output file
    #[structopt(long, parse(from_os_str))]
    output: Option<PathBuf>,

    /// Input file
    #[structopt(long, parse(from_os_str))]
    input: Option<PathBuf>,

    /// Write a JSON genesis ceremony record derived from the input/output genesis file
    #[structopt(long, parse(from_os_str))]
    ceremony_output: Option<PathBuf>,

    /// Network (dev, testnet, mainnet)
    #[structopt(long)]
    network: Option<String>,

    /// Chain ID
    #[structopt(long)]
    chain_id: Option<u64>,

    /// Initial difficulty
    #[structopt(long)]
    initial_difficulty: Option<u64>,

    /// Genesis timestamp
    #[structopt(long)]
    timestamp: Option<u64>,
}

fn main() {
    // Initialize logger
    init_logger();

    // Parse command line arguments
    let opt = Opt::from_args();

    // Generate a default genesis configuration
    if opt.generate {
        let mut config = GenesisConfig::default();

        // Update config with command line arguments
        if let Some(network) = opt.network {
            match network.as_str() {
                "dev" => {
                    config.chain_id = 1337;
                    config.initial_difficulty = 100;
                    config.timestamp = chrono::Utc::now().timestamp() as u64;
                }
                "testnet" => {
                    config.chain_id = 2;
                    config.initial_difficulty = 1000;
                    config.timestamp = 1609459200; // 2021-01-01 00:00:00 UTC
                }
                "mainnet" => {
                    config.chain_id = 1;
                    config.initial_difficulty = 10000;
                    config.timestamp = 1609459200; // 2021-01-01 00:00:00 UTC
                }
                _ => {
                    warn!("Unknown network: {}, using default", network);
                }
            }
        }

        if let Some(chain_id) = opt.chain_id {
            config.chain_id = chain_id;
        }

        if let Some(initial_difficulty) = opt.initial_difficulty {
            config.initial_difficulty = initial_difficulty;
        }

        if let Some(timestamp) = opt.timestamp {
            config.timestamp = timestamp;
        }

        // Save the configuration
        if let Some(output) = opt.output {
            match config.save(&output) {
                Ok(_) => {
                    info!("Genesis configuration saved to {:?}", output);
                    info!(
                        "Genesis config is explicit; nodes can reuse this file with --genesis {:?}",
                        output
                    );

                    // Generate the genesis block
                    match generate_genesis(&output) {
                        Ok((block, account_states)) => {
                            info!(
                                "Genesis block generated with hash: {}",
                                hex::encode(&block.hash)
                            );
                            info!(
                                "Chain identity for chain {}: chain-{}:{}",
                                config.chain_id,
                                config.chain_id,
                                hex::encode(&block.hash)
                            );
                            info!(
                                "To pin this chain in node config, set consensus.genesis_hash = \"{}\"",
                                hex::encode(&block.hash)
                            );
                            info!("Initial accounts: {}", account_states.len());

                            if let Some(ceremony_output) = &opt.ceremony_output {
                                write_ceremony_record(&output, ceremony_output);
                            }
                        }
                        Err(e) => {
                            error!("Failed to generate genesis block: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to save genesis configuration: {}", e);
                    std::process::exit(1);
                }
            }
        } else {
            // Print the configuration to stdout
            match toml::to_string_pretty(&config) {
                Ok(config_str) => {
                    println!("{}", config_str);
                }
                Err(e) => {
                    error!("Failed to serialize genesis configuration: {}", e);
                    std::process::exit(1);
                }
            }
        }
    } else if let Some(input) = opt.input {
        let genesis_config = match GenesisConfig::load(&input) {
            Ok(config) => config,
            Err(e) => {
                error!("Failed to load genesis configuration: {}", e);
                std::process::exit(1);
            }
        };

        // Generate the genesis block from the configuration
        match generate_genesis(&input) {
            Ok((block, account_states)) => {
                info!(
                    "Genesis block generated with hash: {}",
                    hex::encode(&block.hash)
                );
                info!(
                    "Chain identity for this genesis file: chain-{}:{}",
                    genesis_config.chain_id,
                    hex::encode(&block.hash)
                );
                info!("Initial accounts: {}", account_states.len());

                // Print the block details
                println!("Genesis Block:");
                println!("  Config: {:?}", input);
                println!("  Hash: {}", hex::encode(&block.hash));
                println!(
                    "  Chain Identity: chain-{}:{}",
                    genesis_config.chain_id,
                    hex::encode(&block.hash)
                );
                println!("  Height: {}", block.height);
                println!("  Timestamp: {}", block.timestamp);
                println!("  Difficulty: {}", block.difficulty);
                println!("  State Root: {}", hex::encode(&block.state_root));
                println!("  Transaction Root: {}", hex::encode(&block.tx_root));
                println!("  Initial Accounts: {}", account_states.len());

                for (address, state) in &account_states {
                    println!(
                        "    {}: {} cents across {} tokens",
                        encode_address(address),
                        state.balance,
                        state.tokens.len()
                    );
                }

                if let Some(ceremony_output) = &opt.ceremony_output {
                    write_ceremony_record(&input, ceremony_output);
                }
            }
            Err(e) => {
                error!("Failed to generate genesis block: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        // Print help
        println!("{}", Opt::clap().to_string());
    }
}

fn write_ceremony_record(input: &PathBuf, ceremony_output: &PathBuf) {
    match build_genesis_ceremony_record(input) {
        Ok(record) => match serde_json::to_string_pretty(&record) {
            Ok(json) => {
                if let Err(err) = fs::write(ceremony_output, json) {
                    error!(
                        "Failed to write genesis ceremony record to {:?}: {}",
                        ceremony_output, err
                    );
                    std::process::exit(1);
                }
                info!("Genesis ceremony record written to {:?}", ceremony_output);
            }
            Err(err) => {
                error!("Failed to serialize genesis ceremony record: {}", err);
                std::process::exit(1);
            }
        },
        Err(err) => {
            error!("Failed to build genesis ceremony record: {}", err);
            std::process::exit(1);
        }
    }
}

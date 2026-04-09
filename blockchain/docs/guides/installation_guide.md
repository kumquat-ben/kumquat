# Kumquat Installation and Setup Guide

This guide provides comprehensive instructions for installing, configuring, and running a Kumquat node in various environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation Methods](#installation-methods)
   - [Building from Source](#building-from-source)
   - [Docker Installation](#docker-installation)
   - [Binary Installation](#binary-installation)
3. [Configuration](#configuration)
   - [Configuration File](#configuration-file)
   - [Command-Line Options](#command-line-options)
   - [Environment Variables](#environment-variables)
4. [Node Types](#node-types)
   - [Development Node](#development-node)
   - [Testnet Node](#testnet-node)
   - [Mainnet Node](#mainnet-node)
5. [Genesis Block](#genesis-block)
6. [Running a Node](#running-a-node)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)
9. [Upgrading](#upgrading)

## Prerequisites

### Hardware Requirements

- **CPU**: 4+ cores recommended (2 cores minimum)
- **RAM**: 8GB+ recommended (4GB minimum)
- **Storage**: 100GB+ SSD recommended (50GB minimum)
- **Network**: Stable internet connection with 10Mbps+ bandwidth

### Software Requirements

- **Operating System**:
  - Linux (Ubuntu 20.04+, Debian 11+, or similar)
  - macOS (10.15+)
  - Windows 10/11 with WSL2
- **Dependencies**:
  - Rust 1.70+ and Cargo
  - Git
  - CMake 3.10+
  - GCC/Clang 9+
  - OpenSSL 1.1+
  - pkg-config

### Network Requirements

- **Ports**:
  - P2P: 30334 (TCP)
  - RPC: 8545 (TCP)
  - Metrics: 9100 (TCP, optional)
- **Firewall**: Ensure the above ports are open for incoming connections

## Installation Methods

### Building from Source

Building from source is recommended for developers and users who want the latest features.

#### 1. Install Rust and Dependencies

**Ubuntu/Debian**:
```bash
# Update package lists
sudo apt update

# Install dependencies
sudo apt install -y build-essential cmake pkg-config libssl-dev git clang

# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**macOS**:
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install dependencies
brew install cmake pkg-config openssl git llvm

# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**Windows (WSL2)**:
```bash
# Update package lists
sudo apt update

# Install dependencies
sudo apt install -y build-essential cmake pkg-config libssl-dev git clang

# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### 2. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/kumquat/kumquat.git
cd kumquat
```

#### 3. Build the Project

```bash
# Build in release mode
cargo build --release
```

The compiled binary will be available at `./target/release/kumquat`.

### Docker Installation

Docker installation is recommended for users who want a quick and easy setup.

#### 1. Install Docker and Docker Compose

**Ubuntu/Debian**:
```bash
# Install Docker
sudo apt update
sudo apt install -y docker.io docker-compose

# Add your user to the docker group
sudo usermod -aG docker $USER
newgrp docker
```

**macOS**:
```bash
# Install Docker Desktop
brew install --cask docker
```

**Windows**:
- Download and install Docker Desktop from [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)

#### 2. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/kumquat/kumquat.git
cd kumquat
```

#### 3. Start the Node

```bash
# Start a development node
./scripts/bootstrap_devnet.sh
docker-compose -f docker-compose.dev.yml up -d

# Or start a testnet node
./scripts/bootstrap_testnet.sh
docker-compose -f docker-compose.testnet.yml up -d
```

### Binary Installation

Binary installation is recommended for users who want a simple setup without building from source.

#### 1. Download the Latest Release

```bash
# Create a directory for Kumquat
mkdir -p ~/kumquat
cd ~/kumquat

# Download the latest release
curl -L https://github.com/kumquat/kumquat/releases/latest/download/kumquat-linux-x86_64.tar.gz -o kumquat.tar.gz

# Extract the archive
tar -xzf kumquat.tar.gz
```

#### 2. Make the Binary Executable

```bash
chmod +x ./kumquat
```

## Configuration

### Configuration File

Kumquat uses a TOML configuration file for node settings. You can generate a default configuration file using:

```bash
./kumquat-config --generate --output config.toml
```

The configuration file is divided into sections:

#### Node Section

```toml
[node]
# Node identity
name = "my-kumquat-node"
# Data directory
data_dir = "./data/kumquat"
# API settings
api_host = "127.0.0.1"
api_port = 8545
# Metrics settings
metrics_enabled = true
metrics_host = "127.0.0.1"
metrics_port = 9100
```

#### Network Section

```toml
[network]
# P2P settings
listen_addr = "0.0.0.0"
listen_port = 30334
# Bootstrap nodes
bootstrap_nodes = [
    "/dns4/bootstrap1.kumquat.network/tcp/30334/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp",
    "/dns4/bootstrap2.kumquat.network/tcp/30334/p2p/12D3KooWHdiAxVd8uMQR1hGWXccidmfCwLqcMpGwR6QcTP6QRMq9"
]
# Maximum number of peers
max_peers = 50
# Peer reputation settings
min_reputation = -100
```

#### Consensus Section

```toml
[consensus]
# Chain ID
chain_id = 1
# Block time target in seconds
target_block_time = 10
# Initial mining difficulty
initial_difficulty = 10000
# Enable mining
enable_mining = true
# Number of mining threads
mining_threads = 4
```

#### Storage Section

```toml
[storage]
# Database path
db_path = "./data/kumquat/db"
# Cache size in MB
cache_size = 512
# Enable compression
compression = true
# Pruning mode (archive, 1000, 10000)
pruning = "archive"
```

#### Mempool Section

```toml
[mempool]
# Maximum number of transactions
max_transactions = 10000
# Maximum number of transactions per sender
max_transactions_per_sender = 100
# Minimum gas price
min_gas_price = 1
# Transaction timeout in seconds
transaction_timeout = 3600
```

### Command-Line Options

Kumquat supports various command-line options that override the configuration file:

```
USAGE:
    kumquat [OPTIONS]

OPTIONS:
    -c, --config <FILE>              Configuration file
    -g, --genesis <FILE>             Genesis file
    -n, --network <NETWORK>          Network to connect to (dev, testnet, mainnet)
    -d, --data-dir <DIRECTORY>       Directory for blockchain data
    -b, --bootstrap <NODES>          Bootstrap nodes (comma-separated)
    --enable-mining <BOOL>           Enable mining
    --mining-threads <NUMBER>        Mining threads
    --api-port <PORT>                Port for JSON-RPC server
    --api-host <HOST>                Host for JSON-RPC server
    --listen-port <PORT>             Port for P2P communication
    --listen-addr <ADDRESS>          Address for P2P communication
    -h, --help                       Print help information
    -V, --version                    Print version information
```

### Environment Variables

Kumquat also supports environment variables for configuration:

```
KUMQUAT_CONFIG_FILE          Configuration file path
KUMQUAT_GENESIS_FILE         Genesis file path
KUMQUAT_NETWORK              Network to connect to
KUMQUAT_DATA_DIR             Data directory
KUMQUAT_BOOTSTRAP_NODES      Bootstrap nodes
KUMQUAT_ENABLE_MINING        Enable mining
KUMQUAT_MINING_THREADS       Mining threads
KUMQUAT_API_PORT             API port
KUMQUAT_API_HOST             API host
KUMQUAT_LISTEN_PORT          P2P listen port
KUMQUAT_LISTEN_ADDR          P2P listen address
```

## Node Types

### Development Node

A development node is used for local development and testing:

```bash
# Generate a development configuration
./kumquat-config --generate --network dev --output config.dev.toml

# Generate a development genesis block
./kumquat-genesis --generate --network dev --output genesis.dev.toml

# Start the development node
./kumquat --config config.dev.toml --genesis genesis.dev.toml
```

### Testnet Node

A testnet node connects to the Kumquat testnet:

```bash
# Generate a testnet configuration
./kumquat-config --generate --network testnet --output config.testnet.toml

# Start the testnet node
./kumquat --config config.testnet.toml --network testnet
```

### Mainnet Node

A mainnet node connects to the Kumquat mainnet:

```bash
# Generate a mainnet configuration
./kumquat-config --generate --network mainnet --output config.mainnet.toml

# Start the mainnet node
./kumquat --config config.mainnet.toml --network mainnet
```

## Genesis Block

The genesis block is the first block in the blockchain. You can generate a custom genesis block using:

```bash
./kumquat-genesis --generate --output genesis.toml
```

The genesis file is a TOML file with the following structure:

```toml
[genesis]
# Genesis timestamp
timestamp = 1609459200
# Initial difficulty
initial_difficulty = 10000
# Chain ID
chain_id = 1

# Initial accounts
[[genesis.accounts]]
address = "0x1234567890abcdef1234567890abcdef12345678"
balance = 1000000000000000000
account_type = "user"

[[genesis.accounts]]
address = "0xabcdef1234567890abcdef1234567890abcdef12"
balance = 2000000000000000000
account_type = "contract"
```

## Running a Node

### Starting a Node

```bash
# Start with a configuration file
./kumquat --config config.toml

# Start with command-line options
./kumquat --network testnet --data-dir ./data/kumquat --enable-mining true
```

### Stopping a Node

To stop a running node, press `Ctrl+C` in the terminal where the node is running.

### Running as a Service

#### Systemd Service (Linux)

Create a systemd service file:

```bash
sudo nano /etc/systemd/system/kumquat.service
```

Add the following content:

```
[Unit]
Description=Kumquat Node
After=network.target

[Service]
User=kumquat
Group=kumquat
ExecStart=/path/to/kumquat --config /path/to/config.toml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl enable kumquat
sudo systemctl start kumquat
```

## Monitoring

### Log Files

Kumquat logs are written to the standard output and can be redirected to a file:

```bash
./kumquat --config config.toml > kumquat.log 2>&1
```

### Metrics

Kumquat exposes metrics via Prometheus:

```toml
[node]
metrics_enabled = true
metrics_host = "127.0.0.1"
metrics_port = 9100
```

You can visualize these metrics using Grafana.

### Status API

Kumquat provides a status API endpoint:

```bash
curl -X GET http://localhost:8545/status
```

## Troubleshooting

### Common Issues

#### Node Won't Start

- Check if the data directory exists and is writable
- Check if the ports are already in use
- Check if the configuration file is valid

#### Node Can't Connect to Peers

- Check if the bootstrap nodes are correct
- Check if the P2P port is open in your firewall
- Check your internet connection

#### Mining Not Working

- Check if mining is enabled in the configuration
- Check if you have enough CPU resources
- Check if the mining threads setting is appropriate for your hardware

### Logs

Check the logs for error messages:

```bash
tail -f kumquat.log
```

### Database Issues

If you encounter database corruption, you can try to repair it:

```bash
./kumquat --repair-db
```

Or you can delete the database and resync:

```bash
rm -rf ./data/kumquat/db
./kumquat --config config.toml
```

## Upgrading

### Upgrading from Source

```bash
# Pull the latest changes
git pull

# Build the new version
cargo build --release

# Restart the node
./target/release/kumquat --config config.toml
```

### Upgrading Docker Installation

```bash
# Pull the latest image
docker pull kumquat/kumquat:latest

# Restart the container
docker-compose -f docker-compose.yml down
docker-compose -f docker-compose.yml up -d
```

### Upgrading Binary Installation

```bash
# Download the new version
curl -L https://github.com/kumquat/kumquat/releases/latest/download/kumquat-linux-x86_64.tar.gz -o kumquat.tar.gz

# Extract the archive
tar -xzf kumquat.tar.gz

# Make the binary executable
chmod +x ./kumquat

# Restart the node
./kumquat --config config.toml
```

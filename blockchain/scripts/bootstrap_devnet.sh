#!/bin/bash
set -e

# Create directories
mkdir -p data/node1
mkdir -p data/node2
mkdir -p data/node3
mkdir -p data/postgres
mkdir -p monitoring/grafana/dashboards
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/grafana/provisioning/dashboards

# Generate default config for node1
echo "Generating config for node1..."
cargo run --bin kumquat-config -- --generate --output data/node1/config.toml --node-name "kumquat-node1" --network dev --listen-port 30333 --api-port 8545 --metrics-port 9100 --enable-mining true

# Generate default config for node2
echo "Generating config for node2..."
cargo run --bin kumquat-config -- --generate --output data/node2/config.toml --node-name "kumquat-node2" --network dev --listen-port 30333 --api-port 8545 --metrics-port 9100 --enable-mining true --bootstrap "node1:30333"

# Generate default config for node3
echo "Generating config for node3..."
cargo run --bin kumquat-config -- --generate --output data/node3/config.toml --node-name "kumquat-node3" --network dev --listen-port 30333 --api-port 8545 --metrics-port 9100 --enable-mining false --bootstrap "node1:30333"

# Generate genesis block
echo "Generating genesis block..."
cargo run --bin kumquat-genesis -- --generate --output data/node1/genesis.toml

# Copy genesis to other nodes
cp data/node1/genesis.toml data/node2/
cp data/node1/genesis.toml data/node3/

# Create Prometheus config
cat > monitoring/prometheus.yml << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kumquat'
    static_configs:
      - targets: ['node1:9100', 'node2:9100', 'node3:9100']
EOF

# Create Grafana datasource
cat > monitoring/grafana/provisioning/datasources/prometheus.yml << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

# Create Grafana dashboard config
cat > monitoring/grafana/provisioning/dashboards/kumquat.yml << EOF
apiVersion: 1

providers:
  - name: 'Kumquat'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
EOF

# Download Grafana dashboard
curl -o monitoring/grafana/dashboards/kumquat.json https://raw.githubusercontent.com/kumquat/kumquat-dashboards/main/grafana/kumquat.json

echo "Bootstrap complete! Start the devnet with: docker-compose -f docker-compose.dev.yml up -d"

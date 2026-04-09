#!/bin/bash
set -e

# Create directories
mkdir -p data/testnet
mkdir -p monitoring/grafana/dashboards
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/grafana/provisioning/dashboards

# Generate default config
echo "Generating config..."
cargo run --bin kumquat-config -- --generate --output data/testnet/config.toml --node-name "kumquat-testnet" --network testnet --listen-port 30333 --api-port 8545 --metrics-port 9100 --enable-mining true

# Generate genesis block
echo "Generating genesis block..."
cargo run --bin kumquat-genesis -- --generate --output data/testnet/genesis.toml --network testnet

# Create Prometheus config
cat > monitoring/prometheus.yml << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kumquat'
    static_configs:
      - targets: ['node:9100']
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

echo "Bootstrap complete! Start the testnet with: docker-compose -f docker-compose.testnet.yml up -d"

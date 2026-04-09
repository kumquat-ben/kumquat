#!/bin/bash

echo "Starting second Kumquat node..."
./target/release/kumquat --config config_node2.toml --genesis genesis.toml --network dev --enable-mining true

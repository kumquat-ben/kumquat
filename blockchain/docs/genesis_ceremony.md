# Genesis Ceremony

Kumquat genesis is only valid when every operator agrees on the same:

- `genesis.toml`
- `genesis_hash`
- `state_root`
- `chain_identity`

The ceremony exists to make that agreement explicit and auditable.

## Required Outputs

From the agreed `genesis.toml`, generate:

1. The genesis block hash.
2. The pinned chain identity: `chain-<chain_id>:<genesis_hash>`.
3. A JSON ceremony record that every operator can compare byte-for-byte.

Use:

```bash
cargo run --bin kumquat-genesis -- \
  --input genesis.toml \
  --ceremony-output genesis.ceremony.json
```

Or, when creating a fresh genesis config:

```bash
cargo run --bin kumquat-genesis -- \
  --generate \
  --output genesis.toml \
  --chain-id 2 \
  --initial-difficulty 1000 \
  --timestamp 1767225600 \
  --ceremony-output genesis.ceremony.json
```

## Ceremony Procedure

1. Freeze the candidate `genesis.toml`. No edits after review starts.
2. Distribute the exact file to all ceremony participants.
3. Each participant runs `kumquat-genesis` locally and verifies that:
   - `genesis_hash` matches
   - `state_root` matches
   - `chain_identity` matches
   - account count and balances match expectations
4. Record the agreed `genesis_hash` in node configuration as `consensus.genesis_hash`.
5. Store the following artifacts in version control or an operator archive:
   - `genesis.toml`
   - `genesis.ceremony.json`
   - the list of operators who verified the result
6. Only then allow nodes to initialize block `0`.

## What Nodes Must Pin

Every production or testnet node should set:

```toml
[consensus]
chain_id = 2
genesis_hash = "<agreed 64-char hex hash>"
```

If a node’s local genesis hash or computed root differs from the pinned value, startup must fail.

## Ceremony Record

`genesis.ceremony.json` is the canonical audit artifact for the ceremony. It contains:

- `chain_id`
- `genesis_config_path`
- `genesis_hash`
- `chain_identity`
- `state_root`
- `timestamp`
- `initial_difficulty`
- sorted account summaries

That file is intended for review, archival, and out-of-band signing by operators.

# Kumquat Chain Cryptography Module

## Overview

The cryptography module provides the key, signature, verification, and hashing primitives for Kumquat Chain.

In Kumquat's product framing, these primitives are what make a digital cash handoff trustworthy. If value is meant to feel like something you hold and pass to someone else, signatures and hashes are the protocol guarantees behind that transfer.

## Components

### Keys (`keys.rs`)

- `VibeKeypair`: Ed25519 keypair wrapper for signing and verification
- `address_from_pubkey`: derives an address from a public key
- `VibePublicKey`: serializable wrapper for public keys

### Hashing (`hash.rs`)

- `sha256`: computes SHA-256 hashes
- `double_sha256`: computes SHA-256 twice
- `Hash`: serializable wrapper for 32-byte hash values

### Signing (`signer.rs`)

- `sign_message`: signs a message
- `verify_signature`: verifies a signature against a message and public key
- `VibeSignature`: serializable wrapper for Ed25519 signatures

## Usage

```rust
use crate::crypto::{VibeKeypair, sign_message, verify_signature, sha256};

let keypair = VibeKeypair::generate();
let address = keypair.address();

let message = b"Transfer a Kumquat unit to Alice";
let signature = sign_message(&keypair, message);
let is_valid = verify_signature(message, &signature, &keypair.public);
assert!(is_valid);

let hash = sha256(message);
```

## Security Considerations

- secret keys should never be exposed
- use secure randomness
- avoid logging sensitive material
- zeroization is worth considering for future hardening

// Kumquat Cryptography Module
//
// This module provides cryptographic primitives for the Kumquat blockchain:
// - Key generation and management
// - Digital signatures (Ed25519)
// - Message verification
// - Hashing (for blocks, transactions, PoH, state roots)
// - Advanced cryptography (BLS signatures, zero-knowledge proofs) when enabled

pub mod address;
pub mod hash;
pub mod keys;
pub mod signer;

// Advanced cryptography modules (enabled with the "advanced-crypto" feature)
#[cfg(feature = "advanced-crypto")]
pub mod bls;
#[cfg(feature = "advanced-crypto")]
pub mod zk;

// Re-export main components for easier access
pub use address::{
    decode_address, encode_address, normalize_address, AddressCodecError, KUMQUAT_ADDRESS_HRP,
};
pub use hash::{double_sha256, sha256};
pub use keys::{address_from_pubkey, VibeKeypair};
pub use signer::{sign_message, verify_signature};

// Re-export advanced cryptography components when enabled
#[cfg(feature = "advanced-crypto")]
pub use bls::{aggregate_signatures, verify_aggregate_signature, BlsKeypair, BlsSignature};

//! Merkle Patricia Trie implementation for Kumquat
//!
//! This module provides a production-ready implementation of a Merkle Patricia Trie (MPT)
//! for storing and verifying the blockchain state. The MPT enables efficient lookups,
//! updates, and cryptographic verification of the state.
//!
//! The implementation follows the Ethereum Yellow Paper specification with some
//! Kumquat-specific optimizations.

pub mod encode;
pub mod mpt;
pub mod node;

// Re-export main components
pub use mpt::MerklePatriciaTrie;
pub use mpt::{Proof, ProofItem};
pub use node::Node;

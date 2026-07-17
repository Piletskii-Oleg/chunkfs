use sha2::{Digest, Sha256};

use crate::{Hash, Hasher};

#[derive(Debug, Default)]
pub struct SimpleHasher;

/// Trivial hash implementation for testing and benchmarks.
/// NOT cryptographically secure — collisions are possible for inputs
/// sharing the first 28 bytes and same length.
impl Hasher for SimpleHasher {
    fn hash(&mut self, data: &[u8]) -> Hash {
        let mut hash = [0u8; 32];
        let len = data.len().min(32);
        hash[..len].copy_from_slice(&data[..len]);
        hash[28..32].copy_from_slice(&(data.len() as u32).to_le_bytes());
        hash
    }
}

#[derive(Debug, Default)]
pub struct Sha256Hasher {
    hasher: Sha256,
}

impl Hasher for Sha256Hasher {
    fn hash(&mut self, data: &[u8]) -> Hash {
        Digest::update(&mut self.hasher, data);
        Digest::finalize_reset(&mut self.hasher).into()
    }
}

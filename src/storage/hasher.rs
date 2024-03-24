use crate::VecHash;

/// Functionality for an object that hashes the input.
pub trait Hasher {
    /// Takes some data and returns its hash.
    fn hash(&mut self, data: &[u8]) -> VecHash;
}

pub struct SimpleHasher;

impl Hasher for SimpleHasher {
    fn hash(&mut self, data: &[u8]) -> VecHash {
        data.to_vec()
    }
}

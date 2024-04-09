use std::hash;

use crate::VecHash;

/// Functionality for an object that hashes the input.
pub trait Hasher {
    type Hash: hash::Hash + Clone + Eq + PartialEq + Default;
    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Self::Hash;
}

#[derive(Debug)]
pub struct SimpleHasher;

impl Hasher for SimpleHasher {
    type Hash = VecHash;
    fn hash(&mut self, data: &[u8]) -> VecHash {
        data.to_vec()
    }
}

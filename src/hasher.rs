use std::hash;

use crate::VecHash;

/// Functionality for an object that hashes the input.
pub trait Hasher<Hash: hash::Hash + Clone + Eq + PartialEq> {
    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Hash;
}

#[derive(Debug)]
pub struct SimpleHasher;

impl<Hash: hash::Hash + Clone + Eq + PartialEq> Hasher<Hash> for SimpleHasher {
    fn hash(&mut self, data: &[u8]) -> VecHash {
        data.to_vec()
    }
}

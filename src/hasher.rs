use std::hash;

use sha2::digest::Output;
use sha2::{Digest, Sha256};

/// Functionality for an object that hashes the input.
pub trait Hasher {
    type Hash: hash::Hash + Clone + Eq + PartialEq + Default;

    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Self::Hash;
}

#[derive(Debug)]
pub struct SimpleHasher;

impl Hasher for SimpleHasher {
    type Hash = Vec<u8>;

    fn hash(&mut self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }
}

#[derive(Debug, Default)]
pub struct Sha256Hasher {
    hasher: Sha256,
}

impl Hasher for Sha256Hasher {
    type Hash = Output<Sha256>;

    fn hash(&mut self, data: &[u8]) -> Self::Hash {
        Digest::update(&mut self.hasher, data);
        Digest::finalize_reset(&mut self.hasher)
    }
}

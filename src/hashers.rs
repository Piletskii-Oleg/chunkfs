use sha2::{Digest, Sha256};

use crate::Hasher;

#[derive(Debug)]
pub struct SimpleHasher;

impl Hasher for SimpleHasher {
    type Hash = Vec<u8>;

    fn hash(&mut self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }

    fn len(&self, hash: &Self::Hash) -> usize {
        hash.len()
    }
}

#[derive(Debug, Default)]
pub struct Sha256Hasher {
    hasher: Sha256,
}

impl Hasher for Sha256Hasher {
    type Hash = [u8; 32];

    fn hash(&mut self, data: &[u8]) -> Self::Hash {
        Digest::update(&mut self.hasher, data);
        Digest::finalize_reset(&mut self.hasher).into()
    }

    fn len(&self, hash: &Self::Hash) -> usize {
        hash.len()
    }
}

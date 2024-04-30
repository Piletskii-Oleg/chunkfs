use std::hash;

pub trait ChunkHash: hash::Hash + Clone + Eq + PartialEq + Default {}

impl<T: hash::Hash + Clone + Eq + PartialEq + Default> ChunkHash for T {}

/// Functionality for an object that hashes the input.
pub trait Hasher {
    type Hash: ChunkHash;

    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Self::Hash;
}

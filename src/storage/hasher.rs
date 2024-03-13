use crate::VecHash;

/// Functionality for an object that hashes the input.
pub trait Hasher {
    fn hash(&mut self, data: &[u8]) -> VecHash;
}

use crate::Hash;

pub trait Hasher {
    fn hash(&mut self, data: &[u8]) -> Hash;
}

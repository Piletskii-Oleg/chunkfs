use crate::{Base, Chunker, Hasher};

struct Storage<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    chunker: C,
    hasher: H,
    base: B,
}

impl<C, H, B> Storage<C, H, B>
where
    C: Chunker,
    H: Hasher,
    B: Base,
{
    fn write(&mut self, data: &[u8]) -> Vec<(u64, usize)> {
        todo!()
    }
}

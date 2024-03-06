use crate::{Base, Chunker, Hash, Hasher, Segment, Span};

pub struct Storage<C, H, B>
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
    fn write(&mut self, data: &[u8]) -> Vec<Span> {
        todo!()
    }

    fn retrieve_chunks(&mut self, request: Vec<Hash>) -> Vec<Segment> {
        todo!()
    }
}

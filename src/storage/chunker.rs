use crate::storage::Chunk;

pub trait Chunker {
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk>;
}

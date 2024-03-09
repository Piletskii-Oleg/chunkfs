use crate::storage::Chunk;

pub trait Chunker {
    fn change_data(&mut self, data: &[u8]);
    // write to inner buffer
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk>;
}

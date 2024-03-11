use std::cmp::min;

use crate::storage::Chunk;

pub trait Chunker {
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk>;
}

pub struct FSChunker {
    chunk_size: usize,
}

impl FSChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }
}

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8]) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = Vec::with_capacity(data.len() / self.chunk_size + 1);
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }
        chunks
    }
}

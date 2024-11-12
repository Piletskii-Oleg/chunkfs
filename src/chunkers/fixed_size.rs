use std::cmp::min;
use std::fmt::{Debug, Formatter};

use crate::{Chunk, Chunker};

/// Chunker that utilizes Fixed Sized Chunking (FSC) algorithm,
/// splitting file into even-sized chunks.
#[derive(Default)]
pub struct FSChunker {
    chunk_size: usize,
    rest: Vec<u8>,
}

impl FSChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            rest: vec![],
        }
    }
}

impl Debug for FSChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fixed size chunking, chunk size: {}", self.chunk_size)
    }
}

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = empty;
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }

        let last_chunk = chunks.pop().unwrap();
        if last_chunk.length() < self.chunk_size {
            self.rest = data[last_chunk.range()].to_vec();
        } else {
            chunks.push(last_chunk);
            self.rest = vec![];
        }
        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.chunk_size + 1
    }
}

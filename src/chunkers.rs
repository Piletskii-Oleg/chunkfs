use std::cmp::min;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Fixed Sized Chunking (FSC) algorithm,
/// splitting file into even-sized chunks.
#[derive(Debug)]
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

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = empty;
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }

        let last_chunk = chunks.last().unwrap();
        if last_chunk.length() < self.chunk_size {
            self.rest = data[last_chunk.range()].to_vec();
        } else {
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

#[derive(Default, Debug)]
pub struct LeapChunker {
    rest: Vec<u8>,
}

impl Chunker for LeapChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = chunking::leap_based::Chunker::new(data);
        let mut chunks = empty;
        for chunk in chunker {
            chunks.push(Chunk::new(chunk.pos, chunk.len));
        }

        self.rest = data[chunks.pop().unwrap().range()].to_vec();
        chunks
    }

    fn remainder(&self) -> &[u8] {
        &self.rest
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / 1024 * 8
    }
}

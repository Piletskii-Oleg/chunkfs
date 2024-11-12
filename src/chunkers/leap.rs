use std::fmt::{Debug, Formatter};

use chunking::SizeParams;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Leap-based CDC algorithm.
pub struct LeapChunker {
    rest: Vec<u8>,
    sizes: SizeParams,
}

impl LeapChunker {
    pub fn new(sizes: SizeParams) -> Self {
        Self {
            rest: vec![],
            sizes,
        }
    }
}

impl Default for LeapChunker {
    fn default() -> Self {
        Self::new(SizeParams::leap_default())
    }
}

impl Debug for LeapChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeapCDC, sizes: {:?}", self.sizes)
    }
}

impl Chunker for LeapChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = chunking::leap_based::Chunker::new(data, self.sizes);
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
        data.len() / self.sizes.min
    }
}

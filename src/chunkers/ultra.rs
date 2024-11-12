use std::fmt::{Debug, Formatter};

use chunking::SizeParams;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Ultra CDC algorithm.
pub struct UltraChunker {
    rest: Vec<u8>,
    sizes: SizeParams,
}

impl UltraChunker {
    pub fn new(sizes: SizeParams) -> Self {
        Self {
            rest: vec![],
            sizes,
        }
    }
}

impl Default for UltraChunker {
    fn default() -> Self {
        Self::new(SizeParams::ultra_default())
    }
}

impl Debug for UltraChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UltraCDC, sizes: {:?}", self.sizes)
    }
}

impl Chunker for UltraChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = chunking::ultra::Chunker::new(data, self.sizes);
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

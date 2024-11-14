use std::fmt::{Debug, Formatter};

use chunking::SizeParams;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Rabin CDC algorithm.
pub struct RabinChunker {
    rest: Vec<u8>,
    params: Option<chunking::rabin::ChunkerParams>,
    sizes: SizeParams,
}

impl RabinChunker {
    pub fn new(sizes: SizeParams) -> Self {
        Self {
            rest: vec![],
            params: Some(chunking::rabin::ChunkerParams::new()),
            sizes,
        }
    }
}

impl Default for RabinChunker {
    fn default() -> Self {
        RabinChunker::new(SizeParams::rabin_default())
    }
}

impl Debug for RabinChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RabinCDC, sizes: {:?}", self.sizes)
    }
}

impl Chunker for RabinChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut chunker =
            chunking::rabin::Chunker::with_params(data, self.params.take().unwrap(), self.sizes);
        let mut chunks = empty;
        loop {
            match chunker.next() {
                None => break,
                Some(chunk) => chunks.push(Chunk::new(chunk.pos, chunk.len)),
            }
        }

        self.params = Some(chunker.give_params());
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
